-- =========================================================
-- FS25_NetworkSync - core class
-- =========================================================
-- Author: TisonK
-- =========================================================
-- Multiplayer network bedrock for the Realistic Farming ecosystem. One
-- batched 1Hz sync cycle replaces the per-mod event classes. Companion
-- mods register a schema and call markDirty; NetworkSync serializes,
-- batches, and delivers.
--
--   g_networkSync:registerModule(modId, {
--       channel      = "<Mod>_Sync",
--       onWriteState = function() return { ...integer-indexed array... } end,  -- server
--       onReadState  = function(array) ...apply... end,                        -- client
--   })
--   g_networkSync:markDirty(modId)   -- flag for the next 1Hz batch
--
-- A mod that needs schema isolation registers more than one modId
-- (e.g. WorkerCosts_Roster and WorkerCosts_HireHall), exactly like
-- StateLedger.
-- =========================================================

NetworkSync = {}
local NetworkSync_mt = Class(NetworkSync)

NetworkSync.TICK_MS = 1000            -- 1Hz batch
NetworkSync.JOIN_REQUEST_MAX = 5      -- client full-sync request retries
NetworkSync.JOIN_REQUEST_INTERVAL = 2000

function NetworkSync.new()
    local self = setmetatable({}, NetworkSync_mt)

    self.schemas       = {}    -- modId -> { channel, onWriteState, onReadState }
    self.registerOrder = {}    -- stable iteration order
    self.dirtyMods     = {}    -- modId -> bool
    self.accumulator   = 0

    -- Client join-sync request state
    self.needsFullSync   = false
    self.fullSyncAsked   = false
    self.joinAttempts    = 0
    self.joinTimer       = 0

    return self
end

-- =========================================================
-- Registration
-- =========================================================

---@param modId string  unique id, e.g. "WorkerCosts_Roster"
---@param schema table   { channel = string, onWriteState = fn, onReadState = fn }
---@return boolean success
function NetworkSync:registerModule(modId, schema)
    if type(modId) ~= "string" or modId == "" then
        NSLogger.warning("registerModule: invalid modId '%s', ignoring", tostring(modId))
        return false
    end
    if type(schema) ~= "table"
        or type(schema.onWriteState) ~= "function"
        or type(schema.onReadState) ~= "function" then
        NSLogger.warning("registerModule('%s'): needs { onWriteState = fn, onReadState = fn }, ignoring", modId)
        return false
    end

    if self.schemas[modId] == nil then
        table.insert(self.registerOrder, modId)
    else
        NSLogger.warning("registerModule('%s'): already registered, overwriting", modId)
    end
    self.schemas[modId] = {
        channel      = schema.channel or (modId .. "_Sync"),
        onWriteState = schema.onWriteState,
        onReadState  = schema.onReadState,
    }
    NSLogger.debug("Registered module '%s' (channel %s)", modId, self.schemas[modId].channel)
    return true
end

-- Flag a module for the next 1Hz batch. Microscopic cost; safe to call
-- often. No-op if the modId is not registered.
function NetworkSync:markDirty(modId)
    if self.schemas[modId] ~= nil then
        self.dirtyMods[modId] = true
    end
end

-- =========================================================
-- Payload build / apply
-- =========================================================

-- Build { modId -> array } for the given list of modIds, calling each
-- onWriteState inside pcall. A failing or non-array result is skipped.
function NetworkSync:_buildPayload(modIds)
    local payload = {}
    local count = 0
    for _, modId in ipairs(modIds) do
        local schema = self.schemas[modId]
        if schema ~= nil then
            local ok, arr = pcall(schema.onWriteState)
            if not ok then
                NSLogger.error("onWriteState failed for '%s': %s", modId, tostring(arr))
            elseif type(arr) ~= "table" then
                NSLogger.warning("onWriteState for '%s' returned %s, expected array", modId, type(arr))
            else
                payload[modId] = arr
                count = count + 1
            end
        end
    end
    return payload, count
end

-- Client side: apply a received { modId -> array } via each onReadState.
function NetworkSync:applyPayload(payload)
    if type(payload) ~= "table" then
        return
    end
    for modId, arr in pairs(payload) do
        local schema = self.schemas[modId]
        if schema ~= nil then
            local ok, err = pcall(schema.onReadState, arr)
            if not ok then
                NSLogger.error("onReadState failed for '%s': %s", modId, tostring(err))
            end
        end
        -- Unregistered modId: silently ignored (a mod we do not have installed)
    end
end

-- =========================================================
-- Server: 1Hz batch + snapshots
-- =========================================================

-- Broadcast all currently-dirty modules to every client. Server only.
function NetworkSync:_broadcastDirty()
    local dirtyList = {}
    for modId, isDirty in pairs(self.dirtyMods) do
        if isDirty then
            dirtyList[#dirtyList + 1] = modId
            self.dirtyMods[modId] = false
        end
    end
    if #dirtyList == 0 then
        return
    end

    local payload, count = self:_buildPayload(dirtyList)
    if count > 0 and g_server ~= nil then
        g_server:broadcastEvent(RealisticFarmingSyncEvent.new(payload))
    end
end

-- Force one module out to all clients immediately, outside the 1Hz cadence.
-- Used for changes that must not wait up to a second (e.g. an admin setting
-- applied server-side that clients need at once). Server only.
function NetworkSync:syncNow(modId)
    if g_currentMission == nil or not g_currentMission:getIsServer() then
        return
    end
    self.dirtyMods[modId] = false
    local payload, count = self:_buildPayload({ modId })
    if count > 0 and g_server ~= nil then
        g_server:broadcastEvent(RealisticFarmingSyncEvent.new(payload))
    end
end

-- Send a full snapshot (every registered module) to one connection. Server
-- only. Answers a joining client's full-sync request.
function NetworkSync:sendFullSnapshotTo(connection)
    if g_currentMission == nil or not g_currentMission:getIsServer() or connection == nil then
        return
    end
    local payload, count = self:_buildPayload(self.registerOrder)
    if count > 0 then
        connection:sendEvent(RealisticFarmingSyncEvent.new(payload))
        NSLogger.debug("Sent full snapshot (%d module(s)) to a joining client", count)
    end
end

-- =========================================================
-- Client: request a full snapshot on join
-- =========================================================

function NetworkSync:_sendFullSyncRequest()
    if g_client ~= nil then
        local serverConn = g_client:getServerConnection()
        if serverConn ~= nil then
            serverConn:sendEvent(RealisticFarmingSyncRequestEvent.new())
            return true
        end
    end
    return false
end

-- Called from the mission-loaded hook. On a pure client, arm the join
-- full-sync request (retried in update until the server connection is ready).
function NetworkSync:onMissionLoaded()
    if g_currentMission ~= nil and not g_currentMission:getIsServer() then
        self.needsFullSync = true
        self.fullSyncAsked = false
        self.joinAttempts  = 0
        self.joinTimer     = 0
    end
end

-- =========================================================
-- Update (driven from FSBaseMission.update)
-- =========================================================

function NetworkSync:update(dt)
    if g_currentMission == nil then
        return
    end

    if g_currentMission:getIsServer() then
        -- Server: 1Hz dirty batch.
        self.accumulator = self.accumulator + dt
        if self.accumulator >= NetworkSync.TICK_MS then
            self.accumulator = self.accumulator - NetworkSync.TICK_MS
            self:_broadcastDirty()
        end
    elseif self.needsFullSync and not self.fullSyncAsked then
        -- Client: retry the join full-sync request until it lands.
        self.joinTimer = self.joinTimer + dt
        if self.joinAttempts == 0 or self.joinTimer >= NetworkSync.JOIN_REQUEST_INTERVAL then
            self.joinTimer = 0
            self.joinAttempts = self.joinAttempts + 1
            if self:_sendFullSyncRequest() then
                self.fullSyncAsked = true
                NSLogger.debug("Full-sync request sent (attempt %d)", self.joinAttempts)
            elseif self.joinAttempts >= NetworkSync.JOIN_REQUEST_MAX then
                self.needsFullSync = false
                NSLogger.warning("Full-sync request gave up after %d attempts", self.joinAttempts)
            end
        end
    end
end

-- =========================================================
-- Introspection (console command)
-- =========================================================

function NetworkSync:getStatus()
    local role = "unknown"
    if g_currentMission ~= nil then
        role = g_currentMission:getIsServer() and "server" or "client"
    end
    local lines = {}
    table.insert(lines, string.format("NetworkSync: %d module(s), role=%s, tick=%dms",
        #self.registerOrder, role, NetworkSync.TICK_MS))
    for _, modId in ipairs(self.registerOrder) do
        table.insert(lines, string.format("  - %s (channel %s, dirty=%s)",
            modId, self.schemas[modId].channel, tostring(self.dirtyMods[modId] == true)))
    end
    return table.concat(lines, "\n")
end

function NetworkSync:consoleCommandStatus()
    return self:getStatus()
end
