-- =========================================================
-- FS25_NetworkSync - scoped delivery (NS-7)
-- =========================================================
-- Author: TisonK
-- =========================================================
-- Per-connection, private publications alongside the unchanged public batch.
-- A scoped module registers a producer (server) and a consumer (client):
--
--   g_networkSync:registerScopedModule(modId, {
--       buildView = function(context, previous, forceFull) ... end,  -- server
--       applyView = function(publication) ... end,                    -- client
--       clearView = function(reason) ... end,                         -- client
--   })
--   g_networkSync:unregisterScopedModule(modId)
--   g_networkSync:requestScopedFull(modId)
--   g_networkSync:getScopedCapabilities()
--
-- What the transport owns: subscriptions keyed by the actual connection
-- object, the trusted actor context handed to the producer, the identities on
-- the wire (serverSession, subscriptionId, viewEpoch, publicationId), chunking
-- and atomic reassembly, the typed application result, bounded recovery and
-- cleanup on disconnect, unregister and mission end.
--
-- What it never does: persist a producer's records, decide a farming
-- permission, default a farm, accept an identity from a client, put a scoped
-- id into the public batch, or fall back to public traffic when the scoped
-- route is unavailable.
--
-- Every method here is added onto NetworkSync; the file is sourced right after
-- NetworkSync.lua. Engine seams, bound in D:\FS25_Decoded\dataS\scripts_decompiled:
--   Connection fields isConnected / isReadyForEvents / streamId and
--     Connection:sendEvent (network/Connection.lua:22-101, eventId check :76)
--   NetworkNode.LOCAL_STREAM_ID (network/NetworkNode.lua:3)
--   FSBaseMission:getFarmId(connection) (FSBaseMission.lua:1067)
--   UserManager:getUserByConnection (users/UserManager.lua:74), User:getId (:59)
--   FarmManager farm id constants (farms/FarmManager.lua:2-8)
--   FSBaseMission:onConnectionClosed(connection, disconnectReason) (:834)
--   MessageCenter:subscribe / unsubscribeAll (MessageCenter.lua:27, :68),
--     MessageType.PLAYER_FARM_CHANGED (MessageType.lua:25)
-- =========================================================

NetworkSyncScoped = NetworkSyncScoped or {}

-- Protocol facts.
NetworkSyncScoped.BOOTSTRAP_VERSION  = 1
NetworkSyncScoped.PROTOCOL_VERSIONS  = { 1 }
NetworkSyncScoped.MAX_CHUNKS         = 1024
NetworkSyncScoped.PUBLICATION_HEADER_TOKENS = 10

-- View states (producer) and modes.
NetworkSyncScoped.STATE_READY       = "READY"
NetworkSyncScoped.STATE_WAITING     = "WAITING"
NetworkSyncScoped.STATE_DENIED      = "DENIED"
NetworkSyncScoped.STATE_UNAVAILABLE = "UNAVAILABLE"
NetworkSyncScoped.STATE_ERROR       = "ERROR"
NetworkSyncScoped.MODE_FULL      = "FULL"
NetworkSyncScoped.MODE_DELTA     = "DELTA"
NetworkSyncScoped.MODE_UNCHANGED = "UNCHANGED"

-- Actor states handed to the producer.
NetworkSyncScoped.ACTOR_WAITING   = "WAITING"
NetworkSyncScoped.ACTOR_RESOLVED  = "RESOLVED"
NetworkSyncScoped.ACTOR_SPECTATOR = "SPECTATOR"
NetworkSyncScoped.ACTOR_INVALID   = "INVALID"

-- Application outcomes (consumer) and reasons.
NetworkSyncScoped.OUTCOME_APPLIED   = "APPLIED"
NetworkSyncScoped.OUTCOME_RETRYABLE = "RETRYABLE"
NetworkSyncScoped.OUTCOME_TERMINAL  = "TERMINAL"

NetworkSyncScoped.REASON_UNSUPPORTED_PROTOCOL            = "UNSUPPORTED_PROTOCOL"
NetworkSyncScoped.REASON_UNSUPPORTED_APPLICATION_VERSION = "UNSUPPORTED_APPLICATION_VERSION"
NetworkSyncScoped.REASON_APPLY_ERROR            = "APPLY_ERROR"
NetworkSyncScoped.REASON_BASE_MISMATCH          = "BASE_MISMATCH"
NetworkSyncScoped.REASON_MODULE_NOT_REGISTERED  = "MODULE_NOT_REGISTERED"
NetworkSyncScoped.REASON_MODULE_UNREGISTERED    = "MODULE_UNREGISTERED"
NetworkSyncScoped.REASON_PRODUCER_ERROR         = "PRODUCER_ERROR"
NetworkSyncScoped.REASON_MALFORMED              = "MALFORMED"
NetworkSyncScoped.REASON_OVERSIZE               = "OVERSIZE"
NetworkSyncScoped.REASON_NEW_GENERATION         = "NEW_GENERATION"
NetworkSyncScoped.REASON_MISSION_END            = "MISSION_END"
NetworkSyncScoped.REASON_LOCAL_CONTEXT_CHANGED  = "LOCAL_CONTEXT_CHANGED"
NetworkSyncScoped.REASON_UNACKNOWLEDGED         = "UNACKNOWLEDGED"

-- Value tags inside a PUBLICATION body.
NetworkSyncScoped.TAG_BOOL   = "BOOL"
NetworkSyncScoped.TAG_NUMBER = "NUMBER"
NetworkSyncScoped.TAG_STRING = "STRING"

local VALID_STATES = { READY = true, WAITING = true, DENIED = true, UNAVAILABLE = true, ERROR = true }
local NON_READY_CONTROL = { WAITING = true, DENIED = true, UNAVAILABLE = true, ERROR = true }
local VALID_MODES  = { FULL = true, DELTA = true, UNCHANGED = true }

local E = NetworkSyncScopedEvent
local S = NetworkSyncScoped

-- =========================================================
-- Canonical positive decimal counters
-- =========================================================
-- Identities on the wire are strings, never floats. They are incremented as
-- decimal digits and compared by byte length then lexical bytes.

local function isCanonicalDecimal(s)
    if type(s) ~= "string" or s == "" or #s > 64 then return false end
    if s:find("^[1-9][0-9]*$") == nil then return false end
    return true
end
S.isCanonicalDecimal = isCanonicalDecimal

local function incrementDecimal(s)
    if s == nil or s == "" then return "1" end
    local bytes = { s:byte(1, #s) }
    local i = #bytes
    while i >= 1 do
        if bytes[i] < 57 then
            bytes[i] = bytes[i] + 1
            return string.char(unpack(bytes))
        end
        bytes[i] = 48
        i = i - 1
    end
    return "1" .. string.char(unpack(bytes))
end
S.incrementDecimal = incrementDecimal

-- Returns -1, 0 or 1 for canonical decimal strings a and b.
local function compareDecimal(a, b)
    if #a ~= #b then return (#a < #b) and -1 or 1 end
    if a == b then return 0 end
    return (a < b) and -1 or 1
end
S.compareDecimal = compareDecimal

-- The process-wide mission serial allocator. It survives mission resets on
-- purpose so a serverSession is fresh for every mission this process runs.
NetworkSyncScoped._missionSerial = NetworkSyncScoped._missionSerial or "0"

local function tokenBytes(s)
    return 3 + #s
end

-- =========================================================
-- Value codec (protocol 1 typed scalar pairs)
-- =========================================================

local function encodeValue(v)
    local t = type(v)
    if t == "boolean" then
        return S.TAG_BOOL, v and "1" or "0"
    elseif t == "number" then
        if v ~= v or v == math.huge or v == -math.huge then return nil end
        return S.TAG_NUMBER, string.format("%.17g", v)
    elseif t == "string" then
        return S.TAG_STRING, v
    end
    return nil
end
S.encodeValue = encodeValue

local function decodeValue(tag, token)
    if tag == S.TAG_BOOL then
        if token == "1" then return true, true end
        if token == "0" then return false, true end
        return nil, false
    elseif tag == S.TAG_NUMBER then
        if type(token) ~= "string" or token:find("^[-+]?[0-9]*%.?[0-9]*[eE]?[-+]?[0-9]*$") == nil then
            return nil, false
        end
        local n = tonumber(token)
        if n == nil or n ~= n or n == math.huge or n == -math.huge then return nil, false end
        return n, true
    elseif tag == S.TAG_STRING then
        if type(token) ~= "string" then return nil, false end
        return token, true
    end
    return nil, false
end
S.decodeValue = decodeValue

-- =========================================================
-- Registration
-- =========================================================

function NetworkSync:_ensureScopedState()
    if self.scopedSchemas == nil then
        self.scopedSchemas = {}            -- modId -> { buildView, applyView, clearView }
        self.scopedOrder = {}
        self.scopedDirty = {}              -- modId -> bool (server)
        self.scopedSubscriptions = {}      -- connection -> { modId -> sub }
        self.scopedConnectionIds = {}      -- connection -> opaque mission-local id
        self.scopedConnectionSerial = "0"
        self.scopedClient = {}             -- modId -> client state
        self.scopedClientSerial = "0"      -- subscriptionId allocator (client)
        self.scopedServerSession = nil
        self.scopedReady = false
        self.scopedForceFullAll = false
    end
end

---@param modId string
---@param spec table { buildView = fn(context, previous, forceFull), applyView = fn(publication), clearView = fn(reason) }
---@return boolean success
function NetworkSync:registerScopedModule(modId, spec)
    self:_ensureScopedState()
    if type(modId) ~= "string" or modId == "" or #modId > E.MAX_ID_BYTES then
        NSLogger.warning("registerScopedModule: invalid modId '%s', ignoring", tostring(modId))
        return false
    end
    if type(spec) ~= "table" or type(spec.buildView) ~= "function"
        or type(spec.applyView) ~= "function" or type(spec.clearView) ~= "function" then
        NSLogger.warning("registerScopedModule('%s'): needs { buildView = fn, applyView = fn, clearView = fn }, ignoring", modId)
        return false
    end
    if self.schemas ~= nil and self.schemas[modId] ~= nil then
        NSLogger.warning("registerScopedModule('%s'): id is a live PUBLIC module, refusing (kinds cannot mix)", modId)
        return false
    end
    if self.scopedSchemas[modId] == nil then
        table.insert(self.scopedOrder, modId)
    else
        -- A same-id re-registration gets a fresh context: every subscriber is
        -- rebuilt from FULL and any client view of it starts over.
        NSLogger.debug("registerScopedModule('%s'): re-registered, subscribers rebuild from FULL", modId)
        self:_scopedForceFullModule(modId)
        self:_scopedClientResetModule(modId, S.REASON_NEW_GENERATION)
    end
    self.scopedSchemas[modId] = {
        buildView = spec.buildView,
        applyView = spec.applyView,
        clearView = spec.clearView,
    }
    NSLogger.debug("Registered scoped module '%s'", modId)
    return true
end

--- Remove a scoped module. Server: its subscribers get one CONTROL
--- (UNAVAILABLE / MODULE_UNREGISTERED) and are dropped. Client: the view is
--- cleared with that reason and no burst runs until a re-registration.
function NetworkSync:unregisterScopedModule(modId)
    self:_ensureScopedState()
    if self.scopedSchemas[modId] == nil then return false end
    if self:_isServer() then
        for connection, mods in pairs(self.scopedSubscriptions) do
            local sub = mods[modId]
            if sub ~= nil then
                self:_scopedSendControl(connection, modId, sub, S.STATE_UNAVAILABLE, S.REASON_MODULE_UNREGISTERED)
                mods[modId] = nil
            end
        end
    else
        self:_scopedClientClear(modId, S.REASON_MODULE_UNREGISTERED, true)
        self.scopedClient[modId] = nil
    end
    self.scopedSchemas[modId] = nil
    self.scopedDirty[modId] = nil
    for i, id in ipairs(self.scopedOrder) do
        if id == modId then table.remove(self.scopedOrder, i) break end
    end
    NSLogger.debug("Unregistered scoped module '%s'", modId)
    return true
end

function NetworkSync:isScopedModule(modId)
    return self.scopedSchemas ~= nil and self.scopedSchemas[modId] ~= nil
end

--- Capability record for consumers. `ready` means this mission's scoped state
--- and the event class exist, not that any remote connection has event ids.
function NetworkSync:getScopedCapabilities()
    self:_ensureScopedState()
    local ready = self.scopedReady == true and NetworkSyncScopedEvent ~= nil
    return {
        bootstrapVersion = S.BOOTSTRAP_VERSION,
        protocolVersions = { unpack(S.PROTOCOL_VERSIONS) },
        ready = ready,
        reasonCode = ready and "READY" or "NOT_INITIALIZED",
    }
end

function NetworkSync:_isServer()
    return g_currentMission ~= nil and g_currentMission:getIsServer()
end

-- =========================================================
-- Mission lifecycle
-- =========================================================

--- Fresh scoped maps for a mission. The process serial allocator is kept.
function NetworkSync:_scopedOnMissionLoaded()
    self:_ensureScopedState()
    self.scopedSubscriptions = {}
    self.scopedConnectionIds = {}
    self.scopedConnectionSerial = "0"
    self.scopedClient = {}
    self.scopedDirty = {}
    self.scopedForceFullAll = false
    if self:_isServer() then
        S._missionSerial = incrementDecimal(S._missionSerial)
        self.scopedServerSession = S._missionSerial
    else
        self.scopedServerSession = nil
    end
    self.scopedReady = true
    -- Registrations made by companions at their own load time (before this
    -- point) are kept; the previous mission's schemas were dropped at its
    -- teardown, so every producer here registered for this mission.
    -- Client: a local farm change invalidates every scoped view at once.
    if not self:_isServer() and g_messageCenter ~= nil and MessageType ~= nil and MessageType.PLAYER_FARM_CHANGED ~= nil then
        g_messageCenter:subscribe(MessageType.PLAYER_FARM_CHANGED, self._onScopedLocalContextChanged, self)
    end
end

--- Mission teardown: every scoped map goes, nothing is sent.
function NetworkSync:_scopedOnMissionDelete()
    if self.scopedSchemas == nil then return end
    for modId in pairs(self.scopedClient) do
        self:_scopedClientClear(modId, S.REASON_MISSION_END, true)
    end
    self.scopedSubscriptions = {}
    self.scopedConnectionIds = {}
    self.scopedClient = {}
    self.scopedDirty = {}
    self.scopedSchemas = {}
    self.scopedOrder = {}
    self.scopedServerSession = nil
    self.scopedReady = false
    if g_messageCenter ~= nil and g_messageCenter.unsubscribeAll ~= nil then
        g_messageCenter:unsubscribeAll(self)
    end
end

function NetworkSync:_onScopedLocalContextChanged()
    if self:_isServer() or self.scopedClient == nil then return end
    for modId in pairs(self.scopedSchemas or {}) do
        local cs = self.scopedClient[modId]
        if cs == nil or not cs.terminal then
            self:_scopedClientClear(modId, S.REASON_LOCAL_CONTEXT_CHANGED, true)
            self:_scopedStartGeneration(modId)
        end
    end
end

--- Connection closed on the server: drop that connection object's
--- subscriptions and descriptors. Nothing is sent to the closing object.
function NetworkSync:_scopedOnConnectionClosed(connection)
    if self.scopedSubscriptions == nil or connection == nil then return end
    self.scopedSubscriptions[connection] = nil
    self.scopedConnectionIds[connection] = nil
end

-- =========================================================
-- Server: subscriptions and publication
-- =========================================================

function NetworkSync:_scopedConnectionId(connection)
    local id = self.scopedConnectionIds[connection]
    if id == nil then
        self.scopedConnectionSerial = incrementDecimal(self.scopedConnectionSerial)
        id = "c" .. self.scopedConnectionSerial
        self.scopedConnectionIds[connection] = id
    end
    return id
end

-- Is this connection object still one we may send a private publication to?
local function connectionIsCurrent(connection)
    if type(connection) ~= "table" then return false end
    if connection.isConnected ~= true then return false end
    if NetworkNode ~= nil and connection.streamId == NetworkNode.LOCAL_STREAM_ID then return false end
    if connection.streamId == 0 then return false end
    if connection.isReadyForEvents ~= true then return false end
    if NetworkSyncScopedEvent == nil or NetworkSyncScopedEvent.eventId == nil then return false end
    return true
end
S.connectionIsCurrent = connectionIsCurrent

-- Trusted actor resolution for a remote connection. Never defaults a farm.
function NetworkSync:_scopedResolveActor(connection)
    local userId = nil
    local user = nil
    if g_currentMission ~= nil and g_currentMission.userManager ~= nil
        and g_currentMission.userManager.getUserByConnection ~= nil then
        user = g_currentMission.userManager:getUserByConnection(connection)
    end
    if user ~= nil and user.getId ~= nil then userId = user:getId() end

    local farmId = nil
    if g_currentMission ~= nil and g_currentMission.getFarmId ~= nil then
        local ok, id = pcall(g_currentMission.getFarmId, g_currentMission, connection)
        if ok then farmId = id end
    end

    local actorState
    if user == nil or farmId == nil then
        actorState = S.ACTOR_WAITING
    elseif FarmManager ~= nil and farmId == FarmManager.SPECTATOR_FARM_ID then
        actorState = S.ACTOR_SPECTATOR
    elseif FarmManager ~= nil and (farmId == FarmManager.GUIDED_TOUR_FARM_ID or farmId == FarmManager.INVALID_FARM_ID
        or farmId > (FarmManager.MAX_FARM_ID or 8) or farmId < 0) then
        actorState = S.ACTOR_INVALID
    elseif FarmManager == nil and farmId == 0 then
        actorState = S.ACTOR_SPECTATOR
    else
        actorState = S.ACTOR_RESOLVED
    end
    return userId, farmId, actorState, user
end

function NetworkSync:_scopedForceFullModule(modId)
    for _, mods in pairs(self.scopedSubscriptions) do
        local sub = mods[modId]
        if sub ~= nil then sub.forceFull = true end
    end
end

function NetworkSync:_scopedMarkDirty(modId)
    self.scopedDirty[modId] = true
end

--- syncNow for a scoped id: every subscriber of that module gets a FULL now.
function NetworkSync:_scopedSyncNow(modId)
    if not self:_isServer() then return end
    self:_scopedForceFullModule(modId)
    for connection, mods in pairs(self.scopedSubscriptions) do
        local sub = mods[modId]
        if sub ~= nil then
            self:_scopedPublishTo(connection, modId, sub)
        end
    end
    self.scopedDirty[modId] = false
end

--- Drift floor: scoped subscribers are asked for FULL on the next cadence.
function NetworkSync:_scopedDriftFloor()
    self.scopedForceFullAll = true
end

--- The cadence: every subscribed connection, every scoped module. Access is
--- re-evaluated by the producer each time; UNCHANGED costs nothing on the wire.
function NetworkSync:_scopedPublishAll()
    if self.scopedSchemas == nil or not self:_isServer() then return end
    local forceAll = self.scopedForceFullAll
    self.scopedForceFullAll = false
    for connection, mods in pairs(self.scopedSubscriptions) do
        for modId, sub in pairs(mods) do
            if forceAll then sub.forceFull = true end
            self:_scopedPublishTo(connection, modId, sub)
        end
    end
    for modId in pairs(self.scopedDirty) do self.scopedDirty[modId] = false end
end

local function validateProducerResult(result)
    if type(result) ~= "table" then return false, "MALFORMED" end
    if not VALID_STATES[result.state] then return false, "MALFORMED" end
    if result.state ~= S.STATE_READY then
        return true, nil
    end
    if type(result.viewKey) ~= "string" or result.viewKey == "" then return false, "MALFORMED" end
    if type(result.dataRevision) ~= "string" or result.dataRevision == "" or #result.dataRevision > E.MAX_ID_BYTES then
        return false, "MALFORMED"
    end
    if not VALID_MODES[result.mode] then return false, "MALFORMED" end
    if result.mode ~= S.MODE_UNCHANGED and type(result.values) ~= "table" then return false, "MALFORMED" end
    if result.mode == S.MODE_DELTA then
        if type(result.baseRevision) ~= "string" or result.baseRevision == "" then return false, "MALFORMED" end
    end
    return true, nil
end
S.validateProducerResult = validateProducerResult

function NetworkSync:_scopedCallProducer(schema, context, previous, forceFull)
    local ok, result = pcall(schema.buildView, context, previous, forceFull)
    if not ok then
        NSLogger.error("scoped producer '%s' threw: %s", tostring(context.modId), tostring(result))
        return { state = S.STATE_ERROR, reason = S.REASON_PRODUCER_ERROR }
    end
    local valid, why = validateProducerResult(result)
    if not valid then
        NSLogger.warning("scoped producer '%s' returned a malformed view (%s)", tostring(context.modId), tostring(why))
        return { state = S.STATE_ERROR, reason = S.REASON_MALFORMED }
    end
    return result
end

--- Send one CONTROL frame for a subscription. protocolVersion 0 marks a
--- refusal with no mutually supported protocol.
function NetworkSync:_scopedSendControl(connection, modId, sub, state, reason, protocolVersion)
    if not connectionIsCurrent(connection) then return false end
    sub.publicationId = incrementDecimal(sub.publicationId or "0")
    local tokens = {
        self.scopedServerSession or "1",
        sub.viewEpoch or "1",
        sub.publicationId,
        state,
        tostring(reason or ""),
    }
    local ok, why = E.validate(E.KIND_CONTROL, modId, sub.subscriptionId, protocolVersion or sub.protocolVersion or 0, tokens)
    if not ok then
        NSLogger.warning("scoped control for '%s' refused before send: %s", modId, tostring(why))
        return false
    end
    local event = E.new(E.KIND_CONTROL, modId, sub.subscriptionId, protocolVersion or sub.protocolVersion or 0, tokens)
    local sent = pcall(function() connection:sendEvent(event) end)
    if not sent then
        NSLogger.warning("scoped control send failed for '%s'", modId)
    end
    return sent
end

-- Build the PUBLICATION token chunks for a READY result. Returns chunks (array
-- of token arrays) or nil, reason.
local function buildPublicationChunks(serverSession, sub, result, mode, baseRevision, modId)
    local values = result.values or {}
    local pairsOut = {}
    for i = 1, #values do
        local tag, token = encodeValue(values[i])
        if tag == nil then return nil, S.REASON_MALFORMED end
        if #token > 65535 then return nil, S.REASON_OVERSIZE end
        pairsOut[#pairsOut + 1] = { tag, token, tokenBytes(tag) + tokenBytes(token) }
    end

    local headerBytes = 2 + 2 + tokenBytes(modId) + tokenBytes(sub.subscriptionId) + 4 + 4
        + tokenBytes(serverSession) + tokenBytes(sub.viewEpoch) + tokenBytes(sub.publicationId)
        + tokenBytes(S.STATE_READY) + tokenBytes(mode) + tokenBytes(baseRevision)
        + tokenBytes(result.dataRevision) + tokenBytes("9999") * 3
    local budget = NetworkSync.EVENT_BUDGET_BYTES
    if headerBytes >= budget then return nil, S.REASON_OVERSIZE end

    local groups = {}
    local cur, curBytes = {}, headerBytes
    for _, p in ipairs(pairsOut) do
        if #cur > 0 and curBytes + p[3] > budget then
            groups[#groups + 1] = cur
            cur, curBytes = {}, headerBytes
        end
        if headerBytes + p[3] > budget then return nil, S.REASON_OVERSIZE end
        cur[#cur + 1] = p
        curBytes = curBytes + p[3]
    end
    if #cur > 0 or #groups == 0 then groups[#groups + 1] = cur end
    local chunkCount = #groups
    if chunkCount > S.MAX_CHUNKS then return nil, S.REASON_OVERSIZE end

    local chunks = {}
    for idx = 1, chunkCount do
        local g = groups[idx]
        local tokens = {
            serverSession, sub.viewEpoch, sub.publicationId, S.STATE_READY, mode,
            baseRevision, result.dataRevision,
            tostring(idx - 1), tostring(chunkCount), tostring(#g),
        }
        for _, p in ipairs(g) do
            tokens[#tokens + 1] = p[1]
            tokens[#tokens + 1] = p[2]
        end
        if #tokens > E.MAX_BODY_TOKENS then return nil, S.REASON_OVERSIZE end
        chunks[idx] = tokens
    end
    return chunks, nil
end
S.buildPublicationChunks = buildPublicationChunks

--- One producer call and, when warranted, one publication to one connection.
function NetworkSync:_scopedPublishTo(connection, modId, sub)
    local schema = self.scopedSchemas[modId]
    if schema == nil then return end
    local mods = self.scopedSubscriptions[connection]
    if mods == nil or mods[modId] ~= sub then return end

    -- The connection must still be current and the same trusted user.
    if not connectionIsCurrent(connection) then return end
    local userId, farmId, actorState = self:_scopedResolveActor(connection)
    if sub.userId ~= nil and userId ~= sub.userId then
        NSLogger.debug("scoped '%s': connection changed user, dropping subscription", modId)
        mods[modId] = nil
        return
    end

    local forceFull = sub.forceFull == true or sub.previous == nil
    local previous = nil
    if sub.previous ~= nil and not forceFull then
        previous = { viewKey = sub.previous.viewKey, viewEpoch = sub.previous.viewEpoch, dataRevision = sub.previous.dataRevision }
    end
    local context = {
        connection = connection,
        connectionId = self:_scopedConnectionId(connection),
        userId = userId,
        farmId = farmId,
        actorState = actorState,
        serverSession = self.scopedServerSession,
        subscriptionId = sub.subscriptionId,
        modId = modId,
    }
    local result = self:_scopedCallProducer(schema, context, previous, forceFull)
    context.connection = nil

    if result.state ~= S.STATE_READY then
        -- Non-READY invalidates the usable replica; the next READY needs a
        -- new epoch and a FULL. Unchanged non-READY is not resent per tick.
        sub.previous = nil
        sub.needsEpoch = true
        local reason = tostring(result.reason or "")
        local last = sub.lastControl
        if sub.forceFull or last == nil or last.state ~= result.state or last.reason ~= reason then
            self:_scopedSendControl(connection, modId, sub, result.state, reason)
            sub.lastControl = { state = result.state, reason = reason }
        end
        sub.forceFull = false
        return
    end

    -- READY. Decide epoch reset and the mode we can actually send.
    local needReset = sub.previous == nil or sub.needsEpoch == true or sub.previous.viewKey ~= result.viewKey
    local mode = result.mode
    if needReset then
        if mode ~= S.MODE_FULL then
            result = self:_scopedCallProducer(schema, context, nil, true)
            if result.state ~= S.STATE_READY or result.mode ~= S.MODE_FULL then
                self:_scopedSendControl(connection, modId, sub, S.STATE_ERROR, S.REASON_PRODUCER_ERROR)
                sub.lastControl = { state = S.STATE_ERROR, reason = S.REASON_PRODUCER_ERROR }
                sub.previous = nil
                sub.needsEpoch = true
                sub.forceFull = false
                return
            end
            mode = S.MODE_FULL
        end
        sub.viewEpoch = incrementDecimal(sub.viewEpoch or "0")
        sub.needsEpoch = false
    elseif mode == S.MODE_UNCHANGED then
        if result.dataRevision == sub.previous.dataRevision then
            sub.forceFull = false
            return   -- nothing on the wire
        end
        -- A revision change cannot be "unchanged": rebuild FULL.
        result = self:_scopedCallProducer(schema, context, nil, true)
        if result.state ~= S.STATE_READY or result.mode ~= S.MODE_FULL then
            self:_scopedSendControl(connection, modId, sub, S.STATE_ERROR, S.REASON_PRODUCER_ERROR)
            sub.lastControl = { state = S.STATE_ERROR, reason = S.REASON_PRODUCER_ERROR }
            sub.previous = nil
            sub.needsEpoch = true
            sub.forceFull = false
            return
        end
        mode = S.MODE_FULL
    elseif mode == S.MODE_DELTA then
        if result.baseRevision ~= sub.previous.dataRevision then
            result = self:_scopedCallProducer(schema, context, nil, true)
            if result.state ~= S.STATE_READY or result.mode ~= S.MODE_FULL then
                self:_scopedSendControl(connection, modId, sub, S.STATE_ERROR, S.REASON_PRODUCER_ERROR)
                sub.lastControl = { state = S.STATE_ERROR, reason = S.REASON_PRODUCER_ERROR }
                sub.previous = nil
                sub.needsEpoch = true
                sub.forceFull = false
                return
            end
            mode = S.MODE_FULL
        end
    end
    if mode == S.MODE_FULL and sub.viewEpoch == nil then
        sub.viewEpoch = incrementDecimal("0")
    end

    local baseRevision = (mode == S.MODE_DELTA) and result.baseRevision or ""
    sub.publicationId = incrementDecimal(sub.publicationId or "0")
    local chunks, why = buildPublicationChunks(self.scopedServerSession or "1", sub, result, mode, baseRevision, modId)
    if chunks == nil then
        NSLogger.warning("scoped '%s': publication refused (%s), nothing partial is sent", modId, tostring(why))
        self:_scopedSendControl(connection, modId, sub, S.STATE_ERROR, why)
        sub.lastControl = { state = S.STATE_ERROR, reason = why }
        sub.previous = nil
        sub.needsEpoch = true
        sub.forceFull = false
        return
    end

    -- Re-check the object right before sending; send every chunk now.
    if not connectionIsCurrent(connection) then return end
    for idx = 1, #chunks do
        local ok, whyE = E.validate(E.KIND_PUBLICATION, modId, sub.subscriptionId, sub.protocolVersion, chunks[idx])
        if not ok then
            NSLogger.warning("scoped '%s': chunk %d refused before send (%s)", modId, idx - 1, tostring(whyE))
            return
        end
    end
    for idx = 1, #chunks do
        local event = E.new(E.KIND_PUBLICATION, modId, sub.subscriptionId, sub.protocolVersion, chunks[idx])
        local sent = pcall(function() connection:sendEvent(event) end)
        if not sent then
            NSLogger.warning("scoped '%s': send failed on chunk %d; subscriber rebuilds from FULL next cadence", modId, idx - 1)
            sub.previous = nil
            sub.needsEpoch = true
            sub.forceFull = true
            return
        end
    end
    sub.previous = { viewKey = result.viewKey, viewEpoch = sub.viewEpoch, dataRevision = result.dataRevision }
    sub.lastControl = nil
    sub.forceFull = false
end

--- SUBSCRIBE from a remote connection.
function NetworkSync:_scopedOnSubscribe(event, connection)
    local modId = event.modId
    local sub = {
        subscriptionId = event.subscriptionId,
        protocolVersion = 0,
        viewEpoch = nil,
        publicationId = "0",
        previous = nil,
        needsEpoch = true,
        forceFull = true,
        lastControl = nil,
    }
    if not connectionIsCurrent(connection) then return end
    if self.scopedSchemas[modId] == nil then
        -- Not a protocol refusal: echo the advertised protocol so the client
        -- treats this as "no admitted subscriber", not as terminal.
        self:_scopedSendControl(connection, modId, sub, S.STATE_UNAVAILABLE, S.REASON_MODULE_NOT_REGISTERED, event.protocolVersion)
        return
    end
    -- Body: supportedVersionCount then that many distinct positive decimal tokens.
    local count = tonumber(event.tokens[1] or "")
    local negotiated = nil
    if count ~= nil and count >= 1 and count == math.floor(count) and #event.tokens == count + 1 then
        local seen = {}
        for i = 2, count + 1 do
            local v = event.tokens[i]
            if not isCanonicalDecimal(v) or seen[v] then negotiated = nil break end
            seen[v] = true
            for _, supported in ipairs(S.PROTOCOL_VERSIONS) do
                if v == tostring(supported) then negotiated = supported end
            end
        end
    end
    if negotiated == nil then
        self:_scopedSendControl(connection, modId, sub, S.STATE_UNAVAILABLE, S.REASON_UNSUPPORTED_PROTOCOL, 0)
        return
    end
    sub.protocolVersion = negotiated
    local userId = self:_scopedResolveActor(connection)
    sub.userId = userId

    local mods = self.scopedSubscriptions[connection]
    if mods == nil then
        mods = {}
        self.scopedSubscriptions[connection] = mods
    end
    local existing = mods[modId]
    if existing ~= nil and existing.subscriptionId == sub.subscriptionId then
        -- Same-burst repeat: reply again, no second subscriber.
        existing.forceFull = true
        self:_scopedPublishTo(connection, modId, existing)
        return
    end
    mods[modId] = sub
    self:_scopedPublishTo(connection, modId, sub)
end

function NetworkSync:_scopedOnUnsubscribe(event, connection)
    local mods = self.scopedSubscriptions[connection]
    if mods == nil then return end
    local sub = mods[event.modId]
    if sub ~= nil and sub.subscriptionId == event.subscriptionId then
        mods[event.modId] = nil
    end
end

-- =========================================================
-- Client: subscription generations, reassembly, application
-- =========================================================

local function newClientState()
    return {
        subscriptionId = nil,
        protocolVersion = 1,
        serverSession = nil,
        viewEpoch = nil,
        epochFloor = nil,        -- accepted CONTROL epoch; older data cannot restore readiness
        publicationFloor = nil,
        usable = nil,            -- { viewEpoch, dataRevision }
        staging = nil,           -- { publicationId, mode, baseRevision, dataRevision, chunkCount, parts, received, age }
        state = S.STATE_UNAVAILABLE,
        reason = "NOT_SUBSCRIBED",
        terminal = false,
        acknowledged = false,
        burstActive = false,
        attempts = 0,
        timer = 0,
        waitTimer = nil,         -- after an unacknowledged burst
    }
end

function NetworkSync:_scopedClientState(modId)
    local cs = self.scopedClient[modId]
    if cs == nil then
        cs = newClientState()
        self.scopedClient[modId] = cs
    end
    return cs
end

function NetworkSync:_scopedClientCallClear(modId, reason)
    local schema = self.scopedSchemas[modId]
    if schema == nil then return end
    local ok, err = pcall(schema.clearView, reason)
    if not ok then NSLogger.error("scoped clearView '%s' threw: %s", modId, tostring(err)) end
end

--- Clear usable and staged state for a module; the consumer is told why.
function NetworkSync:_scopedClientClear(modId, reason, callConsumer)
    local cs = self.scopedClient[modId]
    if cs == nil then return end
    cs.usable = nil
    cs.staging = nil
    if cs.state == S.STATE_READY then cs.state = S.STATE_UNAVAILABLE end
    cs.reason = reason
    if callConsumer ~= false then self:_scopedClientCallClear(modId, reason) end
end

function NetworkSync:_scopedClientResetModule(modId, reason)
    if self:_isServer() then return end
    local cs = self.scopedClient[modId]
    if cs == nil then return end
    self:_scopedClientClear(modId, reason, true)
    cs.terminal = false
    self:_scopedStartGeneration(modId)
end

--- Start a fresh subscription generation: new id, cleared replica, bounded burst.
function NetworkSync:_scopedStartGeneration(modId)
    local cs = self:_scopedClientState(modId)
    if cs.terminal then return end
    self.scopedClientSerial = incrementDecimal(self.scopedClientSerial)
    cs.subscriptionId = self.scopedClientSerial
    cs.serverSession = nil
    cs.viewEpoch = nil
    cs.epochFloor = nil
    cs.publicationFloor = nil
    cs.usable = nil
    cs.staging = nil
    cs.state = S.STATE_UNAVAILABLE
    cs.reason = S.REASON_NEW_GENERATION
    cs.acknowledged = false
    cs.burstActive = true
    cs.attempts = 0
    cs.timer = 0
    cs.waitTimer = nil
end

--- Explicit recovery request. Client: a new generation. Server: FULL to all.
function NetworkSync:requestScopedFull(modId)
    self:_ensureScopedState()
    if self.scopedSchemas[modId] == nil then return false end
    if self:_isServer() then
        self:_scopedSyncNow(modId)
        return true
    end
    local cs = self:_scopedClientState(modId)
    if cs.terminal then return false end
    self:_scopedClientClear(modId, S.REASON_NEW_GENERATION, true)
    self:_scopedStartGeneration(modId)
    return true
end

-- Can this client send a scoped event right now? Returns the connection or nil.
local function currentServerConnection()
    if g_client == nil or g_client.getServerConnection == nil then return nil end
    local conn = g_client:getServerConnection()
    if conn == nil then return nil end
    if conn.isConnected ~= true then return nil end
    if conn.isReadyForEvents ~= true then return nil end
    if NetworkSyncScopedEvent == nil or NetworkSyncScopedEvent.eventId == nil then return nil end
    return conn
end
S.currentServerConnection = currentServerConnection

function NetworkSync:_scopedSendSubscribe(modId, cs)
    local conn = currentServerConnection()
    if conn == nil then return false end   -- NO ATTEMPT
    local tokens = { tostring(#S.PROTOCOL_VERSIONS) }
    for _, v in ipairs(S.PROTOCOL_VERSIONS) do tokens[#tokens + 1] = tostring(v) end
    local ok = E.validate(E.KIND_SUBSCRIBE, modId, cs.subscriptionId, S.PROTOCOL_VERSIONS[#S.PROTOCOL_VERSIONS], tokens)
    if not ok then return false end
    local event = E.new(E.KIND_SUBSCRIBE, modId, cs.subscriptionId, S.PROTOCOL_VERSIONS[#S.PROTOCOL_VERSIONS], tokens)
    pcall(function() conn:sendEvent(event) end)
    return true   -- attempted, not acknowledged
end

function NetworkSync:_scopedSendUnsubscribe(modId, cs)
    local conn = currentServerConnection()
    if conn == nil or cs.subscriptionId == nil then return false end
    local event = E.new(E.KIND_UNSUBSCRIBE, modId, cs.subscriptionId, cs.protocolVersion or 0, {})
    pcall(function() conn:sendEvent(event) end)
    return true
end

--- Client cadence: bursts, staging expiry and the post-burst wait.
function NetworkSync:_scopedClientUpdate(dt)
    if self.scopedSchemas == nil then return end
    for modId in pairs(self.scopedSchemas) do
        local cs = self.scopedClient[modId]
        if cs == nil then
            self:_scopedStartGeneration(modId)
            cs = self.scopedClient[modId]
        end
        if cs.terminal then
            -- Suspended until a compatible registration or explicit retry.
        elseif cs.burstActive and not cs.acknowledged then
            cs.timer = cs.timer + dt
            if cs.timer >= NetworkSync.JOIN_REQUEST_INTERVAL then
                cs.timer = 0
                local attempted = self:_scopedSendSubscribe(modId, cs)
                if attempted then cs.attempts = cs.attempts + 1 end
                if cs.attempts >= NetworkSync.JOIN_REQUEST_MAX then
                    cs.burstActive = false
                    cs.waitTimer = 0
                    cs.reason = S.REASON_UNACKNOWLEDGED
                    NSLogger.warning("scoped '%s': subscription unacknowledged after %d attempts; retrying after the drift floor",
                        modId, cs.attempts)
                end
            end
        elseif cs.waitTimer ~= nil then
            cs.waitTimer = cs.waitTimer + dt
            if cs.waitTimer >= NetworkSync.DRIFT_FLOOR_MS and currentServerConnection() ~= nil then
                self:_scopedStartGeneration(modId)
            end
        end
        -- An incomplete publication expires on the recovery interval.
        if cs.staging ~= nil then
            cs.staging.age = (cs.staging.age or 0) + dt
            if cs.staging.age >= NetworkSync.DRIFT_FLOOR_MS then
                cs.staging = nil
                self:_scopedRecover(modId, cs, "STAGING_EXPIRED")
            end
        end
    end
end

--- Bounded recovery: clear usable state and start a fresh generation.
function NetworkSync:_scopedRecover(modId, cs, reason)
    self:_scopedClientClear(modId, reason, true)
    cs.acknowledged = false
    self:_scopedStartGeneration(modId)
end

--- TERMINAL: clear, retain the reason, suspend the tuple, withdraw it.
function NetworkSync:_scopedTerminal(modId, cs, reason)
    self:_scopedClientClear(modId, reason, true)
    cs.terminal = true
    cs.state = S.STATE_UNAVAILABLE
    cs.reason = reason
    cs.burstActive = false
    cs.waitTimer = nil
    cs.acknowledged = true
    self:_scopedSendUnsubscribe(modId, cs)
end

local function validateApplyResult(result, dataRevision)
    if type(result) ~= "table" then return S.OUTCOME_RETRYABLE, S.REASON_APPLY_ERROR end
    if result.outcome == S.OUTCOME_APPLIED then
        if result.dataRevision == dataRevision then return S.OUTCOME_APPLIED, nil end
        return S.OUTCOME_RETRYABLE, S.REASON_APPLY_ERROR
    elseif result.outcome == S.OUTCOME_TERMINAL then
        return S.OUTCOME_TERMINAL, tostring(result.reason or S.REASON_UNSUPPORTED_APPLICATION_VERSION)
    elseif result.outcome == S.OUTCOME_RETRYABLE then
        return S.OUTCOME_RETRYABLE, tostring(result.reason or S.REASON_APPLY_ERROR)
    end
    return S.OUTCOME_RETRYABLE, S.REASON_APPLY_ERROR
end
S.validateApplyResult = validateApplyResult

--- A complete publication: decode, apply, dispatch the typed result.
function NetworkSync:_scopedApply(modId, cs, staging, values)
    local schema = self.scopedSchemas[modId]
    if schema == nil then return end
    local publication = {
        modId = modId,
        subscriptionId = cs.subscriptionId,
        serverSession = cs.serverSession,
        viewEpoch = staging.viewEpoch,
        publicationId = staging.publicationId,
        state = S.STATE_READY,
        mode = staging.mode,
        baseRevision = (staging.mode == S.MODE_DELTA) and staging.baseRevision or nil,
        dataRevision = staging.dataRevision,
        values = values,
    }
    local ok, result = pcall(schema.applyView, publication)
    local outcome, reason
    if not ok then
        NSLogger.error("scoped applyView '%s' threw: %s", modId, tostring(result))
        outcome, reason = S.OUTCOME_RETRYABLE, S.REASON_APPLY_ERROR
    else
        outcome, reason = validateApplyResult(result, staging.dataRevision)
    end
    if outcome == S.OUTCOME_APPLIED then
        cs.usable = { viewEpoch = staging.viewEpoch, dataRevision = staging.dataRevision }
        cs.state = S.STATE_READY
        cs.reason = nil
        cs.acknowledged = true
        cs.burstActive = false
        cs.waitTimer = nil
    elseif outcome == S.OUTCOME_TERMINAL then
        self:_scopedTerminal(modId, cs, reason)
    else
        self:_scopedRecover(modId, cs, reason)
    end
end

--- PUBLICATION frame on the client.
function NetworkSync:_scopedOnPublication(event)
    local modId = event.modId
    local cs = self.scopedClient[modId]
    if cs == nil or cs.subscriptionId ~= event.subscriptionId or cs.terminal then return end
    if event.protocolVersion ~= cs.protocolVersion then
        self:_scopedTerminal(modId, cs, S.REASON_UNSUPPORTED_PROTOCOL)
        return
    end
    local t = event.tokens
    if #t < S.PUBLICATION_HEADER_TOKENS then
        self:_scopedRecover(modId, cs, S.REASON_MALFORMED)
        return
    end
    local serverSession, viewEpoch, publicationId, state, mode = t[1], t[2], t[3], t[4], t[5]
    local baseRevision, dataRevision = t[6], t[7]
    local chunkIndex, chunkCount, valueCount = tonumber(t[8]), tonumber(t[9]), tonumber(t[10])
    if not isCanonicalDecimal(serverSession) or not isCanonicalDecimal(viewEpoch) or not isCanonicalDecimal(publicationId)
        or state ~= S.STATE_READY or not VALID_MODES[mode] or mode == S.MODE_UNCHANGED
        or type(dataRevision) ~= "string" or dataRevision == ""
        or chunkIndex == nil or chunkCount == nil or valueCount == nil
        or chunkCount < 1 or chunkCount > S.MAX_CHUNKS or chunkIndex < 0 or chunkIndex >= chunkCount
        or chunkIndex ~= math.floor(chunkIndex) or valueCount ~= math.floor(valueCount) or valueCount < 0
        or #t ~= S.PUBLICATION_HEADER_TOKENS + 2 * valueCount
        or (mode == S.MODE_DELTA and baseRevision == "") or (mode == S.MODE_FULL and baseRevision ~= "") then
        self:_scopedRecover(modId, cs, S.REASON_MALFORMED)
        return
    end
    -- Server session: learned through this subscription's first reply only.
    if cs.serverSession == nil then
        cs.serverSession = serverSession
    elseif cs.serverSession ~= serverSession then
        return   -- another mission's frame
    end
    -- Epoch ordering.
    if cs.epochFloor ~= nil and compareDecimal(viewEpoch, cs.epochFloor) < 0 then return end
    if cs.viewEpoch == nil or compareDecimal(viewEpoch, cs.viewEpoch) > 0 then
        cs.usable = nil
        cs.staging = nil
        cs.viewEpoch = viewEpoch
        if cs.state == S.STATE_READY then cs.state = S.STATE_UNAVAILABLE end
        self:_scopedClientCallClear(modId, "NEW_EPOCH")
    elseif compareDecimal(viewEpoch, cs.viewEpoch) < 0 then
        return
    end
    if cs.publicationFloor ~= nil and compareDecimal(publicationId, cs.publicationFloor) <= 0 then return end

    -- DELTA needs the exact baseline at this epoch.
    if mode == S.MODE_DELTA then
        if cs.usable == nil or cs.usable.viewEpoch ~= viewEpoch or cs.usable.dataRevision ~= baseRevision then
            self:_scopedRecover(modId, cs, S.REASON_BASE_MISMATCH)
            return
        end
    end

    -- Staging: at most one incomplete publication; a newer one replaces it.
    local st = cs.staging
    if st ~= nil then
        local cmp = compareDecimal(publicationId, st.publicationId)
        if cmp < 0 then return end
        if cmp > 0 then st = nil end
    end
    if st == nil then
        st = { publicationId = publicationId, viewEpoch = viewEpoch, mode = mode, baseRevision = baseRevision,
               dataRevision = dataRevision, chunkCount = chunkCount, parts = {}, received = 0, age = 0 }
        cs.staging = st
    else
        if st.mode ~= mode or st.baseRevision ~= baseRevision or st.dataRevision ~= dataRevision
            or st.chunkCount ~= chunkCount or st.viewEpoch ~= viewEpoch then
            cs.staging = nil
            self:_scopedRecover(modId, cs, S.REASON_MALFORMED)
            return
        end
    end
    if st.parts[chunkIndex] ~= nil then
        -- A conflicting duplicate clears the assembly.
        cs.staging = nil
        self:_scopedRecover(modId, cs, S.REASON_MALFORMED)
        return
    end
    local decoded = {}
    for i = 1, valueCount do
        local tag = t[S.PUBLICATION_HEADER_TOKENS + 2 * i - 1]
        local token = t[S.PUBLICATION_HEADER_TOKENS + 2 * i]
        local v, ok = decodeValue(tag, token)
        if not ok then
            cs.staging = nil
            self:_scopedRecover(modId, cs, S.REASON_MALFORMED)
            return
        end
        decoded[i] = v
    end
    st.parts[chunkIndex] = decoded
    st.received = st.received + 1
    if st.received < st.chunkCount then return end

    local values = {}
    for i = 0, st.chunkCount - 1 do
        local part = st.parts[i]
        if part == nil then
            cs.staging = nil
            self:_scopedRecover(modId, cs, S.REASON_MALFORMED)
            return
        end
        for j = 1, #part do values[#values + 1] = part[j] end
    end
    cs.staging = nil
    cs.publicationFloor = publicationId
    self:_scopedApply(modId, cs, st, values)
end

--- CONTROL frame on the client.
function NetworkSync:_scopedOnControl(event)
    local modId = event.modId
    local cs = self.scopedClient[modId]
    if cs == nil or cs.subscriptionId ~= event.subscriptionId or cs.terminal then return end
    local t = event.tokens
    if #t < 5 then return end
    local serverSession, viewEpoch, publicationId, state, reason = t[1], t[2], t[3], t[4], t[5]
    if not isCanonicalDecimal(serverSession) or not isCanonicalDecimal(viewEpoch) or not isCanonicalDecimal(publicationId) then return end
    if not NON_READY_CONTROL[state] then return end
    if cs.serverSession == nil then
        cs.serverSession = serverSession
    elseif cs.serverSession ~= serverSession then
        return
    end
    if cs.epochFloor ~= nil and compareDecimal(viewEpoch, cs.epochFloor) < 0 then return end

    if reason == S.REASON_MODULE_NOT_REGISTERED or reason == S.REASON_MODULE_UNREGISTERED then
        -- No admitted subscriber. Clear, and coalesce a fresh burst after the
        -- drift floor when readiness permits. Receipt acknowledges the request.
        self:_scopedClientClear(modId, reason, true)
        cs.state = S.STATE_UNAVAILABLE
        cs.reason = reason
        cs.acknowledged = true
        cs.burstActive = false
        cs.waitTimer = 0
        return
    end
    if event.protocolVersion == 0 or reason == S.REASON_UNSUPPORTED_PROTOCOL then
        self:_scopedTerminal(modId, cs, S.REASON_UNSUPPORTED_PROTOCOL)
        return
    end
    -- Acknowledged non-READY: a server subscription exists; later updates may
    -- replace it without a new client request.
    cs.epochFloor = viewEpoch
    cs.publicationFloor = publicationId
    self:_scopedClientClear(modId, reason, true)
    cs.state = state
    cs.reason = reason
    cs.acknowledged = true
    cs.burstActive = false
    cs.waitTimer = nil
end

-- =========================================================
-- Event entry point (both roles)
-- =========================================================

function NetworkSync:_receiveScopedEvent(event, connection)
    self:_ensureScopedState()
    if event.malformed ~= nil then
        if not self:_isServer() and self.scopedClient[event.modId or ""] ~= nil then
            local cs = self.scopedClient[event.modId]
            cs.reason = S.REASON_MALFORMED
        end
        NSLogger.debug("scoped event dropped: %s", tostring(event.malformed))
        return
    end
    local ok, why = E.validate(event.kind, event.modId, event.subscriptionId, event.protocolVersion, event.tokens)
    if not ok then
        NSLogger.debug("scoped event dropped: %s", tostring(why))
        return
    end
    if self:_isServer() then
        if connection == nil then return end
        if event.kind == E.KIND_SUBSCRIBE then
            self:_scopedOnSubscribe(event, connection)
        elseif event.kind == E.KIND_UNSUBSCRIBE then
            self:_scopedOnUnsubscribe(event, connection)
        end
        -- PUBLICATION and CONTROL never come from a client.
    else
        if event.kind == E.KIND_PUBLICATION then
            self:_scopedOnPublication(event)
        elseif event.kind == E.KIND_CONTROL then
            self:_scopedOnControl(event)
        end
    end
end

-- =========================================================
-- Diagnostics (no private rows)
-- =========================================================

function NetworkSync:getScopedStatusLines()
    local lines = {}
    if self.scopedSchemas == nil then return lines end
    local caps = self:getScopedCapabilities()
    table.insert(lines, string.format("  scoped: bootstrap %d, protocols {%d}, ready=%s (%s), session=%s",
        caps.bootstrapVersion, S.PROTOCOL_VERSIONS[1], tostring(caps.ready), caps.reasonCode,
        tostring(self.scopedServerSession)))
    for _, modId in ipairs(self.scopedOrder) do
        if self:_isServer() then
            local n = 0
            for _, mods in pairs(self.scopedSubscriptions) do
                if mods[modId] ~= nil then n = n + 1 end
            end
            table.insert(lines, string.format("  - scoped %s: %d subscriber(s)", modId, n))
        else
            local cs = self.scopedClient[modId]
            table.insert(lines, string.format("  - scoped %s: state=%s reason=%s sub=%s terminal=%s",
                modId, cs and cs.state or "n/a", tostring(cs and cs.reason), tostring(cs and cs.subscriptionId),
                tostring(cs and cs.terminal)))
        end
    end
    return lines
end
