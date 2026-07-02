-- =========================================================
-- FS25_NetworkSync - network event classes
-- =========================================================
-- Author: TisonK
-- =========================================================
-- Two event classes carry the entire ecosystem's multiplayer sync,
-- replacing the per-mod event classes:
--
--   RealisticFarmingSyncEvent         server -> client(s): a batch of
--     { modId -> value array }, used for both the 1Hz dirty delta
--     (broadcast) and the full snapshot sent to a joining client.
--
--   RealisticFarmingSyncRequestEvent  client -> server: "send me a full
--     snapshot" on join. The server answers with a SyncEvent to just
--     that connection.
--
-- Per-element type tagging: each value in an array is written as a UInt8
-- tag followed by the typed value. Integers that fit int32 are sent as
-- int32 (exact, unlike the lossy all-float32 sketch in the brief), so
-- money / counts / ids round-trip exactly; fractional and out-of-range
-- numbers fall back to float32; booleans and strings have their own tags.
-- This is the same tagging the shipped SoilFertilizer setting events use.
-- =========================================================

RealisticFarmingSyncEvent = {}
local RealisticFarmingSyncEvent_mt = Class(RealisticFarmingSyncEvent, Event)
InitEventClass(RealisticFarmingSyncEvent, "RealisticFarmingSyncEvent")

-- Value type tags (UInt8).
RealisticFarmingSyncEvent.T_BOOL   = 0
RealisticFarmingSyncEvent.T_INT    = 1
RealisticFarmingSyncEvent.T_FLOAT  = 2
RealisticFarmingSyncEvent.T_STRING = 3

local INT32_MIN = -2147483648
local INT32_MAX = 2147483647

function RealisticFarmingSyncEvent.emptyNew()
    local self = Event.new(RealisticFarmingSyncEvent_mt)
    return self
end

---@param payload table  { modId(string) -> array(integer-indexed) }
function RealisticFarmingSyncEvent.new(payload)
    local self = RealisticFarmingSyncEvent.emptyNew()
    self.payload = payload or {}
    return self
end

-- Write a single tagged value.
local function writeValue(streamId, v)
    local t = type(v)
    if t == "boolean" then
        streamWriteUInt8(streamId, RealisticFarmingSyncEvent.T_BOOL)
        streamWriteBool(streamId, v)
    elseif t == "number" then
        if v == v and v ~= math.huge and v ~= -math.huge
            and math.floor(v) == v and v >= INT32_MIN and v <= INT32_MAX then
            streamWriteUInt8(streamId, RealisticFarmingSyncEvent.T_INT)
            streamWriteInt32(streamId, v)
        else
            -- fractional, out of int32 range, or non-finite -> float32
            streamWriteUInt8(streamId, RealisticFarmingSyncEvent.T_FLOAT)
            streamWriteFloat32(streamId, (v == v) and v or 0)
        end
    elseif t == "string" then
        streamWriteUInt8(streamId, RealisticFarmingSyncEvent.T_STRING)
        streamWriteString(streamId, v)
    else
        -- unsupported (nil/table/function): send a 0 float so index alignment holds
        NSLogger.warning("sync: unsupported array value of type %s, sending 0", t)
        streamWriteUInt8(streamId, RealisticFarmingSyncEvent.T_FLOAT)
        streamWriteFloat32(streamId, 0)
    end
end

local function readValue(streamId)
    local tag = streamReadUInt8(streamId)
    if tag == RealisticFarmingSyncEvent.T_BOOL then
        return streamReadBool(streamId)
    elseif tag == RealisticFarmingSyncEvent.T_INT then
        return streamReadInt32(streamId)
    elseif tag == RealisticFarmingSyncEvent.T_STRING then
        return streamReadString(streamId)
    else
        return streamReadFloat32(streamId)
    end
end

function RealisticFarmingSyncEvent:writeStream(streamId, connection)
    local modIds = {}
    for modId in pairs(self.payload) do
        modIds[#modIds + 1] = modId
    end
    streamWriteInt32(streamId, #modIds)
    for _, modId in ipairs(modIds) do
        local arr = self.payload[modId]
        streamWriteString(streamId, modId)
        local n = #arr
        streamWriteInt32(streamId, n)
        for i = 1, n do
            writeValue(streamId, arr[i])
        end
    end
end

function RealisticFarmingSyncEvent:readStream(streamId, connection)
    self.payload = {}
    local count = streamReadInt32(streamId)
    for _ = 1, count do
        local modId = streamReadString(streamId)
        local n = streamReadInt32(streamId)
        local arr = {}
        for i = 1, n do
            arr[i] = readValue(streamId)
        end
        self.payload[modId] = arr
    end
    self:run(connection)
end

function RealisticFarmingSyncEvent:run(connection)
    -- Only a pure client applies received state. On a listen server (host)
    -- getIsServer() is true and the host already holds authoritative state,
    -- so it never applies its own broadcast.
    if g_currentMission ~= nil and g_currentMission:getIsServer() then
        return
    end
    if g_networkSync ~= nil then
        g_networkSync:applyPayload(self.payload)
    end
end

-- =========================================================
-- Full-sync request (client -> server, on join)
-- =========================================================

RealisticFarmingSyncRequestEvent = {}
local RealisticFarmingSyncRequestEvent_mt = Class(RealisticFarmingSyncRequestEvent, Event)
InitEventClass(RealisticFarmingSyncRequestEvent, "RealisticFarmingSyncRequestEvent")

function RealisticFarmingSyncRequestEvent.emptyNew()
    return Event.new(RealisticFarmingSyncRequestEvent_mt)
end

function RealisticFarmingSyncRequestEvent.new()
    return RealisticFarmingSyncRequestEvent.emptyNew()
end

function RealisticFarmingSyncRequestEvent:writeStream(streamId, connection)
    -- no payload; the request itself is the signal
end

function RealisticFarmingSyncRequestEvent:readStream(streamId, connection)
    self:run(connection)
end

function RealisticFarmingSyncRequestEvent:run(connection)
    -- Server side only: answer the requesting connection with a full snapshot.
    if g_currentMission == nil or not g_currentMission:getIsServer() then
        return
    end
    if g_networkSync ~= nil then
        g_networkSync:sendFullSnapshotTo(connection)
    end
end
