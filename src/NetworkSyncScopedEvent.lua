-- =========================================================
-- FS25_NetworkSync - scoped delivery event (NS-7)
-- =========================================================
-- Author: TisonK
-- =========================================================
-- One event class carries every scoped (per-connection, private) message
-- between a client subscription and the server producer. It is distinct from
-- RealisticFarmingSyncEvent, RealisticFarmingSyncRequestEvent and
-- RealisticFarmingActionEvent and never shares their frames.
--
-- Fixed bootstrap envelope (BS1), always in this order, so a receiver can
-- consume a message whose body it does not understand without desynchronising
-- the stream:
--
--   UInt8   bootstrapVersion = 1
--   UInt8   messageKind      (SUBSCRIBE=1, PUBLICATION=2, CONTROL=3, UNSUBSCRIBE=4)
--   string  modId
--   string  subscriptionId
--   Int32   advertisedProtocolVersion
--   Int32   bodyTokenCount
--   bodyTokenCount x string opaque tokens
--
-- Version-specific meaning lives entirely in the tokens and is decoded only
-- after admission (NetworkSyncScoped.lua). Nothing here maps an unknown value
-- to a float or a zero: a token is a string or the whole event is refused.
--
-- Registered at file load through the native event-class mechanism
-- (InitEventClass, network/EventIds.lua:7). The engine assigns eventId when a
-- network session starts; a nil eventId means "no attempt" for every sender.
-- Sent on the native main reliable channel (NetworkNode.CHANNEL_MAIN,
-- network/NetworkNode.lua:16), the same channel the engine uses for its own
-- reliable ordered events.
-- =========================================================

NetworkSyncScopedEvent = NetworkSyncScopedEvent or {}
local NetworkSyncScopedEvent_mt = Class(NetworkSyncScopedEvent, Event)
InitEventClass(NetworkSyncScopedEvent, "NetworkSyncScopedEvent")

NetworkSyncScopedEvent.BOOTSTRAP_VERSION = 1

NetworkSyncScopedEvent.KIND_SUBSCRIBE   = 1
NetworkSyncScopedEvent.KIND_PUBLICATION = 2
NetworkSyncScopedEvent.KIND_CONTROL     = 3
NetworkSyncScopedEvent.KIND_UNSUBSCRIBE = 4

-- Envelope limits. Anything outside them refuses the whole event before it is
-- written; on read a violation is reported to the receiver as MALFORMED.
NetworkSyncScopedEvent.MAX_ID_BYTES     = 128
NetworkSyncScopedEvent.MAX_BODY_TOKENS  = 2048

local function validIdentity(s)
    return type(s) == "string" and s ~= "" and #s <= NetworkSyncScopedEvent.MAX_ID_BYTES
end

--- Validate the envelope fields before any write. Returns ok, reason.
function NetworkSyncScopedEvent.validate(kind, modId, subscriptionId, protocolVersion, tokens)
    if kind ~= NetworkSyncScopedEvent.KIND_SUBSCRIBE and kind ~= NetworkSyncScopedEvent.KIND_PUBLICATION
        and kind ~= NetworkSyncScopedEvent.KIND_CONTROL and kind ~= NetworkSyncScopedEvent.KIND_UNSUBSCRIBE then
        return false, "INVALID_KIND"
    end
    if not validIdentity(modId) then return false, "INVALID_MOD_ID" end
    if not validIdentity(subscriptionId) then return false, "INVALID_SUBSCRIPTION_ID" end
    if type(protocolVersion) ~= "number" or protocolVersion ~= math.floor(protocolVersion)
        or protocolVersion < 0 or protocolVersion > 2147483647 then
        return false, "INVALID_PROTOCOL_VERSION"
    end
    if type(tokens) ~= "table" then return false, "INVALID_BODY" end
    local n = #tokens
    if n > NetworkSyncScopedEvent.MAX_BODY_TOKENS then return false, "BODY_TOO_LARGE" end
    for i = 1, n do
        if type(tokens[i]) ~= "string" then return false, "INVALID_TOKEN" end
    end
    return true, nil
end

function NetworkSyncScopedEvent.emptyNew()
    local channel = (NetworkNode ~= nil and NetworkNode.CHANNEL_MAIN) or nil
    return Event.new(NetworkSyncScopedEvent_mt, channel)
end

---@param kind number            one of the KIND_* values
---@param modId string
---@param subscriptionId string
---@param protocolVersion number advertised (SUBSCRIBE), negotiated (PUBLICATION) or 0 (CONTROL refusal)
---@param tokens table           array of opaque string tokens
function NetworkSyncScopedEvent.new(kind, modId, subscriptionId, protocolVersion, tokens)
    local self = NetworkSyncScopedEvent.emptyNew()
    self.bootstrapVersion = NetworkSyncScopedEvent.BOOTSTRAP_VERSION
    self.kind = kind
    self.modId = modId
    self.subscriptionId = subscriptionId
    self.protocolVersion = protocolVersion
    self.tokens = tokens or {}
    self.malformed = nil
    return self
end

function NetworkSyncScopedEvent:writeStream(streamId, connection)
    streamWriteUInt8(streamId, NetworkSyncScopedEvent.BOOTSTRAP_VERSION)
    streamWriteUInt8(streamId, self.kind)
    streamWriteString(streamId, self.modId)
    streamWriteString(streamId, self.subscriptionId)
    streamWriteInt32(streamId, self.protocolVersion)
    local n = #self.tokens
    streamWriteInt32(streamId, n)
    for i = 1, n do
        streamWriteString(streamId, self.tokens[i])
    end
end

function NetworkSyncScopedEvent:readStream(streamId, connection)
    self.bootstrapVersion = streamReadUInt8(streamId)
    if self.bootstrapVersion ~= NetworkSyncScopedEvent.BOOTSTRAP_VERSION then
        -- An unknown bootstrap has an unknown layout; nothing after this byte
        -- can be trusted. The receiver records local incompatibility and the
        -- engine's per-event framing discards the rest of this event.
        self.malformed = "UNKNOWN_BOOTSTRAP"
        self:run(connection)
        return
    end
    self.kind = streamReadUInt8(streamId)
    self.modId = streamReadString(streamId)
    self.subscriptionId = streamReadString(streamId)
    self.protocolVersion = streamReadInt32(streamId)
    local n = streamReadInt32(streamId)
    self.tokens = {}
    if n < 0 or n > NetworkSyncScopedEvent.MAX_BODY_TOKENS then
        self.malformed = "BODY_TOO_LARGE"
        self:run(connection)
        return
    end
    -- The complete counted body is consumed before any admission decision.
    for i = 1, n do
        self.tokens[i] = streamReadString(streamId)
    end
    self:run(connection)
end

function NetworkSyncScopedEvent:run(connection)
    if g_networkSync ~= nil and g_networkSync._receiveScopedEvent ~= nil then
        g_networkSync:_receiveScopedEvent(self, connection)
    end
end
