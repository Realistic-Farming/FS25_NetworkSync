-- NS-7: scoped (per-connection, private) delivery contract.
--!load: src/Logger.lua, src/RealisticFarmingSyncEvent.lua, src/NetworkSyncScopedEvent.lua, src/NetworkSync.lua, src/NetworkSyncScoped.lua
-- Part 1 is the delivered reference bar (Office Tyson/StockGuard-First-Family-
-- 2026-09-15/reference-tests/NS-7-scoped_delivery_spec_test.lua), a MODELED
-- contract kept as shipped except for its trailing summary call, which this
-- runner appends itself. Part 2 drives the built transport through the mock
-- stream: SUBSCRIBE -> PUBLICATION/CONTROL -> applyView, chunk identity,
-- typed results, negotiation, recovery, cleanup and the public/scoped fence.
-- Nothing here proves native event ids, packet sizes, MP timing or gameplay.

-- =====================================================================
-- PART 1: the delivered reference bar (modeled)
-- =====================================================================
local identityFields = { "modId", "serverSession", "subscriptionId", "viewEpoch", "publicationId", "mode", "baseRevision", "dataRevision", "chunkCount" }
local function sameIdentity(a, b)
    for _, key in ipairs(identityFields) do if a[key] ~= b[key] then return false end end
    return true
end
local function assemble(parts)
    local first = parts[1]
    if first == nil or first.chunkCount < 1 then return nil, "INVALID_COUNT" end
    local byIndex = {}
    for _, part in ipairs(parts) do
        if not sameIdentity(first, part) then return nil, "IDENTITY_MISMATCH" end
        if part.chunkIndex < 1 or part.chunkIndex > first.chunkCount or part.chunkIndex % 1 ~= 0 or byIndex[part.chunkIndex] then return nil, "INVALID_INDEX" end
        byIndex[part.chunkIndex] = part
    end
    if #parts ~= first.chunkCount then return nil, "INCOMPLETE" end
    local rows = {}
    for i = 1, first.chunkCount do
        if byIndex[i] == nil then return nil, "INCOMPLETE" end
        for _, row in ipairs(byIndex[i].values or {}) do rows[#rows + 1] = row end
    end
    return { rows = rows }
end

local function naiveAssemble(parts)
    -- Bad control: it groups by module and count only.
    if #parts == parts[1].chunkCount then return { rows = { "mixed-row" } } end
    return nil
end

local function applySchema(frame, supportedVersion)
    if frame.applicationVersion ~= supportedVersion then
        return { outcome = "TERMINAL", reason = "UNSUPPORTED_APPLICATION_VERSION", retry = false, ready = false }
    end
    return { outcome = "APPLIED", dataRevision = frame.dataRevision, retry = false, ready = true }
end

local function applyError()
    return { outcome = "RETRYABLE", reason = "APPLY_ERROR", retry = true, ready = false }
end

local function applyDelta(current, delta)
    if current == nil or delta.baseRevision ~= current then
        return { outcome = "RETRYABLE", reason = "BASE_MISMATCH", requestFull = true, rows = nil }
    end
    return { outcome = "APPLIED", rows = delta.rows, dataRevision = delta.dataRevision }
end

local function receiveView(old, incoming)
    if old == nil or old.viewKey ~= incoming.viewKey or old.viewEpoch ~= incoming.viewEpoch then
        old = { rows = {}, viewKey = incoming.viewKey, viewEpoch = incoming.viewEpoch }
    end
    old.rows = incoming.rows
    old.dataRevision = incoming.dataRevision
    return old
end

local function routeResult(route, frame, baseline)
    local result = applySchema(frame, 3)
    if result.outcome == "TERMINAL" then
        result.route = route
        return result
    end
    if frame.mode == "DELTA" then
        local delta = applyDelta(baseline, frame)
        delta.route = route
        return delta
    end
    result.route = route
    return result
end

local p1 = { modId = "stockguard", serverSession = "S1", subscriptionId = "Q1", viewEpoch = "E1", publicationId = "P1", mode = "FULL", dataRevision = "R1", chunkIndex = 1, chunkCount = 2, values = { "known-A" } }
local p2 = { modId = "stockguard", serverSession = "S1", subscriptionId = "Q1", viewEpoch = "E1", publicationId = "P2", mode = "FULL", dataRevision = "R2", chunkIndex = 2, chunkCount = 2, values = { "wrong-B" } }
local mixed = naiveAssemble({ p1, p2 })
T.ok("bad control witness: same-count publications must not be mixed", mixed ~= nil)
local rejected, rejectReason = assemble({ p1, p2 })
T.eq("cross-publication chunks are rejected", rejected, nil)
T.eq("cross-publication rejection identifies the mismatch", rejectReason, "IDENTITY_MISMATCH")
local complete, reason = assemble({ p1, { modId = "stockguard", serverSession = "S1", subscriptionId = "Q1", viewEpoch = "E1", publicationId = "P1", mode = "FULL", dataRevision = "R1", chunkIndex = 2, chunkCount = 2, values = { "unknown-B" } } })
T.eq("exact publication identity accepts matching chunks", reason, nil)
T.ok("matching publication produces a complete assembly", complete ~= nil)
T.eq("first assembled payload is actual first part", complete.rows[1], "known-A")
T.eq("second assembled payload is actual second part", complete.rows[2], "unknown-B")

local unsupported = applySchema({ applicationVersion = 4, dataRevision = "R4" }, 3)
T.eq("incompatible application is terminal", unsupported.outcome, "TERMINAL")
T.eq("terminal incompatibility has explicit reason", unsupported.reason, "UNSUPPORTED_APPLICATION_VERSION")
T.ok("terminal incompatibility does not retry or become ready empty", not unsupported.retry and not unsupported.ready)
local retryable = applyError()
T.eq("application error is retryable", retryable.outcome, "RETRYABLE")
T.ok("retryable application error requests recovery", retryable.retry)

local wrongBase = applyDelta("R5", { baseRevision = "R4", dataRevision = "R6", rows = { "new" } })
T.eq("delta with wrong base is rejected", wrongBase.reason, "BASE_MISMATCH")
T.ok("wrong base requests a full recovery", wrongBase.requestFull and wrongBase.rows == nil)

local oldView = { viewKey = "farm-1|props-A", viewEpoch = "E1", rows = { "private-old" }, dataRevision = "R1" }
local newView = receiveView(oldView, { viewKey = "farm-2|props-B", viewEpoch = "E2", rows = { "private-new" }, dataRevision = "R7" })
T.eq("authorized view change clears old rows before replacement", newView.rows[1], "private-new")
T.ok("old authorized rows are absent after view reset", newView.rows[1] ~= "private-old")
T.eq("authorized view change advances epoch", newView.viewEpoch, "E2")

local registry = {}
local function register(id, kind)
    if registry[id] ~= nil then return false end
    registry[id] = kind
    return true
end
T.ok("public module registers", register("public-soil", "public"))
T.ok("private module registers", register("stock", "scoped"))
T.ok("private ID cannot be registered public", not register("stock", "public"))
T.eq("refused public registration preserves scoped route", registry.stock, "scoped")
T.ok("public ID cannot be registered scoped", not register("public-soil", "scoped"))

local terminalNS = routeResult("NS-7", { applicationVersion = 9, dataRevision = "R9" }, nil)
local terminalFallback = routeResult("SG-fallback", { applicationVersion = 9, dataRevision = "R9" }, nil)
T.eq("selected service and fallback share terminal outcome", terminalNS.outcome, terminalFallback.outcome)
T.eq("selected service and fallback share terminal reason", terminalNS.reason, terminalFallback.reason)
local deltaNS = routeResult("NS-7", { applicationVersion = 3, mode = "DELTA", baseRevision = "old", dataRevision = "new", rows = { "x" } }, "current")
local deltaFallback = routeResult("SG-fallback", { applicationVersion = 3, mode = "DELTA", baseRevision = "old", dataRevision = "new", rows = { "x" } }, "current")
T.eq("selected service and fallback share exact-base rejection", deltaNS.reason, deltaFallback.reason)
T.eq("selected service and fallback both request full", deltaNS.requestFull, deltaFallback.requestFull)

local function copy(frame)
    local c = {}; for k,v in pairs(frame) do c[k] = v end; return c
end
for _, field in ipairs({ "serverSession", "subscriptionId", "viewEpoch", "publicationId", "dataRevision" }) do
    local other = copy(p1); other.chunkIndex = 2; other[field] = "different"
    local result = assemble({ p1, other })
    T.eq("different " .. field .. " cannot share assembly", result, nil)
end
local missing, missingReason = assemble({ p1 })
T.eq("incomplete payload does not publish rows", missing, nil)
T.eq("incomplete payload is explicit", missingReason, "INCOMPLETE")
local badIndex = copy(p1); badIndex.chunkIndex = 3
local invalid, invalidReason = assemble({ p1, badIndex })
T.eq("out-of-range chunk cannot count as completion", invalid, nil)
T.eq("bad index is refused", invalidReason, "INVALID_INDEX")
local function dispatch(result, revision)
    if type(result) ~= "table" then return "UNAVAILABLE", true end
    if result.outcome == "TERMINAL" then return "UNAVAILABLE", false end
    if result.outcome == "APPLIED" and result.dataRevision == revision then return "READY", false end
    return "UNAVAILABLE", true
end
local state, retry = dispatch(unsupported, "R4")
T.eq("typed terminal result remains unavailable", state, "UNAVAILABLE")
T.ok("typed terminal result suppresses full retry", not retry)
state, retry = dispatch({ outcome = "APPLIED", dataRevision = "wrong" }, "expected")
T.eq("false APPLIED revision is not ready", state, "UNAVAILABLE")
T.ok("false APPLIED revision is recoverable", retry)
state, retry = dispatch({ outcome = "APPLIED", dataRevision = "expected" }, "expected")
T.eq("matching committed application becomes ready", state, "READY")
T.ok("applied result does not need full recovery", not retry)

-- =====================================================================
-- PART 2: the built transport through the mock stream
-- =====================================================================

-- Engine surface the scoped path reads (bound in D:\FS25_Decoded).
NetworkNode = NetworkNode or { LOCAL_STREAM_ID = 0, CHANNEL_MAIN = 1, CHANNEL_SECONDARY = 2 }
FarmManager = FarmManager or { SPECTATOR_FARM_ID = 0, SINGLEPLAYER_FARM_ID = 1, MAX_FARM_ID = 8, GUIDED_TOUR_FARM_ID = 14, INVALID_FARM_ID = 15 }
MessageType = MessageType or { PLAYER_FARM_CHANGED = 25 }
NetworkSyncScopedEvent.eventId = 77   -- the engine assigns this when a session starts

local S = NetworkSyncScoped
local E = NetworkSyncScopedEvent

-- A remote client connection as the server sees it. Sent events are captured.
local function newConnection(streamId)
    return { streamId = streamId, isConnected = true, isReadyForEvents = true, isServer = false, sent = {},
        sendEvent = function(self, e) self.sent[#self.sent + 1] = e end,
        getIsLocal = function(self) return self.streamId == 0 end,
        getIsClient = function() return true end }
end

local FARMS, USERS = {}, {}
local function setActor(conn, farmId, user) FARMS[conn] = farmId; USERS[conn] = user end
g_currentMission = { _isServer = true,
    getIsServer = function(self) return self._isServer end,
    getFarmId = function(_, conn) return FARMS[conn] end,
    userManager = { getUserByConnection = function(_, conn) return USERS[conn] end } }
g_messageCenter = { subscribe = function() end, unsubscribeAll = function() end }

-- Copy an event over the mock stream and run it as the receiver.
local function deliver(event, receiverNS, asServer, connection)
    local s = NewStream()
    event:writeStream(s, nil)
    g_networkSync = receiverNS
    g_currentMission._isServer = asServer
    local rx = E.emptyNew()
    rx:readStream(s, connection)
end
local function drain(list) local out = {}; for i = 1, #list do out[i] = list[i] end; for i = #list, 1, -1 do list[i] = nil end; return out end
local function kindsOf(events) local t = {}; for _, e in ipairs(events) do t[#t + 1] = e.kind end; return table.concat(t, ",") end

-- Build a server + client pair around one scoped module.
local function newPair(modId, producer, consumer)
    local server = NetworkSync.new()
    g_currentMission._isServer = true
    server:onMissionLoaded()
    server:registerScopedModule(modId, { buildView = producer, applyView = function() return { outcome = "APPLIED" } end, clearView = function() end })
    local client = NetworkSync.new()
    g_currentMission._isServer = false
    client:onMissionLoaded()
    client:registerScopedModule(modId, { buildView = function() return nil end, applyView = consumer.applyView, clearView = consumer.clearView })
    return server, client
end

-- Client-side server connection the client sends SUBSCRIBE through. Only
-- scoped events are captured; the public join handshake's request event
-- (no .kind) also travels here and is not under test.
local toServer = { streamId = 1, isConnected = true, isReadyForEvents = true, sent = {},
    sendEvent = function(self, e) if e.kind ~= nil then self.sent[#self.sent + 1] = e end end }
g_client = { getServerConnection = function() return toServer end }

-- Drive the client's burst once: one interval elapses, one SUBSCRIBE goes out.
local function clientTick(client, dt)
    g_currentMission._isServer = false
    g_networkSync = client
    client:update(dt or NetworkSync.JOIN_REQUEST_INTERVAL)
end
-- Server cadence: one TICK.
local function serverTick(server)
    g_currentMission._isServer = true
    g_networkSync = server
    g_server = g_server or { broadcastEvent = function(self, e) self.sentEvents = self.sentEvents or {}; table.insert(self.sentEvents, e) end, sentEvents = {} }
    server:update(NetworkSync.TICK_MS)
end
-- Move every captured event from a to b's receiver.
local function relayToServer(server, conn)
    for _, e in ipairs(drain(toServer.sent)) do deliver(e, server, true, conn) end
end
local function relayToClient(client, conn)
    for _, e in ipairs(drain(conn.sent)) do deliver(e, client, false, toServer) end
end

-- (A) Counters and codec.
do
    T.eq("A1 increment 9 -> 10", S.incrementDecimal("9"), "10")
    T.eq("A2 increment 199 -> 200", S.incrementDecimal("199"), "200")
    T.eq("A3 increment from 0", S.incrementDecimal("0"), "1")
    T.eq("A4 compare by length first", S.compareDecimal("9", "10"), -1)
    T.eq("A5 compare equal", S.compareDecimal("42", "42"), 0)
    T.ok("A6 canonical rejects leading zero", not S.isCanonicalDecimal("007"))
    T.ok("A7 canonical rejects empty", not S.isCanonicalDecimal(""))
    local tag, tok = S.encodeValue(0.1)
    T.eq("A8 number tag", tag, "NUMBER")
    T.eq("A9 number round-trips 0.1 exactly", S.decodeValue(tag, tok), 0.1)
    T.eq("A10 number round-trips 1e20", S.decodeValue(S.encodeValue(1e20)), 1e20)
    T.eq("A11 bool true", S.decodeValue(S.encodeValue(true)), true)
    T.eq("A12 bool false", S.decodeValue(S.encodeValue(false)), false)
    T.eq("A13 string exact bytes", S.decodeValue(S.encodeValue("a b|c")), "a b|c")
    T.eq("A14 NaN is refused, not zeroed", S.encodeValue(0/0), nil)
    T.eq("A15 table is refused", S.encodeValue({}), nil)
    local _, okB = S.decodeValue("BOOL", "2")
    T.eq("A16 bool token other than 0|1 is refused", okB, false)
    local _, okN = S.decodeValue("NUMBER", "inf")
    T.eq("A17 non-finite number token is refused", okN, false)
    local _, okT = S.decodeValue("FLOAT", "1")
    T.eq("A18 unknown tag is refused", okT, false)
end

-- (B) Envelope validation and the unknown bootstrap.
do
    T.ok("B1 valid envelope", (E.validate(E.KIND_SUBSCRIBE, "sg", "1", 1, { "1", "1" })))
    local ok, why = E.validate(E.KIND_SUBSCRIBE, string.rep("x", 129), "1", 1, {})
    T.eq("B2 oversize modId refused", why, "INVALID_MOD_ID")
    ok, why = E.validate(E.KIND_SUBSCRIBE, "sg", "", 1, {})
    T.eq("B3 empty subscription id refused", why, "INVALID_SUBSCRIPTION_ID")
    local big = {}; for i = 1, 2049 do big[i] = "t" end
    ok, why = E.validate(E.KIND_PUBLICATION, "sg", "1", 1, big)
    T.eq("B4 more than 2048 tokens refused", why, "BODY_TOO_LARGE")
    ok, why = E.validate(E.KIND_PUBLICATION, "sg", "1", 1, { "a", 5 })
    T.eq("B5 non-string token refused", why, "INVALID_TOKEN")
    ok, why = E.validate(9, "sg", "1", 1, {})
    T.eq("B6 unknown kind refused", why, "INVALID_KIND")

    -- Wire order round trip.
    local ev = E.new(E.KIND_PUBLICATION, "sg", "12", 1, { "a", "b" })
    local s = NewStream(); ev:writeStream(s, nil)
    T.eq("B7 first byte is bootstrap 1", s.cells[1], 1)
    T.eq("B8 second byte is kind", s.cells[2], E.KIND_PUBLICATION)
    T.eq("B9 modId, subscriptionId, protocol, count, tokens in order", table.concat({ s.cells[3], s.cells[4], tostring(s.cells[5]), tostring(s.cells[6]), s.cells[7], s.cells[8] }, "|"), "sg|12|1|2|a|b")

    -- Unknown bootstrap: consumed as malformed, never decoded as a frame.
    local applied = false
    local client = NetworkSync.new(); g_currentMission._isServer = false; client:onMissionLoaded()
    client:registerScopedModule("sg", { buildView = function() end, applyView = function() applied = true return { outcome = "APPLIED" } end, clearView = function() end })
    local bad = NewStream(); streamWriteUInt8(bad, 2); streamWriteUInt8(bad, 2)
    g_networkSync = client
    local rx = E.emptyNew(); rx:readStream(bad, nil)
    T.eq("B10 unknown bootstrap is flagged malformed", rx.malformed, "UNKNOWN_BOOTSTRAP")
    T.eq("B11 unknown bootstrap never reaches applyView", applied, false)
end

-- (C) Registration fence and capabilities.
do
    local ns = NetworkSync.new()
    T.eq("C1 capability before mission: not ready", ns:getScopedCapabilities().ready, false)
    T.eq("C2 capability reason", ns:getScopedCapabilities().reasonCode, "NOT_INITIALIZED")
    g_currentMission._isServer = true
    ns:onMissionLoaded()
    local caps = ns:getScopedCapabilities()
    T.eq("C3 capability after mission: ready", caps.ready, true)
    T.eq("C4 bootstrap 1", caps.bootstrapVersion, 1)
    T.eq("C5 protocol 1 advertised", caps.protocolVersions[1], 1)
    T.ok("C6 serverSession allocated on the server", S.isCanonicalDecimal(ns.scopedServerSession))
    T.ok("C7 public registers", ns:registerModule("pub", { onWriteState = function() return {} end, onReadState = function() end }))
    T.ok("C8 scoped registers", ns:registerScopedModule("stock", { buildView = function() end, applyView = function() end, clearView = function() end }))
    T.ok("C9 scoped id refused as public", not ns:registerModule("stock", { onWriteState = function() return {} end, onReadState = function() end }))
    T.ok("C10 public id refused as scoped", not ns:registerScopedModule("pub", { buildView = function() end, applyView = function() end, clearView = function() end }))
    T.eq("C11 refused public registration preserved the scoped route", ns:isScopedModule("stock"), true)
    T.eq("C12 scoped id never enters public registerOrder", #ns.registerOrder, 1)
    T.ok("C13 invalid scoped spec refused", not ns:registerScopedModule("x", { buildView = function() end }))
    ns:markDirty("stock")
    T.eq("C14 scoped markDirty does not touch the public dirty list", ns.dirtyMods["stock"], nil)
    T.eq("C15 scoped markDirty flags the scoped list", ns.scopedDirty["stock"], true)
    local other = NetworkSync.new(); g_currentMission._isServer = true; other:onMissionLoaded()
    T.ok("C16 the next mission gets a fresh serverSession", S.compareDecimal(other.scopedServerSession, ns.scopedServerSession) > 0)
end

-- (D) Full round trip: SUBSCRIBE burst, publication, atomic apply, READY.
do
    local produced = 0
    local lastContext = nil
    local applied, cleared = {}, {}
    local server, client = newPair("stock",
        function(context, previous, forceFull)
            produced = produced + 1
            lastContext = context
            return { state = "READY", viewKey = "farm-" .. tostring(context.farmId), dataRevision = "R1", mode = "FULL",
                values = { true, 1.5, "hay", 3, false } }
        end,
        { applyView = function(pub) applied[#applied + 1] = pub; return { outcome = "APPLIED", dataRevision = pub.dataRevision } end,
          clearView = function(reason) cleared[#cleared + 1] = reason end })
    local conn = newConnection(5)
    setActor(conn, 2, MakeUser(9, false))

    -- Burst: first interval sends one SUBSCRIBE; same id on every attempt.
    clientTick(client)
    T.eq("D1 one SUBSCRIBE sent after one interval", kindsOf(toServer.sent), tostring(E.KIND_SUBSCRIBE))
    local subId = toServer.sent[1].subscriptionId
    T.ok("D2 subscription id is canonical", S.isCanonicalDecimal(subId))
    T.eq("D3 SUBSCRIBE body advertises protocol 1", table.concat(toServer.sent[1].tokens, ","), "1,1")
    relayToServer(server, conn)
    T.eq("D4 server answered the subscription with a PUBLICATION", kindsOf(conn.sent), tostring(E.KIND_PUBLICATION))
    T.eq("D5 producer saw the resolved farm", lastContext.farmId, 2)
    T.eq("D6 producer saw RESOLVED", lastContext.actorState, "RESOLVED")
    T.eq("D7 producer saw the user id", lastContext.userId, 9)
    T.eq("D8 producer saw the subscription id", lastContext.subscriptionId, subId)
    T.ok("D9 producer saw a mission-local connection id", type(lastContext.connectionId) == "string")
    T.eq("D10 first publication is FULL", conn.sent[1].tokens[5], "FULL")
    T.eq("D11 first publication has empty baseRevision", conn.sent[1].tokens[6], "")
    T.eq("D12 chunkIndex 0 of 1", conn.sent[1].tokens[8] .. "/" .. conn.sent[1].tokens[9], "0/1")
    T.eq("D13 valueCount 5, tokens 10+2*5", #conn.sent[1].tokens, 20)
    relayToClient(client, conn)
    T.eq("D14 applyView called once", #applied, 1)
    T.eq("D15 values arrive typed and in order", table.concat({ tostring(applied[1].values[1]), tostring(applied[1].values[2]), applied[1].values[3], tostring(applied[1].values[4]), tostring(applied[1].values[5]) }, ","), "true,1.5,hay,3,false")
    T.eq("D16 publication carries mode", applied[1].mode, "FULL")
    T.eq("D17 publication carries dataRevision", applied[1].dataRevision, "R1")
    local cs = client.scopedClient["stock"]
    T.eq("D18 client is READY", cs.state, "READY")
    T.eq("D19 usable revision", cs.usable.dataRevision, "R1")
    T.eq("D20 burst acknowledged", cs.acknowledged, true)
    T.eq("D21 no further SUBSCRIBE after acknowledgement", (function() clientTick(client); return #toServer.sent end)(), 0)

    -- Cadence: unchanged view sends nothing; a changed revision sends FULL again.
    produced = 0
    serverTick(server)
    T.eq("D22 cadence re-evaluated access (producer called)", produced, 1)
    T.eq("D23 same revision FULL is still sent (producer chose FULL, not UNCHANGED)", #conn.sent, 1)
    drain(conn.sent)
    T.eq("D24 no public event carried the scoped module", #(g_server.sentEvents or {}), 0)
end

-- (E) UNCHANGED, DELTA against the exact base, and the epoch reset on a viewKey change.
do
    local rev, key, mode, base = "R1", "farm-2", "FULL", nil
    local applied, cleared = {}, {}
    local server, client = newPair("stock",
        function(context, previous, forceFull)
            if forceFull then return { state = "READY", viewKey = key, dataRevision = rev, mode = "FULL", values = { "full", rev } } end
            if mode == "UNCHANGED" then return { state = "READY", viewKey = key, dataRevision = rev, mode = "UNCHANGED" } end
            if mode == "DELTA" then return { state = "READY", viewKey = key, dataRevision = rev, mode = "DELTA", baseRevision = base, values = { "delta", rev } } end
            return { state = "READY", viewKey = key, dataRevision = rev, mode = "FULL", values = { "full", rev } }
        end,
        { applyView = function(pub) applied[#applied + 1] = pub; return { outcome = "APPLIED", dataRevision = pub.dataRevision } end,
          clearView = function(reason) cleared[#cleared + 1] = reason end })
    local conn = newConnection(6)
    setActor(conn, 2, MakeUser(10, false))
    clientTick(client); relayToServer(server, conn); relayToClient(client, conn)
    T.eq("E1 initial FULL applied", applied[1].values[1], "full")
    local epoch1 = client.scopedClient["stock"].viewEpoch

    mode = "UNCHANGED"
    serverTick(server)
    T.eq("E2 UNCHANGED with the same revision sends nothing", #conn.sent, 0)

    mode = "DELTA"; base = "R1"; rev = "R2"
    serverTick(server); relayToClient(client, conn)
    T.eq("E3 DELTA against the emitted revision is sent as DELTA", applied[2].mode, "DELTA")
    T.eq("E4 DELTA carries its base", applied[2].baseRevision, "R1")
    T.eq("E5 usable moved to R2", client.scopedClient["stock"].usable.dataRevision, "R2")
    T.eq("E6 same epoch", client.scopedClient["stock"].viewEpoch, epoch1)

    -- A DELTA whose base is not the emitted revision is rebuilt as FULL by the server.
    base = "R0"; rev = "R3"
    serverTick(server); relayToClient(client, conn)
    T.eq("E7 wrong base: server rebuilt FULL", applied[3].mode, "FULL")
    T.eq("E8 wrong base: FULL revision R3", applied[3].dataRevision, "R3")

    -- viewKey change: new epoch, FULL, client cleared before apply.
    mode = "DELTA"; base = "R3"; rev = "R4"; key = "farm-2|contract"
    local clearedBefore = #cleared
    serverTick(server); relayToClient(client, conn)
    T.eq("E9 changed viewKey forces FULL", applied[4].mode, "FULL")
    T.ok("E10 changed viewKey advances the epoch", S.compareDecimal(client.scopedClient["stock"].viewEpoch, epoch1) > 0)
    T.ok("E11 client cleared the old rows before the new epoch applied", #cleared > clearedBefore)
    T.eq("E12 usable revision R4", client.scopedClient["stock"].usable.dataRevision, "R4")

    -- UNCHANGED with a changed revision is not unchanged: FULL.
    mode = "UNCHANGED"; rev = "R5"
    serverTick(server); relayToClient(client, conn)
    T.eq("E13 UNCHANGED at a new revision is rebuilt FULL", applied[5].mode, "FULL")
    T.eq("E14 and carries R5", applied[5].dataRevision, "R5")
end

-- (F) Chunking: many values, identity on every chunk, atomic apply; a foreign
-- chunk mixed in clears the assembly and recovers.
do
    local values = {}
    for i = 1, 3000 do values[i] = "row-" .. tostring(i) .. "-" .. string.rep("x", 20) end
    local applied = {}
    local server, client = newPair("stock",
        function() return { state = "READY", viewKey = "k", dataRevision = "R1", mode = "FULL", values = values } end,
        { applyView = function(pub) applied[#applied + 1] = pub; return { outcome = "APPLIED", dataRevision = pub.dataRevision } end, clearView = function() end })
    local conn = newConnection(7)
    setActor(conn, 3, MakeUser(11, false))
    clientTick(client); relayToServer(server, conn)
    local n = #conn.sent
    T.ok("F1 split into more than one chunk", n > 1)
    T.ok("F2 at most 1024 chunks", n <= 1024)
    local ids = {}
    for _, e in ipairs(conn.sent) do ids[e.tokens[3]] = true end
    T.eq("F3 every chunk carries the same publicationId", (function() local c = 0; for _ in pairs(ids) do c = c + 1 end; return c end)(), 1)
    T.eq("F4 chunkCount on every chunk equals the count", conn.sent[1].tokens[9], tostring(n))
    T.eq("F5 last chunk index is count-1", conn.sent[n].tokens[8], tostring(n - 1))
    -- Deliver out of order: still one atomic apply.
    local events = drain(conn.sent)
    for i = #events, 1, -1 do deliver(events[i], client, false, toServer) end
    T.eq("F6 one apply for the whole publication", #applied, 1)
    T.eq("F7 all values reassembled", #applied[1].values, 3000)
    T.eq("F8 order preserved", applied[1].values[3000], values[3000])

    -- A chunk from another publication cannot join the assembly.
    local server2, client2 = newPair("stock",
        function() return { state = "READY", viewKey = "k", dataRevision = "R1", mode = "FULL", values = values } end,
        { applyView = function(pub) applied[#applied + 1] = pub; return { outcome = "APPLIED", dataRevision = pub.dataRevision } end, clearView = function() end })
    local conn2 = newConnection(8)
    setActor(conn2, 3, MakeUser(12, false))
    clientTick(client2); relayToServer(server2, conn2)
    local ev = drain(conn2.sent)
    local foreign = E.new(E.KIND_PUBLICATION, "stock", ev[1].subscriptionId, 1, ev[2].tokens)
    foreign.tokens = { unpack(ev[2].tokens) }
    foreign.tokens[3] = S.incrementDecimal(ev[2].tokens[3])   -- a different publicationId, same count
    local before = #applied
    deliver(ev[1], client2, false, toServer)
    deliver(foreign, client2, false, toServer)
    for i = 2, #ev do deliver(ev[i], client2, false, toServer) end
    T.eq("F9 a newer publicationId replaced the staging, so the old chunks never completed", #applied, before)
    T.ok("F10 the client is not READY on a mixed assembly", client2.scopedClient["stock"].usable == nil)
end

-- (G) Non-READY control: sent once, not per tick; client cleared and acknowledged.
do
    local state = "WAITING"
    local cleared = {}
    local server, client = newPair("stock",
        function() if state == "READY" then return { state = "READY", viewKey = "k", dataRevision = "R1", mode = "FULL", values = {} } end return { state = state, reason = "no-farm-yet" } end,
        { applyView = function(pub) return { outcome = "APPLIED", dataRevision = pub.dataRevision } end, clearView = function(r) cleared[#cleared + 1] = r end })
    local conn = newConnection(9)
    setActor(conn, nil, MakeUser(13, false))
    clientTick(client); relayToServer(server, conn)
    T.eq("G1 non-READY answered with CONTROL", kindsOf(conn.sent), tostring(E.KIND_CONTROL))
    T.eq("G2 CONTROL state WAITING", conn.sent[1].tokens[4], "WAITING")
    T.eq("G3 CONTROL reason", conn.sent[1].tokens[5], "no-farm-yet")
    relayToClient(client, conn)
    local cs = client.scopedClient["stock"]
    T.eq("G4 client state WAITING", cs.state, "WAITING")
    T.eq("G5 client acknowledged (no more SUBSCRIBE)", cs.acknowledged, true)
    T.eq("G6 consumer told to clear", cleared[#cleared], "no-farm-yet")
    serverTick(server)
    T.eq("G7 unchanged non-READY is not resent per tick", #conn.sent, 0)
    state = "DENIED"
    serverTick(server)
    T.eq("G8 a changed state is sent", conn.sent[1] and conn.sent[1].tokens[4], "DENIED")
    drain(conn.sent)
    -- Transition to READY: FULL under a new epoch without a new client request.
    state = "READY"; setActor(conn, 4, MakeUser(13, false))
    serverTick(server); relayToClient(client, conn)
    T.eq("G9 READY after WAITING replaces it without a new request", cs.state, "READY")
    T.ok("G10 empty FULL is a valid empty view", cs.usable ~= nil and #cs.usable.dataRevision > 0)
end

-- (H) Actor classification handed to the producer.
do
    local seen = {}
    local server = NetworkSync.new(); g_currentMission._isServer = true; server:onMissionLoaded()
    server:registerScopedModule("stock", { buildView = function(ctx) seen[#seen + 1] = ctx.actorState; return { state = "WAITING", reason = "x" } end, applyView = function() end, clearView = function() end })
    local cases = { { farm = 0, want = "SPECTATOR" }, { farm = 15, want = "INVALID" }, { farm = 14, want = "INVALID" }, { farm = nil, want = "WAITING" }, { farm = 5, want = "RESOLVED" } }
    for i, c in ipairs(cases) do
        local conn = newConnection(20 + i)
        setActor(conn, c.farm, MakeUser(30 + i, false))
        local sub = E.new(E.KIND_SUBSCRIBE, "stock", tostring(100 + i), 1, { "1", "1" })
        deliver(sub, server, true, conn)
        T.eq("H" .. i .. " farm " .. tostring(c.farm) .. " classified " .. c.want, seen[#seen], c.want)
    end
    local noUser = newConnection(40)
    setActor(noUser, 5, nil)
    deliver(E.new(E.KIND_SUBSCRIBE, "stock", "200", 1, { "1", "1" }), server, true, noUser)
    T.eq("H6 no native user is WAITING even with a farm", seen[#seen], "WAITING")
end

-- (I) Negotiation: unsupported protocol is terminal, MODULE_NOT_REGISTERED coalesces a retry.
do
    local server = NetworkSync.new(); g_currentMission._isServer = true; server:onMissionLoaded()
    server:registerScopedModule("stock", { buildView = function() return { state = "READY", viewKey = "k", dataRevision = "R1", mode = "FULL", values = {} } end, applyView = function() end, clearView = function() end })
    local client = NetworkSync.new(); g_currentMission._isServer = false; client:onMissionLoaded()
    local cleared = {}
    client:registerScopedModule("stock", { buildView = function() end, applyView = function() return { outcome = "APPLIED" } end, clearView = function(r) cleared[#cleared + 1] = r end })
    client:registerScopedModule("other", { buildView = function() end, applyView = function() return { outcome = "APPLIED" } end, clearView = function() end })
    local conn = newConnection(50)
    setActor(conn, 2, MakeUser(51, false))
    clientTick(client)
    local subs = drain(toServer.sent)
    T.eq("I1 one SUBSCRIBE per scoped module", #subs, 2)
    -- Tamper the stock subscribe to advertise only version 9.
    for _, e in ipairs(subs) do
        if e.modId == "stock" then e.tokens = { "1", "9" } end
        deliver(e, server, true, conn)
    end
    local ctrls = drain(conn.sent)
    T.eq("I2 both answered with CONTROL", kindsOf(ctrls), tostring(E.KIND_CONTROL) .. "," .. tostring(E.KIND_CONTROL))
    for _, e in ipairs(ctrls) do deliver(e, client, false, toServer) end
    local stock, other = client.scopedClient["stock"], client.scopedClient["other"]
    T.eq("I3 unsupported protocol: terminal", stock.terminal, true)
    T.eq("I4 unsupported protocol: reason retained", stock.reason, "UNSUPPORTED_PROTOCOL")
    T.eq("I5 unsupported protocol: no server subscriber was created", server.scopedSubscriptions[conn] and server.scopedSubscriptions[conn]["stock"], nil)
    T.eq("I6 unregistered module: reason", other.reason, "MODULE_NOT_REGISTERED")
    T.eq("I7 unregistered module: not terminal", other.terminal, false)
    T.eq("I8 unregistered module: waits for the drift floor", other.waitTimer, 0)
    local withdrawn = drain(toServer.sent)
    T.eq("I8b the terminal tuple withdrew itself with UNSUBSCRIBE", withdrawn[1] and withdrawn[1].kind .. "/" .. withdrawn[1].modId, E.KIND_UNSUBSCRIBE .. "/stock")
    clientTick(client, NetworkSync.DRIFT_FLOOR_MS)   -- the wait elapses, a fresh generation starts
    clientTick(client)                               -- its first interval sends the SUBSCRIBE
    local again = drain(toServer.sent)
    T.eq("I9 after the drift floor only the non-terminal module re-subscribes", #again, 1)
    T.eq("I10 and it is the unregistered one", again[1].modId, "other")
    T.ok("I11 a fresh generation has a new subscription id", again[1].subscriptionId ~= subs[2].subscriptionId and again[1].subscriptionId ~= subs[1].subscriptionId)
    clientTick(client, NetworkSync.DRIFT_FLOOR_MS)
    T.eq("I12 the terminal tuple never bursts again", (function() for _, e in ipairs(drain(toServer.sent)) do if e.modId == "stock" then return "burst" end end return "quiet" end)(), "quiet")
end

-- (J) Typed application results: RETRYABLE recovers with a new generation,
-- TERMINAL suspends and withdraws, a false APPLIED revision is not READY.
do
    local answer = "APPLIED"
    local server, client = newPair("stock",
        function() return { state = "READY", viewKey = "k", dataRevision = "R1", mode = "FULL", values = { 1 } } end,
        { applyView = function(pub)
              if answer == "THROW" then error("boom") end
              if answer == "WRONGREV" then return { outcome = "APPLIED", dataRevision = "nope" } end
              if answer == "TERMINAL" then return { outcome = "TERMINAL", reason = "UNSUPPORTED_APPLICATION_VERSION" } end
              if answer == "RETRYABLE" then return { outcome = "RETRYABLE", reason = "APPLY_ERROR" } end
              return { outcome = "APPLIED", dataRevision = pub.dataRevision } end,
          clearView = function() end })
    local conn = newConnection(60)
    setActor(conn, 2, MakeUser(61, false))
    clientTick(client); relayToServer(server, conn)
    local first = drain(conn.sent)
    local cs = client.scopedClient["stock"]
    local gen1 = cs.subscriptionId

    answer = "THROW"
    for _, e in ipairs(first) do deliver(e, client, false, toServer) end
    T.eq("J1 a throwing applyView is not READY", cs.state ~= "READY", true)
    T.ok("J2 a throwing applyView starts a new generation", cs.subscriptionId ~= gen1)
    T.eq("J3 usable cleared", cs.usable, nil)

    answer = "WRONGREV"
    clientTick(client); relayToServer(server, conn); relayToClient(client, conn)
    T.ok("J4 false APPLIED revision is not READY", cs.usable == nil)

    answer = "RETRYABLE"
    local gen2 = cs.subscriptionId
    clientTick(client); relayToServer(server, conn); relayToClient(client, conn)
    T.ok("J5 RETRYABLE starts another generation", cs.subscriptionId ~= gen2)
    T.eq("J6 RETRYABLE is not terminal", cs.terminal, false)

    answer = "TERMINAL"
    clientTick(client); relayToServer(server, conn); relayToClient(client, conn)
    T.eq("J7 TERMINAL suspends the tuple", cs.terminal, true)
    T.eq("J8 TERMINAL retains the reason", cs.reason, "UNSUPPORTED_APPLICATION_VERSION")
    local out = drain(toServer.sent)
    T.eq("J9 TERMINAL withdrew the subscription with UNSUBSCRIBE", out[#out] and out[#out].kind, E.KIND_UNSUBSCRIBE)
    deliver(out[#out], server, true, conn)
    T.eq("J10 server removed only that subscription", server.scopedSubscriptions[conn]["stock"], nil)
    clientTick(client, NetworkSync.DRIFT_FLOOR_MS)
    T.eq("J11 no burst after TERMINAL", #toServer.sent, 0)
    answer = "APPLIED"
    T.eq("J12 requestScopedFull on a terminal tuple is refused", client:requestScopedFull("stock"), false)
end

-- (K) Producer failures become ERROR control, never a public fallback or a partial send.
do
    local mode = "THROW"
    local server, client = newPair("stock",
        function()
            if mode == "THROW" then error("producer down") end
            if mode == "MALFORMED" then return { state = "READY" } end
            if mode == "BADVALUE" then return { state = "READY", viewKey = "k", dataRevision = "R1", mode = "FULL", values = { {} } } end
            return { state = "READY", viewKey = "k", dataRevision = "R1", mode = "FULL", values = { 1 } }
        end,
        { applyView = function(pub) return { outcome = "APPLIED", dataRevision = pub.dataRevision } end, clearView = function() end })
    local conn = newConnection(70)
    setActor(conn, 2, MakeUser(71, false))
    clientTick(client); relayToServer(server, conn)
    T.eq("K1 throwing producer: CONTROL ERROR", conn.sent[1].tokens[4] .. "/" .. conn.sent[1].tokens[5], "ERROR/PRODUCER_ERROR")
    drain(conn.sent)
    mode = "MALFORMED"; serverTick(server)
    T.eq("K2 malformed producer result: CONTROL ERROR", conn.sent[1].tokens[4] .. "/" .. conn.sent[1].tokens[5], "ERROR/MALFORMED")
    drain(conn.sent)
    mode = "BADVALUE"; serverTick(server)
    T.eq("K3 unsupported value: CONTROL ERROR, nothing partial", conn.sent[1].kind .. "/" .. conn.sent[1].tokens[5], E.KIND_CONTROL .. "/MALFORMED")
    T.eq("K4 no public event was produced for the scoped module", #(g_server.sentEvents or {}), 0)
end

-- (L) Cleanup: connection close, unregister, local loopback exclusion, mission end.
do
    local cleared = {}
    local server, client = newPair("stock",
        function() return { state = "READY", viewKey = "k", dataRevision = "R1", mode = "FULL", values = {} } end,
        { applyView = function(pub) return { outcome = "APPLIED", dataRevision = pub.dataRevision } end, clearView = function(r) cleared[#cleared + 1] = r end })
    local conn = newConnection(80)
    setActor(conn, 2, MakeUser(81, false))
    clientTick(client); relayToServer(server, conn); relayToClient(client, conn)
    T.ok("L1 subscribed", server.scopedSubscriptions[conn] ~= nil)
    server:_scopedOnConnectionClosed(conn)
    T.eq("L2 connection close drops that connection's subscriptions", server.scopedSubscriptions[conn], nil)
    T.eq("L3 nothing was sent to the closing connection", #conn.sent, 0)

    -- The server's local loopback never receives a scoped publication.
    local loop = newConnection(0)
    setActor(loop, 1, MakeUser(1, true))
    deliver(E.new(E.KIND_SUBSCRIBE, "stock", "500", 1, { "1", "1" }), server, true, loop)
    T.eq("L4 loopback gets no subscription", server.scopedSubscriptions[loop], nil)
    T.eq("L5 loopback gets no event", #loop.sent, 0)

    -- A connection that lost event readiness is skipped, not sent to.
    local conn2 = newConnection(82)
    setActor(conn2, 2, MakeUser(83, false))
    deliver(E.new(E.KIND_SUBSCRIBE, "stock", "501", 1, { "1", "1" }), server, true, conn2)
    T.eq("L6 ready connection answered", #conn2.sent, 1)
    drain(conn2.sent)
    conn2.isReadyForEvents = false
    server:syncNow("stock")
    T.eq("L7 not-ready connection skipped", #conn2.sent, 0)
    conn2.isReadyForEvents = true

    -- Unregister on the server: one CONTROL, subscriber dropped.
    server:unregisterScopedModule("stock")
    T.eq("L8 unregister sent MODULE_UNREGISTERED", conn2.sent[1] and conn2.sent[1].tokens[5], "MODULE_UNREGISTERED")
    T.eq("L9 unregister dropped the subscriber", server.scopedSubscriptions[conn2]["stock"], nil)
    T.eq("L10 unregistered id is free for a public registration", server:registerModule("stock", { onWriteState = function() return {} end, onReadState = function() end }), true)

    -- Unregister on the client clears the view with the reason (role is the
    -- mission's, so the client instance is exercised under the client role).
    g_currentMission._isServer = false
    client:unregisterScopedModule("stock")
    T.eq("L11 client unregister told the consumer", cleared[#cleared], "MODULE_UNREGISTERED")

    -- Mission end clears everything and readiness.
    local s2, c2 = newPair("stock", function() return { state = "WAITING", reason = "x" } end,
        { applyView = function() end, clearView = function(r) cleared[#cleared + 1] = r end })
    g_currentMission._isServer = false
    c2:_scopedStartGeneration("stock")
    c2:onMissionDelete()
    T.eq("L12 mission end clears client state", next(c2.scopedClient), nil)
    T.eq("L13 mission end tells the consumer", cleared[#cleared], "MISSION_END")
    T.eq("L14 mission end drops readiness", c2:getScopedCapabilities().ready, false)
    s2:onMissionDelete()
    T.eq("L15 mission end clears server subscriptions", next(s2.scopedSubscriptions), nil)
end

-- (M) Burst budget: nil eventId is no attempt; five unanswered attempts then the drift-floor wait.
do
    local client = NetworkSync.new(); g_currentMission._isServer = false; client:onMissionLoaded()
    client:registerScopedModule("stock", { buildView = function() end, applyView = function() return { outcome = "APPLIED" } end, clearView = function() end })
    local savedId = NetworkSyncScopedEvent.eventId
    NetworkSyncScopedEvent.eventId = nil
    clientTick(client); clientTick(client)
    T.eq("M1 nil eventId: no attempt sent", #toServer.sent, 0)
    T.eq("M2 nil eventId: no attempt counted", client.scopedClient["stock"].attempts, 0)
    NetworkSyncScopedEvent.eventId = savedId
    for _ = 1, NetworkSync.JOIN_REQUEST_MAX do clientTick(client) end
    local sent = drain(toServer.sent)
    T.eq("M3 one attempt per interval up to the budget", #sent, NetworkSync.JOIN_REQUEST_MAX)
    T.eq("M4 same subscription id across the burst", sent[1].subscriptionId, sent[#sent].subscriptionId)
    local cs = client.scopedClient["stock"]
    T.eq("M5 burst ended unacknowledged", cs.burstActive, false)
    T.eq("M6 reason UNACKNOWLEDGED", cs.reason, "UNACKNOWLEDGED")
    clientTick(client)
    T.eq("M7 no send before the drift floor", #toServer.sent, 0)
    clientTick(client, NetworkSync.DRIFT_FLOOR_MS)
    clientTick(client)
    local fresh = drain(toServer.sent)
    T.eq("M8 one coalesced fresh generation after the drift floor", #fresh, 1)
    T.ok("M9 with a new subscription id", fresh[1].subscriptionId ~= sent[1].subscriptionId)
end

-- (N) Same-burst repeat and a stale subscription id.
do
    local produced = 0
    local server = NetworkSync.new(); g_currentMission._isServer = true; server:onMissionLoaded()
    server:registerScopedModule("stock", { buildView = function() produced = produced + 1; return { state = "READY", viewKey = "k", dataRevision = "R1", mode = "FULL", values = {} } end, applyView = function() end, clearView = function() end })
    local conn = newConnection(90)
    setActor(conn, 2, MakeUser(91, false))
    deliver(E.new(E.KIND_SUBSCRIBE, "stock", "7", 1, { "1", "1" }), server, true, conn)
    deliver(E.new(E.KIND_SUBSCRIBE, "stock", "7", 1, { "1", "1" }), server, true, conn)
    T.eq("N1 a repeat in the same burst is answered again", #conn.sent, 2)
    local count = 0
    for _ in pairs(server.scopedSubscriptions[conn]) do count = count + 1 end
    T.eq("N2 but creates no second subscriber", count, 1)
    T.ok("N3 publicationId advanced between the two replies", S.compareDecimal(conn.sent[2].tokens[3], conn.sent[1].tokens[3]) > 0)
    -- The client ignores a publication for a subscription it no longer holds.
    local client = NetworkSync.new(); g_currentMission._isServer = false; client:onMissionLoaded()
    local applied = 0
    client:registerScopedModule("stock", { buildView = function() end, applyView = function() applied = applied + 1 return { outcome = "APPLIED", dataRevision = "R1" } end, clearView = function() end })
    client:_scopedStartGeneration("stock")
    deliver(conn.sent[1], client, false, toServer)
    T.eq("N4 a publication for a foreign subscription id is dropped", applied, 0)
end
