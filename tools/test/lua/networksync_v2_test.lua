-- networksync_v2_test.lua - NS-v2 wire + logic round-trip.
-- Proves the three added paths on the v1 base, all through the mock stream so the wire
-- format itself is exercised (writeStream -> readStream -> run -> receiveFrames):
--   Path 1 delta + snapshot-before-delta invariant, Path 2 chunking + reassembly,
--   Path 3 action authorization. Plus v1 FULL backward compatibility.
--!load: src/Logger.lua, src/RealisticFarmingSyncEvent.lua, src/NetworkSync.lua

-- Serialize an event on the "server", read it back on the "client" so run() dispatches
-- into g_networkSync (the client instance). This is the real send-to-apply path.
local function deliver(event, clientNS)
  local s = NewStream()
  event:writeStream(s, nil)
  g_networkSync = clientNS
  g_currentMission._isServer = false
  local rx = RealisticFarmingSyncEvent.emptyNew()
  rx:readStream(s, nil)   -- run() -> clientNS:receiveFrames
end

-- Run a server-side send, capturing every broadcast event.
local function serverSend(fn)
  g_currentMission._isServer = true
  g_server = { sentEvents = {}, broadcastEvent = function(self, e) table.insert(self.sentEvents, e) end }
  fn()
  return g_server.sentEvents
end

-- ── Path 0: v1 FULL round-trip (backward compatibility, mixed types) ──
do
  local nsS = NetworkSync.new()
  nsS:registerModule("AlphaMod", { onWriteState = function() return { 1, 2.5, "hi", true } end, onReadState = function() end })

  local got = nil
  local nsC = NetworkSync.new()
  nsC:registerModule("AlphaMod", { onWriteState = function() return {} end, onReadState = function(arr) got = arr end })

  local events = serverSend(function() nsS:syncNow("AlphaMod") end)
  T.eq("v1 FULL: one event", #events, 1)
  for _, e in ipairs(events) do deliver(e, nsC) end

  T.ok("v1 FULL: applied", got ~= nil)
  T.eq("v1 FULL: int value", got and got[1], 1)
  T.near("v1 FULL: float value", got and got[2], 2.5)
  T.eq("v1 FULL: string value", got and got[3], "hi")
  T.eq("v1 FULL: bool value", got and got[4], true)
end

-- ── Path 1: DELTA applied after the snapshot ──
do
  local nsS = NetworkSync.new()
  nsS:registerModule("BetaMod", {
    onWriteState = function() return { 10, 20, 30 } end,
    onReadState  = function() end,
    onWriteDelta = function() return { 2, 99 } end,   -- index 2 -> 99
    onReadDelta  = function() end,
  })

  local fullGot, deltaGot = nil, nil
  local nsC = NetworkSync.new()
  nsC:registerModule("BetaMod", {
    onWriteState = function() return {} end,
    onReadState  = function(arr) fullGot = arr end,
    onWriteDelta = function() return {} end,
    onReadDelta  = function(pairs) deltaGot = pairs end,
  })

  -- Snapshot first, then a dirty delta.
  for _, e in ipairs(serverSend(function() nsS:syncNow("BetaMod") end)) do deliver(e, nsC) end
  for _, e in ipairs(serverSend(function() nsS:markDirty("BetaMod"); nsS:_broadcastDirty() end)) do deliver(e, nsC) end

  T.ok("delta: snapshot applied", fullGot ~= nil and fullGot[1] == 10)
  T.ok("delta: delta applied", deltaGot ~= nil)
  T.eq("delta: index", deltaGot and deltaGot[1], 2)
  T.eq("delta: value", deltaGot and deltaGot[2], 99)
end

-- ── Path 1 invariant: a DELTA before the snapshot is HELD, then flushed ──
do
  local nsS = NetworkSync.new()
  nsS:registerModule("GammaMod", {
    onWriteState = function() return { 5, 6 } end,
    onReadState  = function() end,
    onWriteDelta = function() return { 1, 500 } end,
    onReadDelta  = function() end,
  })

  local order = {}
  local nsC = NetworkSync.new()
  nsC:registerModule("GammaMod", {
    onWriteState = function() return {} end,
    onReadState  = function() table.insert(order, "full") end,
    onWriteDelta = function() return {} end,
    onReadDelta  = function() table.insert(order, "delta") end,
  })

  -- Deliver the DELTA first (before any snapshot): must be held.
  for _, e in ipairs(serverSend(function() nsS:markDirty("GammaMod"); nsS:_broadcastDirty() end)) do deliver(e, nsC) end
  T.eq("hold: delta not applied before snapshot", #order, 0)

  -- Now the snapshot: applies full, then flushes the held delta.
  for _, e in ipairs(serverSend(function() nsS:syncNow("GammaMod") end)) do deliver(e, nsC) end
  T.eq("hold: two applies after snapshot", #order, 2)
  T.eq("hold: full first", order[1], "full")
  T.eq("hold: delta flushed second", order[2], "delta")
end

-- ── Path 2: chunking + reassembly of an oversized module ──
do
  local N = 5000   -- 5000 * ~5 bytes ≈ 25 KB, well over EVENT_BUDGET_BYTES (8 KB)
  local big = {}
  for i = 1, N do big[i] = i end

  local nsS = NetworkSync.new()
  nsS:registerModule("BigMod", { onWriteState = function() return big end, onReadState = function() end })

  local calls, got = 0, nil
  local nsC = NetworkSync.new()
  nsC:registerModule("BigMod", { onWriteState = function() return {} end,
    onReadState = function(arr) calls = calls + 1; got = arr end })

  local events = serverSend(function() nsS:syncNow("BigMod") end)
  T.ok("chunk: split across multiple events", #events > 1)
  for _, e in ipairs(events) do deliver(e, nsC) end

  T.eq("chunk: reassembled once", calls, 1)
  T.eq("chunk: full length", got and #got or 0, N)
  T.eq("chunk: first value", got and got[1], 1)
  T.eq("chunk: last value", got and got[N], N)
  T.eq("chunk: middle value", got and got[2500], 2500)
end

-- ── Path 3: action channel authorization ──
do
  g_currentMission._isServer = true
  g_currentMission.userManager = { getUserByConnection = function(self, conn) return conn and conn.user or nil end }

  local nsS = NetworkSync.new()
  local applied = {}
  nsS:registerAction("doThing", { onAction = function(userId, args) table.insert(applied, { userId, args[1] }) end, adminOnly = true })

  local adminConn    = { user = MakeUser(1, true) }
  local nonAdminConn = { user = MakeUser(2, false) }

  nsS:_applyAction("doThing", { 42 }, adminConn)
  T.eq("action: admin applied", #applied, 1)
  T.eq("action: admin userId", applied[1] and applied[1][1], 1)
  T.eq("action: admin arg", applied[1] and applied[1][2], 42)

  nsS:_applyAction("doThing", { 43 }, nonAdminConn)
  T.eq("action: non-admin denied", #applied, 1)

  nsS:_applyAction("unknownAction", { 1 }, adminConn)
  T.eq("action: unknown ignored (no crash)", #applied, 1)

  -- Local host (connection nil) is authorized without a user lookup.
  nsS:_applyAction("doThing", { 7 }, nil)
  T.eq("action: local host applied", #applied, 2)
  T.ok("action: local host nil userId", applied[2] and applied[2][1] == nil)
end

-- ── A non-adopting module (no delta callbacks) always goes FULL ──
do
  local nsS = NetworkSync.new()
  nsS:registerModule("PlainMod", { onWriteState = function() return { 1 } end, onReadState = function() end })
  -- Even when marked dirty, with no onWriteDelta the batch sends FULL.
  local events = serverSend(function() nsS:markDirty("PlainMod"); nsS:_broadcastDirty() end)
  T.eq("compat: dirty plain module sends one event", #events, 1)
  T.eq("compat: frame mode is FULL", events[1].frames[1].mode, NetworkSync.MODE_FULL)
  T.eq("compat: single chunk", events[1].frames[1].chunkCount, 1)
end
