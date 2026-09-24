-- NS-118-readstream_count_bounds_test.lua
--
-- MAINTENANCE row 118: the server reads every registered event a client sends
-- (network/Server.lua:436) before any guard in run, so a forged count in the sync or
-- action event's readStream loop was a hang vector on any host. Each count is now held
-- to what the WRITER can produce, derived from the chunker (a frame's values fit
-- EVENT_BUDGET_BYTES at no less than 2 estimated bytes each; an event's frames fit the
-- same budget at no less than 16 each), refused the scoped event's way: malformed,
-- unread past the count, never run. The sync event, which only a server sends, reads
-- nothing at all on a server.
--
-- The legitimate side is the real writer: NetworkSync's own _sendModules and chunker
-- produce the largest frame and the fullest batches, and the real writeStream carries
-- them into readStream on a client core. A forged count is the attacker's one edit on
-- that real stream (one cell of the typed mock set past the bound).
--
--!load: src/Logger.lua, src/RealisticFarmingSyncEvent.lua, src/NetworkSync.lua

NSLogger.warning = function() end
NSLogger.debug = function() end
NSLogger.error = function() end
local E, A, NS = RealisticFarmingSyncEvent, RealisticFarmingActionEvent, NetworkSync

local function group(name, fn)
    local ok, err = pcall(fn)
    if not ok then T.ok(name .. " [group raised: " .. tostring(err) .. "]", false) end
end
--- Run fn as the server and hand back the events it broadcast.
local function serverSend(fn)
    g_currentMission._isServer = true
    g_server = { sentEvents = {}, broadcastEvent = function(self, e) table.insert(self.sentEvents, e) end }
    fn()
    return g_server.sentEvents
end
--- The real writer's stream for an event.
local function written(ev)
    local s = NewStream()
    ev:writeStream(s, nil)
    return s
end
--- Deliver a stream into a fresh instance's readStream on a client core, as the engine
--- does. Returns the received instance and the reads made (the mock's cursor).
local function readOnClient(s, class, clientCore)
    g_currentMission._isServer = false
    g_networkSync = clientCore
    local rx = class.emptyNew()
    local ok, err = pcall(rx.readStream, rx, s, nil)
    return rx, s.r, ok, err
end
local function bools(n)
    local t = {}
    for i = 1, n do t[i] = (i % 2 == 0) end
    return t
end

-- ══════════════════════════════════════════════════════════════════════════
-- B. THE BOUNDS ARE THE WRITER'S OWN CEILINGS
-- ══════════════════════════════════════════════════════════════════════════
group("B", function()
    T.eq("B1 the bounds are the chunker's arithmetic: values per frame from the 2-byte boolean, frames per event from the 16-byte empty frame, and the action's args take the value ceiling",
        E.MAX_FRAME_VALUES .. "/" .. E.MAX_EVENT_FRAMES .. "/" .. A.MAX_ARGS,
        math.floor(NS.EVENT_BUDGET_BYTES / 2) .. "/" .. math.floor(NS.EVENT_BUDGET_BYTES / 16) .. "/" .. math.floor(NS.EVENT_BUDGET_BYTES / 2))

    -- The largest legitimate frame: the chunker fills a frame with exactly MAX_FRAME_VALUES booleans.
    local nsS = NS.new()
    nsS:registerModule("Bools", { onWriteState = function() return bools(E.MAX_FRAME_VALUES) end, onReadState = function() end })
    local events = serverSend(function() nsS:syncNow("Bools") end)
    local got, applied = nil, 0
    local nsC = NS.new()
    nsC:registerModule("Bools", { onWriteState = function() return {} end, onReadState = function(arr) got = arr applied = applied + 1 end })
    local rx, reads = readOnClient(written(events[1]), E, nsC)
    T.eq("B2 the writer's fullest frame (4096 booleans in one frame, the chunker's own ceiling) is read whole on a client and applied",
        #events .. "/" .. #events[1].frames .. "/" .. #events[1].frames[1].values .. "/" .. tostring(rx.malformed) .. "/" .. tostring(got and #got) .. "/" .. applied, "1/1/" .. E.MAX_FRAME_VALUES .. "/nil/" .. E.MAX_FRAME_VALUES .. "/1")
    nsS:registerModule("Bools2", { onWriteState = function() return bools(E.MAX_FRAME_VALUES + 1) end, onReadState = function() end })
    events = serverSend(function() nsS:syncNow("Bools2") end)
    local sizes = {}
    for _, ev in ipairs(events) do for _, f in ipairs(ev.frames) do sizes[#sizes + 1] = #f.values end end
    T.eq("B2b one boolean more and the chunker itself splits the frame, so no legitimate frame ever exceeds the bound", table.concat(sizes, ","), E.MAX_FRAME_VALUES .. ",1")

    -- The fullest batches: many small modules, forced full, batched by the real sender.
    local nsM = NS.new()
    local ids = {}
    for i = 1, 600 do
        local id = string.format("m%03d", i)
        ids[#ids + 1] = id
        nsM:registerModule(id, { onWriteState = function() return {} end, onReadState = function() end })
    end
    events = serverSend(function() nsM:_broadcastDriftFloor() end)
    local most, total = 0, 0
    for _, ev in ipairs(events) do most = math.max(most, #ev.frames) total = total + #ev.frames end
    local nsR = NS.new()
    local received = 0
    for _, id in ipairs(ids) do nsR:registerModule(id, { onWriteState = function() return {} end, onReadState = function() received = received + 1 end }) end
    local allClean = true
    for _, ev in ipairs(events) do
        local r = readOnClient(written(ev), E, nsR)
        if r.malformed ~= nil then allClean = false end
    end
    T.eq("B3 the real batcher never puts more frames in an event than the bound, and every batch of 600 modules is read whole",
        tostring(most <= E.MAX_EVENT_FRAMES and most > E.MAX_EVENT_FRAMES / 2) .. "/" .. total .. "/" .. tostring(allClean) .. "/" .. received, "true/600/true/600")
end)

-- ══════════════════════════════════════════════════════════════════════════
-- F. FORGED COUNTS ARE REFUSED UNREAD
-- ══════════════════════════════════════════════════════════════════════════
group("F", function()
    local nsS = NS.new()
    nsS:registerModule("Mod", { onWriteState = function() return { 1, 2.5, "x", true } end, onReadState = function() end })
    local events = serverSend(function() nsS:syncNow("Mod") end)
    local applied = 0
    local nsC = NS.new()
    nsC:registerModule("Mod", { onWriteState = function() return {} end, onReadState = function() applied = applied + 1 end })
    -- The stream's cells: 1 frame count, 2 modId, 3 chunkIndex, 4 chunkCount, 5 mode, 6 value count, then the values.
    local s = written(events[1])
    s.cells[1] = E.MAX_EVENT_FRAMES + 1
    local rx, reads, ok = readOnClient(s, E, nsC)
    T.eq("F1 a frame count one over the bound is refused after that one read, nothing applied, nothing raised",
        tostring(rx.malformed) .. "/" .. reads .. "/" .. applied .. "/" .. tostring(ok), "FRAME_COUNT_OUT_OF_RANGE/1/0/true")
    s = written(events[1])
    s.cells[6] = E.MAX_FRAME_VALUES + 1
    rx, reads, ok = readOnClient(s, E, nsC)
    T.eq("F2 a value count one over the bound is refused after the frame's header, nothing applied",
        tostring(rx.malformed) .. "/" .. reads .. "/" .. applied .. "/" .. tostring(ok), "VALUE_COUNT_OUT_OF_RANGE/6/0/true")
    s = written(events[1])
    s.cells[1] = -1
    rx, reads = readOnClient(s, E, nsC)
    local s2 = written(events[1])
    s2.cells[6] = -1
    local rx2, reads2 = readOnClient(s2, E, nsC)
    T.eq("F3 negative counts are refused the same way", tostring(rx.malformed) .. "/" .. reads .. "/" .. tostring(rx2.malformed) .. "/" .. reads2, "FRAME_COUNT_OUT_OF_RANGE/1/VALUE_COUNT_OUT_OF_RANGE/6")
    rx, reads, ok = readOnClient(NewStream(), E, nsC)
    T.eq("F4 a stream cut before its count is refused, not looped on or raised", tostring(rx.malformed) .. "/" .. tostring(ok) .. "/" .. applied, "FRAME_COUNT_OUT_OF_RANGE/true/0")
    rx, reads = readOnClient(written(events[1]), E, nsC)
    T.eq("F5 the same stream untouched is read whole and applied", tostring(rx.malformed) .. "/" .. applied, "nil/1")
end)

-- ══════════════════════════════════════════════════════════════════════════
-- A. THE ACTION EVENT
-- ══════════════════════════════════════════════════════════════════════════
group("A", function()
    g_currentMission._isServer = true
    g_currentMission.userManager = { getUserByConnection = function(_, c) return c and c.user or nil end }
    local nsS = NS.new()
    local seen = nil
    nsS:registerAction("big", { adminOnly = false, onAction = function(_, args) seen = #args end })
    g_networkSync = nsS
    local args = bools(A.MAX_ARGS)
    local s = written(A.new("big", args))
    local rx = A.emptyNew()
    rx:readStream(s, { user = MakeUser(3, false) })
    T.eq("A1 an action of exactly the ceiling's args is read whole and runs", tostring(rx.malformed) .. "/" .. tostring(seen), "nil/" .. A.MAX_ARGS)
    seen = nil
    s = written(A.new("big", args))
    s.cells[2] = A.MAX_ARGS + 1
    rx = A.emptyNew()
    local ok = pcall(rx.readStream, rx, s, { user = MakeUser(3, false) })
    T.eq("A2 an arg count one over the ceiling is refused after two reads, never run, never raised", tostring(rx.malformed) .. "/" .. s.r .. "/" .. tostring(seen) .. "/" .. tostring(ok), "ARG_COUNT_OUT_OF_RANGE/2/nil/true")
end)

-- ══════════════════════════════════════════════════════════════════════════
-- S. THE SYNC EVENT READS NOTHING ON A SERVER
-- ══════════════════════════════════════════════════════════════════════════
group("S", function()
    local nsS = NS.new()
    nsS:registerModule("Mod", { onWriteState = function() return { 7 } end, onReadState = function() end })
    local events = serverSend(function() nsS:syncNow("Mod") end)
    local s = written(events[1])
    g_currentMission._isServer = true
    g_networkSync = nsS
    local rx = E.emptyNew()
    rx:readStream(s, { user = MakeUser(3, false) })
    T.eq("S1 a sync event arriving at a server (a client's forgery) is refused before its first read", tostring(rx.malformed) .. "/" .. s.r, "WRONG_SIDE/0")
    local nsC = NS.new()
    local got = nil
    nsC:registerModule("Mod", { onWriteState = function() return {} end, onReadState = function(arr) got = arr end })
    rx = readOnClient(s, E, nsC)
    T.eq("S2 the same stream on a client is read whole and applied", tostring(rx.malformed) .. "/" .. tostring(got and got[1]), "nil/7")
end)

T.summary()
