-- NS-keyed_args_warning_test.lua
--
-- The warn-only guard for keyed action args (Bob's keyed-args intake, 2026-09-24; PLAYER-
-- REPORTS row 199). The action event writes args[1..#args], so a keyed table is empty on
-- the wire, and a host never notices because requestAction applies it in memory. The
-- core now says so once per action, on both paths, and changes nothing else: the host
-- path still applies the table as it always did, the client path still sends.
--
-- Every row goes through the real requestAction; the client path's event is then
-- delivered through the real writeStream into readStream on a server core, so the
-- "arrives empty" half is the transport's own word, not this test's.
--
--!load: src/Logger.lua, src/RealisticFarmingSyncEvent.lua, src/NetworkSync.lua

local WARN = {}
local realWarning = NSLogger.warning
NSLogger.warning = function(fmt, ...) WARN[#WARN + 1] = string.format(fmt, ...) end
NSLogger.debug = function() end
local function warned(needle)
    local n = 0
    for _, l in ipairs(WARN) do if l:find(needle, 1, true) then n = n + 1 end end
    return n
end
local function group(name, fn)
    local ok, err = pcall(fn)
    if not ok then T.ok(name .. " [group raised: " .. tostring(err) .. "]", false) end
end

local SENT = {}
local function asHost() g_currentMission._isServer = true; g_client = nil end
local function asClient()
    g_currentMission._isServer = false
    g_client = { getServerConnection = function() return { sendEvent = function(_, ev) SENT[#SENT + 1] = ev end } end }
end
--- The engine's delivery of a client's event: writeStream, then readStream on a
--- server core, which runs and applies through that core.
local function deliverTo(ev, serverCore, connection)
    local s = NewStream()
    ev:writeStream(s, nil)
    asHost()
    g_networkSync = serverCore
    local rx = RealisticFarmingActionEvent.emptyNew()
    rx:readStream(s, connection)
    return rx
end

-- ══════════════════════════════════════════════════════════════════════════
-- H. THE HOST PATH: WARNED ONCE, APPLIED AS BEFORE
-- ══════════════════════════════════════════════════════════════════════════
group("H", function()
    asHost()
    local ns = NetworkSync.new()
    local applied = {}
    ns:registerAction("mod_buy", { adminOnly = false, onAction = function(_, args) applied[#applied + 1] = args end })
    ns:registerAction("mod_sell", { adminOnly = false, onAction = function(_, args) applied[#applied + 1] = args end })
    local ok = ns:requestAction("mod_buy", { farmId = 1 })
    T.eq("H1 a keyed table on the host path is warned once, naming the action and the key, and still applied in memory as before",
        tostring(ok) .. "/" .. warned("requestAction('mod_buy'): args carries the key 'farmId'") .. "/" .. #applied .. "/" .. tostring(applied[1] and applied[1].farmId), "true/1/1/1")
    ns:requestAction("mod_buy", { farmId = 2 })
    T.eq("H2 the second keyed request of the same action is not warned again, and still applied", warned("mod_buy") .. "/" .. #applied, "1/2")
    ns:requestAction("mod_sell", { barnId = "b7" })
    T.eq("H3 a different action gets its own one warning", warned("mod_sell") .. "/" .. #WARN, "1/2")
    -- Fresh actions, so a wrongly warned array would show: the two above are already warned once.
    ns:registerAction("mod_pos1", { adminOnly = false, onAction = function(_, args) applied[#applied + 1] = args end })
    ns:registerAction("mod_pos2", { adminOnly = false, onAction = function(_, args) applied[#applied + 1] = args end })
    ns:requestAction("mod_pos1", { 1 })
    ns:requestAction("mod_pos2", { "b7", 250 })
    T.eq("H4 a positional array is never warned", #WARN .. "/" .. #applied, "2/5")
    -- A hole ({ [1] = 1, [3] = 3 }) is NOT pinned: Lua's length operator may answer 1 or 3
    -- for it (any border), so the guard sees it only sometimes. A non-integer key is
    -- always outside 1..#args.
    ns:registerAction("mod_float", { adminOnly = false, onAction = function(_, args) applied[#applied + 1] = args end })
    ns:requestAction("mod_float", { [1.5] = 1 })
    T.eq("H5 a non-integer key counts as keyed and is warned once", warned("requestAction('mod_float'): args carries the key '1.5'"), 1)
    -- A nil action id (a misspelled constant) with keyed args: the guard logs and
    -- requestAction goes on to log "not registered" as it always did; nothing raises.
    local okCall, ret = pcall(ns.requestAction, ns, nil, { farmId = 1 })
    T.eq("H6 a nil action id with keyed args logs the keyed warning and the old not-registered line, returns as before, never raises",
        tostring(okCall) .. "/" .. tostring(ret) .. "/" .. warned("requestAction('nil')") .. "/" .. warned("not registered"), "true/true/1/1")
end)

-- ══════════════════════════════════════════════════════════════════════════
-- C. THE CLIENT PATH: WARNED, STILL SENT, AND WHAT ARRIVES IS EMPTY
-- ══════════════════════════════════════════════════════════════════════════
group("C", function()
    WARN = {}
    asClient()
    local nsC = NetworkSync.new()
    local ok = nsC:requestAction("mod_flush", { farmId = 1 })
    T.eq("C1 a keyed table on the client path is warned once and STILL SENT (warn only, never refuse)",
        tostring(ok) .. "/" .. warned("requestAction('mod_flush')") .. "/" .. #SENT, "true/1/1")
    -- The transport's own word: what the server reads is an empty array.
    asHost()
    g_currentMission.userManager = { getUserByConnection = function(_, c) return c and c.user or nil end }
    local nsS = NetworkSync.new()
    local got = nil
    nsS:registerAction("mod_flush", { adminOnly = false, onAction = function(_, args) got = args end })
    deliverTo(SENT[1], nsS, { user = MakeUser(5, false) })
    T.eq("C2 the keyed request arrives on the server with no values, which is the defect the warning names",
        tostring(got ~= nil) .. "/" .. tostring(got and #got) .. "/" .. tostring(got and got.farmId), "true/0/nil")
    nsS:registerAction("mod_pos", { adminOnly = false, onAction = function(_, args) got = args end })
    asClient()
    nsC:requestAction("mod_pos", { 1 })
    deliverTo(SENT[2], nsS, { user = MakeUser(5, false) })
    T.eq("C3 a positional array on a fresh action is not warned and arrives whole", #WARN .. "/" .. tostring(got and got[1]), "1/1")
end)

NSLogger.warning = realWarning
T.summary()
