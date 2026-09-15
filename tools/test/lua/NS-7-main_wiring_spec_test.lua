-- NS-7-main_wiring_spec_test.lua - the real main.lua against mocks.
--
-- Bob's cold review of PR #7 (MAJOR 6): the onConnectionClosed instance
-- wrapper and its Mission00.load / FSBaseMission.delete wiring were untested.
-- This loads main.lua itself (main_wiring_stubs.lua supplies source, getfenv,
-- Utils and the two engine class tables) and proves: the wrapper is installed
-- on the mission instance at Mission00.load, calls the original exactly once
-- with its arguments and passes the return through, runs the scoped cleanup,
-- is restored at FSBaseMission.delete only when it is still the current
-- method, and otherwise retires as a no-op that never cuts another owner's
-- chain. Nothing here proves engine event ids or a live connection.
--
--!load: src/Logger.lua, src/RealisticFarmingSyncEvent.lua, src/NetworkSyncScopedEvent.lua, src/NetworkSync.lua, src/NetworkSyncScoped.lua, tools/test/lua/main_wiring_stubs.lua, main.lua

NetworkNode = NetworkNode or { LOCAL_STREAM_ID = 0, CHANNEL_MAIN = 1, CHANNEL_SECONDARY = 2 }
g_messageCenter = g_messageCenter or { subscribe = function() end, unsubscribeAll = function() end }

T.ok("W0 main.lua published the handle", g_networkSync ~= nil)
local ns = g_networkSync

local function newMission()
    local m = { calls = {}, _isServer = true }
    m.getIsServer = function(self) return self._isServer end
    m.onConnectionClosed = function(self, connection, reason)
        self.calls[#self.calls + 1] = { self = self, connection = connection, reason = reason }
        return "orig-ret"
    end
    return m
end

-- (A) Install, call-through, cleanup, restore.
do
    local mission = newMission()
    local original = mission.onConnectionClosed
    g_currentMission = mission
    Mission00.load(mission)
    T.ok("A1 wrapper installed on the instance", mission.onConnectionClosed ~= original)
    T.eq("A2 handle published on the mission", mission.networkSync, ns)
    ns:_ensureScopedState()
    local conn = { streamId = 5 }
    ns.scopedSubscriptions[conn] = { stock = {} }
    ns.scopedConnectionIds[conn] = "5"
    local ret = mission:onConnectionClosed(conn, 3)
    T.eq("A3 original called exactly once", #mission.calls, 1)
    T.eq("A4 original got the mission as self", mission.calls[1].self, mission)
    T.eq("A5 original got the connection", mission.calls[1].connection, conn)
    T.eq("A6 original got the reason", mission.calls[1].reason, 3)
    T.eq("A7 return passed through", ret, "orig-ret")
    T.eq("A8 scoped subscriptions of that connection dropped", ns.scopedSubscriptions[conn], nil)
    T.eq("A9 scoped descriptor dropped", ns.scopedConnectionIds[conn], nil)
    FSBaseMission.delete(mission)
    T.eq("A10 delete restored the original (still current)", mission.onConnectionClosed, original)
    T.eq("A11 delete cleared the mission handle", mission.networkSync, nil)
    T.eq("A12 delete cleared the global handle", g_networkSync, nil)
    g_networkSync = ns
end

-- (B) Another owner chains over ours after install: delete retires, never cuts.
do
    local mission = newMission()
    local original = mission.onConnectionClosed
    g_currentMission = mission
    Mission00.load(mission)
    local ours = mission.onConnectionClosed
    local otherCalls = 0
    local other = function(self, connection, reason)
        otherCalls = otherCalls + 1
        return "other:" .. tostring(ours(self, connection, reason))
    end
    mission.onConnectionClosed = other
    FSBaseMission.delete(mission)
    T.eq("B1 delete leaves the other owner's method in place", mission.onConnectionClosed, other)
    ns:_ensureScopedState()
    local conn = { streamId = 6 }
    ns.scopedSubscriptions[conn] = { stock = {} }
    local ret = mission:onConnectionClosed(conn, 1)
    T.eq("B2 the chain still reaches the original once", #mission.calls, 1)
    T.eq("B3 the other owner ran", otherCalls, 1)
    T.eq("B4 return chained through the retired wrapper", ret, "other:orig-ret")
    T.ok("B5 retired wrapper no longer runs the scoped cleanup", ns.scopedSubscriptions[conn] ~= nil)
    ns.scopedSubscriptions[conn] = nil
    g_networkSync = ns
end

-- (C) A mission without onConnectionClosed installs nothing and delete is safe.
do
    local mission = { _isServer = true, getIsServer = function(self) return self._isServer end }
    g_currentMission = mission
    Mission00.load(mission)
    T.eq("C1 no wrapper without an original", mission.onConnectionClosed, nil)
    local ok = pcall(function() FSBaseMission.delete(mission) end)
    T.ok("C2 delete without a wrapper is safe", ok)
    g_networkSync = ns
end
