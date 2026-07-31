-- join_handshake_test.lua - the client join handshake, and the latch that killed it.
--
-- Observed on a live dedicated server, 2026-07-30:
--
--   21:19:44.454  Error: Invalid event id for FS25_NetworkSync.RealisticFarmingSyncRequestEvent
--   21:19:44.689  Joined network game (249)
--
-- The request went out 235 ms BEFORE the join completed. `getServerConnection()`
-- already returns an object in that window, but its event table is not valid yet, so
-- the engine rejected the send. The old `_sendFullSyncRequest` returned true on
-- `serverConn ~= nil` alone, the caller latched `fullSyncAsked` on that, and the guard
-- on the retry block was `not self.fullSyncAsked`. Five retries at two seconds were
-- provisioned and NONE of them were ever spent.
--
-- What it cost is why this is graded HIGH rather than MEDIUM. Past that first quarter
-- second there is no error line at all. Every companion simply receives nothing, for
-- ever, and each looks locally broken in its own way: Workplace Triggers applying zero
-- triggers, MarketDynamics re-restoring seven events with identical remaining times,
-- CropStress rebuilding sixty-nine fields. Three teams chase three phantom bugs.
--
-- THE FIX IS TO LATCH ON THE REPLY, NOT ON THE ASK. Whether a send landed is not
-- knowable here; whether the server answered is, because an applied snapshot is proof.
--
--!load: src/Logger.lua, src/RealisticFarmingSyncEvent.lua, src/NetworkSync.lua

local INTERVAL = NetworkSync.JOIN_REQUEST_INTERVAL
local MAX      = NetworkSync.JOIN_REQUEST_MAX

-- A client whose server connection exists but whose sends are rejected, which is
-- exactly the state the log caught.
local function clientWithRejectingConnection(counter)
  g_client = {
    getServerConnection = function()
      return {
        sendEvent = function()
          counter.sent = counter.sent + 1
          error("Invalid event id")   -- the engine raises; it does not return a status
        end,
      }
    end,
  }
end

local function clientWithWorkingConnection(counter)
  g_client = {
    getServerConnection = function()
      return { sendEvent = function() counter.sent = counter.sent + 1 end }
    end,
  }
end

local function newJoiningClient()
  g_currentMission._isServer = false
  local ns = NetworkSync.new()
  ns:registerModule("AlphaMod", { onWriteState = function() return {} end, onReadState = function() end })
  ns:onMissionLoaded()
  return ns
end

-- ── 1. A rejected send does not end the handshake ─────────
-- The assertion that would have caught the outage.

local counter = { sent = 0 }
clientWithRejectingConnection(counter)
local ns = newJoiningClient()

T.ok("a joining client needs a full sync", ns.needsFullSync)

ns:update(INTERVAL)
T.eq("the first request is attempted", counter.sent, 1)
T.ok("and a rejected send does NOT end the handshake", ns.needsFullSync)

ns:update(INTERVAL)
T.eq("so it asks again", counter.sent, 2)
ns:update(INTERVAL)
T.eq("and again", counter.sent, 3)

-- The whole provisioned budget gets spent, which is the point: it existed before and
-- was unreachable.
ns:update(INTERVAL)
ns:update(INTERVAL)
T.eq("the full retry budget is spent", counter.sent, MAX)
T.ok("and only then does it give up", not ns.needsFullSync)

-- ── 2. A raising send cannot take the update loop down ────
-- The engine raises rather than returning a status, and this runs from FSBaseMission
-- update. An unguarded raise here would be a per-frame error for every client.

T.ok("the loop survived five raising sends", ns.joinAttempts == MAX)

-- ── 3. The REPLY ends it, not the ask ─────────────────────

counter = { sent = 0 }
clientWithWorkingConnection(counter)
ns = newJoiningClient()

ns:update(INTERVAL)
T.eq("one request goes out", counter.sent, 1)
T.ok("and the handshake stays open until the server answers", ns.needsFullSync)

-- The server answers: a module snapshot is applied.
ns.snapshotReceived["AlphaMod"] = true
ns:update(INTERVAL)
T.ok("an applied snapshot closes the handshake", not ns.needsFullSync)
T.eq("and no further request is made", counter.sent, 1)

ns:update(INTERVAL * 10)
T.eq("not even much later", counter.sent, 1)

-- ── 4. The first ask waits, because firing instantly cannot work ──
-- The old loop fired on the very first update tick via `joinAttempts == 0`, which is
-- measurably inside the window where the send is rejected. It bought a guaranteed
-- engine error and no sync.

counter = { sent = 0 }
clientWithWorkingConnection(counter)
ns = newJoiningClient()

ns:update(1)
T.eq("nothing is sent on the first tick", counter.sent, 0)
ns:update(INTERVAL - 2)
T.eq("nor before the interval elapses", counter.sent, 0)
ns:update(2)
T.eq("the first ask lands one interval in", counter.sent, 1)

-- ── 5. A server never runs the client handshake ───────────

g_currentMission._isServer = true
counter = { sent = 0 }
clientWithWorkingConnection(counter)
local server = NetworkSync.new()
server:onMissionLoaded()
T.ok("a server needs no full sync", not server.needsFullSync)
server:update(INTERVAL)
T.eq("and never asks itself for one", counter.sent, 0)
g_currentMission._isServer = false

-- ── 6. No modules registered is not a failure ─────────────
-- A client with nothing registered will never receive a snapshot, so the budget runs
-- out. That is silence, not a fault, and must not shout at the player.

counter = { sent = 0 }
clientWithWorkingConnection(counter)
ns = NetworkSync.new()
ns:onMissionLoaded()
for _ = 1, MAX do ns:update(INTERVAL) end
T.ok("the budget is spent", not ns.needsFullSync)
T.ok("with no schemas registered", next(ns.schemas) == nil)

-- ── 7. onMissionLoaded re-arms a rejoin cleanly ───────────
-- A map swap or reconnect must get a fresh handshake, not a stale latch.

counter = { sent = 0 }
clientWithWorkingConnection(counter)
ns = newJoiningClient()
for _ = 1, MAX do ns:update(INTERVAL) end
T.ok("the first session gave up", not ns.needsFullSync)

ns:onMissionLoaded()
T.ok("a rejoin needs a full sync again", ns.needsFullSync)
T.eq("with a fresh attempt count", ns.joinAttempts, 0)
T.eq("and no stale snapshot state", next(ns.snapshotReceived), nil)

local before = counter.sent
ns:update(INTERVAL)
T.eq("so it asks again on the new session", counter.sent, before + 1)

-- ── 8. The latch field is GONE, not merely unset ──────────
-- Leaving `fullSyncAsked` behind would invite the next author to reinstate the guard
-- that caused this. Its absence is part of the fix.

T.eq("there is no fullSyncAsked flag to latch", ns.fullSyncAsked, nil)
