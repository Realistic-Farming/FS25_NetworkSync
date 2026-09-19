-- prelude.lua - minimal FS25 engine mock + tiny test framework for FS25_NetworkSync.
-- Loaded first by run-tests.mjs, before src modules and the test file. Only stubs what
-- module load + the functions under test touch; extend as new tests need more surface.

unpack = unpack or table.unpack

-- ── FS25 OO helper ─────────────────────────────────────────
-- Class(classTable[, parent]): instances get __index = classTable; classTable inherits
-- from parent. Covers both Class(NetworkSync) and Class(Event subclass, Event).
function Class(classTable, parent)
  classTable = classTable or {}
  if parent ~= nil then
    setmetatable(classTable, { __index = parent })
  end
  classTable.__index = classTable
  return classTable
end

-- ── Event base + registration ──────────────────────────────
Event = {}
function Event.new(mt) return setmetatable({}, mt) end
function InitEventClass(class, name) class.eventClassName = name end

-- ── Logging (NSLogger wraps this) ──────────────────────────
Logging = {
  info    = function(...) end,
  warning = function(...) end,
  error   = function(...) end,
}

-- ── table.size (FS25 helper used by getStatus) ─────────────
table.size = table.size or function(t)
  local n = 0
  for _ in pairs(t or {}) do n = n + 1 end
  return n
end

-- ── Network stream mock ────────────────────────────────────
-- A stream is a plain table; write appends cells, read walks a cursor. Values are
-- stored as-is (fengari has no float32 truncation), which is enough to prove the wire
-- FORMAT: that writeStream and readStream agree on order, count, and typing.
--
-- TYPING IS CHECKED, not merely claimed. Until 2026-09-19 this mock stored bare
-- values and its own comment said it proved typing, which it did not: a
-- streamWriteInt32 read back by streamReadString round-tripped clean, and so did a
-- read past the end of the stream. In THIS repo, the one every other mod syncs
-- through, a write/read order mismatch or a type drift is the defect that reaches
-- everything downstream, and the harness was blind to it.
--
-- Each write now records a type tag alongside its value. A paired read checks the
-- tag and counts a mismatch; a read past the end counts an underflow. Neither raises,
-- so a test sees the whole picture rather than dying on the first fault, and
-- `s.typeErrors + s.underflows == 0` is the assertion that makes a round trip mean
-- something. `cells` still holds bare values in write order, so an existing test that
-- asserts a byte's value by position is unaffected.
--
-- No UIntN stub: NetworkSync's production code does not call it. A stub for an unused
-- mechanism is maintenance cost with nothing behind it.
function NewStream()
  return { cells = {}, tags = {}, w = 0, r = 0, typeErrors = 0, underflows = 0 }
end
local function _w(s, tag, v) s.w = s.w + 1; s.cells[s.w] = v; s.tags[s.w] = tag end
local function _r(s, tag)
  if s.r >= s.w then s.underflows = s.underflows + 1; return nil end
  s.r = s.r + 1
  if s.tags[s.r] ~= tag then s.typeErrors = s.typeErrors + 1 end
  return s.cells[s.r]
end

--- True when every read matched its write's type and nothing was read PAST the end.
---
--- Note what this does NOT cover: reading SHORT of the end. Eight cells written
--- against seven read leaves residue on the wire, and that is clean by both counters
--- here. Use StreamDrained for the other direction.
function StreamClean(s)
  return s ~= nil and s.typeErrors == 0 and s.underflows == 0
end

--- True when the reader consumed everything the writer wrote.
---
--- This is the other half of "count", and the engine checks exactly it: after an
--- event's readStream, network/Server.lua:443-444 compares the read offset against
--- the declared length and reports "Not all bits read in event". So a short read is a
--- real fault class with a real in-game symptom, not a tidiness point.
---
--- It is OPT-IN rather than folded into StreamClean, because under-reading is
--- sometimes correct: a frame whose leading byte is an unknown bootstrap is consumed
--- as malformed and deliberately not decoded further, and a blanket residue assertion
--- would fail the one test that gets that right.
function StreamDrained(s)
  return s ~= nil and s.r == s.w
end

--- Round-trip audit. Every round-trip helper in the suite hands its stream here, so
--- one assertion at the end of a file covers every trip in it rather than each test
--- remembering to check.
---
--- The trip COUNT is recorded as well as the faults, and both are asserted. A bare
--- "zero faults" row is satisfied by a run where no round trip happened at all, which
--- is the same shape as a refusal row passing because the fixture never ran.
StreamAudit = { trips = 0, faults = 0, raises = 0, firstFault = nil }

--- A dirty trip fails IMMEDIATELY, at the point of detection, as well as being
--- recorded for the end-of-file rows.
---
--- Reporting only at the end is not enough, and the mutation battery is what showed
--- it: a type drift severe enough to feed a string or a nil into the code under test
--- raises a Lua error, which aborts the whole file BEFORE any end-of-file row runs.
--- The mutation still dies, but it dies as "Lua error while loading/running" with no
--- named row, which hides which trip was at fault and takes every later row with it.
--- Failing here means the diagnosis is already printed when the crash arrives.
function StreamAudit.check(s, label, expectDrained)
  StreamAudit.trips = StreamAudit.trips + 1
  if not StreamClean(s) then
    StreamAudit.faults = StreamAudit.faults + 1
    local detail = string.format("%s: typeErrors=%d underflows=%d",
      tostring(label), s.typeErrors or -1, s.underflows or -1)
    if StreamAudit.firstFault == nil then StreamAudit.firstFault = detail end
    T.ok("stream round trip typed clean: " .. tostring(label), false, detail)
  end
  -- Residue: the reader stopped short and left bytes on the wire. Opt-in, because a
  -- deliberately-unconsumed frame is a correct outcome in some tests.
  if expectDrained and not StreamDrained(s) then
    StreamAudit.faults = StreamAudit.faults + 1
    local detail = string.format("%s: wrote %d cells, read %d, %d left on the wire",
      tostring(label), s.w or -1, s.r or -1, (s.w or 0) - (s.r or 0))
    if StreamAudit.firstFault == nil then StreamAudit.firstFault = detail end
    T.ok("stream round trip fully drained: " .. tostring(label), false, detail)
  end
  return s
end

--- Deliver a stream into a reader, then audit it, reporting both.
---
--- The read is pcall'd on purpose. A wire defect bad enough to feed a string or a nil
--- into the code under test raises INSIDE readStream, before the stream can be
--- examined at all, so an unguarded call aborts the file and the whole run reports
--- only "Lua error while loading/running" with no indication of which trip or why.
--- Guarding it means the type and count diagnosis is printed first and the raise is
--- then reported as its own named row, so a crash keeps its evidence.
---
--- A raise is still a failure here. This does not swallow anything.
function StreamAudit.deliver(s, label, fn, expectDrained)
  local ok, err = pcall(fn)
  StreamAudit.check(s, label, expectDrained)
  if not ok then
    StreamAudit.raises = StreamAudit.raises + 1
    T.ok("stream round trip completed without raising: " .. tostring(label), false,
      string.format("%s raised: %s (stream typeErrors=%s underflows=%s)",
        tostring(label), tostring(err), tostring(s.typeErrors), tostring(s.underflows)))
  end
  return ok, err
end

--- Emit the two audit rows. Call once, at the end of a test file.
function StreamAudit.report()
  T.ok("every stream round trip drained clean (types and count)",
    StreamAudit.faults == 0,
    StreamAudit.firstFault or "a round trip left the stream dirty")
  T.ok("and round trips were actually audited, so the row above is not vacuous",
    StreamAudit.trips > 0,
    "StreamAudit saw zero round trips; the clean result above proves nothing")
  -- Guarding the read keeps the diagnosis, but it also means the file CONTINUES past
  -- a raise that would previously have aborted it. Every row after that point ran
  -- against half-applied state: a receiver part-way through applying, g_networkSync
  -- and _isServer already set. A pass underneath a raise is not evidence, so the
  -- green below one has to be labelled rather than counted silently.
  T.ok("no round trip raised, so no later row ran against half-applied state",
    StreamAudit.raises == 0,
    string.format("%d round trip(s) raised; every row after the first is suspect",
      StreamAudit.raises))
end

function streamWriteInt32(s, v)   _w(s, "i32", math.floor(v)) end
function streamReadInt32(s)        return _r(s, "i32") end
function streamWriteUInt8(s, v)    _w(s, "u8", math.floor(v)) end
function streamReadUInt8(s)         return _r(s, "u8") end
function streamWriteBool(s, v)     _w(s, "bool", v and true or false) end
function streamReadBool(s)          return _r(s, "bool") end
function streamWriteFloat32(s, v)  _w(s, "f32", v) end
function streamReadFloat32(s)       return _r(s, "f32") end
function streamWriteString(s, v)   _w(s, "str", tostring(v)) end
function streamReadString(s)        return _r(s, "str") end
-- Bit offset probe (real engine returns bits written); the mock reports cell count.
function streamGetWriteOffset(s)   return s.w * 8 end

-- ── Mission / server / client stubs (tests set fields as needed) ──
g_currentMission = { _isServer = true }
function g_currentMission:getIsServer() return self._isServer end

g_server = nil   -- tests install a capturing server when they exercise broadcast
g_client = nil

-- Build a fake user for permission tests.
function MakeUser(id, isMaster, nick)
  return {
    _id = id, _master = isMaster, _nick = nick or ("user" .. tostring(id)),
    getId = function(self) return self._id end,
    getIsMasterUser = function(self) return self._master end,
    getNickname = function(self) return self._nick end,
  }
end

-- ── tiny test framework (emits ##TEST_ markers parsed by run-tests.mjs) ──
T = { _pass = 0, _fail = 0 }
local function _pass(name) T._pass = T._pass + 1; print("##TEST_PASS " .. name) end
local function _fail(name, msg) T._fail = T._fail + 1; print("##TEST_FAIL " .. name .. " :: " .. tostring(msg)) end

function T.ok(name, cond, msg)
  if cond then _pass(name) else _fail(name, msg or "expected truthy, got " .. tostring(cond)) end
end
function T.eq(name, got, want)
  if got == want then _pass(name) else _fail(name, "got " .. tostring(got) .. " want " .. tostring(want)) end
end
function T.near(name, got, want, tol)
  tol = tol or 1e-6
  if type(got) == "number" and math.abs(got - want) <= tol then _pass(name)
  else _fail(name, "got " .. tostring(got) .. " want ~" .. tostring(want)) end
end
function T.summary() print("##TEST_SUMMARY " .. T._pass .. " " .. T._fail) end
