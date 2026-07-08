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
function NewStream()
  return { cells = {}, w = 0, r = 0 }
end
local function _w(s, v) s.w = s.w + 1; s.cells[s.w] = v end
local function _r(s)     s.r = s.r + 1; return s.cells[s.r] end

function streamWriteInt32(s, v)   _w(s, math.floor(v)) end
function streamReadInt32(s)        return _r(s) end
function streamWriteUInt8(s, v)    _w(s, math.floor(v)) end
function streamReadUInt8(s)         return _r(s) end
function streamWriteBool(s, v)     _w(s, v and true or false) end
function streamReadBool(s)          return _r(s) end
function streamWriteFloat32(s, v)  _w(s, v) end
function streamReadFloat32(s)       return _r(s) end
function streamWriteString(s, v)   _w(s, tostring(v)) end
function streamReadString(s)        return _r(s) end
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
