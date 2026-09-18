-- main_wiring_stubs.lua - engine surface main.lua touches at file load, so the
-- real main.lua can be loaded under the bench (NS-7-main_wiring_spec_test).
-- Not a test file (no _test suffix); listed in that spec's --!load line right
-- before main.lua. The src modules are already loaded by the runner, so
-- source() is a no-op here.

source = source or function() end
getfenv = getfenv or function() return _G end

Utils = Utils or {}
Utils.appendedFunction = Utils.appendedFunction or function(oldFunc, newFunc)
    if oldFunc == nil then return newFunc end
    return function(...)
        local r = oldFunc(...)
        newFunc(...)
        return r
    end
end
Utils.prependedFunction = Utils.prependedFunction or function(oldFunc, newFunc)
    if oldFunc == nil then return newFunc end
    return function(...)
        newFunc(...)
        return oldFunc(...)
    end
end

Mission00 = Mission00 or {
    load = function() end,
    loadMission00Finished = function() end,
    onStartMission = function() end,
}
FSBaseMission = FSBaseMission or {
    update = function() end,
    delete = function() end,
}

NetworkSyncModDirectory = NetworkSyncModDirectory or "bench/"
