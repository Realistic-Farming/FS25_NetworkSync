-- =========================================================
-- FS25_NetworkSync - mod entry point
-- =========================================================
-- Author: TisonK
-- =========================================================
-- Loads the NetworkSync modules, publishes the g_networkSync handle, and
-- hooks the FS25 mission lifecycle:
--   Mission00.load                   -> publish the cross-mod bridge handle
--   Mission00.loadMission00Finished  -> client arms its join full-sync request
--   FSBaseMission.update             -> drive the 1Hz batch / join retry
--   FSBaseMission.delete             -> drop the handle
--
-- Load order: NetworkSync is the ecosystem's network bedrock and loads
-- before the companions that register with it. StateLedger is NOT required
-- (no modDesc dependency since NS-7); when both are installed either order
-- works. The handle is published as soon as this file runs so companions
-- loading after it can registerModule during their own module load.
-- =========================================================

-- Hot-reload latch (FuelCosts reference): g_currentModDirectory and
-- g_currentModName are nil on a live re-source, so they are latched into
-- module globals on first load, with a g_modsDirectory loose-folder fallback.
NetworkSyncModDirectory = NetworkSyncModDirectory
    or g_currentModDirectory
    or (g_modsDirectory ~= nil and (g_modsDirectory .. "FS25_NetworkSync/") or nil)
NetworkSyncModName = NetworkSyncModName or g_currentModName or "FS25_NetworkSync"
local modDirectory = NetworkSyncModDirectory

source(modDirectory .. "src/Logger.lua")
-- BUILD 22:42: sourced here, at file load, rather than from the Mission00.load
-- append below. The wraps have to exist before the first join OBJECT_UPDATE,
-- and convertFromNetworkFilename is also called during map and save load.
source(modDirectory .. "src/JoinFilenameGuard.lua")
source(modDirectory .. "src/RealisticFarmingSyncEvent.lua")
-- NS-7: the scoped event class registers at file load like the public ones.
source(modDirectory .. "src/NetworkSyncScopedEvent.lua")
source(modDirectory .. "src/NetworkSync.lua")
source(modDirectory .. "src/NetworkSyncScoped.lua")

local networkSync = NetworkSync.new()
getfenv(0)["g_networkSync"] = networkSync

-- ---------------------------------------------------------
-- Mission lifecycle hooks
-- ---------------------------------------------------------

-- NS-7: scoped cleanup rides on the mission's own onConnectionClosed
-- (FSBaseMission.lua:834). The wrapper is installed on the mission instance,
-- calls the original exactly once with its arguments and returns, and is
-- restored at teardown only if it is still the current method; otherwise it
-- retires as a no-op so another owner's chain is never cut.
local connectionClosedWrapper = nil
local connectionClosedOriginal = nil
local connectionClosedRetire = nil

local function installConnectionClosedWrapper(mission)
    if mission == nil or type(mission.onConnectionClosed) ~= "function" then return end
    -- The original and the retired flag are upvalues of THIS wrapper, so a
    -- wrapper another owner chained over keeps calling its original after we
    -- retire it; the module-level slots only identify the current install.
    local original = mission.onConnectionClosed
    local retired = false
    local wrapper = function(self, connection, disconnectReason)
        if not retired and g_networkSync ~= nil and g_networkSync._scopedOnConnectionClosed ~= nil then
            g_networkSync:_scopedOnConnectionClosed(connection)
        end
        return original(self, connection, disconnectReason)
    end
    connectionClosedWrapper = wrapper
    connectionClosedOriginal = original
    connectionClosedRetire = function() retired = true end
    mission.onConnectionClosed = wrapper
end

local function removeConnectionClosedWrapper(mission)
    if connectionClosedRetire ~= nil then connectionClosedRetire() end
    if mission ~= nil and connectionClosedWrapper ~= nil and mission.onConnectionClosed == connectionClosedWrapper then
        mission.onConnectionClosed = connectionClosedOriginal
    end
    connectionClosedWrapper = nil
    connectionClosedOriginal = nil
    connectionClosedRetire = nil
end

local function onMissionLoad(mission)
    if mission ~= nil then
        mission.networkSync = networkSync
        installConnectionClosedWrapper(mission)
    end
    NSLogger.info("NetworkSync active (mod 2, multiplayer batch sync)")
end

local function onMissionLoadedFinished()
    networkSync:onMissionLoaded()
end

local function onMissionUpdate(mission, dt)
    networkSync:update(dt)
end

local function onMissionDelete(mission)
    networkSync:onMissionDelete()
    removeConnectionClosedWrapper(mission or g_currentMission)
    getfenv(0)["g_networkSync"] = nil
    if g_currentMission ~= nil then
        g_currentMission.networkSync = nil
    end
end

Mission00.load = Utils.appendedFunction(Mission00.load, onMissionLoad)
Mission00.loadMission00Finished = Utils.appendedFunction(Mission00.loadMission00Finished, onMissionLoadedFinished)
FSBaseMission.update = Utils.appendedFunction(FSBaseMission.update, onMissionUpdate)
FSBaseMission.delete = Utils.prependedFunction(FSBaseMission.delete, onMissionDelete)

-- ---------------------------------------------------------
-- Console command: nsStatus
-- ---------------------------------------------------------

if addConsoleCommand ~= nil then
    addConsoleCommand("nsStatus", "Show NetworkSync registered modules and role",
        "consoleCommandStatus", networkSync)
end
