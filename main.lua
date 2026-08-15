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
-- Load order: NetworkSync is mod 2 (after StateLedger). The handle is
-- published as soon as this file runs so companions loading after it can
-- registerModule during their own module load.
-- =========================================================

local modDirectory = g_currentModDirectory

source(modDirectory .. "src/Logger.lua")
-- BUILD 22:42: sourced here, at file load, rather than from the Mission00.load
-- append below. The wraps have to exist before the first join OBJECT_UPDATE,
-- and convertFromNetworkFilename is also called during map and save load.
source(modDirectory .. "src/JoinFilenameGuard.lua")
source(modDirectory .. "src/RealisticFarmingSyncEvent.lua")
source(modDirectory .. "src/NetworkSync.lua")

local networkSync = NetworkSync.new()
getfenv(0)["g_networkSync"] = networkSync

-- ---------------------------------------------------------
-- Mission lifecycle hooks
-- ---------------------------------------------------------

local function onMissionLoad(mission)
    if mission ~= nil then
        mission.networkSync = networkSync
    end
    NSLogger.info("NetworkSync active (mod 2, multiplayer batch sync)")
end

local function onMissionLoadedFinished()
    networkSync:onMissionLoaded()
end

local function onMissionUpdate(mission, dt)
    networkSync:update(dt)
end

local function onMissionDelete()
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
