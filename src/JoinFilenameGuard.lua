-- =========================================================
-- FS25_NetworkSync - JoinFilenameGuard
-- =========================================================
-- Author: TisonK
-- =========================================================
-- BUILD 22:42 - Judith Start blank-filename join guard.
--
-- A join CREATE can carry a zero-length network filename. streamReadString
-- gives nil for it on the client, NetworkUtil.convertFromNetworkFilename runs
-- string.lower on that nil and throws, and the throw unwinds the whole of
-- Client:packetReceived. Every later info in that packet is dropped, the
-- objects those infos belonged to never reach SYNCHRONIZED, canStartMission
-- stays false, and Start never appears. It is intermittent because which
-- infos share a packet is packing luck.
--
-- Two throws have to be stopped, not one. Returning "" from the helper only
-- moves the crash one frame down: the loading data then has no store item and
-- the load call indexes a table that was never filled.
--
-- Installed at NetworkSync file load so the wraps exist before any join
-- packet can arrive. Client-side in effect; installing on a dedicated host is
-- harmless and also covers host save load.
-- =========================================================

local BLANK_KEY = "_rfNsBlankNetworkFilename"
local INSTALLED_KEY = "_rfNsBlankFilenameGuardInstalled"

local loggedConvert = false
local loggedSkip = {}

-- ---------------------------------------------------------
-- 1. NetworkUtil.convertFromNetworkFilename
-- ---------------------------------------------------------
-- A free function, not a colon method, so this is a plain local-old wrap.
-- Utils.overwrittenFunction is wrong here: it passes the first argument as
-- `self` and the old function as `superFunc`, which would shift the filename
-- out of position.

if NetworkUtil ~= nil
    and type(NetworkUtil.convertFromNetworkFilename) == "function"
    and not NetworkUtil[INSTALLED_KEY] then

    NetworkUtil[INSTALLED_KEY] = true

    local oldConvertFromNetworkFilename = NetworkUtil.convertFromNetworkFilename

    NetworkUtil.convertFromNetworkFilename = function(filename)
        if filename == nil or filename == "" then
            -- Once. A packet that carries one blank name usually carries
            -- several, and the point of this build is a readable log.
            if not loggedConvert then
                loggedConvert = true
                NSLogger.warning("blank network filename in a join packet, returning empty instead of throwing. Objects with this name are skipped, the rest of the packet is read.")
            end
            return ""
        end

        return oldConvertFromNetworkFilename(filename)
    end
end

-- ---------------------------------------------------------
-- 2. Skip the load for an object whose filename was blank
-- ---------------------------------------------------------
-- The brief named Vehicle:readStream / Placeable:readStream /
-- HandTool:readStream as the wrap point. The guard is one frame deeper than
-- that, on the loading data, and the reason is that the shallower wrap cannot
-- express the behaviour the brief asks for.
--
-- Utils.overwrittenFunction hands the wrapper `superFunc`, an all-or-nothing
-- call. The filename is read from the stream INSIDE that call, so a readStream
-- wrapper cannot see it before deciding. Reading the string first to look at it
-- would consume bits the engine then re-reads from the wrong offset and
-- misalign every later info in the packet. The only other way to skip one
-- trailing statement of an engine body is to reimplement the whole body, which
-- for these three means reproducing Vehicle:superClass().readStream, the
-- placeable preplaced-versus-filename branch, both PropertyState reads and the
-- ConfigurationUtil call order, and re-checking all of it against every patch.
--
-- One frame deeper needs none of that. In all three verified bodies the load
-- call is the LAST statement, after every stream read:
--
--   Vehicle:readStream    filename, configurations, propertyState, then
--                         data:loadVehicleOnClient(...)
--   Placeable:readStream  uniqueId or filename plus pose, configurations,
--                         propertyState, then data:loadPlaceableOnClient(...)
--   HandTool:readStream   filename, canBeDropped, then
--                         data:loadHandToolOnClient(...)
--
-- So letting readStream run untouched consumes every CREATE bit exactly as the
-- engine does, and returning early from the load call skips the load and
-- nothing else. readStream still returns normally, so Client:packetReceived
-- can addObject and finish its numInfos loop.
--
-- The blank fact is carried on a field this mod owns rather than on engine
-- state. VehicleLoadingData happens to expose the same fact as `isValid`,
-- which setStoreItem sets from #self.vehicles and which stays false when the
-- store lookup misses, but the placeable and hand tool classes hold it
-- differently and guessing at their internals is how a guard becomes the next
-- crash.

local function installLoadingDataGuard(className, class, loadFnName)
    if class == nil then
        return false
    end

    if class[INSTALLED_KEY] then
        return true
    end

    if type(class.setFilename) ~= "function" or type(class[loadFnName]) ~= "function" then
        NSLogger.warning("blank-filename guard: %s has no setFilename or %s, not wrapped", className, loadFnName)
        return false
    end

    class[INSTALLED_KEY] = true

    class.setFilename = Utils.overwrittenFunction(class.setFilename,
        function(self, superFunc, filename, ...)
            -- Marked before the engine call, not after, so the mark survives
            -- whatever the engine's own lookup does with an empty name.
            if filename == nil or filename == "" then
                self[BLANK_KEY] = true
            end

            return superFunc(self, filename, ...)
        end)

    class[loadFnName] = Utils.overwrittenFunction(class[loadFnName],
        function(self, superFunc, ...)
            if self[BLANK_KEY] then
                if not loggedSkip[className] then
                    loggedSkip[className] = true
                    NSLogger.warning("skipping %s for an object with a blank filename. The packet is still read to the end.", loadFnName)
                end

                -- No async callback is run, so no OBJECT_LOADED and no
                -- onObjectFinishedAsyncLoading for this id. That is deliberate:
                -- the server keeps the id delayed and will not SYNC it. The
                -- ghost is left registered on purpose as well, because deleting
                -- it would make a later SYNC find no object and return out of
                -- the whole packet, which is the other abort this build is not
                -- allowed to trade for.
                return
            end

            return superFunc(self, ...)
        end)

    return true
end

installLoadingDataGuard("VehicleLoadingData", VehicleLoadingData, "loadVehicleOnClient")
installLoadingDataGuard("PlaceableLoadingData", PlaceableLoadingData, "loadPlaceableOnClient")
installLoadingDataGuard("HandToolLoadingData", HandToolLoadingData, "loadHandToolOnClient")
