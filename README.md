# FS25_NetworkSync

**Version:** 1.0.0.0
**Author:** TisonK

The multiplayer network bedrock of the Realistic Farming mod ecosystem. NetworkSync is mod 2 in the load order (after StateLedger). It replaces each companion mod's own network event classes with one batched sync cycle: companions register a schema and call `markDirty`, and NetworkSync serializes, batches at 1Hz, and delivers. This cuts multiplayer traffic and stops the cross-mod rubber-banding that comes from many mods each syncing on their own timer.

There are no settings and nothing to configure. Install it, keep it loaded, and let the companion mods use it.

## How companion mods use it

Register once, at init, guarding against NetworkSync being absent:

```lua
if g_networkSync then
    g_networkSync:registerModule("MyMod_Sync", {
        channel      = "MyMod_Sync",
        onWriteState = function()        return self:toArray() end,   -- server: integer-indexed array
        onReadState  = function(array)   self:fromArray(array) end,   -- client: apply the array
    })
end

-- whenever multiplayer-relevant state changes:
if g_networkSync then g_networkSync:markDirty("MyMod_Sync") end
```

- **onWriteState** runs on the server and returns a strict integer-indexed array (values may be booleans, integers, floats, or strings). Runs inside `pcall`; a failure skips that module for the tick without affecting others.
- **onReadState** runs on the client with the same array. Append new fields at the end of the array as your schema grows, never insert in the middle, so older clients that receive a shorter array simply ignore the missing tail. If you need an explicit schema version, put it in `array[1]`.
- Call **markDirty** only when state actually changes, not every frame. The 1Hz loop batches every dirty module into one packet.

A mod that needs schema isolation registers more than one modId (for example `WorkerCosts_Roster` and `WorkerCosts_HireHall`), exactly like StateLedger.

## Value encoding

Each array element is sent with a one-byte type tag: booleans as a bool, whole numbers within 32-bit range as an exact int32 (so money, counts, and ids round-trip exactly), other numbers as float32, and strings as strings. This fixes the fidelity loss of an all-float32 array while keeping packets compact.

## Join sync

When a client joins mid-game it requests a full snapshot from the server (retried until the connection is ready), and the server answers that one connection with every registered module's current state. After that, the client receives only the 1Hz dirty deltas.

For a change that must reach clients immediately rather than waiting up to a second (for example an admin setting applied server-side), the server can call `g_networkSync:syncNow(modId)`.

## Single-player

A no-op. The batch loop runs but broadcasts to no one; companions call `markDirty` without needing to know whether they are in single-player or multiplayer.

## Console

- `nsStatus` - list registered modules, the local role (server/client), and dirty flags.
