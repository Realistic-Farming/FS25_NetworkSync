# FS25_NetworkSync

**Version:** 2.0.0.0
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

When a client joins mid-game it requests a full snapshot from the server (retried until the connection is ready), and the server answers that one connection with every registered module's current state. After that, the client receives only the 1Hz dirty updates.

For a change that must reach clients immediately rather than waiting up to a second (for example an admin setting applied server-side), the server can call `g_networkSync:syncNow(modId)`.

## Sending only what changed (v2, optional)

Everything above is unchanged from v1. The v2 additions are additive opt-ins: a module that ignores them behaves exactly as it did in v1.

A heavy module that changes one field at a time does not need to resend its whole array every tick. Register two more callbacks and NetworkSync sends only the changed elements:

```lua
g_networkSync:registerModule("MyMod_Sync", {
    channel      = "MyMod_Sync",
    onWriteState = function()       return self:toArray() end,        -- still required (snapshots + drift floor)
    onReadState  = function(array)  self:fromArray(array) end,
    onWriteDelta = function()       return self:changedPairs() end,   -- server: { idx1, val1, idx2, val2, ... }
    onReadDelta  = function(pairs)  self:applyPairs(pairs) end,       -- client: apply the changed indices
})
```

- The 1Hz batch sends `onWriteDelta` (mode DELTA) when it is present; the join snapshot, `syncNow`, and the drift floor always send the full `onWriteState`, so `onWriteState` stays required.
- Two guarantees hold the client consistent: a client applies no delta for a module until it has applied that module's full snapshot (deltas that arrive first are held, then flushed in order), and a slow full resync (the drift floor) self-heals any missed delta within a bounded window.

## Large modules (v2)

Any module whose payload for a tick exceeds a safe per-event size is split into ordered chunks that reassemble on the client, including the join snapshot. A module that fits one event is a single chunk, so nothing changes for light modules. This is automatic; companions do nothing.

## Server-authoritative actions (v2)

The one sanctioned client-initiated path. A client asks the server to run a named action; the server authorizes the requester and applies it, and the resulting state change flows back down through the normal sync path. Use it for admin-gated or server-authoritative actions (settings, recovery hatches), never routine gameplay traffic.

```lua
-- server: register the handler once
g_networkSync:registerAction("MyMod_Reset", {
    onAction  = function(userId, args) self:resetField(args[1]) end,
    adminOnly = true,   -- default true: only a master user (admin) may run it
})

-- client (or host): request it
g_networkSync:requestAction("MyMod_Reset", { fieldId })
```

An unauthorized, unknown, or malformed request applies nothing (a plain server-side early return). On a listen-server host the request applies directly.

## Single-player

A no-op. The batch loop runs but broadcasts to no one; companions call `markDirty` without needing to know whether they are in single-player or multiplayer.

## Console

- `nsStatus` - list registered modules, the local role (server/client), and dirty flags.
