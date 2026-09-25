# Changelog

All notable changes to FS25_NetworkSync will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Changelog tracking for this mod begins **2026-08-22** under the suite-wide ruling
(see the ecosystem ledger, entry for Arissani and Wizard). Prior history lives in
the repo's git history and README.

---

## [Unreleased]

### Fixed
- **A mod that asks the server for an action with a keyed table is now warned once, in the log, on any machine.** The transport carries a positional array only, so such a request reached the server empty from a joined client while working for the host; the warning names the action and the key so the defect shows in singleplayer and host testing. Nothing is refused and nothing on the wire changes.

## [2.1.0.0] - 2026-09-18

### Added
- **Scoped per-connection delivery (NS-7).** An opt-in private route beside the public batch: `registerScopedModule`, `unregisterScopedModule`, `requestScopedFull` and `getScopedCapabilities` on the existing handle. Each subscribed client gets its own publication built from a trusted server-side actor context (farm, user, WAITING/RESOLVED/SPECTATOR/INVALID); chunks carry a full identity and are applied atomically only on the consumer's APPLIED result; unsupported protocol or application versions are terminal and never fall back to public traffic. A scoped id never enters the public frames.

### Changed
- `FS25_StateLedger` is no longer a hard modDesc dependency. NetworkSync runs its service standalone; the optional ledger bridges in companion mods are unaffected.

## [2.0.1.0] - 2026-08-26

### Added
- Changelog file established (suite ruling 2026-08-22).

### Fixed
- A blank join filename no longer aborts the rest of the sync during multiplayer join.
- The join handshake now latches on the server's reply instead of the client's own send.

## [2.0.0.1] - 2026-08-22

- First entry under changelog tracking.
