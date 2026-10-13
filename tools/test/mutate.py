# NetworkSync stream-harness mutation battery.
#
# The point of this file is narrow: the typed stream mock added 2026-09-19 is only
# worth having if it DETECTS. A green suite after adding type tags proves nothing,
# because the suite was green before them too. Each mutation below introduces the
# kind of wire defect the tags exist to catch, and must turn the StreamAudit rows red.
#
# For each mutation: assert the edit LANDED (exact occurrence count), run the suite,
# record KILLED/SURVIVED with the NAMED rows that failed, restore byte-for-byte and
# prove the restore with a hash. A no-op edit is indistinguishable from an unpinned
# rule: both report SURVIVED, which is why the count assert is not optional.
#
# Anchors are written with "\n"; in a CRLF file they are matched as "\r\n".
#
# Usage: py tools/test/mutate.py [id-prefix ...]
import hashlib, os, re, subprocess, sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
def p(rel): return os.path.join(ROOT, rel)

SCOPED = "src/NetworkSyncScopedEvent.lua"
SYNC   = "src/RealisticFarmingSyncEvent.lua"

# (id, file, [(old, new, want), ...], the wire defect it introduces)
MUTATIONS = [
 ("M1-type-drift-int-read-as-string", SCOPED,
  [("    self.protocolVersion = streamReadInt32(streamId)",
    "    self.protocolVersion = streamReadString(streamId)", 1)],
  "the reader takes a string where the writer put an Int32: a classic type drift"),

 ("M2-type-drift-write-side", SCOPED,
  [("    streamWriteInt32(streamId, self.protocolVersion)",
    "    streamWriteString(streamId, self.protocolVersion)", 1)],
  "the writer emits a string where the reader expects an Int32, same drift from the other side"),

 ("M3-count-drift-reader-reads-one-extra", SCOPED,
  [("    local n = streamReadInt32(streamId)",
    "    local n = streamReadInt32(streamId); streamReadInt32(streamId)", 1)],
  "the reader consumes one field more than the writer wrote: underflow at the end of the frame"),

 ("M5-short-read-leaves-residue", SCOPED,
  # The OTHER half of count. StreamClean cannot see this: reading SHORT of the end
  # leaves r < w with zero underflows and zero type errors. The engine checks exactly
  # this at network/Server.lua:443-444 and reports "Not all bits read in event".
  [("    self.protocolVersion = streamReadInt32(streamId)\n    local n = streamReadInt32(streamId)",
    "    self.protocolVersion = streamReadInt32(streamId)\n    local n = 0", 1)],
  "the reader stops one field short, leaving bytes on the wire that the engine would reject"),

 ("M6-dropped-length-prefix-raises-inside-readStream", SCOPED,
  # The one path the other five miss, and the only one that can exercise the raise
  # row. Those five all crash DOWNSTREAM in the test file, which aborts it before any
  # end-of-file row can run, so the raise row never fires and cannot be proven.
  #
  # Dropping the length prefix makes the writer emit token strings where the count
  # belonged. The reader's streamReadInt32 pulls a cell tagged "str", counts a type
  # error, and returns a string; `n < 0` at :127 then raises "attempt to compare
  # string with number" INSIDE readStream, inside deliver's pcall. The raise is
  # caught, the file continues, and report() can finally assert on raises.
  #
  # It is also a realistic defect rather than a contrived one: dropping a length
  # prefix is an ordinary editing mistake. A mutation tuned until it dies proves
  # nothing; one a developer could plausibly write proves something.
  [("    local n = #self.tokens\n    streamWriteInt32(streamId, n)",
    "    local n = #self.tokens", 1)],
  "the length prefix is dropped, so the reader compares a string against a number inside readStream"),

 ("M4-order-swap-on-the-write-side", SYNC,
  [("    streamWriteInt32(streamId, #self.frames)",
    "    streamWriteString(streamId, tostring(#self.frames))", 1)],
  "the frame count changes type, so every field after it is read against the wrong tag"),
]


def sha(b): return hashlib.sha256(b).hexdigest()


def run_suite():
    r = subprocess.run(["node", "run-tests.mjs"], cwd=os.path.join(ROOT, "tools", "test"),
                       capture_output=True, text=True, encoding="utf-8", errors="replace")
    out = r.stdout + r.stderr
    # Strip ANSI colour AND non-ASCII. This console is cp1252 and the runner emits a
    # check glyph; printing it raises UnicodeEncodeError, which aborts the battery
    # midway and looks like a hang rather than a failure.
    strip = lambda l: (re.sub(r"\x1b\[[0-9;]*m", "", l).strip()
                       .encode("ascii", "replace").decode("ascii"))
    fails = [strip(l) for l in out.splitlines() if "FAIL" in l and "assertions passed" not in l]
    crashes = [strip(l) for l in out.splitlines() if "Lua error while loading/running" in l]
    return r.returncode, fails, crashes


only = sys.argv[1:]
rc, fails, crashes = run_suite()
if rc != 0:
    print("BASELINE IS NOT GREEN; fix that before trusting any mutation result.")
    for l in fails[:10]:
        print("   " + l)
    sys.exit(2)
print("baseline green")

killed, crashkills, survived, badedit = [], [], [], []

for mid, rel, edits, why in MUTATIONS:
    if only and not any(mid.startswith(o) for o in only):
        continue
    path = p(rel)
    with open(path, "rb") as f:
        original = f.read()
    crlf = b"\r\n" in original
    enc = lambda s: (s.replace("\n", "\r\n") if crlf else s).encode("utf-8")

    ok, mutated = True, original
    for old, new, want in edits:
        ob, nb = enc(old), enc(new)
        n = mutated.count(ob)
        if n != want:
            badedit.append((mid, "anchor matched %dx, expected %d" % (n, want)))
            print("  !! %s: ANCHOR MISMATCH (%d != %d), mutation NOT applied" % (mid, n, want))
            ok = False
            break
        mutated = mutated.replace(ob, nb, want)
    if not ok:
        continue

    with open(path, "wb") as f:
        f.write(mutated)
    with open(path, "rb") as f:
        landed = f.read()
    if landed == original or landed != mutated:
        with open(path, "wb") as f:
            f.write(original)
        badedit.append((mid, "edit did not land"))
        print("  !! %s: EDIT DID NOT LAND" % mid)
        continue

    try:
        rc, fails, crashes = run_suite()
    finally:
        with open(path, "wb") as f:
            f.write(original)
    with open(path, "rb") as f:
        if sha(f.read()) != sha(original):
            print("  !! %s: RESTORE FAILED, stopping" % mid)
            sys.exit(3)

    named = [l for l in fails if l.startswith("FAIL ")]
    if rc != 0:
        killed.append(mid)
        tag = "KILLED  "
        if crashes and not named:
            crashkills.append(mid)
            tag = "KILLED* "
    else:
        survived.append((mid, why))
        tag = "SURVIVED"
    print("  %s %s  (%s)" % (tag, mid, why))
    for l in named[:5]:
        print("        " + l[:170])
    for l in crashes[:2]:
        print("        CRASH " + l[:170])

print("\n==== MUTATION RESULT ====")
print("killed   %d (of which %d only by a Lua error, marked KILLED*)" % (len(killed), len(crashkills)))
print("survived %d" % len(survived))
print("bad edit %d" % len(badedit))
for mid, why in survived:
    print("--- SURVIVED %s: %s" % (mid, why))
for mid, msg in badedit:
    print("--- BAD EDIT %s: %s" % (mid, msg))
print("all files restored byte-identical (hash-checked per mutation)")
sys.exit(1 if (survived or badedit or crashkills) else 0)
