# MAINTENANCE row 118 mutation battery: the sync and action events' count bounds and the sync
# event's wrong-side return (src/RealisticFarmingSyncEvent.lua). Rows live in
# NS-118-readstream_count_bounds_test.lua; the other bars run with it.
#
# Each mutation removes or bends one clause and must be KILLED by a named row. For each:
# assert the edit LANDED (exact occurrence count), run the suite, record KILLED/SURVIVED with
# the named rows, restore byte-for-byte and PROVE the restore with a hash. "DID NOT APPLY"
# never counts as a kill. KILLED* means killed only by a Lua error: a weak kill, a failure.
#
# Not run, and why:
# - the chunker's own ceilings (NetworkSync.lua's estimate and split): the writer, not the
#   reader; B1 pins that the reader's bounds equal its arithmetic, and a drifted reader bound
#   is mutated below (D1, D2).
# - the scoped event's MAX_BODY_TOKENS refusal: the model this follows, untouched here.
#
# Anchors are written with "\n"; in a CRLF file they are matched as "\r\n".
#
# Usage (from the repo root): py tools/test/mutate_readstream_count_bounds.py [id-prefix ...]
import hashlib, os, re, subprocess, sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
def p(rel): return os.path.join(ROOT, rel)

EV = "src/RealisticFarmingSyncEvent.lua"

MUTATIONS = [
 ("C1-frame-count-unbounded", EV,
  [("    if not countWithinBound(count, RealisticFarmingSyncEvent.MAX_EVENT_FRAMES) then\n        self.malformed = \"FRAME_COUNT_OUT_OF_RANGE\"\n        return\n    end\n", "", 1)],
  "a forged frame count is looped on"),
 ("C2-value-count-unbounded", EV,
  [("        if not countWithinBound(n, RealisticFarmingSyncEvent.MAX_FRAME_VALUES) then\n            self.malformed = \"VALUE_COUNT_OUT_OF_RANGE\"\n            return\n        end\n", "", 1)],
  "a forged value count is looped on"),
 ("C3-arg-count-unbounded", EV,
  [("    if not countWithinBound(n, RealisticFarmingActionEvent.MAX_ARGS) then\n        self.malformed = \"ARG_COUNT_OUT_OF_RANGE\"\n        return\n    end\n", "", 1)],
  "a forged action arg count is looped on"),
 ("W1-sync-reads-on-a-server", EV,
  [("    if g_currentMission ~= nil and g_currentMission:getIsServer() then\n        self.malformed = \"WRONG_SIDE\"\n        return\n    end\n    local count = streamReadInt32(streamId)\n",
    "    local count = streamReadInt32(streamId)\n", 1)],
  "a server reads a client's forged sync event before run refuses it"),
 ("O1-bound-is-exclusive", EV,
  [("    return type(n) == \"number\" and n >= 0 and n <= bound\n", "    return type(n) == \"number\" and n >= 0 and n < bound\n", 1)],
  "the writer's fullest frame and fullest action are refused"),
 ("N1-nil-count-passes", EV,
  [("    return type(n) == \"number\" and n >= 0 and n <= bound\n", "    return n == nil or (type(n) == \"number\" and n >= 0 and n <= bound)\n", 1)],
  "a short stream is looped on"),
 ("D1-value-bound-drifted-low", EV,
  [("RealisticFarmingSyncEvent.MAX_FRAME_VALUES = 4096", "RealisticFarmingSyncEvent.MAX_FRAME_VALUES = 2048", 1)],
  "the reader refuses a frame the chunker legitimately fills"),
 ("D2-frame-bound-drifted-low", EV,
  [("RealisticFarmingSyncEvent.MAX_EVENT_FRAMES = 512", "RealisticFarmingSyncEvent.MAX_EVENT_FRAMES = 256", 1)],
  "the reader refuses a batch the sender legitimately builds"),
 ("D3-value-bound-drifted-high", EV,
  [("RealisticFarmingSyncEvent.MAX_FRAME_VALUES = 4096", "RealisticFarmingSyncEvent.MAX_FRAME_VALUES = 8192", 1)],
  "the reader accepts twice what the chunker can write"),
]

def sha(b): return hashlib.sha256(b).hexdigest()


def run_suite():
    r = subprocess.run(["node", "run-tests.mjs"], cwd=os.path.join(ROOT, "tools", "test"),
                       capture_output=True, text=True, encoding="utf-8", errors="replace")
    out = r.stdout + r.stderr
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
    print("  %s %s  [%s]" % (tag, mid, rel))
    print("        (%s)" % why)
    for l in named[:4]:
        print("        " + l[:170])
    for l in crashes[:2]:
        print("        CRASH " + l[:170])

print("\n==== MUTATION RESULT ====")
print("killed   %d (of which %d only by a Lua error, marked KILLED*)" % (len(killed), len(crashkills)))
print("survived %d" % len(survived))
for mid, why in survived:
    print("   SURVIVED %s: %s" % (mid, why))
print("bad edit %d" % len(badedit))
for mid, why in badedit:
    print("   BAD EDIT %s: %s" % (mid, why))
print("all files restored byte-identical (hash-checked per mutation)")
sys.exit(1 if (survived or badedit or crashkills) else 0)
