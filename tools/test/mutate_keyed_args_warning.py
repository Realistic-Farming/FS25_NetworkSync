# NetworkSync keyed-args warning battery: requestAction warns once per action when args carries a
# key outside 1..#args, on both paths, and never refuses (src/NetworkSync.lua _warnKeyedArgs and its
# call). Rows live in NS-keyed_args_warning_test.lua; the other bars run with it.
#
# Each mutation bends one clause and must be KILLED by a named row. For each: assert the edit
# LANDED (exact occurrence count), run the suite, record KILLED/SURVIVED with the named rows,
# restore byte-for-byte and PROVE the restore with a hash. "DID NOT APPLY" never counts as a
# kill. KILLED* means killed only by a Lua error: a weak kill, a failure.
#
# Not run, and why:
# - the wire itself (writeStream writing args[1..#args]): the stream-harness battery's subject
#   (mutate.py); row C2 reads it, does not change it.
# - an array with a hole: Lua's length operator answers any border, so the guard sees a hole
#   only sometimes; no row pins it (the bar says so at H5).
#
# Anchors are written with "\n"; in a CRLF file they are matched as "\r\n".
#
# Usage (from the repo root): py tools/test/mutate_keyed_args_warning.py [id-prefix ...]
import hashlib, os, re, subprocess, sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
def p(rel): return os.path.join(ROOT, rel)

NS = "src/NetworkSync.lua"

MUTATIONS = [
 ("G1-guard-not-called", NS,
  [("function NetworkSync:requestAction(actionId, args)\n    self:_warnKeyedArgs(actionId, args)\n",
    "function NetworkSync:requestAction(actionId, args)\n", 1)],
  "a keyed table is never warned on either path"),
 ("G0-memo-keyed-on-the-raw-id", NS,
  [("            local memo = tostring(actionId)\n", "            local memo = actionId\n", 1)],
  "a nil action id with keyed args raises 'table index is nil' in the caller (Bob's MAJOR on #11)"),
 ("G2-warns-every-time", NS,
  [("            if not self.keyedArgsWarned[memo] then\n                self.keyedArgsWarned[memo] = true\n",
    "            if true then\n", 1)],
  "every keyed request warns again: the log floods"),
 ("G3-warning-refuses", NS,
  [("function NetworkSync:requestAction(actionId, args)\n    self:_warnKeyedArgs(actionId, args)\n",
    "function NetworkSync:requestAction(actionId, args)\n    if self:_warnKeyedArgs(actionId, args) then return false end\n", 1)],
  "a keyed request is refused instead of warned: a host with an older caller breaks"),
 ("G4-only-string-keys-count", NS,
  [("        if type(k) ~= \"number\" or k < 1 or k > n or math.floor(k) ~= k then\n",
    "        if type(k) ~= \"number\" then\n", 1)],
  "a non-integer key is not seen as keyed"),
 ("G5-positional-arrays-warned-too", NS,
  [("        if type(k) ~= \"number\" or k < 1 or k > n or math.floor(k) ~= k then\n",
    "        if true then\n", 1)],
  "a proper array is warned as if keyed"),
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
