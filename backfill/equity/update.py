"""The daily equity update: runs the existing equity CLIs in order, each as a subprocess with the same Python.

    reference    nse_reference.py                                   (only with --with-reference; network)
    bhavcopy     cm_bhav.py --missing                               (network)
    master       security_master.py build, security_master.py check
    actions      adjustments.py fetch-actions                       (network)
    adjustments  adjustments.py build, adjustments.py check
    exits        exits.py build, exits.py check

No rule or data logic of its own: every decision is the step's. It only decides what to run (--offline skips the
network steps; a build whose build_info.json is newer than all its inputs is "up to date"), classifies the exit
codes (a download's 1 is a WARNING and the update goes on; a build's or check's non-zero, or any 2, stops it),
logs every output to <folder>/equity_update.log and prints a summary. Its own lock: <folder>/equity_update.lock."""
import os
import re
import subprocess
import sys
import time
from glob import glob

from backfill.cm_bhav import READY_AT, now_ist, store_dir
from backfill.equity import adjustments as adj
from backfill.equity import exits as ex
from backfill.equity import security_master as sm
from backfill.locks import LockBusy, RunLock  # noqa: F401 - LockBusy is re-exported for the CLI
from config import equity_data_dir

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG = "equity_update.log"
LOCK = "equity_update.lock"
TAIL = 20                 # output lines shown on screen for a warning or a failure
CRASH = "Traceback (most recent call last)"

# name, network step?, commands (script + arguments)
STEPS = [
    ("reference", True, [["nse_reference.py"]]),
    ("bhavcopy", True, [["cm_bhav.py", "--missing"]]),
    ("master", False, [["security_master.py", "build"], ["security_master.py", "check"]]),
    ("actions", True, [["adjustments.py", "fetch-actions"]]),
    ("adjustments", False, [["adjustments.py", "build"], ["adjustments.py", "check"]]),
    ("exits", False, [["exits.py", "build"], ["exits.py", "check"]]),
]
STEP_NAMES = [s[0] for s in STEPS]

N = r"(\d(?:[\d,]*\d)?)"     # a number as the CLIs print it (1,234), never a trailing comma
# key numbers: (regex, format) on the last matching line of the step's output; "-" when nothing matches
NUMBERS = {
    "reference": [(r"Downloaded " + N + r" new file\(s\)", "downloads {0}")],
    "bhavcopy": [(r"This run: ok " + N + r", holidays " + N + r", failed " + N + r", pending " + N,
                  "ok {0}, holiday {1}, failed {2}, pending {3}")],
    "master": [(r"^  securities: " + N, "securities {0}")],
    "actions": [(r"^saved " + N + r", skipped " + N + r", failed " + N, "saved {0}, skipped {1}, failed {2}")],
    "adjustments": [(r"^  applied: " + N + r" \(", "applied {0}")],
    "exits": [(r"^  EXITED: " + N, "exited {0}"), (r"^Review rows: " + N, "review {0}")],
}
CHECK_LINE = re.compile(r"^(Check: (?:ok|FAILED)|Known-case checks: (?:all found|SOME NOT FOUND))\s*$", re.M)


def ist(now=None):
    return f"{(now or now_ist()):%Y-%m-%d %H:%M:%S} IST"


# ---- freshness (the same comparisons the CLIs make for their stale checks, plus their input folders) ----

def inputs_of(folder, name):
    """(output build_info.json, [input files]) of a build step."""
    years = sm.year_files(store_dir(folder))
    master = os.path.join(sm.out_dir(folder), "build_info.json")
    factors = os.path.join(adj.out_dir(folder), "build_info.json")
    if name == "master":
        return master, years
    if name == "adjustments":
        return factors, [master] + sorted(glob(os.path.join(adj.out_dir(folder), adj.INPUT, "*.csv"))) \
            + [os.path.join(adj.out_dir(folder), adj.MANUAL)] + years
    refs = [os.path.join(ex.reference_dir(folder), n) for n in ex.REFERENCE_FILES]
    return os.path.join(ex.out_dir(folder), "build_info.json"), [master, factors] + refs + years


def freshness(folder, name):
    """(up_to_date, text): up to date when the build's build_info.json is at least as new as every input that
    exists. A missing build_info.json is never up to date."""
    out, ins = inputs_of(folder, name)
    if not os.path.isfile(out):
        return False, f"no {os.path.relpath(out, folder)} yet"
    have = [p for p in ins if os.path.isfile(p)]
    newer = [p for p in have if os.path.getmtime(p) > os.path.getmtime(out)]
    if newer:
        newest = max(newer, key=os.path.getmtime)
        return False, f"{os.path.relpath(newest, folder)} is newer than {os.path.relpath(out, folder)}" + \
            (f" (and {len(newer) - 1} more)" if len(newer) > 1 else "")
    return True, f"{os.path.relpath(out, folder)} is newer than its {len(have)} inputs"


# ---- running one command ----

def command_text(argv):
    return "python " + " ".join(f'"{a}"' if " " in a else a for a in argv)


def run_step(name, argv, log):
    """Run one CLI (argv = script + arguments) with this Python, no shell, from the repo root. The output (stderr
    merged into stdout) and the exit code are appended to the log. Returns (exit code, output)."""
    log(f"----- {name}: {command_text(argv)} ({ist()})\n")
    start = time.monotonic()
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    p = subprocess.run([sys.executable, os.path.join(REPO, argv[0]), *argv[1:]], cwd=REPO, env=env,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8",
                       errors="replace")
    output = p.stdout or ""
    log(output + ("" if output.endswith("\n") or not output else "\n"))
    log(f"----- exit {p.returncode}, {time.monotonic() - start:.1f} s\n")
    return p.returncode, output


def key_numbers(name, outputs):
    text = "\n".join(outputs)
    parts = []
    for rx, fmt in NUMBERS[name]:
        found = re.findall(rx, text, re.M)
        values = (found[-1] if isinstance(found[-1], tuple) else (found[-1],)) if found else ("-",) * re.compile(rx).groups
        parts.append(fmt.format(*values))
    checks = CHECK_LINE.findall(text)
    if len(outputs) > 1:
        parts.append(f"check {'ok' if checks and checks[-1] in ('Check: ok', 'Known-case checks: all found') else '-'}"
                     if checks else "check -")
    return "; ".join(parts)


def tail(output, n=TAIL):
    return output.rstrip("\n").splitlines()[-n:]


# ---- the update ----

def plan(offline=False, with_reference=False, from_step=None):
    """[(name, network, commands, skip reason or None)] before freshness, which is checked when a build is reached."""
    start = STEP_NAMES.index(from_step) if from_step else 0
    out = []
    for i, (name, network, cmds) in enumerate(STEPS):
        reason = None
        if i < start:
            reason = "before --from-step"
        elif name == "reference" and not with_reference:
            reason = "not asked; add --with-reference"
        elif network and offline:
            reason = "--offline"
        out.append((name, network, cmds, reason))
    return out


def with_folder(cmd, folder_arg):
    return cmd + (["--folder", folder_arg] if folder_arg else [])


def summary(results):
    rows = [("step", "result", "time", "key numbers")] + [
        (r["step"], r["result"], f"{r['time']:.0f} s" if r["time"] is not None else "-", r["numbers"])
        for r in results]
    w = [max(len(row[i]) for row in rows) for i in range(3)]
    lines = [" | ".join([row[0].ljust(w[0]), row[1].ljust(w[1]), row[2].rjust(w[2]), row[3]]).rstrip()
             for row in rows]
    lines.insert(1, "-+-".join(["-" * w[0], "-" * w[1], "-" * w[2], "-" * 11]))
    return "\n".join(lines)


def run(folder_arg=None, offline=False, with_reference=False, force=False, from_step=None, dry_run=False,
        runner=run_step, now=None, out=None, argv=None):
    """Run the update; returns the exit code: 0 all OK (warnings allowed), 1 a build or check failed (or a step
    crashed), 2 a step answered 2 (lock busy / arguments) or this update's own lock is busy."""
    out = out or print
    folder = folder_arg or equity_data_dir()
    now = now or now_ist()
    steps = plan(offline, with_reference, from_step)
    if any(name == "bhavcopy" and reason is None for name, _, _, reason in steps) and now.time() < READY_AT:
        out(f"Note: it is {now:%H:%M} IST, before {READY_AT:%H:%M}: today's bhavcopy may not be published yet "
            "(cm_bhav marks it pending).")
    if dry_run:
        out(f"Dry run (nothing is run) for {folder}:")
        for i, (name, network, cmds, reason) in enumerate(steps, 1):
            if reason:
                out(f"[{i}/{len(steps)}] {name}: skipped ({reason})")
                continue
            if not network and not force:
                fresh, text = freshness(folder, name)
                state = f"up to date now ({text}); an earlier step may change that" if fresh else f"would run ({text})"
            else:
                state = "would run" + (" (--force)" if not network else "")
            out(f"[{i}/{len(steps)}] {name}: {state}")
            for cmd in cmds:
                out(f"      {command_text(with_folder(cmd, folder_arg))}")
        return 0

    try:
        lock = RunLock(os.path.join(folder, LOCK)).acquire("equity_update.py")
    except LockBusy as e:
        out(f"Not run: {e}")
        return 2
    try:
        with open(os.path.join(folder, LOG), "a", encoding="utf-8", newline="\n") as fh:
            def log(text):
                fh.write(text)
                fh.flush()
            log(f"===== equity_update {ist(now)} argv: {' '.join(argv or [])}\n")
            code, results = execute(steps, folder, folder_arg, force, runner, log, out)
            table = summary(results)
            log(table + f"\n===== exit {code}\n")
    finally:
        lock.release()
    out("")
    out(table)
    warnings = [r["step"] for r in results if r["result"] == "WARNING"]
    if warnings:
        out(f"Warnings: {', '.join(warnings)} (see {os.path.join(folder, LOG)})")
    out(f"Log: {os.path.join(folder, LOG)}")
    return code


def execute(steps, folder, folder_arg, force, runner, log, out):
    results, code, stopped = [], 0, False
    for i, (name, network, cmds, reason) in enumerate(steps, 1):
        head = f"[{i}/{len(steps)}] {name}"
        if stopped:
            results.append({"step": name, "result": "not run", "time": None, "numbers": "-"})
            continue
        if reason:
            results.append({"step": name, "result": "SKIPPED", "time": None, "numbers": reason})
            out(f"{head} ... SKIPPED ({reason})")
            log(f"----- {name}: SKIPPED ({reason})\n")
            continue
        if not network and not force:
            fresh, text = freshness(folder, name)
            if fresh:
                results.append({"step": name, "result": "up to date", "time": None, "numbers": text})
                out(f"{head} ... up to date ({text})")
                log(f"----- {name}: up to date ({text})\n")
                continue
        start, outputs, result = time.monotonic(), [], "OK"
        for cmd in cmds:
            rc, output = runner(name, with_folder(cmd, folder_arg), log)
            outputs.append(output)
            if rc == 0:
                continue
            if network and rc == 1 and CRASH not in output:
                result = "WARNING"
                continue
            result = "FAILED"
            code = 2 if rc == 2 else 1
            stopped = True
            break
        secs = time.monotonic() - start
        numbers = key_numbers(name, outputs)
        if result == "FAILED":
            numbers = f"exit {rc} from {' '.join(cmd)}; " + numbers
        results.append({"step": name, "result": result, "time": secs, "numbers": numbers})
        out(f"{head} ... {result} ({secs:.0f} s)")
        if result in ("WARNING", "FAILED"):
            for line in tail(outputs[-1]):
                out(f"      {line}")
    return code, results
