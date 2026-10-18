"""backfill/equity/update.py and equity_update.py with a fake subprocess.run (no real subprocess, no network) and
fake build_info.json / input files with set times in tmp_path; .env is never read (config.load_env stubbed)."""
import os
import subprocess
from datetime import datetime

import pytest

import config
import equity_update as cli
from backfill.cm_bhav import IST
from backfill.equity import exits as ex
from backfill.equity import update as upd
from backfill.locks import RunLock

EVENING = datetime(2026, 10, 19, 20, 0, tzinfo=IST)
MORNING = datetime(2026, 10, 19, 10, 0, tzinfo=IST)
T0 = 1_700_000_000         # input files
T1 = T0 + 1000             # build outputs (newer: up to date)

OUTPUT = {
    ("nse_reference.py",): "Saved in X\nDownloaded 3 new file(s), 12 request(s).\n",
    ("cm_bhav.py", "--missing"): "Saved in X\nThis run: ok 1, holidays 0, failed 0, pending 1, already done 2900; "
                                 "4 request(s)\n",
    ("security_master.py", "build"): "Totals:\n  securities: 3,702\nKnown-case checks: all found\n",
    ("security_master.py", "check"): "  [found] HDFC\nKnown-case checks: all found\n",
    ("adjustments.py", "fetch-actions"): "saved 1, skipped 11, failed 0 (years 2015 to 2026)\n",
    ("adjustments.py", "build"): "Events:\n  applied: 822 (SPLIT_OR_BONUS 821, CONSOLIDATION 1, OTHER 0), x\n",
    ("adjustments.py", "check"): "Known cases: all found\nCheck: ok\n",
    ("exits.py", "build"): "Status:\n  EXITED: 714\nReview rows: 271 (unknown_large 50)\n",
    ("exits.py", "check"): "Known cases: all found\nCheck: ok\n",
    ("index_membership.py", "fetch"): "amfi: saved 1, refreshed 1, skipped 18, not found 0, failed 0\n"
                                      "nifty: saved 0, refreshed 8, skipped 0, not found 0, failed 0\n"
                                      "changes: saved 2, refreshed 1, skipped 254, not found 0, failed 1\n"
                                      "snapshots: saved 0, refreshed 16, skipped 68, not found 0, failed 0\n",
    # the real report's line shapes: coverage "DATE to DATE", reliable-from "DATE    why" / "not reliable  why",
    # snapshots "DATE (file"
    ("index_membership.py", "build"): "Coverage (reliable from the sources):\n"
                                      "  NIFTY 50             2015-01-01 to 2026-10-16  (current list 2026-10-18)\n"
                                      "Reliable from (at the expected size ...):\n"
                                      "  NIFTY 50             2015-01-01    100.0% of 2,911 days at the expected size\n"
                                      "  NIFTY 500            not reliable  no day from which 98% of the days ...\n"
                                      "  NIFTY MIDCAP 150     2016-04-01    98.1% of 2,603 days at the expected size\n"
                                      "Snapshots:\n"
                                      "  NIFTY 50             2016-03-10 (NIFTY_50_20160310050302.csv): same 50\n"
                                      "Known-case checks: all found\n\nReview rows: 205 (unmapped_current: 8)\n",
    ("index_membership.py", "check"): "  [found] RELIANCE\nCheck: ok\n",
}
BUILD_INFO = {"security_master.py": "security_master", "adjustments.py": "adjustments", "exits.py": "exits",
              "index_membership.py": "index_membership"}
INDEX_INPUTS = ["index_membership/input/nifty_current/NIFTY_50_2026-10-18.csv",
                "index_membership/input/nifty_changes/ind_prs10082026.pdf",
                "index_membership/input/snapshots/NIFTY_50_20261015151352.csv",
                "index_membership/input/amfi/AverageMarketCapitalization30Jun2026.xlsx",
                "index_membership/input/manual_changes.csv"]


@pytest.fixture(autouse=True)
def no_env(monkeypatch):
    monkeypatch.setattr(config, "load_env", lambda: {})


def touch(path, t):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("x")
    os.utime(path, (t, t))


def inputs(folder):
    store = os.path.join(folder, "NSE_CM_bhav")
    return ([os.path.join(store, f"{y}.parquet") for y in (2025, 2026)]
            + [os.path.join(folder, "adjustments", "input", "corporate_actions_2026.csv"),
               os.path.join(folder, "adjustments", "manual_factors.csv")]
            + [os.path.join(store, "reference", n) for n in ex.REFERENCE_FILES]
            + [os.path.join(folder, *p.split("/")) for p in INDEX_INPUTS])


def outputs(folder):
    return {k: os.path.join(folder, v, "build_info.json") for k, v in
            (("master", "security_master"), ("adjustments", "adjustments"), ("exits", "exits"),
             ("index", "index_membership"))}


@pytest.fixture
def folder(tmp_path):
    """Inputs at T0, the four build_info.json at T1 (so every build is up to date)."""
    f = str(tmp_path)
    for p in inputs(f):
        touch(p, T0)
    for i, p in enumerate(outputs(f).values()):
        touch(p, T1 + i)
    return f


class FakeRun:
    """Stands in for subprocess.run: records the commands, answers from OUTPUT (or `codes`), and a successful
    build touches its build_info.json (now), like the real build."""

    def __init__(self, folder, codes=None, texts=None):
        self.folder, self.codes, self.texts, self.calls, self.kw = folder, codes or {}, texts or {}, [], []

    def __call__(self, args, **kw):
        assert isinstance(args, list) and "shell" not in kw
        script, rest = os.path.basename(args[1]), args[2:]
        key = (script, *[a for a in rest if a != "--folder" and not os.path.isabs(a)][:1])
        self.calls.append([script, *rest])
        self.kw.append(kw)
        code = self.codes.get(key, 0)
        text = self.texts.get(key, OUTPUT.get(key, "done\n"))
        if code == 0 and key[1:] == ("build",):
            touch(os.path.join(self.folder, BUILD_INFO[script], "build_info.json"), T1 + 100 + len(self.calls))
        return subprocess.CompletedProcess(args, code, stdout=text)

    def scripts(self):
        return [" ".join(c[:2]) if len(c) > 1 and not c[1].startswith("-") or c[1:2] == ["--missing"] else c[0]
                for c in self.calls]


def go(folder, monkeypatch, *argv, codes=None, texts=None, now=EVENING, folder_arg=True):
    fake = FakeRun(folder, codes, texts)
    monkeypatch.setattr(upd.subprocess, "run", fake)
    monkeypatch.setattr(upd, "equity_data_dir", lambda: folder)
    lines = []
    monkeypatch.setattr(upd, "print", lambda *a, **k: lines.append(" ".join(map(str, a))), raising=False)
    args = (["--folder", folder] if folder_arg else []) + list(argv)
    code = cli.main(args, now=now)
    return code, fake, "\n".join(lines)


def log_text(folder):
    with open(os.path.join(folder, upd.LOG), encoding="utf-8") as fh:
        return fh.read()


def row(screen, step):
    return next(x for x in screen.splitlines() if x.startswith(step + " ") and " | " in x)


ALL = ["cm_bhav.py --missing", "security_master.py build", "security_master.py check", "adjustments.py fetch-actions",
       "adjustments.py build", "adjustments.py check", "exits.py build", "exits.py check",
       "index_membership.py build", "index_membership.py check"]
# with --with-reference: nse_reference first, the index fetch between exits and the index build
ALL_REF = ["nse_reference.py"] + ALL[:8] + ["index_membership.py fetch"] + ALL[8:]
INDEX = ["index_membership.py build", "index_membership.py check"]


# ---- order and selection ----

def test_order_with_force(folder, monkeypatch):
    code, fake, screen = go(folder, monkeypatch, "--force")
    assert code == 0
    assert fake.scripts() == ALL
    assert "SKIPPED" in row(screen, "reference") and "add --with-reference" in row(screen, "reference")
    assert "SKIPPED" in row(screen, "index_fetch") and "add --with-reference" in row(screen, "index_fetch")


def test_with_reference_adds_step_1_and_the_index_fetch(folder, monkeypatch):
    code, fake, screen = go(folder, monkeypatch, "--force", "--with-reference")
    assert code == 0 and fake.scripts() == ALL_REF
    assert "downloads 3" in row(screen, "reference")
    assert row(screen, "index_fetch").split(" | ")[1].strip() == "OK"
    assert row(screen, "index_fetch").endswith("| saved 3, failed 1")      # summed over the four parts


def test_offline_skips_network_steps(folder, monkeypatch):
    code, fake, screen = go(folder, monkeypatch, "--offline", "--force", "--with-reference")
    assert code == 0
    assert fake.scripts() == ["security_master.py build", "security_master.py check", "adjustments.py build",
                              "adjustments.py check", "exits.py build", "exits.py check"] + INDEX
    for step in ("reference", "bhavcopy", "actions", "index_fetch"):
        assert "SKIPPED" in row(screen, step) and "--offline" in row(screen, step)


def test_folder_passed_to_every_command(folder, monkeypatch):
    _, fake, _ = go(folder, monkeypatch, "--force", "--with-reference")
    assert all(c[-2:] == ["--folder", folder] for c in fake.calls)
    _, fake, _ = go(folder, monkeypatch, "--force", folder_arg=False)
    assert all("--folder" not in c for c in fake.calls)


def test_same_python_no_shell_repo_cwd(folder, monkeypatch):
    seen = []

    def fake_run(args, **kw):
        seen.append((args, kw))
        return subprocess.CompletedProcess(args, 0, stdout="hello\n")
    monkeypatch.setattr(upd.subprocess, "run", fake_run)
    logged = []
    code, out = upd.run_step("exits", ["exits.py", "build", "--folder", folder], logged.append)
    (args, kw), = seen
    assert args[0] == upd.sys.executable and args[1] == os.path.join(upd.REPO, "exits.py")
    assert args[2:] == ["build", "--folder", folder]
    assert kw["cwd"] == upd.REPO and kw["stderr"] == subprocess.STDOUT and "shell" not in kw
    assert kw["env"]["PYTHONIOENCODING"] == "utf-8"
    assert (code, out) == (0, "hello\n")
    assert logged[0].startswith("----- exits: python exits.py build --folder") and logged[1] == "hello\n"
    assert logged[2].startswith("----- exit 0, ")


# ---- exit codes of the steps ----

def test_download_exit_1_is_a_warning_and_goes_on(folder, monkeypatch):
    text = "".join(f"line {i}\n" for i in range(30)) + "This run: ok 0, holidays 0, failed 2, pending 0, already done 1\n"
    code, fake, screen = go(folder, monkeypatch, "--force", codes={("cm_bhav.py", "--missing"): 1},
                            texts={("cm_bhav.py", "--missing"): text})
    assert code == 0 and fake.scripts() == ALL
    assert "WARNING" in row(screen, "bhavcopy") and "failed 2" in row(screen, "bhavcopy")
    assert "      line 29" in screen and "      line 11" in screen and "      line 10\n" not in screen   # last 20
    assert "Warnings: bhavcopy" in screen


def test_actions_exit_1_warning(folder, monkeypatch):
    code, fake, screen = go(folder, monkeypatch, "--force", codes={("adjustments.py", "fetch-actions"): 1})
    assert code == 0 and "WARNING" in row(screen, "actions") and fake.scripts() == ALL


def test_reference_exit_1_warning(folder, monkeypatch):
    code, fake, screen = go(folder, monkeypatch, "--force", "--with-reference", codes={("nse_reference.py",): 1})
    assert code == 0 and "WARNING" in row(screen, "reference") and len(fake.calls) == 12


def test_index_fetch_exit_1_is_a_warning_and_the_index_build_runs(folder, monkeypatch):
    code, fake, screen = go(folder, monkeypatch, "--force", "--with-reference",
                            codes={("index_membership.py", "fetch"): 1})
    assert code == 0 and fake.scripts() == ALL_REF
    assert "WARNING" in row(screen, "index_fetch") and "Warnings: index_fetch" in screen


def test_index_fetch_crash_stops(folder, monkeypatch):
    code, fake, screen = go(folder, monkeypatch, "--force", "--with-reference",
                            codes={("index_membership.py", "fetch"): 1},
                            texts={("index_membership.py", "fetch"): "Traceback (most recent call last):\n  boom\n"})
    assert code == 1 and fake.scripts() == ALL_REF[:10]
    assert "FAILED" in row(screen, "index_fetch") and "not run" in row(screen, "index")


def test_index_fetch_exit_2_stops(folder, monkeypatch):
    code, fake, screen = go(folder, monkeypatch, "--force", "--with-reference",
                            codes={("index_membership.py", "fetch"): 2})
    assert code == 2 and fake.scripts() == ALL_REF[:10] and "not run" in row(screen, "index")


def test_download_crash_stops(folder, monkeypatch):
    code, fake, screen = go(folder, monkeypatch, "--force", codes={("cm_bhav.py", "--missing"): 1},
                            texts={("cm_bhav.py", "--missing"): "Traceback (most recent call last):\n  boom\n"})
    assert code == 1 and fake.scripts() == ["cm_bhav.py --missing"]
    assert "FAILED" in row(screen, "bhavcopy") and "not run" in row(screen, "exits")


@pytest.mark.parametrize("key, ran", [(("security_master.py", "build"), 2), (("adjustments.py", "build"), 5),
                                      (("exits.py", "build"), 7), (("index_membership.py", "build"), 9)])
def test_build_failure_stops(folder, monkeypatch, key, ran):
    code, fake, screen = go(folder, monkeypatch, "--force", codes={key: 1})
    assert code == 1 and fake.scripts() == ALL[:ran]
    assert "FAILED" in screen and "exit 1 from" in screen


@pytest.mark.parametrize("key, ran", [(("security_master.py", "check"), 3), (("adjustments.py", "check"), 6),
                                      (("exits.py", "check"), 8), (("index_membership.py", "check"), 10)])
def test_check_failure_stops(folder, monkeypatch, key, ran):
    code, fake, screen = go(folder, monkeypatch, "--force", codes={key: 1})
    assert code == 1 and fake.scripts() == ALL[:ran]


@pytest.mark.parametrize("key, ran", [(("cm_bhav.py", "--missing"), 1), (("adjustments.py", "fetch-actions"), 4),
                                      (("security_master.py", "build"), 2), (("exits.py", "check"), 8),
                                      (("index_membership.py", "check"), 10)])
def test_exit_2_from_any_step_stops(folder, monkeypatch, key, ran):
    code, fake, screen = go(folder, monkeypatch, "--force", codes={key: 2})
    assert code == 2 and fake.scripts() == ALL[:ran]


# ---- up to date ----

def test_all_up_to_date_offline(folder, monkeypatch):
    code, fake, screen = go(folder, monkeypatch, "--offline")
    assert code == 0 and fake.calls == []
    for step in ("master", "adjustments", "exits", "index"):
        assert "up to date" in row(screen, step)


@pytest.mark.parametrize("stale, expect", [
    ("NSE_CM_bhav/2026.parquet", ["security_master.py build", "security_master.py check", "adjustments.py build",
                                  "adjustments.py check", "exits.py build", "exits.py check"] + INDEX),
    ("adjustments/input/corporate_actions_2026.csv", ["adjustments.py build", "adjustments.py check",
                                                      "exits.py build", "exits.py check"]),
    ("adjustments/manual_factors.csv", ["adjustments.py build", "adjustments.py check", "exits.py build",
                                        "exits.py check"]),
    ("NSE_CM_bhav/reference/manual_mergers.csv", ["exits.py build", "exits.py check"]),
    ("NSE_CM_bhav/reference/suspensions.csv", ["exits.py build", "exits.py check"]),
    ("security_master/build_info.json", ["adjustments.py build", "adjustments.py check", "exits.py build",
                                         "exits.py check"] + INDEX),
] + [(p, INDEX) for p in INDEX_INPUTS])
def test_stale_input_runs_its_build_and_the_later_ones(folder, monkeypatch, stale, expect):
    touch(os.path.join(folder, *stale.split("/")), T1 + 50)
    code, fake, _ = go(folder, monkeypatch, "--offline")
    assert code == 0 and fake.scripts() == expect


def test_missing_build_info_runs(folder, monkeypatch):
    os.remove(outputs(folder)["exits"])
    _, fake, screen = go(folder, monkeypatch, "--offline")
    assert fake.scripts() == ["exits.py build", "exits.py check"]
    os.remove(outputs(folder)["index"])
    _, fake, screen = go(folder, monkeypatch, "--offline")
    assert fake.scripts() == INDEX
    assert upd.freshness(folder, "index")[0]            # the (fake) build wrote it again: up to date


def test_index_freshness_inputs(folder):
    out, ins = upd.inputs_of(folder, "index")
    assert out == outputs(folder)["index"]
    rel = {os.path.relpath(p, folder).replace(os.sep, "/") for p in ins}
    assert rel == {"security_master/build_info.json", "NSE_CM_bhav/2025.parquet", "NSE_CM_bhav/2026.parquet"} \
        | set(INDEX_INPUTS)
    assert upd.freshness(folder, "index") == (True, os.path.join("index_membership", "build_info.json")
                                              + " is newer than its 8 inputs")
    touch(os.path.join(folder, "index_membership", "input", "snapshots", "NIFTY_500_20261101000000.csv"), T1 + 50)
    fresh, text = upd.freshness(folder, "index")       # a new file in a sub-folder counts too
    assert not fresh and "NIFTY_500_20261101000000.csv is newer" in text


def test_a_fetch_in_the_same_run_makes_index_stale(folder, monkeypatch):
    def fetch_writes(args, **kw):
        if os.path.basename(args[1]) == "index_membership.py" and args[2] == "fetch":
            touch(os.path.join(folder, "index_membership", "input", "nifty_current", "NIFTY_50_2026-11-01.csv"),
                  T1 + 500)
        return fake(args, **kw)
    fake = FakeRun(folder)
    monkeypatch.setattr(upd.subprocess, "run", fetch_writes)
    monkeypatch.setattr(upd, "print", lambda *a, **k: None, raising=False)
    code = cli.main(["--folder", folder, "--with-reference", "--offline"], now=EVENING)
    assert code == 0 and fake.calls == []                  # offline: no fetch, everything up to date
    code = cli.main(["--folder", folder, "--with-reference", "--from-step", "index_fetch"], now=EVENING)
    assert code == 0 and fake.scripts() == ["index_membership.py fetch"] + INDEX   # the fetch made index stale


def test_freshness_texts(folder):
    assert upd.freshness(folder, "exits") == (True, os.path.join("exits", "build_info.json")
                                              + " is newer than its 10 inputs")
    touch(os.path.join(folder, "NSE_CM_bhav", "2025.parquet"), T1 + 50)
    fresh, text = upd.freshness(folder, "adjustments")
    assert not fresh and text.startswith(os.path.join("NSE_CM_bhav", "2025.parquet") + " is newer")


def test_force_runs_up_to_date_builds(folder, monkeypatch):
    _, fake, _ = go(folder, monkeypatch, "--offline", "--force")
    assert len(fake.calls) == 8 and fake.scripts()[-2:] == INDEX


# ---- --from-step, --dry-run, lock, arguments ----

def test_from_step(folder, monkeypatch):
    code, fake, screen = go(folder, monkeypatch, "--force", "--from-step", "adjustments")
    assert code == 0 and fake.scripts() == ALL[4:]
    assert "before --from-step" in row(screen, "bhavcopy") and "before --from-step" in row(screen, "master")


def test_from_step_new_steps(folder, monkeypatch):
    code, fake, screen = go(folder, monkeypatch, "--force", "--with-reference", "--from-step", "index_fetch")
    assert code == 0 and fake.scripts() == ["index_membership.py fetch"] + INDEX
    assert "before --from-step" in row(screen, "exits") and "before --from-step" in row(screen, "reference")
    code, fake, screen = go(folder, monkeypatch, "--force", "--with-reference", "--from-step", "index")
    assert code == 0 and fake.scripts() == INDEX and "before --from-step" in row(screen, "index_fetch")


def test_from_step_invalid(folder, monkeypatch, capsys):
    with pytest.raises(SystemExit) as e:
        cli.main(["--from-step", "nope"])
    assert e.value.code == 2


def test_dry_run_runs_nothing(folder, monkeypatch):
    code, fake, screen = go(folder, monkeypatch, "--dry-run", "--with-reference")
    assert code == 0 and fake.calls == []
    assert not os.path.exists(os.path.join(folder, upd.LOG))
    assert "python nse_reference.py --folder" in screen and "python exits.py check --folder" in screen
    assert "master: up to date now" in screen and "bhavcopy: would run" in screen
    assert "index_fetch: would run" in screen and "python index_membership.py fetch --folder" in screen
    assert "index: up to date now" in screen and "python index_membership.py check --folder" in screen
    _, _, screen = go(folder, monkeypatch, "--dry-run")
    assert "index_fetch: skipped (not asked; add --with-reference)" in screen


def test_own_lock_busy(folder, monkeypatch):
    lock = RunLock(os.path.join(folder, upd.LOCK)).acquire("test")
    try:
        code, fake, screen = go(folder, monkeypatch, "--force")
    finally:
        lock.release()
    assert code == 2 and fake.calls == [] and "Not run" in screen


def test_relative_folder_is_an_error():
    with pytest.raises(SystemExit) as e:
        cli.main(["--folder", "relative"])
    assert e.value.code == 2


def test_note_before_19(folder, monkeypatch):
    _, _, screen = go(folder, monkeypatch, "--offline", now=MORNING)
    assert "may not be published yet" not in screen        # no bhavcopy step offline
    _, _, screen = go(folder, monkeypatch, now=MORNING)
    assert "10:00 IST, before 19:00: today's bhavcopy may not be published yet" in screen
    _, _, screen = go(folder, monkeypatch, now=EVENING)
    assert "may not be published yet" not in screen


# ---- log and summary ----

def test_log_and_summary(folder, monkeypatch):
    code, _, screen = go(folder, monkeypatch, "--force")
    log = log_text(folder)
    assert log.startswith("===== equity_update 2026-10-19 20:00:00 IST argv: --folder")
    assert "----- reference: SKIPPED (not asked; add --with-reference)" in log
    assert "----- bhavcopy: python cm_bhav.py --missing --folder" in log
    assert "This run: ok 1, holidays 0" in log and "----- exit 0, " in log
    assert log.rstrip().endswith("===== exit 0")
    assert row(screen, "bhavcopy").split(" | ")[1].strip() == "OK"
    assert row(screen, "bhavcopy").endswith("| ok 1, holiday 0, failed 0, pending 1")   # no trailing comma
    assert row(screen, "master").endswith("| securities 3,702; check ok")
    assert "saved 1, skipped 11, failed 0" in row(screen, "actions")
    assert "applied 822; check ok" in row(screen, "adjustments")
    assert "exited 714; review 271; check ok" in row(screen, "exits")
    assert row(screen, "index").endswith("| reliable 2 of 3; review 205; check ok")
    assert screen.splitlines()[-1].startswith("Log: ")
    go(folder, monkeypatch, "--offline")
    assert log_text(folder).count("===== equity_update ") == 2       # appended


def test_numbers_not_parsed_show_dash(folder, monkeypatch):
    _, _, screen = go(folder, monkeypatch, "--force", texts={("exits.py", "build"): "something else\n"})
    assert "exited -; review -; check ok" in row(screen, "exits")
    _, _, screen = go(folder, monkeypatch, "--force", "--with-reference",
                      texts={("index_membership.py", "build"): "Known-case checks: all found\n",
                             ("index_membership.py", "fetch"): "nothing\n"})
    assert row(screen, "index").endswith("| reliable - of -; review -; check ok")
    assert row(screen, "index_fetch").endswith("| saved -, failed -")


def test_summary_table_shape():
    text = upd.summary([{"step": "master", "result": "OK", "time": 12.4, "numbers": "securities 1"},
                        {"step": "exits", "result": "not run", "time": None, "numbers": "-"}])
    lines = text.splitlines()
    assert lines[0].split(" | ")[0].strip() == "step" and "key numbers" in lines[0]
    assert set(lines[1]) <= {"-", "+"}
    assert " | 12 s | securities 1" in lines[2] and lines[3].split(" | ")[2].strip() == "-"
