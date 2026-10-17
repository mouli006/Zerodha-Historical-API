"""backfill/equity/ never imports F&O modules (checked with ast, nothing is imported)."""
import ast
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EQUITY = os.path.join(ROOT, "backfill", "equity")


def forbidden(name):
    """True for an F&O module or anything inside one."""
    parts = name.split(".")
    top = parts[0]
    if top in ("dhan", "fno_store") or top.startswith("nifty_"):
        return True
    if top == "backfill" and len(parts) > 1:
        sub = parts[1]
        return sub.startswith("fno") or sub.startswith("bhav") or sub == "dhan_api"
    return False


def imported_names(source, package):
    """Every module name an import in source can load; relative imports resolved against package."""
    names = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                parent = package.split(".")[: len(package.split(".")) - node.level + 1]
                base = ".".join(parent + ([node.module] if node.module else []))
            names.append(base)
            names += [f"{base}.{a.name}" for a in node.names]  # from backfill import fno
    return names


def violations(source, package):
    return [n for n in imported_names(source, package) if forbidden(n)]


def equity_files():
    for folder, _, files in os.walk(EQUITY):
        for name in files:
            if name.endswith(".py"):
                yield os.path.join(folder, name)


def test_equity_never_imports_fno():
    files = list(equity_files())
    assert files, "backfill/equity/ has no .py files"
    bad = {}
    for path in files:
        rel = os.path.relpath(path, ROOT)
        package = ".".join(os.path.dirname(rel).split(os.sep))
        with open(path, encoding="utf-8") as f:
            found = violations(f.read(), package)
        if found:
            bad[rel] = found
    assert not bad, f"equity code imports F&O modules: {bad}"


CAUGHT = [
    "import backfill.fno",
    "import backfill.fno_store as s",
    "from backfill.bhav_export import export",
    "from backfill import bhav",
    "from backfill import dhan_api",
    "from ..fno import RunLock",
    "from .. import fno_loader",
    "import dhan",
    "import nifty_daily",
    "from nifty_bhav import main",
    "import fno_store",
    "def f():\n    import backfill.fno_publish",
]

ALLOWED = [
    "from backfill.locks import LockBusy, RunLock",
    "from backfill import locks, cm_bhav",
    "from ..nse_ref.fetch import NseClient",
    "from . import master",
    "import config, pandas",
]


def test_checker_catches_fno_imports_only():
    for source in CAUGHT:
        assert violations(source, "backfill.equity"), source
    for source in ALLOWED:
        assert not violations(source, "backfill.equity"), source
