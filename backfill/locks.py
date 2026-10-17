"""Shared run lock for code outside F&O: import RunLock and LockBusy from here, never from backfill.fno.

The lock itself still lives in backfill/fno.py (an OS lock on a .lock file, dropped when the process dies);
this module only re-exports it, so it can later move here without touching any caller."""
import os

from backfill.fno import LockBusy, RunLock

__all__ = ["LockBusy", "RunLock", "held"]


def held(path):
    """True while another run holds the RunLock on path. Read only: the file is opened for reading, locked and
    unlocked at once, and never created or written (RunLock.acquire would rewrite its owner text)."""
    if not os.path.isfile(path):
        return False
    probe = RunLock(path)
    with open(path, "rb") as f:
        try:
            probe._lock(f, True)
        except OSError:
            return True
        try:
            probe._lock(f, False)
        except OSError:
            pass
    return False
