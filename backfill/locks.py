"""Shared run lock for code outside F&O: import RunLock and LockBusy from here, never from backfill.fno.

The lock itself still lives in backfill/fno.py (an OS lock on a .lock file, dropped when the process dies);
this module only re-exports it, so it can later move here without touching any caller."""
from backfill.fno import LockBusy, RunLock

__all__ = ["LockBusy", "RunLock"]
