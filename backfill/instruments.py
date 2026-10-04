T2T_SERIES = {"BE", "BZ"}
MAX_STOCKS = 3000  # more than this means the series filter broke; refuse to start


def is_etf(inst):
    sym, name = inst["tradingsymbol"].upper(), (inst.get("name") or "").upper()
    return "ETF" in name or "ETF" in sym or sym.endswith("BEES")


def series_suffix(inst):
    """NSE series suffix of a tradingsymbol ("BE", "GB", "N1", ...) or "" for normal
    equity. Suffixes are always 2 characters, so BAJAJ-AUTO / NAM-INDIA are plain equity."""
    _, sep, suffix = inst["tradingsymbol"].rpartition("-")
    return suffix.upper() if sep and len(suffix) == 2 else ""


def is_equity(inst, include_t2t):
    """Normal equity, plus trade-to-trade -BE / -BZ if asked. Bonds, NCDs, G-secs,
    T-bills, SGBs (-N*, -Y*, -Z*, -GB, -GS, -SG, -TB, -RR, ...) and SME are excluded."""
    suffix = series_suffix(inst)
    return not suffix or (include_t2t and suffix in T2T_SERIES)
