"""IST clock and NSE session utilities."""

from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")
REGULAR_OPEN = time(9, 15)
REGULAR_CLOSE = time(15, 30)


def now_ist() -> datetime:
    """Return the current timezone-aware IST timestamp."""

    return datetime.now(tz=IST)


def is_regular_market_time(value: datetime) -> bool:
    """Return whether a timestamp is inside the regular NSE session."""

    local = value.astimezone(IST)
    if local.weekday() >= 5:
        return False
    current = local.time()
    return REGULAR_OPEN <= current <= REGULAR_CLOSE
