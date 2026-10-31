"""Shared helpers for the repository modules."""

import copy
import re
import threading
import time
from datetime import date, datetime, time as dtime, timedelta
from typing import Any, Callable, Dict, Optional, Tuple

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.database.models import Order

_CACHE: Dict[Tuple, Tuple[float, Any]] = {}
_LOCK = threading.Lock()


def ttl_cache(seconds: int = 60) -> Callable:
    """Cache a repository function's result for a short time.

    Dashboards call many endpoints per page view and refresh often; identical queries
    against a remote database are served from memory for `seconds`. The first
    argument must be the SQLAlchemy session, which is excluded from the key.
    """

    def decorator(fn: Callable) -> Callable:
        def wrapper(db: Session, *args, **kwargs):
            key = (fn.__module__, fn.__name__, id(db.get_bind()), args, tuple(sorted(kwargs.items())))
            now = time.time()
            with _LOCK:
                hit = _CACHE.get(key)
            if hit and now - hit[0] < seconds:
                return copy.deepcopy(hit[1])
            value = fn(db, *args, **kwargs)
            with _LOCK:
                _CACHE[key] = (now, value)
            return copy.deepcopy(value)

        wrapper.__wrapped__ = fn
        wrapper.__name__ = fn.__name__
        wrapper.__doc__ = fn.__doc__
        return wrapper

    return decorator


def clear_cache() -> None:
    with _LOCK:
        _CACHE.clear()


def f(value: Any, ndigits: Optional[int] = None) -> float:
    """Decimal/None -> float (rounded)."""
    out = float(value or 0)
    return round(out, ndigits) if ndigits is not None else out


def day_start(d: date) -> datetime:
    return datetime.combine(d, dtime.min)


def parse_date(value: Optional[str], default: date) -> date:
    if not value:
        return default
    return datetime.strptime(value[:10], "%Y-%m-%d").date()


_RANGE = re.compile(r"^(\d+)([mhdwy])$")
_UNITS = {"m": "minutes", "h": "hours", "d": "days", "w": "weeks"}


def parse_window(value: str, default: timedelta = timedelta(days=30)) -> timedelta:
    """'5m', '1h', '30d', '1y' -> timedelta."""
    m = _RANGE.match((value or "").strip().lower())
    if not m:
        return default
    n, unit = int(m.group(1)), m.group(2)
    return timedelta(days=365 * n) if unit == "y" else timedelta(**{_UNITS[unit]: n})


@ttl_cache(10)
def data_now(db: Session) -> datetime:
    """The newest order timestamp: the 'present' of the data set.

    For a live system this is ~ the real current time. For a seeded database that has
    gone stale it keeps the "last 30 days" style views populated instead of empty.
    """
    latest = db.execute(select(func.max(Order.order_date))).scalar()
    if latest is None:
        return datetime.utcnow()
    if isinstance(latest, str):
        latest = datetime.fromisoformat(latest)
    return min(max(latest, datetime.utcnow() - timedelta(days=3650)), datetime.utcnow() + timedelta(days=1))


def month_index(value: Any) -> int:
    """year*12 + month for a datetime/date/'YYYY-MM-DD' value."""
    if isinstance(value, str):
        value = datetime.fromisoformat(value[:19])
    return value.year * 12 + value.month - 1
