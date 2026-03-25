from __future__ import annotations

from datetime import datetime, timedelta, timezone

from models import WindowLiteral


WINDOW_TO_DAYS = {
    "24h": 1,
    "7d": 7,
    "30d": 30,
    "90d": 90,
}

WINDOW_TO_TIMEFRAME = {
    "24h": "Last 24h",
    "7d": "Last 7 Days",
    "30d": "Last Month",
    "90d": "Last 3 Months",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def window_to_days(window: WindowLiteral) -> int:
    return WINDOW_TO_DAYS[window]


def window_to_timeframe(window: WindowLiteral) -> str:
    return WINDOW_TO_TIMEFRAME[window]


def dashboard_date_range(window: WindowLiteral, now: datetime | None = None) -> tuple[str, str]:
    current = now or utc_now()
    start_date = (current - timedelta(days=window_to_days(window))).date().isoformat()
    end_date = current.date().isoformat()
    return start_date, end_date
