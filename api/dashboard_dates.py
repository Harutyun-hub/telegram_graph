from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone


@dataclass(frozen=True)
class DashboardDateContext:
    from_date: date
    to_date: date
    start_at: datetime
    end_at: datetime
    previous_start_at: datetime
    previous_end_at: datetime
    days: int
    cache_key: str

    @property
    def is_operational(self) -> bool:
        return self.days < 15

    @property
    def range_label(self) -> str:
        if self.days == 1 and self.from_date == self.to_date:
            return self.from_date.isoformat()
        return f"{self.from_date.isoformat()}..{self.to_date.isoformat()}"


def _as_utc_day_start(value: date) -> datetime:
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def build_dashboard_date_context(from_value: str, to_value: str) -> DashboardDateContext:
    from_date = date.fromisoformat(from_value)
    to_date = date.fromisoformat(to_value)
    if to_date < from_date:
        from_date, to_date = to_date, from_date

    days = max(1, (to_date - from_date).days + 1)
    start_at = _as_utc_day_start(from_date)
    end_at = _as_utc_day_start(to_date + timedelta(days=1))
    previous_end_at = start_at
    previous_start_at = start_at - timedelta(days=days)

    return DashboardDateContext(
        from_date=from_date,
        to_date=to_date,
        start_at=start_at,
        end_at=end_at,
        previous_start_at=previous_start_at,
        previous_end_at=previous_end_at,
        days=days,
        cache_key=f"{from_date.isoformat()}:{to_date.isoformat()}",
    )
