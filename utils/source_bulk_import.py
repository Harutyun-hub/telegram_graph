from __future__ import annotations

import csv
import json
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from utils.source_normalization import canonical_channel_username, normalize_channel_username

DEFAULT_SCRAPE_DEPTH_DAYS = 7
DEFAULT_SCRAPE_COMMENTS = True
DEFAULT_WAVE_SIZE = 50
DEFAULT_POST_BACKLOG_THRESHOLD = 250
DEFAULT_COMMENT_BACKLOG_THRESHOLD = 120

RESULT_FIELDNAMES = [
    "csv_row_number",
    "channel_name",
    "raw_username",
    "raw_telegram_url",
    "input_source",
    "normalized_handle",
    "canonical_username",
    "fallback_title",
    "preflight_status",
    "duplicate_of_row",
    "existing_id",
    "existing_is_active",
    "wave_number",
    "import_action",
    "final_id",
    "final_is_active",
    "source_type",
    "resolution_status",
    "last_resolution_error",
    "api_error",
    "state_error",
    "note",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _friendly_error_message(payload: Any, fallback: str) -> str:
    helper_error = payload.get("error") if isinstance(payload, dict) else None
    if isinstance(helper_error, dict):
        message = str(helper_error.get("message") or "").strip()
        if message:
            return message
    if isinstance(payload, dict):
        detail = str(payload.get("detail") or "").strip()
        if detail:
            return detail
    return fallback.strip() or "Request failed"


def normalize_api_base(api_base: str) -> str:
    base = str(api_base or "").strip().rstrip("/")
    if not base:
        raise ValueError("Missing API base URL")
    return base if base.endswith("/api") else f"{base}/api"


def normalize_bearer_token(token: str) -> str:
    value = str(token or "").strip()
    if value.lower().startswith("bearer "):
        value = value[7:].strip()
    return value


def _request_json(
    *,
    url: str,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    payload: dict[str, Any] | None = None,
    timeout_seconds: float = 30.0,
) -> Any:
    request_headers = {"Accept": "application/json"}
    if headers:
        request_headers.update(headers)

    data: bytes | None = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")

    request = urllib.request.Request(
        url,
        data=data,
        headers=request_headers,
        method=method.upper(),
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            raw_body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        raw_body = exc.read().decode("utf-8", errors="replace")
        payload_body: Any = None
        if raw_body.strip():
            try:
                payload_body = json.loads(raw_body)
            except json.JSONDecodeError:
                payload_body = None
        message = _friendly_error_message(payload_body, raw_body or str(exc))
        raise ApiCallError(status_code=exc.code, message=message, payload=payload_body) from exc
    except urllib.error.URLError as exc:
        raise ApiCallError(status_code=None, message=str(exc.reason or exc), payload=None) from exc

    if not raw_body.strip():
        return {}
    try:
        return json.loads(raw_body)
    except json.JSONDecodeError:
        raise ApiCallError(status_code=None, message=f"Invalid JSON response from {url}", payload=raw_body)


class ApiCallError(RuntimeError):
    def __init__(self, *, status_code: int | None, message: str, payload: Any) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


@dataclass
class SourceImportRow:
    csv_row_number: int
    channel_name: str
    raw_username: str
    raw_telegram_url: str
    input_source: str
    normalized_handle: str
    canonical_username: str
    fallback_title: str
    preflight_status: str
    duplicate_of_row: int | None = None
    existing_id: str | None = None
    existing_is_active: bool | None = None
    wave_number: int | None = None
    import_action: str | None = None
    final_id: str | None = None
    final_is_active: bool | None = None
    source_type: str | None = None
    resolution_status: str | None = None
    last_resolution_error: str | None = None
    api_error: str | None = None
    state_error: str | None = None
    note: str = ""

    @property
    def is_valid(self) -> bool:
        return self.preflight_status != "invalid"

    @property
    def was_imported(self) -> bool:
        return bool(self.final_id)

    def as_csv_row(self) -> dict[str, Any]:
        return {
            "csv_row_number": self.csv_row_number,
            "channel_name": self.channel_name,
            "raw_username": self.raw_username,
            "raw_telegram_url": self.raw_telegram_url,
            "input_source": self.input_source,
            "normalized_handle": self.normalized_handle,
            "canonical_username": self.canonical_username,
            "fallback_title": self.fallback_title,
            "preflight_status": self.preflight_status,
            "duplicate_of_row": self.duplicate_of_row or "",
            "existing_id": self.existing_id or "",
            "existing_is_active": "" if self.existing_is_active is None else str(self.existing_is_active).lower(),
            "wave_number": self.wave_number or "",
            "import_action": self.import_action or "",
            "final_id": self.final_id or "",
            "final_is_active": "" if self.final_is_active is None else str(self.final_is_active).lower(),
            "source_type": self.source_type or "",
            "resolution_status": self.resolution_status or "",
            "last_resolution_error": self.last_resolution_error or "",
            "api_error": self.api_error or "",
            "state_error": self.state_error or "",
            "note": self.note,
        }


@dataclass
class WaveGateDecision:
    healthy: bool
    clean_cycle_completed: bool
    scheduler_error: str | None
    unprocessed_posts: int
    unprocessed_comments: int
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "healthy": self.healthy,
            "clean_cycle_completed": self.clean_cycle_completed,
            "scheduler_error": self.scheduler_error,
            "unprocessed_posts": self.unprocessed_posts,
            "unprocessed_comments": self.unprocessed_comments,
            "reason": self.reason,
        }


class SourceImportApiClient:
    def __init__(self, *, api_base: str, auth_token: str, timeout_seconds: float = 30.0) -> None:
        self.api_base = normalize_api_base(api_base)
        self.auth_token = normalize_bearer_token(auth_token)
        self.timeout_seconds = max(5.0, float(timeout_seconds))

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.auth_token:
            headers["X-Supabase-Authorization"] = f"Bearer {self.auth_token}"
        return headers

    def _request(self, path: str, *, method: str = "GET", payload: dict[str, Any] | None = None) -> Any:
        normalized_path = "/" + str(path or "").lstrip("/")
        return _request_json(
            url=f"{self.api_base}{normalized_path}",
            method=method,
            headers=self._headers(),
            payload=payload,
            timeout_seconds=self.timeout_seconds,
        )

    def list_sources(self) -> dict[str, Any]:
        return self._request("/sources/channels")

    def create_source(
        self,
        *,
        channel_username: str,
        channel_title: str,
        scrape_depth_days: int = DEFAULT_SCRAPE_DEPTH_DAYS,
        scrape_comments: bool = DEFAULT_SCRAPE_COMMENTS,
    ) -> dict[str, Any]:
        return self._request(
            "/sources/channels",
            method="POST",
            payload={
                "channel_username": channel_username,
                "channel_title": channel_title,
                "scrape_depth_days": int(scrape_depth_days),
                "scrape_comments": bool(scrape_comments),
            },
        )

    def update_source(self, channel_id: str, *, is_active: bool) -> dict[str, Any]:
        return self._request(
            f"/sources/channels/{channel_id}",
            method="PATCH",
            payload={"is_active": bool(is_active)},
        )

    def get_scheduler_status(self) -> dict[str, Any]:
        return self._request("/scraper/scheduler")

    def stop_scheduler(self) -> dict[str, Any]:
        return self._request("/scraper/scheduler/stop", method="POST")

    def start_scheduler(self) -> dict[str, Any]:
        return self._request("/scraper/scheduler/start", method="POST")

    def run_scheduler_once(self) -> dict[str, Any]:
        return self._request("/scraper/scheduler/run-once", method="POST")

    def get_freshness(self) -> dict[str, Any]:
        return self._request("/freshness?force=true")


def _existing_source_lookup(existing_sources: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for item in existing_sources:
        handle = normalize_channel_username(str(item.get("channel_username") or ""))
        if handle and handle not in lookup:
            lookup[handle] = item
    return lookup


def _extract_handle(raw_username: str, raw_telegram_url: str) -> tuple[str, str, str]:
    username = str(raw_username or "").strip()
    telegram_url = str(raw_telegram_url or "").strip()

    if username:
        normalized = normalize_channel_username(username)
        note = "" if normalized else "invalid_username"
        return normalized, "username", note
    if telegram_url:
        normalized = normalize_channel_username(telegram_url)
        note = "" if normalized else "invalid_telegram_url"
        return normalized, "telegram_url", note
    return "", "missing", "missing_username_and_url"


def build_preflight_rows(
    csv_path: str | Path,
    existing_sources: Iterable[dict[str, Any]],
    *,
    wave_size: int = DEFAULT_WAVE_SIZE,
) -> list[SourceImportRow]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    lookup = _existing_source_lookup(existing_sources)
    rows: list[SourceImportRow] = []
    seen_rows: dict[str, int] = {}

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"CSV file is missing a header row: {path}")

        for csv_row_number, raw_row in enumerate(reader, start=2):
            channel_name = str(raw_row.get("Channel Name") or "").strip()
            raw_username = str(raw_row.get("Username") or "").strip()
            raw_telegram_url = str(raw_row.get("Telegram URL") or "").strip()
            normalized_handle, input_source, note = _extract_handle(raw_username, raw_telegram_url)
            fallback_title = channel_name or normalized_handle or raw_username or raw_telegram_url

            row = SourceImportRow(
                csv_row_number=csv_row_number,
                channel_name=channel_name,
                raw_username=raw_username,
                raw_telegram_url=raw_telegram_url,
                input_source=input_source,
                normalized_handle=normalized_handle,
                canonical_username=canonical_channel_username(normalized_handle),
                fallback_title=fallback_title,
                preflight_status="invalid",
                note=note,
            )

            if not normalized_handle:
                rows.append(row)
                continue

            if normalized_handle in seen_rows:
                row.note = f"duplicate_handle_in_csv_first_seen_at_row_{seen_rows[normalized_handle]}"
                row.duplicate_of_row = seen_rows[normalized_handle]
                rows.append(row)
                continue

            seen_rows[normalized_handle] = csv_row_number
            existing = lookup.get(normalized_handle)
            if existing:
                row.existing_id = str(existing.get("id") or "").strip() or None
                row.existing_is_active = bool(existing.get("is_active", False))
                row.preflight_status = "existing-active" if row.existing_is_active else "existing-inactive"
            else:
                row.preflight_status = "new"
            rows.append(row)

    valid_rows = [row for row in rows if row.is_valid]
    safe_wave_size = max(1, int(wave_size))
    for ordinal, row in enumerate(valid_rows, start=1):
        row.wave_number = ((ordinal - 1) // safe_wave_size) + 1

    return rows


def count_values(values: Iterable[str]) -> dict[str, int]:
    counts = Counter(str(value or "") for value in values)
    return {key: counts[key] for key in sorted(counts)}


def record_source_state(row: SourceImportRow, item: dict[str, Any], *, import_action: str | None = None) -> None:
    if import_action:
        row.import_action = str(import_action)
    row.final_id = str(item.get("id") or "").strip() or row.final_id
    if "is_active" in item:
        row.final_is_active = bool(item.get("is_active"))
    row.source_type = str(item.get("source_type") or "").strip() or row.source_type
    row.resolution_status = str(item.get("resolution_status") or "").strip() or row.resolution_status
    row.last_resolution_error = str(item.get("last_resolution_error") or "").strip() or None


def should_retry_api_error(error: ApiCallError) -> bool:
    if error.status_code is None:
        return True
    return error.status_code >= 500 or error.status_code == 429


def run_with_retry(
    func,
    *,
    max_attempts: int = 3,
    base_delay_seconds: float = 1.0,
):
    attempt = 0
    while True:
        attempt += 1
        try:
            return func()
        except ApiCallError as exc:
            if attempt >= max(1, max_attempts) or not should_retry_api_error(exc):
                raise
            time.sleep(max(0.0, float(base_delay_seconds)) * (2 ** (attempt - 1)))


def wait_for_clean_cycle(
    client: SourceImportApiClient,
    *,
    poll_interval_seconds: float = 15.0,
    timeout_seconds: float = 1800.0,
) -> dict[str, Any]:
    before = client.get_scheduler_status()
    before_started_at = parse_iso_datetime(before.get("last_run_started_at"))
    before_finished_at = parse_iso_datetime(before.get("last_run_finished_at"))
    trigger_time = utc_now()

    client.run_scheduler_once()
    deadline = time.time() + max(float(timeout_seconds), float(poll_interval_seconds))

    while time.time() < deadline:
        status = client.get_scheduler_status()
        started_at = parse_iso_datetime(status.get("last_run_started_at"))
        finished_at = parse_iso_datetime(status.get("last_run_finished_at"))

        started_new_run = (
            started_at is not None
            and (
                before_started_at is None
                or started_at > before_started_at
                or started_at >= (trigger_time - timedelta(seconds=5))
            )
        )
        finished_new_run = (
            finished_at is not None
            and (
                before_finished_at is None
                or finished_at > before_finished_at
                or finished_at >= (trigger_time - timedelta(seconds=5))
            )
        )

        if started_new_run and finished_new_run and not bool(status.get("running_now", False)):
            last_success_at = parse_iso_datetime(status.get("last_success_at"))
            clean_cycle_completed = bool(
                not status.get("last_error")
                and last_success_at is not None
                and started_at is not None
                and last_success_at >= started_at
            )
            return {
                "timed_out": False,
                "clean_cycle_completed": clean_cycle_completed,
                "scheduler_status": status,
            }

        time.sleep(max(1.0, float(poll_interval_seconds)))

    status = client.get_scheduler_status()
    return {
        "timed_out": True,
        "clean_cycle_completed": False,
        "scheduler_status": status,
    }


def evaluate_wave_gate(
    freshness: dict[str, Any],
    scheduler_status: dict[str, Any],
    *,
    clean_cycle_completed: bool,
    post_threshold: int = DEFAULT_POST_BACKLOG_THRESHOLD,
    comment_threshold: int = DEFAULT_COMMENT_BACKLOG_THRESHOLD,
) -> WaveGateDecision:
    backlog = freshness.get("backlog") or {}
    unprocessed_posts = int(backlog.get("unprocessed_posts") or 0)
    unprocessed_comments = int(backlog.get("unprocessed_comments") or 0)
    scheduler_error = str(scheduler_status.get("last_error") or "").strip() or None

    reasons: list[str] = []
    if not clean_cycle_completed:
        reasons.append("clean_cycle_not_completed")
    if scheduler_error:
        reasons.append("scheduler_error")
    if unprocessed_posts >= int(post_threshold):
        reasons.append("post_backlog_threshold_exceeded")
    if unprocessed_comments >= int(comment_threshold):
        reasons.append("comment_backlog_threshold_exceeded")

    return WaveGateDecision(
        healthy=not reasons,
        clean_cycle_completed=clean_cycle_completed,
        scheduler_error=scheduler_error,
        unprocessed_posts=unprocessed_posts,
        unprocessed_comments=unprocessed_comments,
        reason=", ".join(reasons) if reasons else "healthy",
    )


def verify_final_state(
    rows: Iterable[SourceImportRow],
    final_sources: Iterable[dict[str, Any]],
    *,
    highest_activated_wave: int,
) -> dict[str, Any]:
    handle_to_sources: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in final_sources:
        handle = normalize_channel_username(str(item.get("channel_username") or ""))
        if handle:
            handle_to_sources[handle].append(item)

    duplicate_handles = sorted(handle for handle, items in handle_to_sources.items() if len(items) > 1)
    missing_handles: list[str] = []
    unexpected_active: list[str] = []
    unexpected_inactive: list[str] = []

    for row in rows:
        if not row.final_id or not row.normalized_handle:
            continue
        matches = handle_to_sources.get(row.normalized_handle) or []
        if not matches:
            missing_handles.append(row.normalized_handle)
            continue

        matched = next((item for item in matches if str(item.get("id") or "") == row.final_id), matches[0])
        record_source_state(row, matched)

        if row.wave_number and row.wave_number > highest_activated_wave and bool(matched.get("is_active", False)):
            unexpected_active.append(row.normalized_handle)
        if row.wave_number and row.wave_number <= highest_activated_wave and not bool(matched.get("is_active", False)):
            unexpected_inactive.append(row.normalized_handle)

    return {
        "duplicate_handles": duplicate_handles,
        "missing_handles": sorted(set(missing_handles)),
        "unexpected_active_handles": sorted(set(unexpected_active)),
        "unexpected_inactive_handles": sorted(set(unexpected_inactive)),
    }


def write_results_csv(rows: Iterable[SourceImportRow], output_path: str | Path) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RESULT_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.as_csv_row())


def write_summary_json(summary: dict[str, Any], output_path: str | Path) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")


def build_summary(
    *,
    csv_path: str | Path,
    api_base: str,
    dry_run: bool,
    rows: list[SourceImportRow],
    wave_size: int,
    highest_activated_wave: int,
    wave_history: list[dict[str, Any]],
    verification: dict[str, Any],
    scheduler_stop_status: dict[str, Any] | None,
    scheduler_start_status: dict[str, Any] | None,
) -> dict[str, Any]:
    valid_rows = [row for row in rows if row.is_valid]
    imported_rows = [row for row in rows if row.was_imported]
    total_waves = max((row.wave_number or 0) for row in valid_rows) if valid_rows else 0
    action_counts = count_values(row.import_action or "api_failed" for row in valid_rows if row.import_action or row.api_error)
    preflight_counts = count_values(row.preflight_status for row in rows)
    resolution_counts = {
        "resolved": sum(1 for row in imported_rows if row.resolution_status == "resolved"),
        "pending_resolution": sum(1 for row in imported_rows if row.resolution_status != "resolved"),
    }

    return {
        "generated_at": utc_now().isoformat(),
        "csv_path": str(Path(csv_path)),
        "api_base": api_base,
        "dry_run": bool(dry_run),
        "wave_size": int(wave_size),
        "total_rows": len(rows),
        "valid_rows": len(valid_rows),
        "invalid_rows": len(rows) - len(valid_rows),
        "imported_rows": len(imported_rows),
        "preflight_counts": preflight_counts,
        "run_counts": {
            "created": action_counts.get("created", 0),
            "reactivated": action_counts.get("reactivated", 0),
            "exists": action_counts.get("exists", 0),
            "invalid": preflight_counts.get("invalid", 0),
            "api_failed": sum(1 for row in valid_rows if row.api_error and not row.final_id),
            "resolved": resolution_counts["resolved"],
            "pending_resolution": resolution_counts["pending_resolution"],
        },
        "overlap_existing": preflight_counts.get("existing-active", 0) + preflight_counts.get("existing-inactive", 0),
        "total_waves": total_waves,
        "highest_activated_wave": highest_activated_wave,
        "completed_all_waves": total_waves == 0 or highest_activated_wave >= total_waves,
        "wave_history": wave_history,
        "scheduler": {
            "stopped": scheduler_stop_status,
            "started": scheduler_start_status,
        },
        "verification": verification,
    }


def execute_bulk_import(
    *,
    csv_path: str | Path,
    client: SourceImportApiClient,
    wave_size: int = DEFAULT_WAVE_SIZE,
    scrape_depth_days: int = DEFAULT_SCRAPE_DEPTH_DAYS,
    scrape_comments: bool = DEFAULT_SCRAPE_COMMENTS,
    dry_run: bool = False,
    max_attempts: int = 3,
    retry_backoff_seconds: float = 1.0,
    poll_interval_seconds: float = 15.0,
    cycle_timeout_seconds: float = 1800.0,
    post_backlog_threshold: int = DEFAULT_POST_BACKLOG_THRESHOLD,
    comment_backlog_threshold: int = DEFAULT_COMMENT_BACKLOG_THRESHOLD,
) -> tuple[list[SourceImportRow], dict[str, Any]]:
    existing_sources_payload = client.list_sources()
    existing_sources = list(existing_sources_payload.get("items") or [])
    rows = build_preflight_rows(csv_path, existing_sources, wave_size=wave_size)

    scheduler_stop_status: dict[str, Any] | None = None
    scheduler_start_status: dict[str, Any] | None = None
    wave_history: list[dict[str, Any]] = []
    highest_activated_wave = 0
    verification: dict[str, Any] = {
        "duplicate_handles": [],
        "missing_handles": [],
        "unexpected_active_handles": [],
        "unexpected_inactive_handles": [],
    }

    if dry_run:
        summary = build_summary(
            csv_path=csv_path,
            api_base=client.api_base,
            dry_run=True,
            rows=rows,
            wave_size=wave_size,
            highest_activated_wave=highest_activated_wave,
            wave_history=wave_history,
            verification=verification,
            scheduler_stop_status=scheduler_stop_status,
            scheduler_start_status=scheduler_start_status,
        )
        return rows, summary

    scheduler_stop_status = client.stop_scheduler()

    valid_rows = [row for row in rows if row.is_valid]
    for row in valid_rows:
        try:
            response = run_with_retry(
                lambda row=row: client.create_source(
                    channel_username=row.canonical_username,
                    channel_title=row.fallback_title,
                    scrape_depth_days=scrape_depth_days,
                    scrape_comments=scrape_comments,
                ),
                max_attempts=max_attempts,
                base_delay_seconds=retry_backoff_seconds,
            )
        except ApiCallError as exc:
            row.api_error = str(exc)
            continue

        row.import_action = str(response.get("action") or "").strip() or None
        item = response.get("item") or {}
        record_source_state(row, item, import_action=row.import_action)

    successful_rows = [row for row in valid_rows if row.was_imported]
    first_wave = 1 if any(row.wave_number == 1 for row in successful_rows) else 0
    highest_activated_wave = first_wave

    for row in successful_rows:
        if (row.wave_number or 0) <= 1:
            continue
        if not row.final_id:
            continue
        try:
            response = run_with_retry(
                lambda row=row: client.update_source(row.final_id or "", is_active=False),
                max_attempts=max_attempts,
                base_delay_seconds=retry_backoff_seconds,
            )
        except ApiCallError as exc:
            row.state_error = str(exc)
            continue

        item = response.get("item") or {}
        record_source_state(row, item)

    scheduler_start_status = client.start_scheduler()

    total_waves = max((row.wave_number or 0) for row in valid_rows) if valid_rows else 0
    current_wave = highest_activated_wave
    while current_wave and current_wave < total_waves:
        cycle_result = wait_for_clean_cycle(
            client,
            poll_interval_seconds=poll_interval_seconds,
            timeout_seconds=cycle_timeout_seconds,
        )
        scheduler_status = cycle_result.get("scheduler_status") or {}
        freshness = client.get_freshness()
        decision = evaluate_wave_gate(
            freshness,
            scheduler_status,
            clean_cycle_completed=bool(cycle_result.get("clean_cycle_completed", False)),
            post_threshold=post_backlog_threshold,
            comment_threshold=comment_backlog_threshold,
        )

        history_entry: dict[str, Any] = {
            "wave_completed": current_wave,
            "next_wave": current_wave + 1,
            "timed_out": bool(cycle_result.get("timed_out", False)),
            "decision": decision.as_dict(),
        }

        if not decision.healthy:
            history_entry["activated_next_wave"] = False
            wave_history.append(history_entry)
            break

        next_wave_rows = [row for row in successful_rows if row.wave_number == current_wave + 1]
        activation_errors: list[str] = []
        for row in next_wave_rows:
            if not row.final_id:
                continue
            try:
                response = run_with_retry(
                    lambda row=row: client.update_source(row.final_id or "", is_active=True),
                    max_attempts=max_attempts,
                    base_delay_seconds=retry_backoff_seconds,
                )
            except ApiCallError as exc:
                row.state_error = str(exc)
                activation_errors.append(row.normalized_handle)
                continue

            item = response.get("item") or {}
            record_source_state(row, item)

        if activation_errors:
            history_entry["activated_next_wave"] = False
            history_entry["activation_errors"] = activation_errors
            wave_history.append(history_entry)
            break
        history_entry["activated_next_wave"] = True
        wave_history.append(history_entry)
        highest_activated_wave = current_wave + 1
        current_wave += 1

    final_sources_payload = client.list_sources()
    final_sources = list(final_sources_payload.get("items") or [])
    verification = verify_final_state(rows, final_sources, highest_activated_wave=highest_activated_wave)
    summary = build_summary(
        csv_path=csv_path,
        api_base=client.api_base,
        dry_run=False,
        rows=rows,
        wave_size=wave_size,
        highest_activated_wave=highest_activated_wave,
        wave_history=wave_history,
        verification=verification,
        scheduler_stop_status=scheduler_stop_status,
        scheduler_start_status=scheduler_start_status,
    )
    return rows, summary
