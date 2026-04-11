from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from api.source_resolution import build_pending_source_payload, enqueue_missing_peer_ref_backfill, ensure_resolution_job
from buffer.supabase_writer import SupabaseWriter
from utils.source_bulk_import import SourceImportApiClient, utc_now


def _env(name: str) -> str:
    return str(os.getenv(name, "") or "").strip()


def _parse_iso_datetime(value: str | None) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def _is_legacy_flood_wait(item: dict) -> bool:
    if bool(item.get("is_active")):
        return False
    if str(item.get("resolution_status") or "").strip().lower() != "error":
        return False
    error_code = str(item.get("resolution_error_code") or "").strip().lower()
    message = str(item.get("last_resolution_error") or "").strip().lower()
    if error_code == "flood_wait":
        return True
    return "wait of" in message or "floodwait" in message


def _is_invalid_permanent_error(item: dict) -> bool:
    error_code = str(item.get("resolution_error_code") or "").strip().lower()
    return error_code in {
        "username_missing",
        "username_unacceptable",
        "channel_private",
        "unsupported_peer",
    }


def _resolved_inactive_candidates(items: list[dict]) -> list[dict]:
    rows = [
        item
        for item in items
        if not bool(item.get("is_active"))
        and str(item.get("resolution_status") or "").strip().lower() == "resolved"
    ]

    def sort_key(item: dict) -> tuple:
        scraped_rank = 0 if item.get("last_scraped_at") else 1
        source_type = str(item.get("source_type") or "").strip().lower()
        source_rank = 0 if source_type == "channel" else 1
        member_count = int(item.get("member_count") or 0)
        return (scraped_rank, source_rank, -member_count, str(item.get("channel_username") or ""))

    return sorted(rows, key=sort_key)


def _cleanup_candidates(items: list[dict]) -> list[dict]:
    rows = [
        item for item in items
        if _is_legacy_flood_wait(item) and not _is_invalid_permanent_error(item)
    ]
    return sorted(rows, key=lambda item: (_parse_iso_datetime(item.get("updated_at")), str(item.get("channel_username") or "")))


def _active_non_resolved(items: list[dict]) -> list[dict]:
    return [
        item
        for item in items
        if bool(item.get("is_active"))
        and str(item.get("resolution_status") or "").strip().lower() != "resolved"
    ]


@dataclass
class QueueState:
    running: bool
    due_jobs: int
    leased_jobs: int
    cooldown_slots: int
    active_pending_sources: int
    active_missing_peer_refs: int

    @property
    def idle_for_activation(self) -> bool:
        return (
            not self.running
            and self.due_jobs == 0
            and self.leased_jobs == 0
            and self.cooldown_slots == 0
            and self.active_pending_sources == 0
            and self.active_missing_peer_refs == 0
        )


def _queue_state(snapshot: dict) -> QueueState:
    return QueueState(
        running=bool(snapshot.get("running_now")),
        due_jobs=int(snapshot.get("snapshot", {}).get("due_jobs", snapshot.get("due_jobs", 0)) or 0),
        leased_jobs=int(snapshot.get("snapshot", {}).get("leased_jobs", snapshot.get("leased_jobs", 0)) or 0),
        cooldown_slots=int(snapshot.get("snapshot", {}).get("cooldown_slots", snapshot.get("cooldown_slots", 0)) or 0),
        active_pending_sources=int(snapshot.get("snapshot", {}).get("active_pending_sources", snapshot.get("active_pending_sources", 0)) or 0),
        active_missing_peer_refs=int(snapshot.get("snapshot", {}).get("active_missing_peer_refs", snapshot.get("active_missing_peer_refs", 0)) or 0),
    )


def default_output_dir() -> Path:
    stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    return PROJECT_ROOT / "tmp" / f"source-wave-manager-{stamp}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Automate Telegram source wave activation and flood-wait cleanup.")
    parser.add_argument("--api-base", default=_env("SOURCE_IMPORT_API_BASE"), help="Base API URL, with or without /api.")
    parser.add_argument(
        "--auth-token",
        default=_env("SOURCE_IMPORT_AUTH_TOKEN"),
        help="Optional Supabase access token. Accepts raw or Bearer token.",
    )
    parser.add_argument("--supabase-url", default=_env("SUPABASE_URL"))
    parser.add_argument("--service-role-key", default=_env("SUPABASE_SERVICE_ROLE_KEY"))
    parser.add_argument("--activate-wave-size", type=int, default=10)
    parser.add_argument("--cleanup-batch-size", type=int, default=20)
    parser.add_argument("--max-steps", type=int, default=1, help="How many activation/cleanup steps to execute in this run.")
    parser.add_argument("--poll-interval-seconds", type=float, default=15.0)
    parser.add_argument("--wait-timeout-seconds", type=float, default=600.0)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-dir", default="", help="Directory for a JSON summary of actions taken.")
    return parser


class DirectSupabaseSourceClient:
    def __init__(self, *, supabase_url: str, service_role_key: str) -> None:
        if not supabase_url or not service_role_key:
            raise ValueError("Direct Supabase mode requires both SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")
        os.environ.setdefault("SUPABASE_URL", supabase_url)
        os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", service_role_key)
        self.api_base = "direct-supabase"
        self.writer = SupabaseWriter()

    def list_sources(self) -> dict:
        return {"items": self.writer.list_channels()}

    def update_source(self, channel_id: str, *, is_active: bool | None = None) -> dict:
        payload: dict = {}
        if is_active is not None:
            payload["is_active"] = bool(is_active)
        updated = self.writer.update_channel(channel_id, payload) or self.writer.get_channel_by_id(channel_id) or {}
        if bool(updated.get("is_active")) and str(updated.get("resolution_status") or "").strip().lower() != "resolved":
            pending_title = (updated.get("channel_title") or updated.get("channel_username") or "").strip()
            self.writer.update_channel(
                channel_id,
                build_pending_source_payload(channel_title=pending_title or ""),
            )
            updated = self.writer.get_channel_by_id(channel_id) or updated
            ensure_resolution_job(self.writer, updated)
            updated = self.writer.get_channel_by_id(channel_id) or updated
        return {"item": updated}

    def get_source_resolution_status(self) -> dict:
        return {
            "enabled": True,
            "running_now": False,
            "snapshot": self.writer.get_source_resolution_snapshot(),
        }

    def run_source_resolution_once(self) -> dict:
        return {
            "ok": True,
            "mode": "direct-supabase",
            "note": "No direct worker trigger in service-role mode; the production resolution worker will pick up queued jobs automatically.",
            "snapshot": self.writer.get_source_resolution_snapshot(),
        }

    def backfill_peer_refs(self, *, active_only: bool = True, limit: int = 100) -> dict:
        queued = enqueue_missing_peer_ref_backfill(
            self.writer,
            active_only=bool(active_only),
            limit=int(limit),
        )
        return {
            "queued": queued,
            "active_only": bool(active_only),
            "limit": int(limit),
            "resolution": {
                "enabled": True,
                "running_now": False,
                "snapshot": self.writer.get_source_resolution_snapshot(),
            },
        }


def _load_sources(client: SourceImportApiClient) -> list[dict]:
    payload = client.list_sources()
    return list(payload.get("items") or [])


def _activate_wave(
    client: SourceImportApiClient,
    *,
    candidates: list[dict],
    wave_size: int,
    dry_run: bool,
) -> list[dict]:
    selected = candidates[: max(1, int(wave_size))]
    results: list[dict] = []
    for item in selected:
        row = {
            "channel_id": item["id"],
            "channel_username": item["channel_username"],
            "previous_is_active": bool(item.get("is_active")),
            "previous_resolution_status": item.get("resolution_status"),
            "action": "activate",
        }
        if dry_run:
            row["result"] = "planned"
        else:
            updated = client.update_source(item["id"], is_active=True).get("item") or {}
            row.update(
                {
                    "result": "updated",
                    "is_active": bool(updated.get("is_active")),
                    "resolution_status": updated.get("resolution_status"),
                    "source_type": updated.get("source_type"),
                }
            )
        results.append(row)
    return results


def _requeue_cleanup_batch(
    client: SourceImportApiClient,
    *,
    candidates: list[dict],
    batch_size: int,
    dry_run: bool,
) -> list[dict]:
    selected = candidates[: max(1, int(batch_size))]
    results: list[dict] = []
    for item in selected:
        row = {
            "channel_id": item["id"],
            "channel_username": item["channel_username"],
            "previous_resolution_status": item.get("resolution_status"),
            "previous_error": item.get("last_resolution_error"),
            "action": "requeue_cleanup",
        }
        if dry_run:
            row["result"] = "planned"
        else:
            client.update_source(item["id"], is_active=True)
            updated = client.update_source(item["id"], is_active=False).get("item") or {}
            row.update(
                {
                    "result": "updated",
                    "is_active": bool(updated.get("is_active")),
                    "resolution_status": updated.get("resolution_status"),
                    "last_resolution_error": updated.get("last_resolution_error"),
                    "resolution_error_code": updated.get("resolution_error_code"),
                }
            )
        results.append(row)
    return results


def _wait_for_queue(
    client: SourceImportApiClient,
    *,
    poll_interval_seconds: float,
    wait_timeout_seconds: float,
) -> dict:
    deadline = time.monotonic() + max(1.0, float(wait_timeout_seconds))
    last_snapshot: dict = client.get_source_resolution_status()

    while True:
        state = _queue_state(last_snapshot)
        if state.idle_for_activation:
            return last_snapshot

        if time.monotonic() >= deadline:
            return last_snapshot

        if state.active_missing_peer_refs > 0 and not state.running and state.due_jobs == 0 and state.leased_jobs == 0:
            client.backfill_peer_refs(active_only=True, limit=state.active_missing_peer_refs)
            client.run_source_resolution_once()

        time.sleep(max(1.0, float(poll_interval_seconds)))
        last_snapshot = client.get_source_resolution_status()


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    use_direct_supabase = bool(args.supabase_url and args.service_role_key)
    if not args.api_base and not use_direct_supabase:
        raise SystemExit(
            "Missing execution target. Pass --api-base/--auth-token for API mode or --supabase-url/--service-role-key for direct Supabase mode."
        )

    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else default_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    if use_direct_supabase:
        client = DirectSupabaseSourceClient(
            supabase_url=args.supabase_url,
            service_role_key=args.service_role_key,
        )
    else:
        client = SourceImportApiClient(
            api_base=args.api_base,
            auth_token=args.auth_token,
            timeout_seconds=args.timeout_seconds,
        )

    summary: dict = {
        "started_at": utc_now().isoformat(),
        "api_base": client.api_base,
        "dry_run": bool(args.dry_run),
        "activate_wave_size": int(args.activate_wave_size),
        "cleanup_batch_size": int(args.cleanup_batch_size),
        "max_steps": int(args.max_steps),
        "steps": [],
    }

    for step_index in range(max(1, int(args.max_steps))):
        queue_before = _wait_for_queue(
            client,
            poll_interval_seconds=float(args.poll_interval_seconds),
            wait_timeout_seconds=float(args.wait_timeout_seconds),
        )
        queue_state_before = _queue_state(queue_before)
        sources = _load_sources(client)
        activation_candidates = _resolved_inactive_candidates(sources)
        cleanup_candidates = _cleanup_candidates(sources)
        active_non_resolved = _active_non_resolved(sources)

        step_summary: dict = {
            "step": step_index + 1,
            "queued_before": queue_before,
            "activation_candidates": len(activation_candidates),
            "cleanup_candidates": len(cleanup_candidates),
            "active_non_resolved": [
                {
                    "channel_username": item.get("channel_username"),
                    "resolution_status": item.get("resolution_status"),
                    "last_resolution_error": item.get("last_resolution_error"),
                }
                for item in active_non_resolved
            ],
        }

        if not queue_state_before.idle_for_activation:
            step_summary["status"] = "queue_busy"
            summary["steps"].append(step_summary)
            break

        activated = _activate_wave(
            client,
            candidates=activation_candidates,
            wave_size=int(args.activate_wave_size),
            dry_run=bool(args.dry_run),
        )
        cleanup = _requeue_cleanup_batch(
            client,
            candidates=cleanup_candidates,
            batch_size=int(args.cleanup_batch_size),
            dry_run=bool(args.dry_run),
        )
        step_summary["activated"] = activated
        step_summary["cleanup_requeued"] = cleanup

        if not args.dry_run:
            if activated:
                client.backfill_peer_refs(active_only=True, limit=len(activated))
            if activated or cleanup:
                client.run_source_resolution_once()

        step_summary["status"] = "completed"
        step_summary["queue_after_trigger"] = client.get_source_resolution_status()
        step_summary["remaining_activation_candidates"] = max(0, len(activation_candidates) - len(activated))
        step_summary["remaining_cleanup_candidates"] = max(0, len(cleanup_candidates) - len(cleanup))
        summary["steps"].append(step_summary)

        if not activated and not cleanup:
            break

    summary["finished_at"] = utc_now().isoformat()
    output_path = output_dir / "summary.json"
    output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"[wave-manager] summary_json={output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
