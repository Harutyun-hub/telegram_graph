from __future__ import annotations

import argparse
from typing import Any

from social.store import SocialStore
from social.text_cleaning import clean_social_text_content, looks_like_raw_social_payload


def _trimmed(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _load_candidate_rows(store: SocialStore, *, limit: int) -> list[dict[str, Any]]:
    response = (
        store.client.table("social_activities")
        .select("id,activity_uid,text_content,provider_payload,analysis_status,graph_status")
        .limit(max(1, int(limit)))
        .execute()
    )
    return list(response.data or [])


def _repair_payload(row: dict[str, Any]) -> dict[str, Any] | None:
    current = _trimmed(row.get("text_content"))
    if not looks_like_raw_social_payload(current):
        return None
    cleaned = clean_social_text_content(
        current,
        provider_payload=row.get("provider_payload"),
    )
    if not cleaned or cleaned == current:
        return None
    return {
        "text_content": cleaned,
        "analysis_status": "pending",
        "graph_status": "not_ready",
        "analysis_claimed_at": None,
        "analysis_claimed_by": None,
        "graph_claimed_at": None,
        "graph_claimed_by": None,
        "last_error": None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair polluted social_activities.text_content rows.")
    parser.add_argument("--limit", type=int, default=500, help="Maximum rows to inspect.")
    parser.add_argument("--apply", action="store_true", help="Write repairs. Defaults to dry-run.")
    args = parser.parse_args()

    store = SocialStore()
    rows = _load_candidate_rows(store, limit=args.limit)
    repairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for row in rows:
        payload = _repair_payload(row)
        if payload:
            repairs.append((row, payload))

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"{mode}: inspected={len(rows)} repairable={len(repairs)}")
    for row, payload in repairs[:10]:
        before = _trimmed(row.get("text_content"))[:180].replace("\n", " ")
        after = _trimmed(payload.get("text_content"))[:180].replace("\n", " ")
        print(f"- {row.get('activity_uid')}:")
        print(f"  before: {before}")
        print(f"  after:  {after}")

    if not args.apply:
        return 0

    for row, payload in repairs:
        store.client.table("social_activities").update(payload).eq("id", row["id"]).execute()
    print(f"Applied repairs: {len(repairs)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
