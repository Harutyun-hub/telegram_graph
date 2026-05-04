from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

import config  # noqa: E402
from social.analysis import SocialActivityAnalyzer  # noqa: E402
from social.store import SocialStore  # noqa: E402
from social.text_cleaning import clean_social_activity_row  # noqa: E402


def _topic_names(value: Any) -> list[str]:
    names: list[str] = []
    items = value if isinstance(value, list) else []
    for item in items:
        if isinstance(item, dict):
            text = str(item.get("name") or item.get("taxonomy_topic") or item.get("proposed_topic") or "").strip()
        else:
            text = str(item or "").strip()
        if text and text not in names:
            names.append(text)
    return names


def _short(value: Any, limit: int = 110) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[: limit - 3].rstrip() + "..."


def _load_recent_activities(store: SocialStore, limit: int) -> list[dict[str, Any]]:
    rows = store._select_rows(
        "social_activities",
        filters=(("eq", "ingest_status", "normalized"),),
        order_by="last_seen_at",
        desc=True,
        limit=limit,
    )
    activity_ids = [row["id"] for row in rows if row.get("id")]
    entity_ids = sorted({row["entity_id"] for row in rows if row.get("entity_id")})
    entities = {
        row["id"]: row
        for row in store._select_rows("social_entities", filters=(("in", "id", entity_ids),))
    } if entity_ids else {}
    analyses = {
        row["activity_id"]: row
        for row in store._select_rows(
            "social_activity_analysis",
            filters=(("in", "activity_id", activity_ids),),
        )
    } if activity_ids else {}
    return [
        clean_social_activity_row(
            {
                **row,
                "entity": entities.get(row.get("entity_id")),
                "analysis": analyses.get(row.get("id")),
            }
        )
        for row in rows
    ]


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Social Lens Replay Comparison",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Applied to Supabase: `{report['applied']}`",
        f"- Activities compared: `{len(report['items'])}`",
        "",
        "| Activity | Platform | Old topics | New topics | Lens | Quality | Summary shift |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in report["items"]:
        lines.append(
            "| {activity_uid} | {platform} | {old_topics} | {new_topics} | {lens} | {quality} | {summary} |".format(
                activity_uid=item["activity_uid"],
                platform=item["platform"],
                old_topics=", ".join(item["old_topics"]) or "-",
                new_topics=", ".join(item["new_topics"]) or "-",
                lens=", ".join(item["matched_lenses"]) or "-",
                quality=item["lens_quality"] or "-",
                summary=_short(item["new_summary"]),
            )
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay recent social activities through the active AI lens and compare old vs new analysis.")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--apply", action="store_true", help="Overwrite social_activity_analysis rows with the new lens-aware output.")
    parser.add_argument("--output-dir", default="tmp/lens-replay")
    args = parser.parse_args()

    store = SocialStore()
    analyzer = SocialActivityAnalyzer()
    activities = _load_recent_activities(store, max(1, args.limit))

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for activity in activities:
        grouped[(str(activity.get("entity_id")), str(activity.get("platform")))].append(activity)

    new_rows: list[dict[str, Any]] = []
    batch_size = max(1, int(config.SOCIAL_ANALYSIS_BATCH_SIZE))
    for batch in grouped.values():
        for offset in range(0, len(batch), batch_size):
            new_rows.extend(analyzer.analyze_batch(batch[offset:offset + batch_size]))

    old_by_id = {activity["id"]: activity for activity in activities}
    items: list[dict[str, Any]] = []
    for row in new_rows:
        activity = old_by_id.get(row["activity_id"], {})
        old_payload = (activity.get("analysis") or {}).get("analysis_payload") or {}
        new_payload = row.get("analysis_payload") or {}
        items.append(
            {
                "activity_id": row["activity_id"],
                "activity_uid": row["activity_uid"],
                "platform": row["platform"],
                "old_summary": old_payload.get("summary"),
                "new_summary": new_payload.get("summary"),
                "old_topics": _topic_names(old_payload.get("topics")),
                "new_topics": _topic_names(new_payload.get("topics")),
                "lens_relevance": new_payload.get("lens_relevance"),
                "matched_lenses": new_payload.get("matched_lenses") or [],
                "lens_signals": new_payload.get("lens_signals") or [],
                "lens_quality": new_payload.get("lens_quality"),
                "analysis_lens_ids": new_payload.get("analysis_lens_ids") or [],
                "analysis_lens_signature": new_payload.get("analysis_lens_signature"),
            }
        )
        if args.apply:
            store.save_analysis(**row)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "applied": bool(args.apply),
        "items": items,
    }
    json_path = output_dir / f"social_lens_replay_{stamp}.json"
    md_path = output_dir / f"social_lens_replay_{stamp}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_markdown(report), encoding="utf-8")
    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    print(_render_markdown(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
