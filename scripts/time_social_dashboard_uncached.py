#!/usr/bin/env python3
"""Measure uncached Social dashboard snapshot builds without disabling live cache.

This is an operator QA helper. It calls the internal Social dashboard builder with
use_cache=False so we can inspect true cold-build timings while keeping the
public /social endpoint cache-first and safe for users.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.social_dashboard import build_social_dashboard_snapshot  # noqa: E402
from social.store import SocialStore  # noqa: E402


def _source_label(row: dict[str, Any]) -> str:
    return str(row.get("company_name") or row.get("display_url") or row.get("entity_id") or "").strip()


def _find_entity_id(source_rows: list[dict[str, Any]], query: str) -> str:
    wanted = query.strip().casefold()
    candidates: list[tuple[int, str, dict[str, Any]]] = []
    for row in source_rows:
        label = _source_label(row)
        folded = label.casefold()
        if not folded:
            continue
        if folded == wanted:
            candidates.append((0, label, row))
        elif wanted in folded:
            candidates.append((1, label, row))
    if not candidates:
        available = ", ".join(sorted({_source_label(row) for row in source_rows if _source_label(row)})[:12])
        raise ValueError(f"Could not find social source matching {query!r}. Available examples: {available}")
    candidates.sort(key=lambda item: (item[0], item[1]))
    entity_id = str(candidates[0][2].get("entity_id") or "").strip()
    if not entity_id:
        raise ValueError(f"Matched source {candidates[0][1]!r} has no entity_id")
    return entity_id


def _run_probe(
    store: SocialStore,
    *,
    name: str,
    from_date: str,
    to_date: str,
    entity_id: str | None = None,
    compare_entity_id: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    snapshot = build_social_dashboard_snapshot(
        store,
        from_date=from_date,
        to_date=to_date,
        entity_id=entity_id,
        compare_entity_id=compare_entity_id,
        platform=platform,
        use_cache=False,
    )
    elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
    meta = snapshot.get("meta") or {}
    return {
        "name": name,
        "elapsedMs": elapsed_ms,
        "usedActivities": meta.get("usedActivities"),
        "scannedActivities": meta.get("scannedActivities"),
        "degradedSections": meta.get("degradedSections") or [],
        "timingsMs": meta.get("timingsMs") or {},
    }


def main() -> int:
    today = date.today()
    parser = argparse.ArgumentParser(description="Time uncached Social dashboard snapshots.")
    parser.add_argument("--from", dest="from_date", default=(today - timedelta(days=15)).isoformat())
    parser.add_argument("--to", dest="to_date", default=today.isoformat())
    parser.add_argument("--single-source", default="Nikol Pashinyan")
    parser.add_argument("--compare-source-a", default="Azbevs")
    parser.add_argument("--compare-source-b", default="Edmon Marukyan")
    parser.add_argument("--max-seconds", type=float, default=15.0)
    parser.add_argument("--json", action="store_true", help="Print compact JSON only.")
    args = parser.parse_args()

    store = SocialStore()
    sources = store.list_source_rows()
    single_entity_id = _find_entity_id(sources, args.single_source)
    compare_a_id = _find_entity_id(sources, args.compare_source_a)
    compare_b_id = _find_entity_id(sources, args.compare_source_b)

    probes = [
        {"name": "all_sources", "entity_id": None, "compare_entity_id": None, "platform": None},
        {"name": f"single_source:{args.single_source}", "entity_id": single_entity_id, "compare_entity_id": None, "platform": None},
        {
            "name": f"compare:{args.compare_source_a}:vs:{args.compare_source_b}",
            "entity_id": compare_a_id,
            "compare_entity_id": compare_b_id,
            "platform": None,
        },
        {"name": "platform:facebook", "entity_id": None, "compare_entity_id": None, "platform": "facebook"},
    ]

    results: list[dict[str, Any]] = []
    failures: list[str] = []
    for probe in probes:
        try:
            result = _run_probe(
                store,
                name=str(probe["name"]),
                from_date=args.from_date,
                to_date=args.to_date,
                entity_id=probe["entity_id"],
                compare_entity_id=probe["compare_entity_id"],
                platform=probe["platform"],
            )
            if result["elapsedMs"] > args.max_seconds * 1000:
                failures.append(f"{result['name']} exceeded {args.max_seconds:.1f}s ({result['elapsedMs']}ms)")
            results.append(result)
        except Exception as exc:
            failures.append(f"{probe['name']} failed: {exc}")
            results.append({"name": probe["name"], "error": str(exc)})

    payload = {
        "window": {"from": args.from_date, "to": args.to_date},
        "maxSeconds": args.max_seconds,
        "results": results,
        "failures": failures,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
