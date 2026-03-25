#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
from statistics import median
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
TOPIC_PRESENTATION_PATH = ROOT / "frontend" / "src" / "app" / "config" / "topicPresentation.json"

sys.path.insert(0, str(ROOT))

from utils.topic_normalizer import classify_topic  # noqa: E402

ALLOWED_GROUPS = ["Living", "Work", "Family", "Finance", "Lifestyle", "Integration", "Admin"]


def _load_topic_presentation() -> dict[str, Any]:
    return json.loads(TOPIC_PRESENTATION_PATH.read_text(encoding="utf-8"))


def _fetch_json(base_url: str, path: str, params: dict[str, Any]) -> Any:
    url = f"{base_url}{path}?{urlencode({k: v for k, v in params.items() if v is not None})}"
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def _timed_fetch(base_url: str, path: str, params: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    payload = _fetch_json(base_url, path, params)
    elapsed = time.perf_counter() - started
    return {"elapsed": elapsed, "payload": payload}


def _serial_samples(base_url: str, path: str, params: dict[str, Any], samples: int = 5) -> dict[str, Any]:
    timings: list[float] = []
    payload: Any = None
    for _ in range(samples):
        result = _timed_fetch(base_url, path, params)
        timings.append(result["elapsed"])
        payload = result["payload"]
    return {
        "path": path,
        "params": params,
        "samples": timings,
        "median": median(timings),
        "max": max(timings),
        "payload": payload,
    }


def _build_topic_paths(base_url: str, from_date: str, to_date: str) -> dict[str, Any]:
    topics = _fetch_json(base_url, "/api/topics", {"from": from_date, "to": to_date, "size": 200, "page": 0})
    if not topics:
        raise RuntimeError("No topics returned from /api/topics")
    first = topics[0]
    topic_name = first.get("sourceTopic") or first.get("name")
    category = first.get("category")
    if not topic_name or not category:
        raise RuntimeError("First topic row is missing sourceTopic/name or category")
    return {
        "topics": {"path": "/api/topics", "params": {"from": from_date, "to": to_date, "size": 200, "page": 0}},
        "detail": {"path": "/api/topics/detail", "params": {"from": from_date, "to": to_date, "topic": topic_name, "category": category}},
        "evidence": {"path": "/api/topics/evidence", "params": {"from": from_date, "to": to_date, "topic": topic_name, "category": category, "page": 0, "size": 20}},
    }


def capture_baseline(base_url: str, from_date: str, to_date: str, out_path: str) -> dict[str, Any]:
    paths = _build_topic_paths(base_url, from_date, to_date)
    baseline = {
        "generatedAt": time.time(),
        "baseUrl": base_url,
        "from": from_date,
        "to": to_date,
        "endpoints": {
            key: {k: v for k, v in _serial_samples(base_url, spec["path"], spec["params"]).items() if k != "payload"}
            for key, spec in paths.items()
        },
    }
    Path(out_path).write_text(json.dumps(baseline, indent=2), encoding="utf-8")
    return baseline


def run_contract(base_url: str, from_date: str, to_date: str) -> dict[str, Any]:
    topics = _fetch_json(base_url, "/api/topics", {"from": from_date, "to": to_date, "size": 500, "page": 0})
    violations: list[dict[str, Any]] = []
    for row in topics:
        name = str(row.get("name") or "")
        category = str(row.get("category") or "")
        if int(row.get("mentionCount") or 0) <= 0:
            violations.append({"topic": name, "reason": "mentionCount<=0"})
        if int(row.get("evidenceCount") or 0) < 1:
            violations.append({"topic": name, "reason": "evidenceCount<1"})
        if not str(row.get("sampleEvidenceId") or "").strip():
            violations.append({"topic": name, "reason": "missing sampleEvidenceId"})
        if not str(row.get("sampleQuote") or "").strip():
            violations.append({"topic": name, "reason": "missing sampleQuote"})
        if str(row.get("topicGroup") or "") not in ALLOWED_GROUPS:
            violations.append({"topic": name, "reason": "invalid topicGroup", "value": row.get("topicGroup")})
        if category == "General":
            violations.append({"topic": name, "reason": "category=General"})
        if classify_topic(name) is None:
            violations.append({"topic": name, "reason": "classify_topic returned None"})

    consistency_failures: list[dict[str, Any]] = []
    for row in topics[:10]:
        topic_name = row.get("sourceTopic") or row.get("name")
        category = row.get("category")
        detail = _fetch_json(base_url, "/api/topics/detail", {"from": from_date, "to": to_date, "topic": topic_name, "category": category})
        evidence = _fetch_json(base_url, "/api/topics/evidence", {"from": from_date, "to": to_date, "topic": topic_name, "category": category, "page": 0, "size": 5})
        detail_ids = {item.get("id") for item in detail.get("evidence", [])}
        feed_ids = {item.get("id") for item in evidence.get("items", [])}
        if detail.get("sourceTopic") != row.get("sourceTopic"):
            consistency_failures.append({"topic": topic_name, "reason": "sourceTopic mismatch"})
        if int(detail.get("mentionCount") or 0) != int(row.get("mentionCount") or 0):
            consistency_failures.append({"topic": topic_name, "reason": "mentionCount mismatch"})
        if str(detail.get("sampleEvidenceId") or "") not in detail_ids:
            consistency_failures.append({"topic": topic_name, "reason": "detail sampleEvidenceId missing from detail evidence"})
        if str(row.get("sampleEvidenceId") or "") not in feed_ids:
            consistency_failures.append({"topic": topic_name, "reason": "summary sampleEvidenceId missing from evidence feed"})

    passed = not violations and not consistency_failures
    return {
        "check": "contract",
        "passed": passed,
        "topicCount": len(topics),
        "violations": violations,
        "consistencyFailures": consistency_failures,
    }


def run_translations(base_url: str, from_date: str, to_date: str) -> dict[str, Any]:
    topics = _fetch_json(base_url, "/api/topics", {"from": from_date, "to": to_date, "size": 500, "page": 0})
    presentation = _load_topic_presentation()
    topic_ru = presentation.get("topicRu", {})
    category_ru = presentation.get("categoryRu", {})
    missing_topics = sorted({row.get("name") for row in topics if row.get("name") not in topic_ru})
    missing_categories = sorted({row.get("category") for row in topics if row.get("category") not in category_ru})
    return {
        "check": "translations",
        "passed": not missing_topics and not missing_categories,
        "missingTopics": missing_topics,
        "missingCategories": missing_categories,
    }


def run_perf(base_url: str, from_date: str, to_date: str, baseline_path: str, concurrency: int) -> dict[str, Any]:
    baseline = json.loads(Path(baseline_path).read_text(encoding="utf-8"))
    paths = _build_topic_paths(base_url, from_date, to_date)
    serial = {
        key: {k: v for k, v in _serial_samples(base_url, spec["path"], spec["params"]).items() if k != "payload"}
        for key, spec in paths.items()
    }

    topics_spec = paths["topics"]

    def _one_request(_: int) -> float:
        return _timed_fetch(base_url, topics_spec["path"], topics_spec["params"])["elapsed"]

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
        concurrent_samples = list(executor.map(_one_request, range(max(1, concurrency))))

    failures: list[str] = []
    for key, result in serial.items():
        baseline_median = float(baseline["endpoints"][key]["median"])
        threshold = max(1.5 * baseline_median, 1.20)
        if float(result["median"]) > threshold:
            failures.append(f"{key} serial median {result['median']:.3f}s > {threshold:.3f}s")

    topics_baseline = float(baseline["endpoints"]["topics"]["median"])
    concurrent_median = float(median(concurrent_samples))
    concurrent_max = float(max(concurrent_samples))
    if concurrent_median > 2.0 * float(serial["topics"]["median"]):
        failures.append("topics concurrent median exceeded 2.0x postchange serial median")
    if concurrent_max > 3.0 * topics_baseline:
        failures.append("topics concurrent max exceeded 3.0x baseline serial median")

    return {
        "check": "perf",
        "passed": not failures,
        "serial": serial,
        "concurrent": {
            "samples": concurrent_samples,
            "median": concurrent_median,
            "max": concurrent_max,
        },
        "failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Topics page QA harness")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name in ("capture-baseline", "contract", "translations", "perf"):
        sub = subparsers.add_parser(name)
        sub.add_argument("--base-url", required=True)
        sub.add_argument("--from", dest="from_date", required=True)
        sub.add_argument("--to", dest="to_date", required=True)
        if name == "capture-baseline":
            sub.add_argument("--out", required=True)
        if name == "perf":
            sub.add_argument("--baseline", required=True)
            sub.add_argument("--concurrency", type=int, default=10)

    args = parser.parse_args()
    try:
        if args.command == "capture-baseline":
            result = capture_baseline(args.base_url, args.from_date, args.to_date, args.out)
            result["passed"] = Path(args.out).exists()
        elif args.command == "contract":
            result = run_contract(args.base_url, args.from_date, args.to_date)
        elif args.command == "translations":
            result = run_translations(args.base_url, args.from_date, args.to_date)
        else:
            result = run_perf(args.base_url, args.from_date, args.to_date, args.baseline, args.concurrency)
    except (HTTPError, URLError, RuntimeError, ValueError, KeyError) as exc:
        result = {
            "check": args.command,
            "passed": False,
            "error": str(exc),
        }

    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
