from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from difflib import SequenceMatcher

from loguru import logger

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from buffer.supabase_writer import SupabaseWriter
from utils.taxonomy import TAXONOMY_DOMAINS, TOPIC_ALIASES, iter_topics


_NOISE_TERMS = {
    "null",
    "none",
    "n/a",
    "na",
    "unknown",
    "general",
    "undefined",
    "other",
    "misc",
}


def _normalize_text(value: str) -> str:
    return " ".join(str(value or "").strip().lower().replace("&", "and").split())


_CANONICAL_TOPICS = list(iter_topics())
_CANONICAL_BY_NORMALIZED = {_normalize_text(topic): topic for topic in _CANONICAL_TOPICS}
_CATEGORY_SET = {
    category
    for categories in TAXONOMY_DOMAINS.values()
    for category in categories.keys()
}
_DOMAIN_SET = set(TAXONOMY_DOMAINS.keys())


def _is_noise_topic(name: str) -> tuple[bool, str]:
    value = str(name or "").strip()
    lowered = value.lower()
    if not value:
        return True, "empty"
    if lowered in _NOISE_TERMS:
        return True, f"noise_term:{lowered}"
    if len(value) < 3:
        return True, "too_short"
    if re.fullmatch(r"[\W_]+", value):
        return True, "symbol_only"
    if re.fullmatch(r"\d+", value):
        return True, "numeric_only"
    return False, ""


def _is_structure_label(name: str) -> tuple[bool, str]:
    normalized = _normalize_text(name)
    for category in _CATEGORY_SET:
        if normalized == _normalize_text(category):
            return True, "category_label"
    for domain in _DOMAIN_SET:
        if normalized == _normalize_text(domain):
            return True, "domain_label"
    return False, ""


def _canonical_alias_target(name: str) -> tuple[str | None, str]:
    normalized = _normalize_text(name)
    if not normalized:
        return None, ""

    alias_direct = TOPIC_ALIASES.get(normalized)
    if alias_direct:
        return alias_direct, "taxonomy_alias"

    direct = _CANONICAL_BY_NORMALIZED.get(normalized)
    if direct:
        return direct, "normalized_exact"

    best_topic = None
    best_score = 0.0
    for key, topic in _CANONICAL_BY_NORMALIZED.items():
        score = SequenceMatcher(None, normalized, key).ratio()
        if score > best_score:
            best_score = score
            best_topic = topic

    if best_topic and best_score >= 0.92:
        return best_topic, f"high_similarity:{best_score:.3f}"
    return None, ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Triage pending topic proposals with enterprise rules")
    parser.add_argument("--limit", type=int, default=300, help="Maximum pending proposals to scan")
    parser.add_argument("--apply", action="store_true", help="Apply rejections (default is dry-run)")
    parser.add_argument("--reviewed-by", default="auto-triage", help="Reviewer name stored in review metadata")
    parser.add_argument(
        "--allow-structure-labels",
        action="store_true",
        help="Do not auto-reject proposals that match taxonomy category/domain labels",
    )
    parser.add_argument(
        "--approve-aliases",
        action="store_true",
        help="Auto-approve obvious canonical aliases (exact normalized or high similarity)",
    )
    parser.add_argument(
        "--min-signals",
        type=int,
        default=2,
        help="Minimum distinct-content/proposed signal count before auto-approval",
    )
    args = parser.parse_args()

    writer = SupabaseWriter()
    proposals = writer.list_topic_proposals(status="pending", limit=max(1, int(args.limit)))

    reject_candidates: list[tuple[str, str]] = []
    approve_candidates: list[tuple[str, str, str]] = []
    macro_candidates: list[tuple[str, str]] = []

    for row in proposals:
        topic_name = str(row.get("topic_name") or "").strip()
        if not topic_name:
            continue

        distinct_content_count = int(row.get("distinct_content_count") or 0)
        proposed_count = int(row.get("proposed_count") or 0)
        signal_count = max(distinct_content_count, proposed_count)

        is_noise, reason = _is_noise_topic(topic_name)
        if is_noise:
            reject_candidates.append((topic_name, reason))
            continue

        is_structure, structure_reason = _is_structure_label(topic_name)
        if is_structure:
            if args.allow_structure_labels:
                macro_candidates.append((topic_name, structure_reason))
            else:
                reject_candidates.append((topic_name, structure_reason))
            continue

        if args.approve_aliases:
            canonical_topic, alias_reason = _canonical_alias_target(topic_name)
            if canonical_topic:
                if signal_count < max(1, int(args.min_signals)):
                    continue
                approve_candidates.append((topic_name, canonical_topic, alias_reason))

    logger.info(
        "Pending proposals scanned={} reject_candidates={} approve_candidates={} macro_candidates={} apply={}"
        .format(
            len(proposals),
            len(reject_candidates),
            len(approve_candidates),
            len(macro_candidates),
            args.apply,
        )
    )
    by_name = {
        str(row.get("topic_name") or "").strip(): row
        for row in proposals
    }
    for topic_name, reason in reject_candidates:
        row = by_name.get(topic_name) or {}
        logger.info(
            "  REJECT_CANDIDATE: {!r} reason={} proposed_count={} distinct_content_count={}".format(
                topic_name,
                reason,
                int(row.get("proposed_count") or 0),
                int(row.get("distinct_content_count") or 0),
            )
        )
    for topic_name, canonical_topic, reason in approve_candidates:
        row = by_name.get(topic_name) or {}
        logger.info(
            "  APPROVE_CANDIDATE: {!r} -> {!r} reason={} proposed_count={} distinct_content_count={}".format(
                topic_name,
                canonical_topic,
                reason,
                int(row.get("proposed_count") or 0),
                int(row.get("distinct_content_count") or 0),
            )
        )
    for topic_name, reason in macro_candidates:
        logger.info(f"  MACRO_CANDIDATE: {topic_name!r} reason={reason}")

    if not args.apply:
        logger.info("Dry run complete (no proposal statuses changed). Use --apply to execute.")
        return 0

    rejected = 0
    for topic_name, reason in reject_candidates:
        row = writer.review_topic_proposal(
            topic_name=topic_name,
            decision="reject",
            notes=f"Auto-triage rejection: {reason}",
            reviewed_by=args.reviewed_by,
        )
        if row:
            rejected += 1

    approved = 0
    for topic_name, canonical_topic, reason in approve_candidates:
        row = writer.review_topic_proposal(
            topic_name=topic_name,
            decision="approve",
            canonical_topic=canonical_topic,
            aliases=[topic_name],
            notes=f"Auto-triage approval: {reason}",
            reviewed_by=args.reviewed_by,
        )
        if row:
            approved += 1

    logger.success(
        "Auto-triage complete: rejected={} approved={} macro_candidates={} scanned={}"
        .format(rejected, approved, len(macro_candidates), len(proposals))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
