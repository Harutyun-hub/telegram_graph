from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any, Iterable

from api.dashboard_dates import DashboardDateContext, build_dashboard_date_context


_TOKEN_RE = re.compile(r"[a-zA-Z0-9а-яА-ЯёЁ]+")


@dataclass(frozen=True)
class WidgetSnapshotPaths:
    latest_path: str
    history_folder: str
    state_path: str
    lock_folder: str
    scope_key: str
    mode: str
    window_start: str | None
    window_end: str | None


def build_widget_snapshot_paths(family: str, ctx: DashboardDateContext | None = None) -> WidgetSnapshotPaths:
    root = str(family or "").strip().strip("/")
    if not root:
        raise ValueError("family is required")

    if ctx is None:
        return WidgetSnapshotPaths(
            latest_path=f"{root}/latest.json",
            history_folder=f"{root}/snapshots",
            state_path=f"{root}/state.json",
            lock_folder=f"{root}/locks",
            scope_key="global",
            mode="latest_global",
            window_start=None,
            window_end=None,
        )

    range_key = f"{ctx.from_date.isoformat()}__{ctx.to_date.isoformat()}"
    range_root = f"{root}/ranges/{range_key}"
    return WidgetSnapshotPaths(
        latest_path=f"{range_root}/latest.json",
        history_folder=f"{range_root}/snapshots",
        state_path=f"{range_root}/state.json",
        lock_folder=f"{range_root}/locks",
        scope_key=ctx.cache_key,
        mode="exact_range",
        window_start=ctx.from_date.isoformat(),
        window_end=ctx.to_date.isoformat(),
    )


def load_latest_widget_payload(
    store,
    *,
    latest_path: str,
    history_folder: str,
    default: dict | None = None,
    timeout_seconds: float = 1.5,
) -> tuple[dict, bool]:
    fallback = dict(default or {})
    if not store:
        return fallback, False

    try:
        result = store.read_runtime_json(
            latest_path,
            prefer_signed_read=False,
            timeout_seconds=max(0.1, float(timeout_seconds)),
        )
    except TypeError:
        result = store.read_runtime_json(latest_path)

    if isinstance(result, dict) and result.get("status") == "ok":
        payload = result.get("payload")
        if isinstance(payload, dict):
            return dict(payload), True

    rows = store.list_runtime_files(history_folder)
    json_rows = [row for row in rows if str(row.get("name") or "").endswith(".json")]
    if not json_rows:
        return fallback, False

    latest = sorted(
        json_rows,
        key=lambda row: (str(row.get("updated_at") or ""), str(row.get("name") or "")),
        reverse=True,
    )[0]
    name = str(latest.get("name") or "").strip()
    if not name:
        return fallback, False

    payload = store.get_runtime_json(f"{history_folder}/{name}", default=fallback)
    return (dict(payload) if isinstance(payload, dict) else fallback), True


def load_widget_state_payload(
    store,
    *,
    state_path: str,
    default: dict | None = None,
    fallback_history_folder: str | None = None,
    timeout_seconds: float = 1.5,
) -> dict:
    fallback = dict(default or {})
    if not store:
        return fallback

    try:
        result = store.read_runtime_json(
            state_path,
            prefer_signed_read=False,
            timeout_seconds=max(0.1, float(timeout_seconds)),
        )
    except TypeError:
        result = store.read_runtime_json(state_path)

    if isinstance(result, dict) and result.get("status") == "ok":
        payload = result.get("payload")
        if isinstance(payload, dict):
            return dict(payload)

    if fallback_history_folder:
        payload, exists = load_latest_widget_payload(
            store,
            latest_path=state_path,
            history_folder=fallback_history_folder,
            default=fallback,
            timeout_seconds=timeout_seconds,
        )
        if exists:
            return payload

    return fallback


def save_widget_state_payload(store, *, state_path: str, payload: dict) -> bool:
    if not store:
        return False
    return bool(store.save_runtime_json(state_path, payload if isinstance(payload, dict) else {}))


def prune_runtime_folder(store, folder: str, *, keep: int = 12) -> None:
    if not store:
        return

    rows = store.list_runtime_files(folder)
    json_rows = [row for row in rows if str(row.get("name") or "").endswith(".json")]
    if len(json_rows) <= keep:
        return

    stale = sorted(
        json_rows,
        key=lambda row: (str(row.get("updated_at") or ""), str(row.get("name") or "")),
        reverse=True,
    )[keep:]
    delete_paths = [f"{folder}/{str(row.get('name') or '').strip()}" for row in stale if str(row.get("name") or "").strip()]
    if delete_paths:
        store.delete_runtime_files(delete_paths)


def save_widget_snapshot_payload(
    store,
    *,
    latest_path: str,
    history_folder: str,
    payload: dict,
    instance_id: str,
    keep: int = 12,
) -> bool:
    if not store:
        return False

    data = payload if isinstance(payload, dict) else {}
    if not store.save_runtime_json(latest_path, data):
        return False

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    digest = hashlib.sha1(json.dumps(data, ensure_ascii=True, sort_keys=True).encode("utf-8")).hexdigest()[:10]
    history_key = f"{history_folder}/{stamp}-{instance_id}-{digest}.json"
    if not store.save_runtime_json(history_key, data):
        return False

    prune_runtime_folder(store, history_folder, keep=keep)
    return True


def normalize_card_text(value: Any) -> str:
    tokens = [token.lower() for token in _TOKEN_RE.findall(str(value or ""))]
    return " ".join(tokens)


def card_evidence_ids(card: dict) -> set[str]:
    output: set[str] = set()
    sample_id = str(card.get("sampleEvidenceId") or "").strip()
    if sample_id:
        output.add(sample_id)
    for item in card.get("evidence") or []:
        if not isinstance(item, dict):
            continue
        evidence_id = str(item.get("id") or "").strip()
        if evidence_id:
            output.add(evidence_id)
    return output


def _first_text(card: dict, title_fields: Iterable[str]) -> str:
    for field in title_fields:
        value = str(card.get(field) or "").strip()
        if value:
            return value
    return ""


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    union = len(left.union(right))
    if union <= 0:
        return 0.0
    return len(left.intersection(right)) / union


def _is_duplicate_card(
    card: dict,
    seen: list[tuple[str, set[str], set[str]]],
    *,
    title_fields: Iterable[str],
) -> bool:
    raw_text = _first_text(card, title_fields)
    normalized = normalize_card_text(raw_text)
    tokens = set(normalized.split()) if normalized else set()
    evidence_ids = card_evidence_ids(card)

    for existing_text, existing_tokens, existing_evidence in seen:
        if normalized and existing_text and normalized == existing_text:
            return True
        if tokens and existing_tokens and _jaccard(tokens, existing_tokens) >= 0.85:
            return True
        if evidence_ids and existing_evidence and _jaccard(evidence_ids, existing_evidence) >= 0.6:
            return True
    return False


def dedupe_cards(cards: list[dict], *, title_fields: Iterable[str], max_cards: int) -> list[dict]:
    deduped: list[dict] = []
    seen: list[tuple[str, set[str], set[str]]] = []

    for card in cards:
        if not isinstance(card, dict):
            continue
        if _is_duplicate_card(card, seen, title_fields=title_fields):
            continue

        raw_text = _first_text(card, title_fields)
        normalized = normalize_card_text(raw_text)
        tokens = set(normalized.split()) if normalized else set()
        evidence_ids = card_evidence_ids(card)
        deduped.append(card)
        seen.append((normalized, tokens, evidence_ids))
        if len(deduped) >= max(1, int(max_cards)):
            break

    return deduped


def select_portfolio_cards(
    cards: list[dict],
    *,
    title_fields: Iterable[str],
    max_cards: int,
    topic_field: str = "topic",
) -> list[dict]:
    limit = max(1, int(max_cards))
    selected: list[dict] = []
    selected_keys: set[str] = set()
    seen: list[tuple[str, set[str], set[str]]] = []
    topic_counts: dict[str, int] = {}

    def _card_key(card: dict, idx: int) -> str:
        return str(card.get("id") or card.get("clusterId") or f"row-{idx}")

    def _remember(card: dict) -> None:
        raw_text = _first_text(card, title_fields)
        normalized = normalize_card_text(raw_text)
        tokens = set(normalized.split()) if normalized else set()
        evidence_ids = card_evidence_ids(card)
        selected.append(card)
        seen.append((normalized, tokens, evidence_ids))
        topic = str(card.get(topic_field) or "").strip().lower()
        if topic:
            topic_counts[topic] = topic_counts.get(topic, 0) + 1

    for phase in ("coverage", "fill"):
        for idx, card in enumerate(cards):
            if not isinstance(card, dict):
                continue
            key = _card_key(card, idx)
            if key in selected_keys:
                continue
            if _is_duplicate_card(card, seen, title_fields=title_fields):
                continue
            topic = str(card.get(topic_field) or "").strip().lower()
            if phase == "coverage" and topic and topic_counts.get(topic, 0) >= 1:
                continue
            selected_keys.add(key)
            _remember(card)
            if len(selected) >= limit:
                return selected

    return selected


def load_nearest_shorter_range_cards(
    store,
    *,
    family: str,
    ctx: DashboardDateContext | None,
    title_fields: Iterable[str],
    max_cards: int,
    topic_field: str = "topic",
) -> list[dict]:
    if not store or ctx is None or int(getattr(ctx, "days", 0)) <= 1:
        return []

    try:
        rows = store.list_runtime_files(f"{str(family).strip().strip('/')}/ranges")
    except Exception:
        return []

    range_keys: set[str] = set()
    for row in rows or []:
        name = str((row or {}).get("name") or "").strip().strip("/")
        if not name:
            continue
        candidate = name.split("/", 1)[0]
        if "__" in candidate:
            range_keys.add(candidate)

    best_ctx: DashboardDateContext | None = None
    best_days = 0
    for range_key in range_keys:
        try:
            start_raw, end_raw = range_key.split("__", 1)
            candidate_ctx = build_dashboard_date_context(start_raw, end_raw)
        except Exception:
            continue
        if candidate_ctx.to_date != ctx.to_date:
            continue
        if candidate_ctx.days >= ctx.days:
            continue
        if candidate_ctx.from_date < ctx.from_date:
            continue
        if candidate_ctx.days > best_days:
            best_ctx = candidate_ctx
            best_days = candidate_ctx.days

    if best_ctx is None:
        return []

    paths = build_widget_snapshot_paths(str(family).strip().strip("/"), best_ctx)
    payload, exists = load_latest_widget_payload(
        store,
        latest_path=paths.latest_path,
        history_folder=paths.history_folder,
        default={"cards": []},
    )
    cards = payload.get("cards") if isinstance(payload, dict) else []
    parsed = cards if isinstance(cards, list) else []
    if not exists and not parsed:
        return []
    return select_portfolio_cards(
        parsed,
        title_fields=title_fields,
        max_cards=max(1, int(max_cards)),
        topic_field=topic_field,
    )
