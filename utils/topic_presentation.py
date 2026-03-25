from __future__ import annotations

"""Shared presentation metadata for Topics-page grouping and Russian labels."""

from functools import lru_cache
import json
from pathlib import Path
from typing import Any


_TOPIC_PRESENTATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "frontend"
    / "src"
    / "app"
    / "config"
    / "topicPresentation.json"
)


@lru_cache(maxsize=1)
def _load_topic_presentation() -> dict[str, Any]:
    with _TOPIC_PRESENTATION_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def topic_group_for_category(category: str) -> str:
    name = str(category or "").strip()
    if not name:
        return "Admin"
    payload = _load_topic_presentation()
    return str(payload.get("categoryToGroup", {}).get(name, "Admin"))


def category_ru(category: str) -> str:
    name = str(category or "").strip()
    if not name:
        return name
    payload = _load_topic_presentation()
    return str(payload.get("categoryRu", {}).get(name, name))


def topic_ru(topic: str) -> str:
    name = str(topic or "").strip()
    if not name:
        return name
    payload = _load_topic_presentation()
    return str(payload.get("topicRu", {}).get(name, name))


def topics_page_groups_en() -> list[str]:
    payload = _load_topic_presentation()
    groups = payload.get("groups", {})
    return [str(item) for item in groups.get("order", [])]

