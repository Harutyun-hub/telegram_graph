from __future__ import annotations

from typing import Any


COVERAGE_ROW_KIND = "coverage_marker"

_MISSING_TOKEN = "_"
_COVERAGE_ROW_DIMENSIONS = {"scope": "all"}


def sanitize_dashboard_v2_row_key_value(value: Any) -> str:
    text = str(value).strip().lower()
    if not text:
        return _MISSING_TOKEN
    return text.replace("|", "_").replace("=", "_")


def build_dashboard_v2_row_key(kind: str, **dimensions: Any) -> str:
    normalized_kind = sanitize_dashboard_v2_row_key_value(kind)
    normalized_dimensions = {
        sanitize_dashboard_v2_row_key_value(key): sanitize_dashboard_v2_row_key_value(value)
        for key, value in dimensions.items()
    }
    parts = [f"kind={normalized_kind}"]
    for key in sorted(normalized_dimensions):
        parts.append(f"{key}={normalized_dimensions[key]}")
    return "|".join(parts)


def build_dashboard_v2_coverage_row_key() -> str:
    return build_dashboard_v2_row_key(COVERAGE_ROW_KIND, **_COVERAGE_ROW_DIMENSIONS)
