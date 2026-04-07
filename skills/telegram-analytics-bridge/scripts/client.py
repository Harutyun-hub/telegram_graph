from __future__ import annotations

import json
import logging
import socket
import time
from typing import Any
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from models import ClientConfig
from windows import dashboard_date_range, window_to_timeframe


RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
logger = logging.getLogger(__name__)


class AnalyticsAPIError(RuntimeError):
    def __init__(self, message: str, *, error_type: str = "api_error", status_code: int | None = None):
        super().__init__(message)
        self.message = message
        self.error_type = error_type
        self.status_code = status_code


class AnalyticsClient:
    def __init__(self, config: ClientConfig):
        self.base_url = config.base_url.rstrip("/")
        self.api_key = config.api_key
        self.timeout = config.timeout
        self.max_retries = config.max_retries
        self.backoff_base = config.backoff_base

    def get_dashboard(self, window: str | None = None) -> dict[str, Any]:
        query = None
        if window:
            from_date, to_date = dashboard_date_range(window)  # type: ignore[arg-type]
            query = {"from": from_date, "to": to_date}
        return self._request_json("GET", "/api/dashboard", query=query)

    def get_sentiment_distribution(self, window: str) -> list[dict[str, Any]]:
        return self._request_json(
            "GET",
            "/api/sentiment-distribution",
            query={"timeframe": window_to_timeframe(window)},  # type: ignore[arg-type]
        )

    def get_insight_cards(self, window: str) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/api/insights/cards",
            payload={
                "filters": {
                    "timeframe": window_to_timeframe(window),  # type: ignore[arg-type]
                },
                "audience": "analyst",
            },
        )

    def search_entities(self, query: str, limit: int = 5) -> list[dict[str, Any]]:
        return self._request_json(
            "GET",
            "/api/search",
            query={"query": query, "limit": limit},
        )

    def get_topic_detail(self, topic: str, category: str | None = None, window: str = "7d") -> dict[str, Any]:
        from_date, to_date = dashboard_date_range(window)  # type: ignore[arg-type]
        return self._request_json(
            "GET",
            "/api/topics/detail",
            query={
                "topic": topic,
                "category": category,
                "from": from_date,
                "to": to_date,
            },
        )

    def get_topic_evidence(
        self,
        topic: str,
        category: str | None = None,
        view: str = "all",
        page: int = 0,
        size: int = 5,
        focus_id: str | None = None,
        window: str = "7d",
    ) -> dict[str, Any]:
        from_date, to_date = dashboard_date_range(window)  # type: ignore[arg-type]
        return self._request_json(
            "GET",
            "/api/topics/evidence",
            query={
                "topic": topic,
                "category": category,
                "view": view,
                "page": page,
                "size": size,
                "focusId": focus_id,
                "from": from_date,
                "to": to_date,
            },
        )

    def get_freshness_status(self, force: bool = False) -> dict[str, Any]:
        return self._request_json(
            "GET",
            "/api/freshness",
            query={"force": "true" if force else "false"},
        )

    def get_graph_data(
        self,
        window: str = "7d",
        *,
        category: str | None = None,
        signal_focus: str | None = None,
        max_nodes: int = 12,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "timeframe": window_to_timeframe(window),  # type: ignore[arg-type]
            "sourceDetail": "minimal",
            "max_nodes": max_nodes,
        }
        if category:
            payload["category"] = category
        if signal_focus and signal_focus != "all":
            payload["signalFocus"] = signal_focus
        return self._request_json("POST", "/api/graph", payload=payload)

    def get_graph_insights(self, window: str = "7d") -> dict[str, Any]:
        return self._request_json(
            "GET",
            "/api/graph-insights",
            query={"timeframe": window_to_timeframe(window)},  # type: ignore[arg-type]
        )

    def get_top_channels(self, limit: int = 5, window: str = "7d") -> list[dict[str, Any]]:
        return self._request_json(
            "GET",
            "/api/top-channels",
            query={"limit": limit, "timeframe": window_to_timeframe(window)},  # type: ignore[arg-type]
        )

    def get_trending_topics(self, limit: int = 5, window: str = "7d") -> list[dict[str, Any]]:
        return self._request_json(
            "GET",
            "/api/trending-topics",
            query={"limit": limit, "timeframe": window_to_timeframe(window)},  # type: ignore[arg-type]
        )

    def get_node_details(self, node_id: str, node_type: str, window: str = "7d") -> dict[str, Any]:
        return self._request_json(
            "GET",
            "/api/node-details",
            query={
                "nodeId": node_id,
                "nodeType": node_type,
                "timeframe": window_to_timeframe(window),  # type: ignore[arg-type]
            },
        )

    def get_channel_detail(self, channel: str, window: str = "7d") -> dict[str, Any]:
        from_date, to_date = dashboard_date_range(window)  # type: ignore[arg-type]
        return self._request_json(
            "GET",
            "/api/channels/detail",
            query={"channel": channel, "from": from_date, "to": to_date},
        )

    def get_channel_posts(
        self,
        channel: str,
        *,
        limit: int = 5,
        page: int = 0,
        window: str = "7d",
    ) -> dict[str, Any]:
        from_date, to_date = dashboard_date_range(window)  # type: ignore[arg-type]
        return self._request_json(
            "GET",
            "/api/channels/posts",
            query={
                "channel": channel,
                "page": page,
                "size": limit,
                "from": from_date,
                "to": to_date,
            },
        )

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        url = self._build_url(path, query=query)
        data = None
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")

        request = urllib_request.Request(url=url, data=data, headers=headers, method=method.upper())
        attempts = self.max_retries + 1
        last_error: AnalyticsAPIError | None = None

        for attempt in range(1, attempts + 1):
            try:
                with urllib_request.urlopen(request, timeout=self.timeout) as response:
                    raw = response.read().decode("utf-8")
                    if not raw.strip():
                        raise AnalyticsAPIError(
                            "The analytics API returned an empty response.",
                            error_type="empty_response",
                        )
                    return json.loads(raw)
            except urllib_error.HTTPError as exc:
                last_error = self._map_http_error(exc)
                if exc.code in RETRYABLE_STATUS_CODES and attempt < attempts:
                    logger.info(
                        "Retrying analytics request after HTTP %s for %s %s (attempt %s/%s)",
                        exc.code,
                        method.upper(),
                        path,
                        attempt,
                        attempts,
                    )
                    time.sleep(self.backoff_base * (2 ** (attempt - 1)))
                    continue
                raise last_error
            except (urllib_error.URLError, socket.timeout, TimeoutError) as exc:
                is_timeout = isinstance(exc, (socket.timeout, TimeoutError))
                last_error = AnalyticsAPIError(
                    (
                        "The analytics backend is taking too long to respond right now. "
                        "It may be waking up. Please try again in a moment."
                    )
                    if is_timeout
                    else "The analytics backend could not be reached right now. Please try again.",
                    error_type="timeout" if is_timeout else "network_error",
                )
                if attempt < attempts:
                    logger.info(
                        "Retrying analytics request after %s for %s %s (attempt %s/%s)",
                        "timeout" if is_timeout else "network_error",
                        method.upper(),
                        path,
                        attempt,
                        attempts,
                    )
                    time.sleep(self.backoff_base * (2 ** (attempt - 1)))
                    continue
                logger.info(
                    "Analytics request exhausted retries due to %s for %s %s",
                    "timeout" if is_timeout else "network_error",
                    method.upper(),
                    path,
                )
                raise last_error
            except json.JSONDecodeError as exc:
                raise AnalyticsAPIError(
                    f"The analytics API returned invalid JSON: {exc}",
                    error_type="invalid_json",
                ) from exc

        if last_error is None:
            raise AnalyticsAPIError("Unknown analytics API error.", error_type="api_error")
        raise last_error

    def _build_url(self, path: str, *, query: dict[str, Any] | None = None) -> str:
        url = f"{self.base_url}{path}"
        if query:
            filtered = {key: value for key, value in query.items() if value is not None and value != ""}
            if filtered:
                url = f"{url}?{urllib_parse.urlencode(filtered)}"
        return url

    def _map_http_error(self, exc: urllib_error.HTTPError) -> AnalyticsAPIError:
        detail = ""
        try:
            body = exc.read().decode("utf-8")
            if body.strip():
                parsed = json.loads(body)
                if isinstance(parsed, dict):
                    detail = str(parsed.get("detail") or parsed.get("message") or "").strip()
                else:
                    detail = body.strip()
        except Exception:
            detail = ""

        if exc.code in {401, 403}:
            message = "Authentication failed. Check ANALYTICS_API_KEY and API access."
            return AnalyticsAPIError(message, error_type="auth_error", status_code=exc.code)
        if exc.code == 400:
            message = detail or "The analytics API rejected the request parameters."
            return AnalyticsAPIError(message, error_type="invalid_request", status_code=exc.code)
        if exc.code == 404:
            message = detail or "The requested analytics endpoint is not available."
            return AnalyticsAPIError(message, error_type="not_found", status_code=exc.code)

        message = detail or f"The analytics API returned HTTP {exc.code}."
        return AnalyticsAPIError(message, error_type="upstream_error", status_code=exc.code)
