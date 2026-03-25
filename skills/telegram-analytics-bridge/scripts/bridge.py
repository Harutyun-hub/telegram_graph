#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys

from actions import (
    ask_insights,
    get_active_alerts,
    get_declining_topics,
    get_problem_spikes,
    get_question_clusters,
    get_sentiment_overview,
    get_top_topics,
)
from client import AnalyticsAPIError, AnalyticsClient
from formatters import build_error
from models import (
    AskInsightsRequest,
    ClientConfig,
    GetActiveAlertsRequest,
    GetDecliningTopicsRequest,
    GetProblemSpikesRequest,
    GetQuestionClustersRequest,
    GetSentimentOverviewRequest,
    GetTopTopicsRequest,
)
from pydantic import ValidationError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="telegram-analytics-bridge CLI")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--base-url", default=os.getenv("ANALYTICS_API_BASE_URL", ""))
    common.add_argument("--api-key", default=os.getenv("ANALYTICS_API_KEY", ""))
    common.add_argument("--timeout", type=float, default=35.0)
    common.add_argument("--max-retries", type=int, default=2)
    common.add_argument("--backoff-base", type=float, default=0.5)
    common.add_argument("--json", action="store_true", help="emit compact JSON")

    subparsers = parser.add_subparsers(dest="action", required=True)

    top_topics = subparsers.add_parser("get_top_topics", parents=[common])
    top_topics.add_argument("--window", default="7d")
    top_topics.add_argument("--limit", type=int, default=5)

    declining = subparsers.add_parser("get_declining_topics", parents=[common])
    declining.add_argument("--window", default="7d")
    declining.add_argument("--limit", type=int, default=5)

    problem_spikes = subparsers.add_parser("get_problem_spikes", parents=[common])
    problem_spikes.add_argument("--window", default="7d")

    question_clusters = subparsers.add_parser("get_question_clusters", parents=[common])
    question_clusters.add_argument("--window", default="7d")
    question_clusters.add_argument("--topic", default=None)

    sentiment = subparsers.add_parser("get_sentiment_overview", parents=[common])
    sentiment.add_argument("--window", default="7d")

    subparsers.add_parser("get_active_alerts", parents=[common])

    insights = subparsers.add_parser("ask_insights", parents=[common])
    insights.add_argument("--window", default="7d")
    insights.add_argument("--question", required=True)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        client = AnalyticsClient(
            ClientConfig(
                base_url=args.base_url,
                api_key=args.api_key,
                timeout=args.timeout,
                max_retries=args.max_retries,
                backoff_base=args.backoff_base,
            )
        )

        if args.action == "get_top_topics":
            payload = get_top_topics(client, GetTopTopicsRequest(window=args.window, limit=args.limit))
        elif args.action == "get_declining_topics":
            payload = get_declining_topics(client, GetDecliningTopicsRequest(window=args.window, limit=args.limit))
        elif args.action == "get_problem_spikes":
            payload = get_problem_spikes(client, GetProblemSpikesRequest(window=args.window))
        elif args.action == "get_question_clusters":
            payload = get_question_clusters(client, GetQuestionClustersRequest(window=args.window, topic=args.topic))
        elif args.action == "get_sentiment_overview":
            payload = get_sentiment_overview(client, GetSentimentOverviewRequest(window=args.window))
        elif args.action == "get_active_alerts":
            payload = get_active_alerts(client, GetActiveAlertsRequest())
        elif args.action == "ask_insights":
            payload = ask_insights(client, AskInsightsRequest(window=args.window, question=args.question))
        else:
            payload = build_error(action=args.action, window=None, error_type="invalid_action", message="Unknown action.")
            _emit(payload, compact=args.json)
            return 2
    except ValidationError as exc:
        payload = build_error(
            action=getattr(args, "action", "unknown"),
            window=getattr(args, "window", None),
            error_type="validation_error",
            message=str(exc),
        )
        _emit(payload, compact=getattr(args, "json", False))
        return 2
    except AnalyticsAPIError as exc:
        payload = build_error(
            action=getattr(args, "action", "unknown"),
            window=getattr(args, "window", None),
            error_type=exc.error_type,
            message=exc.message,
        )
        _emit(payload, compact=getattr(args, "json", False))
        return 1
    except Exception as exc:
        payload = build_error(
            action=getattr(args, "action", "unknown"),
            window=getattr(args, "window", None),
            error_type="unexpected_error",
            message=f"Unexpected skill error: {exc}",
        )
        _emit(payload, compact=getattr(args, "json", False))
        return 1

    _emit(payload, compact=args.json)
    return 0


def _emit(payload: dict, *, compact: bool) -> None:
    if compact:
        sys.stdout.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n")
    else:
        sys.stdout.write(json.dumps(payload, ensure_ascii=True, indent=2) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
