#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from actions import (
    ask_insights,
    compare_channels,
    compare_topics,
    get_freshness_status,
    get_active_alerts,
    get_declining_topics,
    get_graph_snapshot,
    get_node_context,
    get_problem_spikes,
    get_question_clusters,
    get_sentiment_overview,
    get_topic_detail,
    get_topic_evidence,
    get_top_topics,
    investigate_channel,
    investigate_question,
    investigate_topic,
    search_entities,
)
from client import AnalyticsAPIError, AnalyticsClient
from formatters import build_error
from models import (
    AskInsightsRequest,
    ClientConfig,
    DEFAULT_BACKOFF_BASE,
    DEFAULT_MAX_RETRIES,
    DEFAULT_TIMEOUT,
    CompareChannelsRequest,
    CompareTopicsRequest,
    GetFreshnessStatusRequest,
    GetActiveAlertsRequest,
    GetDecliningTopicsRequest,
    GetGraphSnapshotRequest,
    GetNodeContextRequest,
    GetProblemSpikesRequest,
    GetQuestionClustersRequest,
    GetSentimentOverviewRequest,
    GetTopicDetailRequest,
    GetTopicEvidenceRequest,
    GetTopTopicsRequest,
    InvestigateChannelRequest,
    InvestigateQuestionRequest,
    InvestigateTopicRequest,
    SearchEntitiesRequest,
    ValidationError,
)


def _configure_logging() -> None:
    if os.getenv("TELEGRAM_ANALYTICS_BRIDGE_DEBUG") == "1":
        logging.basicConfig(
            level=logging.INFO,
            format="%(levelname)s %(name)s: %(message)s",
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="telegram-analytics-bridge CLI")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--base-url", default=os.getenv("ANALYTICS_API_BASE_URL", ""))
    common.add_argument("--api-key", default=os.getenv("ANALYTICS_API_KEY", ""))
    common.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    common.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    common.add_argument("--backoff-base", type=float, default=DEFAULT_BACKOFF_BASE)
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

    search = subparsers.add_parser("search_entities", parents=[common])
    search.add_argument("--query", required=True)
    search.add_argument("--limit", type=int, default=5)

    topic_detail = subparsers.add_parser("get_topic_detail", parents=[common])
    topic_detail.add_argument("--topic", required=True)
    topic_detail.add_argument("--category", default=None)
    topic_detail.add_argument("--window", default="7d")

    topic_evidence = subparsers.add_parser("get_topic_evidence", parents=[common])
    topic_evidence.add_argument("--topic", required=True)
    topic_evidence.add_argument("--category", default=None)
    topic_evidence.add_argument("--view", default="all")
    topic_evidence.add_argument("--limit", type=int, default=5)
    topic_evidence.add_argument("--focus-id", default=None)
    topic_evidence.add_argument("--window", default="7d")

    freshness = subparsers.add_parser("get_freshness_status", parents=[common])
    freshness.add_argument("--force", action="store_true")

    graph_snapshot = subparsers.add_parser("get_graph_snapshot", parents=[common])
    graph_snapshot.add_argument("--window", default="7d")
    graph_snapshot.add_argument("--category", default=None)
    graph_snapshot.add_argument("--signal-focus", default="all")
    graph_snapshot.add_argument("--max-nodes", type=int, default=12)

    node_context = subparsers.add_parser("get_node_context", parents=[common])
    node_context.add_argument("--entity", required=True)
    node_context.add_argument("--type", default="auto")
    node_context.add_argument("--window", default="7d")

    investigate_topic_parser = subparsers.add_parser("investigate_topic", parents=[common])
    investigate_topic_parser.add_argument("--topic", required=True)
    investigate_topic_parser.add_argument("--category", default=None)
    investigate_topic_parser.add_argument("--window", default="7d")

    investigate_channel_parser = subparsers.add_parser("investigate_channel", parents=[common])
    investigate_channel_parser.add_argument("--channel", required=True)
    investigate_channel_parser.add_argument("--window", default="7d")

    compare_topics_parser = subparsers.add_parser("compare_topics", parents=[common])
    compare_topics_parser.add_argument("--topic-a", required=True)
    compare_topics_parser.add_argument("--topic-b", required=True)
    compare_topics_parser.add_argument("--window", default="7d")

    compare_channels_parser = subparsers.add_parser("compare_channels", parents=[common])
    compare_channels_parser.add_argument("--channel-a", required=True)
    compare_channels_parser.add_argument("--channel-b", required=True)
    compare_channels_parser.add_argument("--window", default="7d")

    insights = subparsers.add_parser("ask_insights", parents=[common])
    insights.add_argument("--window", default="7d")
    insights.add_argument("--question", required=True)

    investigate_question_parser = subparsers.add_parser("investigate_question", parents=[common])
    investigate_question_parser.add_argument("--window", default="7d")
    investigate_question_parser.add_argument("--question", required=True)

    return parser


def main() -> int:
    _configure_logging()
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
        elif args.action == "search_entities":
            payload = search_entities(client, SearchEntitiesRequest(query=args.query, limit=args.limit))
        elif args.action == "get_topic_detail":
            payload = get_topic_detail(
                client,
                GetTopicDetailRequest(window=args.window, topic=args.topic, category=args.category),
            )
        elif args.action == "get_topic_evidence":
            payload = get_topic_evidence(
                client,
                GetTopicEvidenceRequest(
                    window=args.window,
                    topic=args.topic,
                    category=args.category,
                    view=args.view,
                    limit=args.limit,
                    focus_id=args.focus_id,
                ),
            )
        elif args.action == "get_freshness_status":
            payload = get_freshness_status(client, GetFreshnessStatusRequest(force=args.force))
        elif args.action == "get_graph_snapshot":
            payload = get_graph_snapshot(
                client,
                GetGraphSnapshotRequest(
                    window=args.window,
                    category=args.category,
                    signal_focus=args.signal_focus,
                    max_nodes=args.max_nodes,
                ),
            )
        elif args.action == "get_node_context":
            payload = get_node_context(
                client,
                GetNodeContextRequest(window=args.window, entity=args.entity, type=args.type),
            )
        elif args.action == "investigate_topic":
            payload = investigate_topic(
                client,
                InvestigateTopicRequest(window=args.window, topic=args.topic, category=args.category),
            )
        elif args.action == "investigate_channel":
            payload = investigate_channel(
                client,
                InvestigateChannelRequest(window=args.window, channel=args.channel),
            )
        elif args.action == "compare_topics":
            payload = compare_topics(
                client,
                CompareTopicsRequest(window=args.window, topic_a=args.topic_a, topic_b=args.topic_b),
            )
        elif args.action == "compare_channels":
            payload = compare_channels(
                client,
                CompareChannelsRequest(window=args.window, channel_a=args.channel_a, channel_b=args.channel_b),
            )
        elif args.action == "ask_insights":
            payload = ask_insights(client, AskInsightsRequest(window=args.window, question=args.question))
        elif args.action == "investigate_question":
            payload = investigate_question(
                client,
                InvestigateQuestionRequest(window=args.window, question=args.question),
            )
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
