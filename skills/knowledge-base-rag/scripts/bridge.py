#!/usr/bin/env python3
"""
bridge.py — CLI entry point for the Knowledge Base RAG skill.

Usage:
  python3 bridge.py ask_kb --question "What are the milestones?" --collection work --json
  python3 bridge.py add_url --url https://example.com/doc --collection research --json
  python3 bridge.py list_collections --json
  python3 bridge.py search_kb --query "pricing model" --collection default --top-k 5 --json
"""
from __future__ import annotations

import argparse
import json
import os
import sys

# Allow running from the scripts directory directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from actions import ask_kb, add_url, search_kb, list_collections
from client import KBAPIError, KBClient
from formatters import build_error
from models import (
    AskKbRequest, AddUrlRequest, SearchKbRequest, ListCollectionsRequest, ClientConfig,
)
from pydantic import ValidationError


# ─────────────────────────────────────────────────────────────────────────────

def _emit(payload: dict, *, compact: bool) -> None:
    if compact:
        sys.stdout.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n")
    else:
        sys.stdout.write(json.dumps(payload, ensure_ascii=True, indent=2) + "\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bridge",
        description="Knowledge Base RAG — OpenClaw skill CLI",
    )

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--base-url",     default=os.getenv("ANALYTICS_API_BASE_URL", ""))
    common.add_argument("--api-key",      default=os.getenv("ANALYTICS_API_KEY", ""))
    common.add_argument("--timeout",      type=float, default=float(os.getenv("KB_TIMEOUT", "35")))
    common.add_argument("--max-retries",  type=int,   default=int(os.getenv("KB_MAX_RETRIES", "2")))
    common.add_argument("--backoff-base", type=float, default=float(os.getenv("KB_BACKOFF_BASE", "0.5")))
    common.add_argument("--json",         action="store_true", help="Emit compact JSON (required by OpenClaw)")

    sub = parser.add_subparsers(dest="action", required=True)

    # ask_kb
    p_ask = sub.add_parser("ask_kb", parents=[common], help="Answer a question from a collection")
    p_ask.add_argument("--question",   required=True)
    p_ask.add_argument("--collection", default="default")

    # add_url
    p_url = sub.add_parser("add_url", parents=[common], help="Ingest a URL into a collection")
    p_url.add_argument("--url",        required=True)
    p_url.add_argument("--collection", default="default")
    p_url.add_argument("--doc-title",  default="")

    # list_collections
    sub.add_parser("list_collections", parents=[common], help="List all collections")

    # search_kb
    p_search = sub.add_parser("search_kb", parents=[common], help="Search a collection")
    p_search.add_argument("--query",      required=True)
    p_search.add_argument("--collection", default="default")
    p_search.add_argument("--top-k",      type=int, default=5)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    action = args.action
    exit_code = 0
    payload: dict = {}

    try:
        client_cfg = ClientConfig(
            base_url=args.base_url,
            api_key=args.api_key,
            timeout=args.timeout,
            max_retries=args.max_retries,
            backoff_base=args.backoff_base,
        )
        client = KBClient(client_cfg)

        if action == "ask_kb":
            req = AskKbRequest(question=args.question, collection=args.collection)
            payload = ask_kb(client, req)

        elif action == "add_url":
            req = AddUrlRequest(url=args.url, collection=args.collection, doc_title=args.doc_title)
            payload = add_url(client, req)

        elif action == "list_collections":
            req = ListCollectionsRequest()
            payload = list_collections(client, req)

        elif action == "search_kb":
            req = SearchKbRequest(query=args.query, collection=args.collection, top_k=args.top_k)
            payload = search_kb(client, req)

        else:
            payload = build_error(action=action, message=f"Unknown action: {action}", error_type="invalid_action")
            exit_code = 1

    except ValidationError as exc:
        first = exc.errors()[0] if exc.errors() else {}
        message = first.get("msg", str(exc))
        payload = build_error(action=action, message=f"Validation error: {message}", error_type="validation_error")
        exit_code = 2

    except KBAPIError as exc:
        payload = build_error(action=action, message=exc.message, error_type=exc.error_type)
        exit_code = 1

    except Exception as exc:
        payload = build_error(action=action, message=f"Unexpected error: {exc}", error_type="unexpected_error")
        exit_code = 1

    _emit(payload, compact=args.json)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
