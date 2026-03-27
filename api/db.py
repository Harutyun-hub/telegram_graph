"""
db.py — Shared Neo4j driver for the API layer.

Reuses config.py credentials. Provides a context-managed session helper.
"""
from __future__ import annotations
import os
import time

import config
from neo4j import GraphDatabase
from loguru import logger

_driver = None

NEO4J_MAX_CONNECTION_POOL_SIZE = max(1, int(os.getenv("NEO4J_MAX_CONNECTION_POOL_SIZE", "20")))
NEO4J_CONNECTION_ACQUISITION_TIMEOUT_SECONDS = max(
    1.0,
    float(os.getenv("NEO4J_CONNECTION_ACQUISITION_TIMEOUT_SECONDS", "10")),
)
NEO4J_MAX_TRANSACTION_RETRY_TIME_SECONDS = max(
    1.0,
    float(os.getenv("NEO4J_MAX_TRANSACTION_RETRY_TIME_SECONDS", "15")),
)
NEO4J_SLOW_QUERY_MS = max(0.0, float(os.getenv("NEO4J_SLOW_QUERY_MS", "750")))


def get_driver():
    global _driver
    if _driver is None:
        uri = config.NEO4J_URI
        if uri.startswith("neo4j+s://"):
            uri = uri.replace("neo4j+s://", "neo4j+ssc://")
        started_at = time.perf_counter()
        _driver = GraphDatabase.driver(
            uri,
            auth=(config.NEO4J_USERNAME, config.NEO4J_PASSWORD),
            max_connection_pool_size=NEO4J_MAX_CONNECTION_POOL_SIZE,
            connection_acquisition_timeout=NEO4J_CONNECTION_ACQUISITION_TIMEOUT_SECONDS,
            max_transaction_retry_time=NEO4J_MAX_TRANSACTION_RETRY_TIME_SECONDS,
        )
        _driver.verify_connectivity()
        elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
        logger.info(
            "Neo4j API driver connected "
            f"| uri={uri[:40]}... pool={NEO4J_MAX_CONNECTION_POOL_SIZE} "
            f"acquire_timeout_s={NEO4J_CONNECTION_ACQUISITION_TIMEOUT_SECONDS} "
            f"retry_time_s={NEO4J_MAX_TRANSACTION_RETRY_TIME_SECONDS} "
            f"connect_ms={elapsed_ms}"
        )
    return _driver


def run_query(cypher: str, params: dict | None = None) -> list[dict]:
    """Execute a Cypher query and return results as list of dicts."""
    driver = get_driver()
    session_started_at = time.perf_counter()
    with driver.session(database=config.NEO4J_DATABASE) as session:
        session_open_ms = round((time.perf_counter() - session_started_at) * 1000, 2)
        query_started_at = time.perf_counter()
        result = session.run(cypher, params or {})
        rows = [dict(record) for record in result]
        query_elapsed_ms = round((time.perf_counter() - query_started_at) * 1000, 2)
        if query_elapsed_ms >= NEO4J_SLOW_QUERY_MS:
            logger.warning(
                "Neo4j slow query detected "
                f"| open_ms={session_open_ms} query_ms={query_elapsed_ms} "
                f"rows={len(rows)} database={config.NEO4J_DATABASE}"
            )
        return rows


def run_single(cypher: str, params: dict | None = None) -> dict | None:
    """Execute a Cypher query expecting a single result."""
    rows = run_query(cypher, params)
    return rows[0] if rows else None


def close():
    global _driver
    if _driver:
        _driver.close()
        _driver = None
