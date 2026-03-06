"""
db.py — Shared Neo4j driver for the API layer.

Reuses config.py credentials. Provides a context-managed session helper.
"""
from __future__ import annotations
import config
from neo4j import GraphDatabase
from loguru import logger

_driver = None


def get_driver():
    global _driver
    if _driver is None:
        uri = config.NEO4J_URI
        if uri.startswith("neo4j+s://"):
            uri = uri.replace("neo4j+s://", "neo4j+ssc://")
        _driver = GraphDatabase.driver(
            uri, auth=(config.NEO4J_USERNAME, config.NEO4J_PASSWORD)
        )
        _driver.verify_connectivity()
        logger.info(f"Neo4j API driver connected to {uri[:40]}...")
    return _driver


def run_query(cypher: str, params: dict | None = None) -> list[dict]:
    """Execute a Cypher query and return results as list of dicts."""
    driver = get_driver()
    with driver.session(database=config.NEO4J_DATABASE) as session:
        result = session.run(cypher, params or {})
        return [dict(record) for record in result]


def run_single(cypher: str, params: dict | None = None) -> dict | None:
    """Execute a Cypher query expecting a single result."""
    rows = run_query(cypher, params)
    return rows[0] if rows else None


def close():
    global _driver
    if _driver:
        _driver.close()
        _driver = None
