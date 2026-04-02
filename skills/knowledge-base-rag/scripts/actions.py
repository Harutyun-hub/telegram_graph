"""
actions.py — Business logic for the 4 KB skill actions.
"""
from __future__ import annotations

from typing import Any

from client import KBClient
from formatters import build_success, _trim_text
from models import AskKbRequest, AddUrlRequest, SearchKbRequest, ListCollectionsRequest


def list_collections(client: KBClient, request: ListCollectionsRequest) -> dict[str, Any]:
    data = client.list_collections()
    collections = data.get("collections", [])

    if not collections:
        return build_success(
            action="list_collections",
            summary="Your knowledge base has no collections yet.",
            confidence="low_confidence",
            bullets=["Create a collection and upload documents via the web dashboard at /agent."],
            items=[],
            caveat="No collections found.",
        )

    total_docs = sum(c.get("doc_count", 0) for c in collections)
    total_chunks = sum(c.get("chunk_count", 0) for c in collections)

    summary = f"You have {len(collections)} collection(s) with {total_docs} document(s) and {total_chunks} indexed chunk(s)."
    bullets = []
    items = []
    for c in collections:
        name = c.get("name", "")
        doc_count = c.get("doc_count", 0)
        chunk_count = c.get("chunk_count", 0)
        desc = c.get("description", "")
        bullets.append(f"{name}: {doc_count} docs, {chunk_count} chunks" + (f" — {desc}" if desc else ""))
        items.append({
            "name": name,
            "description": desc,
            "doc_count": doc_count,
            "chunk_count": chunk_count,
        })

    return build_success(
        action="list_collections",
        summary=summary,
        confidence="high",
        bullets=bullets,
        items=items,
    )


def ask_kb(client: KBClient, request: AskKbRequest) -> dict[str, Any]:
    data = client.ask_kb(request.question, request.collection)

    answer = data.get("answer", "")
    citations = data.get("citations", [])
    confidence = data.get("confidence", "medium")
    caveat = data.get("caveat", "")

    # Build citation bullets
    citation_bullets = []
    for c in citations[:3]:
        title = c.get("doc_title", "")
        page = c.get("page", "")
        if title:
            citation_bullets.append(f"[Source: {title}{f', p.{page}' if page else ''}]")

    # Summary is the answer, bullets are citations
    bullets = citation_bullets if citation_bullets else []

    # Telegram-friendly text: answer + citations
    telegram_summary = answer
    if citation_bullets:
        telegram_summary = f"{answer}\n\n{chr(10).join(citation_bullets)}"

    return build_success(
        action="ask_kb",
        summary=_trim_text(telegram_summary, 700),
        confidence=confidence,
        bullets=bullets,
        items=[{
            "answer": answer,
            "citations": citations,
            "confidence": confidence,
        }],
        citations=citations,
        caveat=caveat or None,
        suggested_follow_up="Try a more specific question or upload more relevant documents."
        if confidence == "low_confidence" else None,
    )


def add_url(client: KBClient, request: AddUrlRequest) -> dict[str, Any]:
    data = client.add_url(request.url, request.collection, request.doc_title)

    doc_title = data.get("doc_title", request.url[:60])
    chunk_count = data.get("chunk_count", 0)
    collection = data.get("collection", request.collection)

    summary = f"Successfully indexed '{doc_title}' into collection '{collection}' ({chunk_count} chunks)."
    bullets = [
        f"Collection: {collection}",
        f"Chunks indexed: {chunk_count}",
        f"Source: {request.url[:80]}",
    ]

    return build_success(
        action="add_url",
        summary=summary,
        confidence="high",
        bullets=bullets,
        items=[{
            "doc_id": data.get("doc_id", ""),
            "doc_title": doc_title,
            "chunk_count": chunk_count,
            "collection": collection,
            "url": request.url,
        }],
    )


def search_kb(client: KBClient, request: SearchKbRequest) -> dict[str, Any]:
    data = client.search_kb(request.query, request.collection, request.top_k)

    results = data.get("results", [])

    if not results:
        return build_success(
            action="search_kb",
            summary=f"No results found for '{request.query}' in collection '{request.collection}'.",
            confidence="low_confidence",
            bullets=["Try uploading more documents or broadening your search."],
            items=[],
            caveat="No matching content found.",
        )

    top_score = results[0].get("score", 0)
    confidence = "high" if top_score >= 0.75 else "medium" if top_score >= 0.50 else "low_confidence"

    summary = f"Found {len(results)} result(s) for '{request.query}' in '{request.collection}'."
    bullets = []
    items = []
    for r in results:
        snippet = _trim_text(r.get("text", ""), 120)
        title = r.get("doc_title", "")
        page = r.get("page", "")
        score = r.get("score", 0)
        bullets.append(f"[{title}{f' p.{page}' if page else ''}] {snippet}")
        items.append({
            "text": r.get("text", ""),
            "doc_title": title,
            "page": page,
            "score": score,
            "source_type": r.get("source_type", ""),
        })

    return build_success(
        action="search_kb",
        summary=summary,
        confidence=confidence,
        bullets=bullets,
        items=items,
    )
