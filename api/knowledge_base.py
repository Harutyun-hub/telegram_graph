"""
api/knowledge_base.py — Shared Knowledge Base RAG engine.

Provides document ingestion, Gemini Embedding 2 vectorization,
ChromaDB hybrid retrieval, and OpenAI-powered grounded answer generation.
Used by both /api/kb/* FastAPI endpoints and the OpenClaw skill bridge.
"""
from __future__ import annotations

import io
import os
import re
import textwrap
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from utils.ai_usage import log_openai_usage

# ── Optional heavy imports (guarded so the module loads even if a dep is missing) ──

def _require(pkg: str, extra: str = "") -> Any:
    import importlib
    try:
        return importlib.import_module(pkg)
    except ImportError as exc:
        hint = f"  pip install {extra or pkg}"
        raise ImportError(f"Missing dependency '{pkg}'.{hint}") from exc


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 — DOCUMENT PARSERS
# ─────────────────────────────────────────────────────────────────────────────

class ParseError(RuntimeError):
    pass


class UnsupportedFormatError(ParseError):
    pass


def parse_pdf(data: bytes) -> tuple[str, dict]:
    """Extract text from PDF bytes. Returns (text, metadata)."""
    pdfplumber = _require("pdfplumber")
    pages: list[str] = []
    try:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            for i, page in enumerate(pdf.pages, 1):
                text = page.extract_text() or ""
                if text.strip():
                    pages.append(f"[Page {i}]\n{text}")
    except Exception as exc:
        raise ParseError(f"PDF parse failed: {exc}") from exc
    return "\n\n".join(pages), {"page_count": len(pages), "source_type": "pdf"}


def parse_docx(data: bytes) -> tuple[str, dict]:
    """Extract text from DOCX bytes preserving heading structure."""
    docx = _require("docx", "python-docx")
    try:
        doc = docx.Document(io.BytesIO(data))
    except Exception as exc:
        raise ParseError(f"DOCX parse failed: {exc}") from exc

    parts: list[str] = []
    for para in doc.paragraphs:
        text = para.text.strip()
        if not text:
            continue
        style = para.style.name if para.style else ""
        if style.startswith("Heading"):
            parts.append(f"\n## {text}\n")
        else:
            parts.append(text)
    return "\n".join(parts), {"source_type": "docx"}


def parse_text(data: bytes, filename: str = "") -> tuple[str, dict]:
    """Parse plain text or markdown."""
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return data.decode(enc), {"source_type": "txt"}
        except UnicodeDecodeError:
            continue
    raise ParseError("Cannot decode text file — unknown encoding.")


def fetch_url(url: str) -> tuple[str, dict]:
    """Fetch a URL and extract clean article text."""
    httpx = _require("httpx")
    readability = _require("readability", "readability-lxml")
    html2text = _require("html2text")

    try:
        resp = httpx.get(url, timeout=30, follow_redirects=True,
                         headers={"User-Agent": "Mozilla/5.0 (compatible; KBBot/1.0)"})
        resp.raise_for_status()
        html = resp.text
    except Exception as exc:
        raise ParseError(f"URL fetch failed: {exc}") from exc

    try:
        doc = readability.Document(html)
        h = html2text.HTML2Text()
        h.ignore_links = True
        h.ignore_images = True
        text = h.handle(doc.summary())
        title = doc.title() or url
    except Exception as exc:
        raise ParseError(f"HTML extraction failed: {exc}") from exc

    return text, {"source_type": "url", "source_url": url, "title": title}


def route_file(file_path_or_url: str, data: bytes | None = None,
               filename: str = "") -> tuple[str, dict]:
    """
    Dispatch to the right parser based on extension or URL scheme.
    Returns (text, metadata).
    """
    s = file_path_or_url.lower()

    if s.startswith(("http://", "https://")):
        return fetch_url(file_path_or_url)

    ext = Path(s).suffix.lstrip(".")
    if data is None:
        data = Path(file_path_or_url).read_bytes()
    fname = filename or Path(file_path_or_url).name

    if ext == "pdf":
        return parse_pdf(data)
    if ext == "docx":
        return parse_docx(data)
    if ext in ("txt", "md", "markdown", "rst"):
        return parse_text(data, fname)

    raise UnsupportedFormatError(
        f"Unsupported format: .{ext}. Supported: pdf, docx, txt, md, url."
    )


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 — CHUNKER
# ─────────────────────────────────────────────────────────────────────────────

def _approx_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token."""
    return len(text) // 4


def chunk_text(
    text: str,
    chunk_size: int = 1500,
    overlap: int = 200,
) -> list[dict]:
    """
    Split text into overlapping chunks preserving document structure.
    Returns list of {text, chunk_index, heading, page}.
    """
    # Extract page markers added by parse_pdf
    current_page = 1
    page_pattern = re.compile(r"\[Page (\d+)\]")

    # Split on headings and double newlines first
    section_pattern = re.compile(r"(?m)^(#{1,4}\s.+)$")
    raw_sections = section_pattern.split(text)

    sections: list[dict] = []
    current_heading = ""
    for part in raw_sections:
        if section_pattern.match(part.strip()):
            current_heading = part.strip().lstrip("#").strip()
            continue
        # Extract page number if present
        pm = page_pattern.search(part)
        if pm:
            current_page = int(pm.group(1))
            part = page_pattern.sub("", part)
        if part.strip():
            sections.append({
                "text": part.strip(),
                "heading": current_heading,
                "page": current_page,
            })

    if not sections:
        sections = [{"text": text.strip(), "heading": "", "page": 1}]

    # Split oversized sections recursively
    chunks: list[dict] = []
    chunk_size_chars = chunk_size * 4  # approx
    overlap_chars = overlap * 4

    for section in sections:
        body = section["text"]
        if _approx_tokens(body) <= chunk_size:
            chunks.append({
                "text": body,
                "heading": section["heading"],
                "page": section["page"],
            })
        else:
            # Recursive character split on paragraphs then sentences
            paragraphs = [p.strip() for p in re.split(r"\n{2,}", body) if p.strip()]
            buffer = ""
            for para in paragraphs:
                if _approx_tokens(buffer + para) <= chunk_size:
                    buffer = (buffer + "\n\n" + para).strip()
                else:
                    if buffer:
                        chunks.append({
                            "text": buffer,
                            "heading": section["heading"],
                            "page": section["page"],
                        })
                    # If single paragraph is still too long, split on sentences
                    if _approx_tokens(para) > chunk_size:
                        sentences = re.split(r"(?<=[.!?])\s+", para)
                        buf2 = ""
                        for sent in sentences:
                            if _approx_tokens(buf2 + sent) <= chunk_size:
                                buf2 = (buf2 + " " + sent).strip()
                            else:
                                if buf2:
                                    chunks.append({
                                        "text": buf2,
                                        "heading": section["heading"],
                                        "page": section["page"],
                                    })
                                buf2 = sent
                        if buf2:
                            chunks.append({
                                "text": buf2,
                                "heading": section["heading"],
                                "page": section["page"],
                            })
                        buffer = ""
                    else:
                        buffer = para
            if buffer:
                chunks.append({
                    "text": buffer,
                    "heading": section["heading"],
                    "page": section["page"],
                })

    # Add overlap and assign indexes
    result: list[dict] = []
    for i, chunk in enumerate(chunks):
        text_body = chunk["text"]
        if i > 0 and overlap_chars > 0:
            prev = result[-1]["text"]
            tail = prev[-overlap_chars:]
            # Only prepend if it doesn't create a duplicate
            if not text_body.startswith(tail[:50]):
                text_body = tail + "\n" + text_body
        result.append({
            "text": text_body,
            "chunk_index": i,
            "heading": chunk["heading"],
            "page": chunk["page"],
        })

    return result


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 — GEMINI EMBEDDER
# ─────────────────────────────────────────────────────────────────────────────

class GeminiEmbedder:
    """
    Wraps Gemini Embedding 2 with asymmetric task types for best-in-class
    retrieval quality (30–40% improvement over symmetric embeddings).
    """
    MODEL = "gemini-embedding-2-preview"
    BATCH_SIZE = 100

    def __init__(self, api_key: str, dim: int = 768):
        genai = _require("google.genai", "google-genai")
        self._client = genai.Client(api_key=api_key)
        self.dim = dim

    def _embed(self, texts: list[str], task_type: str) -> list[list[float]]:
        results: list[list[float]] = []
        for i in range(0, len(texts), self.BATCH_SIZE):
            batch = texts[i : i + self.BATCH_SIZE]
            resp = self._client.models.embed_content(
                model=self.MODEL,
                contents=batch,
                config={
                    "task_type": task_type,
                    "output_dimensionality": self.dim,
                },
            )
            for emb in resp.embeddings:
                results.append(emb.values)
        return results

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed text chunks for storage (RETRIEVAL_DOCUMENT task type)."""
        return self._embed(texts, "RETRIEVAL_DOCUMENT")

    def embed_query(self, text: str) -> list[float]:
        """Embed a user query (RETRIEVAL_QUERY task type)."""
        return self._embed([text], "RETRIEVAL_QUERY")[0]


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4 — CHROMA VECTOR STORE
# ─────────────────────────────────────────────────────────────────────────────

class KBVectorStore:
    """
    ChromaDB wrapper providing vector search, keyword search, and document management.
    Persists to KB_STORAGE_PATH Docker volume.
    """

    def __init__(self, storage_path: str):
        chromadb = _require("chromadb")
        os.makedirs(storage_path, exist_ok=True)
        self._client = chromadb.PersistentClient(path=storage_path)

    def get_or_create_collection(self, name: str, description: str = "") -> Any:
        if description:
            return self._client.get_or_create_collection(
                name=name,
                metadata={"description": description},
            )
        return self._client.get_or_create_collection(name=name)

    def list_collections(self) -> list[dict]:
        cols = self._client.list_collections()
        result = []
        for col in cols:
            coll = self._client.get_collection(col.name)
            count = coll.count()
            # Count unique doc_ids
            if count > 0:
                try:
                    all_meta = coll.get(include=["metadatas"])["metadatas"] or []
                    doc_ids = {m.get("doc_id", "") for m in all_meta if m}
                    doc_count = len(doc_ids - {""})
                except Exception:
                    doc_count = 0
            else:
                doc_count = 0
            result.append({
                "name": col.name,
                "description": (col.metadata or {}).get("description", ""),
                "chunk_count": count,
                "doc_count": doc_count,
            })
        return result

    def delete_collection(self, name: str) -> None:
        try:
            self._client.delete_collection(name)
        except Exception:
            pass

    def upsert_chunks(self, collection_name: str, chunks: list[dict]) -> None:
        """
        chunks: list of {id, text, embedding, metadata}
        metadata should include: doc_id, doc_title, source, page, chunk_index
        """
        coll = self.get_or_create_collection(collection_name)
        ids = [c["id"] for c in chunks]
        texts = [c["text"] for c in chunks]
        embeddings = [c["embedding"] for c in chunks]
        metadatas = [c["metadata"] for c in chunks]
        coll.upsert(ids=ids, documents=texts, embeddings=embeddings, metadatas=metadatas)

    def vector_search(
        self, collection_name: str, query_embedding: list[float], top_k: int
    ) -> list[dict]:
        """Cosine similarity search. Returns scored chunks."""
        try:
            coll = self._client.get_collection(collection_name)
        except Exception:
            return []
        if coll.count() == 0:
            return []
        results = coll.query(
            query_embeddings=[query_embedding],
            n_results=min(top_k, coll.count()),
            include=["documents", "metadatas", "distances"],
        )
        chunks = []
        docs = results.get("documents", [[]])[0]
        metas = results.get("metadatas", [[]])[0]
        dists = results.get("distances", [[]])[0]
        for text, meta, dist in zip(docs, metas, dists):
            # ChromaDB returns L2 or cosine distance; convert to similarity
            score = 1.0 - (dist / 2.0)  # cosine distance → similarity
            chunks.append({"text": text, "metadata": meta or {}, "score": round(score, 4)})
        return chunks

    def keyword_search(
        self, collection_name: str, query: str, top_k: int
    ) -> list[dict]:
        """Full-text contains filter — BM25-like keyword matching."""
        try:
            coll = self._client.get_collection(collection_name)
        except Exception:
            return []
        if coll.count() == 0:
            return []
        # Use chromadb's where_document for keyword search
        words = [w for w in query.lower().split() if len(w) > 2]
        if not words:
            return []
        try:
            results = coll.get(
                where_document={"$contains": words[0]},
                include=["documents", "metadatas"],
                limit=top_k * 2,
            )
            docs = results.get("documents", []) or []
            metas = results.get("metadatas", []) or []
            chunks = []
            for text, meta in zip(docs, metas):
                # Score by how many query words appear
                text_lower = text.lower()
                hit_count = sum(1 for w in words if w in text_lower)
                score = hit_count / len(words)
                if score > 0:
                    chunks.append({"text": text, "metadata": meta or {}, "score": round(score * 0.8, 4)})
            chunks.sort(key=lambda x: x["score"], reverse=True)
            return chunks[:top_k]
        except Exception:
            return []

    def delete_document(self, collection_name: str, doc_id: str) -> int:
        """Remove all chunks belonging to a document. Returns count deleted."""
        try:
            coll = self._client.get_collection(collection_name)
        except Exception:
            return 0
        # Find chunk IDs for this doc
        results = coll.get(where={"doc_id": doc_id}, include=[])
        ids = results.get("ids", []) or []
        if ids:
            coll.delete(ids=ids)
        return len(ids)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5 — HYBRID RETRIEVAL
# ─────────────────────────────────────────────────────────────────────────────

def hybrid_search(
    store: KBVectorStore,
    embedder: GeminiEmbedder,
    collection_name: str,
    query: str,
    top_k: int = 8,
) -> list[dict]:
    """
    Merge vector similarity and keyword results, re-rank by combined score.
    """
    query_vec = embedder.embed_query(query)
    vec_results = store.vector_search(collection_name, query_vec, top_k)
    kw_results = store.keyword_search(collection_name, query, top_k)

    # Merge by text identity, keep best combined score
    merged: dict[str, dict] = {}
    for chunk in vec_results:
        key = chunk["text"][:200]
        merged[key] = {**chunk, "vector_score": chunk["score"], "keyword_score": 0.0}
    for chunk in kw_results:
        key = chunk["text"][:200]
        if key in merged:
            merged[key]["keyword_score"] = chunk["score"]
            merged[key]["score"] = merged[key]["vector_score"] * 0.7 + chunk["score"] * 0.3
        else:
            merged[key] = {**chunk, "vector_score": 0.0, "keyword_score": chunk["score"]}

    ranked = sorted(merged.values(), key=lambda x: x["score"], reverse=True)
    return ranked[:top_k]


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6 — ANSWER GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = textwrap.dedent("""
    You are a precise research assistant. Answer the user's question using ONLY
    the context passages provided below. Each passage is prefixed with its source.

    Rules:
    - Cite sources inline as [Source: <title>, p.<page>]
    - If the answer cannot be found in the context, say so clearly
    - Be concise and direct
    - Do not fabricate information
""").strip()


def _build_context(chunks: list[dict]) -> str:
    parts = []
    for chunk in chunks:
        meta = chunk.get("metadata", {})
        title = meta.get("doc_title", "Unknown")
        page = meta.get("page", "?")
        parts.append(f"[Source: {title}, p.{page}]\n{chunk['text']}")
    return "\n\n---\n\n".join(parts)


def _confidence_level(top_score: float) -> str:
    if top_score >= 0.75:
        return "high"
    if top_score >= 0.50:
        return "medium"
    return "low_confidence"


def generate_answer(
    question: str,
    chunks: list[dict],
    openai_api_key: str,
    model: str = "gpt-4o-mini",
) -> dict:
    """
    Generate a grounded answer with citations.
    Returns {answer, citations, confidence}.
    """
    openai = _require("openai")
    client = openai.OpenAI(api_key=openai_api_key)

    context = _build_context(chunks)
    top_score = chunks[0]["score"] if chunks else 0.0
    confidence = _confidence_level(top_score)

    caveat = ""
    if confidence == "low_confidence":
        caveat = "Note: Retrieved context may not fully address this question."

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"Context:\n\n{context}\n\nQuestion: {question}"},
    ]

    request_started_at = time.perf_counter()
    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.1,
        max_completion_tokens=800,
    )
    log_openai_usage(
        feature="knowledge_base_answer",
        model=model,
        response=resp,
        started_at=request_started_at,
        extra={"max_completion_tokens": 800},
    )
    answer = resp.choices[0].message.content.strip()

    # Build citation list from chunks
    seen = set()
    citations = []
    for chunk in chunks:
        meta = chunk.get("metadata", {})
        key = (meta.get("doc_title", ""), meta.get("page", ""))
        if key not in seen:
            seen.add(key)
            citations.append({
                "doc_title": meta.get("doc_title", "Unknown"),
                "page": meta.get("page", "?"),
                "doc_id": meta.get("doc_id", ""),
                "source": meta.get("source", ""),
            })

    return {
        "answer": answer,
        "citations": citations,
        "confidence": confidence,
        "caveat": caveat,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 7 — HIGH-LEVEL INGESTION PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def ingest(
    file_path_or_url: str,
    collection_name: str,
    store: KBVectorStore,
    embedder: GeminiEmbedder,
    chunk_size: int = 1500,
    chunk_overlap: int = 200,
    doc_title: str = "",
    data: bytes | None = None,
    filename: str = "",
) -> dict:
    """
    Full ingestion pipeline: parse → chunk → embed → upsert.
    Returns {doc_id, doc_title, chunk_count, collection}.
    """
    text, parse_meta = route_file(file_path_or_url, data=data, filename=filename)
    if not text.strip():
        raise ParseError("Document appears to be empty after parsing.")

    # Derive display title
    if not doc_title:
        if parse_meta.get("title"):
            doc_title = parse_meta["title"][:120]
        elif filename:
            doc_title = filename
        elif not file_path_or_url.startswith("http"):
            doc_title = Path(file_path_or_url).name
        else:
            doc_title = file_path_or_url[:80]

    doc_id = str(uuid.uuid4())
    chunks = chunk_text(text, chunk_size=chunk_size, overlap=chunk_overlap)

    # Embed all chunk texts in batch
    texts = [c["text"] for c in chunks]
    embeddings = embedder.embed_documents(texts)

    # Build store-ready records
    records = []
    for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
        records.append({
            "id": f"{doc_id}_{i}",
            "text": chunk["text"],
            "embedding": emb,
            "metadata": {
                "doc_id": doc_id,
                "doc_title": doc_title,
                "source": parse_meta.get("source_url", file_path_or_url),
                "source_type": parse_meta.get("source_type", "unknown"),
                "page": chunk["page"],
                "chunk_index": chunk["chunk_index"],
                "heading": chunk["heading"],
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            },
        })

    store.upsert_chunks(collection_name, records)

    return {
        "doc_id": doc_id,
        "doc_title": doc_title,
        "chunk_count": len(records),
        "collection": collection_name,
        "source_type": parse_meta.get("source_type", "unknown"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 8 — CONVENIENCE FACTORY
# ─────────────────────────────────────────────────────────────────────────────

def make_kb_components(
    gemini_api_key: str,
    storage_path: str,
    embed_dim: int = 768,
) -> tuple[KBVectorStore, GeminiEmbedder]:
    """Instantiate the vector store and embedder from config values."""
    store = KBVectorStore(storage_path)
    embedder = GeminiEmbedder(api_key=gemini_api_key, dim=embed_dim)
    return store, embedder
