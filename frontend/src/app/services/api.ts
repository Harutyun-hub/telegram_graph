// ================================================================
// API Service — Backend connectivity layer
// ================================================================
// NOTE: Graph-specific API functions have been REMOVED.
// The Graph page now embeds a separate dedicated graph application.
// Graph API calls (getGraphData, getNodeDetails, searchGraph, etc.)
// live in that separate app. See integration.md Section 7 for details.
// ================================================================

// ── Configuration ──────────────────────────────────────────────
// Prefer VITE_API_BASE_URL for local/prod environments.
const API_BASE_URL =
  (import.meta as any)?.env?.VITE_API_BASE_URL?.toString()?.replace(/\/$/, '') ||
  '/api';
const DEFAULT_TIMEOUT_MS = 15_000;

/**
 * Returns an auth token for API calls.
 * Replace with Supabase session token or JWT when connecting backend.
 * Example: return supabase.auth.session()?.access_token ?? '';
 */
function getAuthToken(): string | null {
  // TODO: Replace with real auth token retrieval
  // import { supabase } from './supabaseClient';
  // const { data: { session } } = await supabase.auth.getSession();
  // return session?.access_token ?? null;
  return null;
}

/**
 * Centralized fetch wrapper with timeout, auth, and error normalization.
 * Use this for all real API calls.
 */
export async function apiFetch<T>(
  path: string,
  options: RequestInit & { timeoutMs?: number } = {},
): Promise<T> {
  const { timeoutMs = DEFAULT_TIMEOUT_MS, ...fetchOptions } = options;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(fetchOptions.headers as Record<string, string> ?? {}),
  };

  const token = getAuthToken();
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }

  try {
    const response = await fetch(`${API_BASE_URL}${path}`, {
      ...fetchOptions,
      headers,
      signal: controller.signal,
    });

    if (!response.ok) {
      const body = await response.text().catch(() => '');
      throw new Error(
        `API ${response.status}: ${body || response.statusText}`
      );
    }

    return await response.json() as T;
  } catch (err: any) {
    if (err.name === 'AbortError') {
      throw new Error(`Request to ${path} timed out after ${timeoutMs}ms`);
    }
    throw err;
  } finally {
    clearTimeout(timeout);
  }
}

// ── AI Assistant ──────────────────────────────────────────────

/**
 * POST /api/ai/query
 * Sends a natural-language query to GPT-4o-mini with Neo4j context.
 *
 * BACKEND NOTE: When connecting, consider:
 *   - Streaming responses (SSE or WebSocket) for better UX
 *   - AbortController support for cancellation
 *   - Rate limiting awareness (429 handling)
 *   - Token counting for context window management
 */
export async function askAI(query: string): Promise<{
  query: string;
  answer: string;
  timestamp: string;
}> {
  return apiFetch('/ai/query', {
    method: 'POST',
    body: JSON.stringify({ query }),
  });
}

/**
 * GET /api/health
 * Use this to verify backend connectivity before loading the app.
 */
export async function healthCheck(): Promise<{ status: string }> {
  return apiFetch('/health');
}

// ── Knowledge Base (RAG) ──────────────────────────────────────────────────────

export interface KBCollection {
  name: string;
  description: string;
  chunk_count: number;
  doc_count: number;
}

export interface KBDocument {
  doc_id: string;
  doc_title: string;
  source: string;
  source_type: string;
  ingested_at: string;
  chunk_count: number;
}

export interface KBCitation {
  doc_title: string;
  page: number | string;
  doc_id: string;
  source: string;
}

export interface KBAskResult {
  answer: string;
  citations: KBCitation[];
  confidence: 'high' | 'medium' | 'low_confidence';
  caveat?: string;
}

export interface KBSearchResult {
  query: string;
  collection: string;
  results: Array<{
    text: string;
    score: number;
    doc_title: string;
    page: number | string;
    source_type: string;
  }>;
}

export interface KBIngestResult {
  doc_id: string;
  doc_title: string;
  chunk_count: number;
  collection: string;
  source_type: string;
}

/** Create a named knowledge base collection. */
export async function kbCreateCollection(
  name: string,
  description = '',
): Promise<{ name: string; description: string; created: boolean }> {
  return apiFetch('/kb/collections', {
    method: 'POST',
    body: JSON.stringify({ name, description }),
  });
}

/** List all collections with stats. */
export async function kbListCollections(): Promise<{ collections: KBCollection[] }> {
  return apiFetch('/kb/collections');
}

/** Delete a collection and all its documents. */
export async function kbDeleteCollection(name: string): Promise<{ deleted: string }> {
  return apiFetch(`/kb/collections/${encodeURIComponent(name)}`, { method: 'DELETE' });
}

/** Upload a file to a collection. */
export async function kbUploadDocument(
  collectionName: string,
  file: File,
  docTitle = '',
): Promise<KBIngestResult> {
  const form = new FormData();
  form.append('file', file);
  if (docTitle) form.append('doc_title', docTitle);

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 120_000);
  try {
    const response = await fetch(`${API_BASE_URL}/kb/collections/${encodeURIComponent(collectionName)}/upload`, {
      method: 'POST',
      body: form,
      signal: controller.signal,
    });
    if (!response.ok) {
      const body = await response.text().catch(() => '');
      throw new Error(`Upload failed ${response.status}: ${body || response.statusText}`);
    }
    return response.json();
  } catch (err: any) {
    if (err.name === 'AbortError') throw new Error('Upload timed out (>2 min)');
    throw err;
  } finally {
    clearTimeout(timeout);
  }
}

/** Add a URL to a collection. */
export async function kbAddUrl(
  collectionName: string,
  url: string,
  docTitle = '',
): Promise<KBIngestResult> {
  return apiFetch(`/kb/collections/${encodeURIComponent(collectionName)}/add-url`, {
    method: 'POST',
    body: JSON.stringify({ url, doc_title: docTitle }),
    timeoutMs: 60_000,
  });
}

/** List all documents in a collection. */
export async function kbListDocuments(collectionName: string): Promise<{ documents: KBDocument[] }> {
  return apiFetch(`/kb/collections/${encodeURIComponent(collectionName)}/documents`);
}

/** Delete a document from a collection. */
export async function kbDeleteDocument(
  collectionName: string,
  docId: string,
): Promise<{ doc_id: string; chunks_deleted: number }> {
  return apiFetch(
    `/kb/documents/${encodeURIComponent(docId)}?collection=${encodeURIComponent(collectionName)}`,
    { method: 'DELETE' },
  );
}

/** Ask a question grounded in a collection. */
export async function kbAsk(
  collectionName: string,
  question: string,
): Promise<KBAskResult> {
  return apiFetch('/kb/ask', {
    method: 'POST',
    body: JSON.stringify({ collection: collectionName, question }),
    timeoutMs: 45_000,
  });
}

/** Semantic + keyword search returning ranked snippets. */
export async function kbSearch(
  collectionName: string,
  query: string,
  topK = 5,
): Promise<KBSearchResult> {
  return apiFetch(
    `/kb/search?collection=${encodeURIComponent(collectionName)}&q=${encodeURIComponent(query)}&top_k=${topK}`,
  );
}
