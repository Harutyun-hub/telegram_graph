// ================================================================
// API Service — Backend connectivity layer
// ================================================================
// NOTE: Graph-specific API functions have been REMOVED.
// The Graph page now embeds a separate dedicated graph application.
// Graph API calls (getGraphData, getNodeDetails, searchGraph, etc.)
// live in that separate app. See integration.md Section 7 for details.
// ================================================================

import { buildSimpleAuthApiAuthorization } from '../auth';
import { getSupabaseBrowserClient } from './supabaseClient';

// ── Configuration ──────────────────────────────────────────────
// Prefer VITE_API_BASE_URL for local/prod environments.
const viteEnv = import.meta.env as Record<string, string | boolean | undefined>;
const API_BASE_URL = String(viteEnv.VITE_API_BASE_URL ?? '').replace(/\/$/, '') || '/api';
const DEFAULT_TIMEOUT_MS = 15_000;

/**
 * Returns an auth token for API calls.
 * This is used only for routes that require a real user session.
 */
async function getAuthToken(): Promise<string | null> {
  const supabase = getSupabaseBrowserClient();
  if (!supabase) {
    return null;
  }

  try {
    const { data: { session } } = await supabase.auth.getSession();
    return session?.access_token ?? null;
  } catch {
    return null;
  }
}

function getFriendlyErrorMessage(payload: any, fallback: string): string {
  const helperError = payload?.error;
  if (helperError && typeof helperError?.message === 'string' && helperError.message.trim()) {
    return helperError.message.trim();
  }
  if (typeof payload?.detail === 'string' && payload.detail.trim()) {
    return payload.detail.trim();
  }
  return fallback;
}

/**
 * Centralized fetch wrapper with timeout, auth, and error normalization.
 * Use this for all real API calls.
 */
export async function apiFetch<T>(
  path: string,
  options: RequestInit & { timeoutMs?: number; includeUserAuth?: boolean } = {},
): Promise<T> {
  const { timeoutMs = DEFAULT_TIMEOUT_MS, includeUserAuth = false, ...fetchOptions } = options;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const headers = new Headers(fetchOptions.headers ?? undefined);
  const isFormDataBody = typeof FormData !== 'undefined' && fetchOptions.body instanceof FormData;

  if (!isFormDataBody && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json');
  }

  if (includeUserAuth) {
    const token = await getAuthToken();
    if (token) {
      headers.set('X-Supabase-Authorization', `Bearer ${token}`);
    } else {
      const simpleAuthHeader = buildSimpleAuthApiAuthorization();
      if (simpleAuthHeader) {
        headers.set('X-Admin-Authorization', simpleAuthHeader);
      }
    }
  }

  try {
    const response = await fetch(`${API_BASE_URL}${path}`, {
      ...fetchOptions,
      headers,
      signal: controller.signal,
    });

    if (!response.ok) {
      const rawBody = await response.text().catch(() => '');
      let parsedBody: any = null;
      if (rawBody.trim()) {
        try {
          parsedBody = JSON.parse(rawBody);
        } catch {
          parsedBody = null;
        }
      }
      throw new Error(
        `API ${response.status}: ${getFriendlyErrorMessage(parsedBody, rawBody || response.statusText)}`
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
  // Deprecated legacy route. The floating helper uses /api/ai-helper/* now.
  return apiFetch('/ai/query', {
    method: 'POST',
    body: JSON.stringify({ query }),
  });
}

export interface AIHelperMessage {
  role: 'user' | 'assistant';
  text: string;
  timestamp: string;
}

export async function aiHelperChat(message: string, sessionId: string): Promise<AIHelperMessage> {
  const payload = await apiFetch<{ ok: boolean; sessionId?: string; message: AIHelperMessage }>('/ai/chat', {
    method: 'POST',
    body: JSON.stringify({ message, sessionId }),
    includeUserAuth: true,
    timeoutMs: 95_000,
  });
  return payload.message;
}

export async function getAiHelperHistory(sessionId: string, limit: number = 50): Promise<AIHelperMessage[]> {
  const payload = await apiFetch<{ ok: boolean; sessionId?: string; messages: AIHelperMessage[] }>(
    `/ai/chat/history?limit=${limit}&sessionId=${encodeURIComponent(sessionId)}`,
    {
    method: 'GET',
    includeUserAuth: true,
    timeoutMs: 20_000,
    }
  );
  return Array.isArray(payload?.messages) ? payload.messages : [];
}

export async function resetAiHelper(
  sessionId: string,
): Promise<{ ok: boolean; reset: boolean; sessionId?: string; timestamp: string }> {
  return apiFetch('/ai/chat/reset', {
    method: 'POST',
    body: JSON.stringify({ sessionId }),
    includeUserAuth: true,
    timeoutMs: 20_000,
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
    includeUserAuth: true,
  });
}

/** List all collections with stats. */
export async function kbListCollections(): Promise<{ collections: KBCollection[] }> {
  return apiFetch('/kb/collections', { includeUserAuth: true });
}

/** Delete a collection and all its documents. */
export async function kbDeleteCollection(name: string): Promise<{ deleted: string }> {
  return apiFetch(`/kb/collections/${encodeURIComponent(name)}`, {
    method: 'DELETE',
    includeUserAuth: true,
  });
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
  return apiFetch(`/kb/collections/${encodeURIComponent(collectionName)}/upload`, {
    method: 'POST',
    body: form,
    includeUserAuth: true,
    timeoutMs: 120_000,
  });
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
    includeUserAuth: true,
    timeoutMs: 60_000,
  });
}

/** List all documents in a collection. */
export async function kbListDocuments(collectionName: string): Promise<{ documents: KBDocument[] }> {
  return apiFetch(`/kb/collections/${encodeURIComponent(collectionName)}/documents`, {
    includeUserAuth: true,
  });
}

/** Delete a document from a collection. */
export async function kbDeleteDocument(
  collectionName: string,
  docId: string,
): Promise<{ doc_id: string; chunks_deleted: number }> {
  return apiFetch(
    `/kb/documents/${encodeURIComponent(docId)}?collection=${encodeURIComponent(collectionName)}`,
    {
      method: 'DELETE',
      includeUserAuth: true,
    },
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
    includeUserAuth: true,
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
    { includeUserAuth: true },
  );
}
