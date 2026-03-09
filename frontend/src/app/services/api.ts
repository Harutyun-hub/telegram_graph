// ================================================================
// API Service — Backend connectivity layer
// ================================================================
// Currently returns mock data. To connect to a real backend:
//   1. Set API_BASE_URL to your backend endpoint
//   2. Set getAuthToken() to return a valid Supabase/JWT token
//   3. Uncomment the real fetch logic in each function
//
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
  // Real: return apiFetch('/ai/query', { method: 'POST', body: JSON.stringify({ query }) });
  return Promise.resolve({
    query,
    answer: 'This is a mock AI response. Connect your backend to enable real GPT-4o-mini queries.',
    timestamp: new Date().toISOString(),
  });
}

/**
 * GET /api/health
 * Use this to verify backend connectivity before loading the app.
 */
export async function healthCheck(): Promise<{ status: string }> {
  // Real: return apiFetch('/health');
  return Promise.resolve({ status: 'mock-ok' });
}
