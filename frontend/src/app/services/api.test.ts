import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const { getSession, buildSimpleAuthApiAuthorization } = vi.hoisted(() => ({
  getSession: vi.fn(),
  buildSimpleAuthApiAuthorization: vi.fn(),
}));

import { apiFetch, kbListCollections, kbUploadDocument } from './api';

vi.mock('../auth', () => ({
  buildSimpleAuthApiAuthorization,
}));

vi.mock('./supabaseClient', () => ({
  getSupabaseBrowserClient: () => ({
    auth: {
      getSession,
    },
  }),
}));

describe('apiFetch', () => {
  beforeEach(() => {
    getSession.mockReset();
    buildSimpleAuthApiAuthorization.mockReset();
    buildSimpleAuthApiAuthorization.mockReturnValue(null);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('attaches the user session token when includeUserAuth is enabled', async () => {
    getSession.mockResolvedValue({
      data: {
        session: {
          access_token: 'user-token',
        },
      },
    });

    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ ok: true }),
    });
    vi.stubGlobal('fetch', fetchMock);

    await apiFetch('/admin/config', { includeUserAuth: true });

    expect(fetchMock).toHaveBeenCalledWith('/api/admin/config', expect.any(Object));
    const init = fetchMock.mock.calls[0]?.[1] as RequestInit;
    const headers = new Headers(init.headers);
    expect(headers.get('X-Supabase-Authorization')).toBe('Bearer user-token');
  });

  it('falls back to simple auth for protected routes when no Supabase session exists', async () => {
    getSession.mockResolvedValue({ data: { session: null } });
    buildSimpleAuthApiAuthorization.mockReturnValue('Basic YWRtaW46c2VjcmV0');

    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ ok: true }),
    });
    vi.stubGlobal('fetch', fetchMock);

    await apiFetch('/admin/config', { includeUserAuth: true });

    const init = fetchMock.mock.calls[0]?.[1] as RequestInit;
    const headers = new Headers(init.headers);
    expect(headers.get('X-Admin-Authorization')).toBe('Basic YWRtaW46c2VjcmV0');
    expect(headers.get('X-Supabase-Authorization')).toBeNull();
  });

  it('surfaces API detail messages on failure', async () => {
    getSession.mockResolvedValue({ data: { session: null } });
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 403,
      statusText: 'Forbidden',
      text: async () => JSON.stringify({ detail: 'Admin access required' }),
    });
    vi.stubGlobal('fetch', fetchMock);

    await expect(apiFetch('/admin/config')).rejects.toThrow('Admin access required');
  });

  it('attaches the user session token to KB collection requests', async () => {
    getSession.mockResolvedValue({
      data: {
        session: {
          access_token: 'user-token',
        },
      },
    });

    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ collections: [] }),
    });
    vi.stubGlobal('fetch', fetchMock);

    await kbListCollections();

    expect(fetchMock).toHaveBeenCalledWith('/api/kb/collections', expect.any(Object));
    const init = fetchMock.mock.calls[0]?.[1] as RequestInit;
    const headers = new Headers(init.headers);
    expect(headers.get('X-Supabase-Authorization')).toBe('Bearer user-token');
  });

  it('does not force JSON content type for KB multipart uploads', async () => {
    getSession.mockResolvedValue({
      data: {
        session: {
          access_token: 'user-token',
        },
      },
    });

    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ doc_id: 'doc-1', doc_title: 'Doc', chunk_count: 1, collection: 'demo', source_type: 'txt' }),
    });
    vi.stubGlobal('fetch', fetchMock);

    const blob = new Blob(['hello'], { type: 'text/plain' }) as unknown as File;
    await kbUploadDocument('demo', blob, 'Doc');

    expect(fetchMock).toHaveBeenCalledWith('/api/kb/collections/demo/upload', expect.any(Object));
    const init = fetchMock.mock.calls[0]?.[1] as RequestInit;
    const headers = new Headers(init.headers);
    expect(headers.get('X-Supabase-Authorization')).toBe('Bearer user-token');
    expect(headers.has('Content-Type')).toBe(false);
    expect(init.body).toBeInstanceOf(FormData);
  });
});
