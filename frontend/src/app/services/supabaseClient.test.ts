import { afterEach, describe, expect, it, vi } from 'vitest';

const createClient = vi.fn(() => ({ kind: 'supabase-client' }));

vi.mock('@supabase/supabase-js', () => ({
  createClient,
}));

describe('getSupabaseBrowserClient', () => {
  afterEach(() => {
    vi.resetModules();
    vi.unstubAllEnvs();
    createClient.mockClear();
  });

  it('returns null when browser auth env is missing', async () => {
    vi.stubEnv('VITE_SUPABASE_URL', '');
    vi.stubEnv('VITE_SUPABASE_ANON_KEY', '');

    const { getSupabaseBrowserClient } = await import('./supabaseClient');

    expect(getSupabaseBrowserClient()).toBeNull();
    expect(createClient).not.toHaveBeenCalled();
  });

  it('creates a browser client when Vite env values are present', async () => {
    vi.stubEnv('VITE_SUPABASE_URL', 'https://example.supabase.co');
    vi.stubEnv('VITE_SUPABASE_ANON_KEY', 'anon-public-key');

    const { getSupabaseBrowserClient } = await import('./supabaseClient');

    expect(getSupabaseBrowserClient()).toEqual({ kind: 'supabase-client' });
    expect(createClient).toHaveBeenCalledWith(
      'https://example.supabase.co',
      'anon-public-key',
      {
        auth: {
          autoRefreshToken: true,
          persistSession: true,
        },
      },
    );
  });
});
