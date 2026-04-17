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
    vi.stubEnv('VITE_ENABLE_SIMPLE_AUTH', '');
    vi.stubEnv('VITE_SIMPLE_AUTH_USERNAME', '');
    vi.stubEnv('VITE_SIMPLE_AUTH_PASSWORD', '');
    vi.stubEnv('VITE_SUPABASE_URL', '');
    vi.stubEnv('VITE_SUPABASE_ANON_KEY', '');

    const { getSupabaseBrowserClient } = await import('./supabaseClient');

    expect(getSupabaseBrowserClient()).toBeNull();
    expect(createClient).not.toHaveBeenCalled();
  });

  it('creates a browser client when Vite env values are present', async () => {
    vi.stubEnv('VITE_ENABLE_SIMPLE_AUTH', '');
    vi.stubEnv('VITE_SIMPLE_AUTH_USERNAME', '');
    vi.stubEnv('VITE_SIMPLE_AUTH_PASSWORD', '');
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

  it('returns null when simple auth is enabled even if Supabase env values are present', async () => {
    vi.stubEnv('VITE_ENABLE_SIMPLE_AUTH', 'true');
    vi.stubEnv('VITE_SIMPLE_AUTH_USERNAME', 'Admin');
    vi.stubEnv('VITE_SIMPLE_AUTH_PASSWORD', 'secret');
    vi.stubEnv('VITE_SUPABASE_URL', 'https://example.supabase.co');
    vi.stubEnv('VITE_SUPABASE_ANON_KEY', 'anon-public-key');

    const { getSupabaseBrowserClient } = await import('./supabaseClient');

    expect(getSupabaseBrowserClient()).toBeNull();
    expect(createClient).not.toHaveBeenCalled();
  });
});
