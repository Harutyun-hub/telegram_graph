import { createClient, type SupabaseClient } from '@supabase/supabase-js';
import { isSimpleAuthEnabled } from '../auth';

let browserClient: SupabaseClient | null | undefined;
const viteEnv = import.meta.env as Record<string, string | boolean | undefined>;

export function getSupabaseBrowserClient(): SupabaseClient | null {
  if (browserClient !== undefined) {
    return browserClient;
  }

  if (isSimpleAuthEnabled()) {
    browserClient = null;
    return browserClient;
  }

  const url = String(viteEnv.VITE_SUPABASE_URL ?? '').trim();
  const anonKey = String(viteEnv.VITE_SUPABASE_ANON_KEY ?? '').trim();

  if (!url || !anonKey) {
    browserClient = null;
    return browserClient;
  }

  browserClient = createClient(url, anonKey, {
    auth: {
      autoRefreshToken: true,
      persistSession: true,
    },
  });

  return browserClient;
}
