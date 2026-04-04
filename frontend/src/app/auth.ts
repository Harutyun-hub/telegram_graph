export const SIMPLE_AUTH_STORAGE_KEY = 'radar.simple-auth.v1';

export interface SimpleAuthSession {
  authenticated: true;
  username: string;
}

const viteEnv = import.meta.env as Record<string, string | boolean | undefined>;

interface RedirectStateLike {
  from?: {
    pathname?: string;
    search?: string;
    hash?: string;
  } | null;
}

function envString(name: string): string {
  const value = viteEnv[name];
  return value == null ? '' : String(value).trim();
}

function envBool(name: string, fallback: boolean): boolean {
  const raw = envString(name);
  if (!raw) {
    return fallback;
  }
  return ['1', 'true', 'yes', 'on'].includes(raw.toLowerCase());
}

function normalizeSimpleUsername(value: string): string {
  return value.trim().toLowerCase();
}

function getSimpleAuthCredentials(): { username: string; password: string } | null {
  const username = envString('VITE_SIMPLE_AUTH_USERNAME');
  const password = envString('VITE_SIMPLE_AUTH_PASSWORD');
  if (!username || !password) {
    return null;
  }
  return { username, password };
}

export function isSimpleAuthEnabled(): boolean {
  return envBool('VITE_ENABLE_SIMPLE_AUTH', false) && Boolean(getSimpleAuthCredentials());
}

export function validateSimpleCredentials(username: string, password: string): boolean {
  const configured = getSimpleAuthCredentials();
  if (!configured || !isSimpleAuthEnabled()) {
    return false;
  }

  const normalizedInputUsername = normalizeSimpleUsername(username);
  const normalizedConfiguredUsername = normalizeSimpleUsername(configured.username);
  const passwordCandidates = password === password.trim() ? [password] : [password, password.trim()];

  return (
    normalizedInputUsername === normalizedConfiguredUsername &&
    passwordCandidates.includes(configured.password)
  );
}

export function loadStoredSimpleAuthSession(): SimpleAuthSession | null {
  if (typeof window === 'undefined' || !isSimpleAuthEnabled()) {
    return null;
  }

  try {
    const raw = window.localStorage.getItem(SIMPLE_AUTH_STORAGE_KEY);
    if (!raw) {
      return null;
    }

    const parsed = JSON.parse(raw) as Partial<SimpleAuthSession> | null;
    const configured = getSimpleAuthCredentials();
    if (
      parsed?.authenticated !== true ||
      !configured ||
      normalizeSimpleUsername(parsed.username ?? '') !== normalizeSimpleUsername(configured.username)
    ) {
      return null;
    }

    return {
      authenticated: true,
      username: parsed.username,
    };
  } catch {
    return null;
  }
}

export function persistSimpleAuthSession(session: SimpleAuthSession): void {
  if (typeof window === 'undefined') {
    return;
  }

  try {
    window.localStorage.setItem(SIMPLE_AUTH_STORAGE_KEY, JSON.stringify(session));
  } catch {
    // Ignore storage failures and keep the in-memory session active.
  }
}

export function clearStoredSimpleAuthSession(): void {
  if (typeof window === 'undefined') {
    return;
  }

  try {
    window.localStorage.removeItem(SIMPLE_AUTH_STORAGE_KEY);
  } catch {
    // Ignore storage failures.
  }
}

export function resolveAuthRedirectTarget(state: unknown): string {
  const redirectState = state as RedirectStateLike | null;
  const pathname = redirectState?.from?.pathname ?? '/';
  const search = redirectState?.from?.search ?? '';
  const hash = redirectState?.from?.hash ?? '';
  const target = `${pathname}${search}${hash}`;

  if (!target.startsWith('/') || target === '/login') {
    return '/';
  }

  return target;
}
