export const SIMPLE_AUTH_STORAGE_KEY = 'radar.simple-auth.v1';
export const SIMPLE_AUTH_USERNAME = 'Admin';
export const SIMPLE_AUTH_PASSWORD = 'A457!dsdhfi850';

export interface SimpleAuthSession {
  authenticated: true;
  username: string;
}

interface RedirectStateLike {
  from?: {
    pathname?: string;
    search?: string;
    hash?: string;
  } | null;
}

export function validateSimpleCredentials(username: string, password: string): boolean {
  return username === SIMPLE_AUTH_USERNAME && password === SIMPLE_AUTH_PASSWORD;
}

export function loadStoredSimpleAuthSession(): SimpleAuthSession | null {
  if (typeof window === 'undefined') {
    return null;
  }

  try {
    const raw = window.localStorage.getItem(SIMPLE_AUTH_STORAGE_KEY);
    if (!raw) {
      return null;
    }

    const parsed = JSON.parse(raw) as Partial<SimpleAuthSession> | null;
    if (parsed?.authenticated !== true || parsed.username !== SIMPLE_AUTH_USERNAME) {
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
