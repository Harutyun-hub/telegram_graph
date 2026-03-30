import * as Sentry from '@sentry/react';

const viteEnv = import.meta.env as Record<string, string | boolean | undefined>;

function toFloat(value: string | boolean | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function initFrontendSentry(): void {
  const dsn = String(viteEnv.VITE_SENTRY_DSN ?? '').trim();
  if (!dsn) {
    return;
  }

  Sentry.init({
    dsn,
    environment: String(viteEnv.VITE_SENTRY_ENVIRONMENT ?? viteEnv.MODE ?? 'production'),
    release: String(viteEnv.VITE_SENTRY_RELEASE ?? '').trim() || undefined,
    tracesSampleRate: Math.max(0, Math.min(1, toFloat(viteEnv.VITE_SENTRY_TRACES_SAMPLE_RATE, 0.05))),
    sendDefaultPii: false,
  });
}

export { Sentry };
