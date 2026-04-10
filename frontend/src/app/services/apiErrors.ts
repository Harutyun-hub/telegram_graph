export function getApiErrorStatus(error: unknown): number | null {
  const message = error instanceof Error ? error.message : String(error ?? '');
  const match = message.match(/\bAPI\s+(\d{3})\b/);
  if (!match) return null;
  const status = Number(match[1]);
  return Number.isFinite(status) ? status : null;
}

export function isAccessDeniedError(error: unknown): boolean {
  const status = getApiErrorStatus(error);
  return status === 401 || status === 403;
}
