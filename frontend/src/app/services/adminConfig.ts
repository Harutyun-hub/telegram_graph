import { createDefaultAdminConfig } from '../admin/catalog';
import type { AdminConfig, AdminConfigPatch } from '../types/admin';
import { apiFetch } from './api';

function mergeAdminConfig(payload: Partial<AdminConfig> | null | undefined): AdminConfig {
  const defaults = createDefaultAdminConfig();
  return {
    widgets: {
      ...defaults.widgets,
      ...(payload?.widgets || {}),
    },
    prompts: {
      ...defaults.prompts,
      ...(payload?.prompts || {}),
    },
    promptDefaults: {
      ...(defaults.promptDefaults || {}),
      ...(payload?.promptDefaults || {}),
    },
    runtime: {
      ...defaults.runtime,
      ...(payload?.runtime || {}),
    },
  };
}

export async function getAdminConfig(): Promise<AdminConfig> {
  const payload = await apiFetch<Partial<AdminConfig>>('/admin/config', {
    method: 'GET',
    headers: { Accept: 'application/json' },
    cache: 'no-store',
    timeoutMs: 25_000,
  });
  return mergeAdminConfig(payload);
}

export async function patchAdminConfig(patch: AdminConfigPatch): Promise<AdminConfig> {
  const payload = await apiFetch<Partial<AdminConfig>>('/admin/config', {
    method: 'PATCH',
    body: JSON.stringify(patch),
    timeoutMs: 25_000,
  });
  return mergeAdminConfig(payload);
}
