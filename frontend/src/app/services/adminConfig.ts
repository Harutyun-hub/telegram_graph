import { createDefaultAdminConfig } from '../admin/catalog';
import type { AdminConfig, AdminConfigEnvelope, AdminConfigPatch, AdminConfigResult } from '../types/admin';
import { apiFetch } from './api';

const ADMIN_CONFIG_CACHE_KEY = 'admin-config-cache-v1';

function mergeAdminConfig(payload: Partial<AdminConfig> | null | undefined): AdminConfig {
  const defaults = createDefaultAdminConfig();
  const analysisLensCatalog = Array.isArray(payload?.analysisLensCatalog) && payload.analysisLensCatalog.length > 0
    ? payload.analysisLensCatalog
    : defaults.analysisLensCatalog;
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
    effectivePrompts: {
      ...(defaults.effectivePrompts || {}),
      ...(payload?.effectivePrompts || {}),
    },
    runtime: {
      ...defaults.runtime,
      ...(payload?.runtime || {}),
    },
    analysisLensCatalog,
    analysisLensSelectionSource: payload?.analysisLensSelectionSource ?? defaults.analysisLensSelectionSource,
  };
}

export function readCachedAdminConfig(): AdminConfig | null {
  if (typeof window === 'undefined') {
    return null;
  }

  try {
    const raw = window.localStorage.getItem(ADMIN_CONFIG_CACHE_KEY);
    if (!raw) {
      return null;
    }
    return mergeAdminConfig(JSON.parse(raw) as Partial<AdminConfig>);
  } catch {
    return null;
  }
}

function writeCachedAdminConfig(config: AdminConfig) {
  if (typeof window === 'undefined') {
    return;
  }

  try {
    window.localStorage.setItem(ADMIN_CONFIG_CACHE_KEY, JSON.stringify(config));
  } catch {
    // Ignore storage failures and continue with runtime state.
  }
}

export async function getAdminConfig(): Promise<AdminConfigResult> {
  const payload = await apiFetch<AdminConfigEnvelope>('/admin/config', {
    method: 'GET',
    headers: { Accept: 'application/json' },
    cache: 'no-store',
    includeUserAuth: true,
    timeoutMs: 25_000,
  });
  const config = mergeAdminConfig(payload);
  writeCachedAdminConfig(config);
  return {
    config,
    warning: typeof payload?.warning === 'string' && payload.warning.trim() ? payload.warning : null,
  };
}

export async function patchAdminConfig(patch: AdminConfigPatch): Promise<AdminConfigResult> {
  const payload = await apiFetch<AdminConfigEnvelope>('/admin/config', {
    method: 'PATCH',
    body: JSON.stringify(patch),
    includeUserAuth: true,
    timeoutMs: 25_000,
  });
  const config = mergeAdminConfig(payload);
  writeCachedAdminConfig(config);
  return {
    config,
    warning: typeof payload?.warning === 'string' && payload.warning.trim() ? payload.warning : null,
  };
}
