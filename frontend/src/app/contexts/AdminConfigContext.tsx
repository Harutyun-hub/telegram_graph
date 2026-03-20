import { createContext, useCallback, useContext, useEffect, useState } from 'react';
import type { ReactNode } from 'react';
import { createDefaultAdminConfig } from '../admin/catalog';
import { getAdminConfig, patchAdminConfig, readCachedAdminConfig } from '../services/adminConfig';
import type { AdminConfig, AdminConfigPatch } from '../types/admin';

interface AdminConfigContextValue {
  config: AdminConfig;
  loading: boolean;
  saving: boolean;
  error: string | null;
  refresh: () => Promise<void>;
  updateConfig: (patch: AdminConfigPatch) => Promise<void>;
  isWidgetEnabled: (widgetId: string) => boolean;
}

const defaultConfig = createDefaultAdminConfig();
const cachedConfig = readCachedAdminConfig();

const AdminConfigContext = createContext<AdminConfigContextValue>({
  config: cachedConfig || defaultConfig,
  loading: false,
  saving: false,
  error: null,
  refresh: async () => {},
  updateConfig: async () => {},
  isWidgetEnabled: () => true,
});

export function AdminConfigProvider({ children }: { children: ReactNode }) {
  const [config, setConfig] = useState<AdminConfig>(cachedConfig || defaultConfig);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const nextConfig = await getAdminConfig();
      setConfig(nextConfig);
    } catch (err: any) {
      setError(err?.message ?? 'Failed to load admin config');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const updateConfig = useCallback(async (patch: AdminConfigPatch) => {
    setSaving(true);
    setError(null);
    try {
      const nextConfig = await patchAdminConfig(patch);
      setConfig(nextConfig);
    } catch (err: any) {
      const message = err?.message ?? 'Failed to save admin config';
      setError(message);
      throw new Error(message);
    } finally {
      setSaving(false);
    }
  }, []);

  const value: AdminConfigContextValue = {
    config,
    loading,
    saving,
    error,
    refresh,
    updateConfig,
    isWidgetEnabled: (widgetId: string) => config.widgets[widgetId]?.enabled ?? true,
  };

  return (
    <AdminConfigContext.Provider value={value}>
      {children}
    </AdminConfigContext.Provider>
  );
}

export function useAdminConfig() {
  return useContext(AdminConfigContext);
}
