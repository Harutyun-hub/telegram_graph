import { useEffect, useMemo, useRef, useState } from 'react';
import {
  AlertCircle,
  CheckCircle2,
  Clock,
  Database,
  MoreVertical,
  Network,
  Plus,
  RefreshCw,
  Search,
  Brain,
} from 'lucide-react';

import { apiFetch } from '../../services/api';

type SocialSourceRow = {
  id: string;
  entity_id: string;
  company_name: string;
  platform: 'facebook' | 'instagram' | 'google' | 'tiktok';
  display_url: string | null;
  account_external_id: string | null;
  is_active: boolean;
  health_status: 'unknown' | 'healthy' | 'invalid_identifier' | 'provider_404' | 'rate_limited' | 'auth_error' | 'network_error';
  last_collected_at: string | null;
  last_error: string | null;
  metadata: Record<string, unknown>;
};

type SocialSourceListResponse = {
  count: number;
  items: SocialSourceRow[];
};

type SocialSourceCreateResponse = {
  action: 'created' | 'reactivated' | 'exists';
  item: SocialSourceRow;
};

type SocialSourceUpdateResponse = {
  item: SocialSourceRow;
};

type SocialRuntimeStatus = {
  status: 'active' | 'stopped';
  is_active: boolean;
  interval_minutes: number;
  running_now: boolean;
  last_run_started_at: string | null;
  last_run_finished_at: string | null;
  last_success_at: string | null;
  next_run_at: string | null;
  last_error: string | null;
  last_result?: {
    accounts_total?: number;
    accounts_processed?: number;
    activities_collected?: number;
    activities_analyzed?: number;
    activities_graph_synced?: number;
    collect_failures?: number;
    analysis_failures?: number;
    graph_failures?: number;
  } | null;
  run_history?: Array<{
    finished_at: string | null;
    accounts_processed: number;
    activities_collected: number;
    activities_analyzed: number;
    activities_graph_synced: number;
    collect_failures: number;
    analysis_failures: number;
    graph_failures: number;
  }>;
};

type SocialSourceStatus = 'active' | 'paused' | 'error';

const socialStatusConfig: Record<SocialSourceStatus, { labelEn: string; labelRu: string; bg: string; text: string; dot: string }> = {
  active: { labelEn: 'Active', labelRu: 'Активен', bg: 'bg-emerald-50', text: 'text-emerald-700', dot: 'bg-emerald-500' },
  paused: { labelEn: 'Paused', labelRu: 'Пауза', bg: 'bg-amber-50', text: 'text-amber-700', dot: 'bg-amber-500' },
  error: { labelEn: 'Error', labelRu: 'Ошибка', bg: 'bg-red-50', text: 'text-red-700', dot: 'bg-red-500' },
};

function socialRowStatus(item: SocialSourceRow): SocialSourceStatus {
  if (!item.is_active) return 'paused';
  if (item.last_error) return 'error';
  if (item.health_status !== 'unknown' && item.health_status !== 'healthy') return 'error';
  return 'active';
}

function relativeTime(iso: string | null, ru: boolean): string {
  if (!iso) return ru ? 'Никогда' : 'Never';
  const date = new Date(iso);
  const diffMs = Date.now() - date.getTime();
  if (!Number.isFinite(diffMs) || diffMs < 0) return ru ? 'Только что' : 'Just now';
  const mins = Math.floor(diffMs / 60000);
  if (mins < 1) return ru ? 'Только что' : 'Just now';
  if (mins < 60) return ru ? `${mins} мин назад` : `${mins} min ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return ru ? `${hours} ч назад` : `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return ru ? `${days} д назад` : `${days}d ago`;
}

function formatDateTime(iso: string | null, ru: boolean): string {
  if (!iso) return '—';
  const date = new Date(iso);
  if (!Number.isFinite(date.getTime())) return '—';
  return date.toLocaleString(ru ? 'ru-RU' : 'en-US', {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function lastResultLabel(lastResult: SocialRuntimeStatus['last_result'], runtime: SocialRuntimeStatus | null, ru: boolean) {
  if (lastResult) {
    const collected = lastResult.activities_collected ?? 0;
    const analyzed = lastResult.activities_analyzed ?? 0;
    const synced = lastResult.activities_graph_synced ?? 0;
    return ru
      ? `${collected} собрано · ${analyzed} AI · ${synced} Neo4j`
      : `${collected} collected · ${analyzed} AI · ${synced} Neo4j`;
  }
  if (runtime?.last_run_started_at || runtime?.is_active) {
    return ru ? 'Ожидаем завершённый цикл' : 'Awaiting completed cycle';
  }
  return ru ? 'Нет данных' : 'No data';
}

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const normalizedPath = path.startsWith('/api/') ? path.slice(4) : path;
  return apiFetch<T>(normalizedPath, {
    ...init,
    includeUserAuth: true,
  });
}

function SocialStatusBadge({ status, ru }: { status: SocialSourceStatus; ru: boolean }) {
  const cfg = socialStatusConfig[status];
  return (
    <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs ${cfg.bg} ${cfg.text}`} style={{ fontWeight: 500 }}>
      <span className={`w-1.5 h-1.5 rounded-full ${cfg.dot}`} />
      {ru ? cfg.labelRu : cfg.labelEn}
    </span>
  );
}

function PlatformBadge({ platform }: { platform: SocialSourceRow['platform'] }) {
  const label = platform.charAt(0).toUpperCase() + platform.slice(1);
  const isFacebook = platform === 'facebook';
  return (
    <div className="flex items-center gap-2">
      <div
        className={`w-9 h-9 rounded-full flex items-center justify-center flex-shrink-0 text-white ${isFacebook ? '' : 'bg-slate-700'}`}
        style={isFacebook ? { background: 'linear-gradient(135deg, #2563eb, #1d4ed8)', fontWeight: 700 } : { fontWeight: 600 }}
      >
        {isFacebook ? 'f' : label.charAt(0)}
      </div>
      <span className="text-xs text-gray-500">{label}</span>
    </div>
  );
}

function AddFacebookSourceModal({
  open,
  ru,
  onClose,
  onSubmit,
}: {
  open: boolean;
  ru: boolean;
  onClose: () => void;
  onSubmit: (payload: { url: string }) => Promise<void>;
}) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [url, setUrl] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    if (!open) return;
    setUrl('');
    setSaving(false);
    setError('');
    window.setTimeout(() => inputRef.current?.focus(), 80);
  }, [open]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/50 backdrop-blur-sm" onClick={onClose} />
      <div className="relative bg-white rounded-2xl shadow-2xl w-full max-w-lg overflow-hidden">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
          <div className="flex items-center gap-3">
            <div
              className="w-9 h-9 rounded-xl flex items-center justify-center text-white"
              style={{ background: 'linear-gradient(135deg, #2563eb, #1d4ed8)', fontWeight: 700 }}
            >
              f
            </div>
            <div>
              <h3 className="text-gray-900" style={{ fontSize: '1rem', fontWeight: 600 }}>
                {ru ? 'Добавить Facebook-источник' : 'Add Facebook Source'}
              </h3>
              <p className="text-xs text-gray-500 mt-0.5">
                {ru ? 'Достаточно одной ссылки на страницу' : 'One Facebook page URL is enough'}
              </p>
            </div>
          </div>
          <button onClick={onClose} className="p-1.5 rounded-lg hover:bg-gray-100 transition-colors">
            <span className="sr-only">{ru ? 'Закрыть' : 'Close'}</span>
            ×
          </button>
        </div>

        <div className="px-6 py-5 space-y-4">
          <div>
            <label className="text-xs text-gray-600 block mb-1.5" style={{ fontWeight: 500 }}>
              Facebook URL
            </label>
            <input
              ref={inputRef}
              type="text"
              value={url}
              onChange={(event) => {
                setUrl(event.target.value);
                setError('');
              }}
              placeholder="https://www.facebook.com/nikol.pashinyan"
              className="w-full px-3.5 py-2.5 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-all"
            />
          </div>

          {error && (
            <div className="flex items-center gap-1.5 text-xs text-red-600">
              <AlertCircle className="w-3.5 h-3.5" />
              {error}
            </div>
          )}

          <div className="bg-gray-50 border border-gray-200 rounded-xl p-3.5 text-xs text-gray-500">
            {ru
              ? 'Источник сохранится сразу. Social worker заберет его на следующем цикле или по запуску вручную.'
              : 'The source is saved immediately. The social worker will pick it up on the next cycle or when run manually.'}
          </div>
        </div>

        <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gray-100 bg-gray-50/50">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm text-gray-600 hover:text-gray-800 hover:bg-gray-100 rounded-lg transition-colors"
            style={{ fontWeight: 500 }}
          >
            {ru ? 'Отмена' : 'Cancel'}
          </button>
          <button
            onClick={async () => {
              if (!url.trim()) {
                setError(ru ? 'Введите ссылку Facebook' : 'Enter a Facebook URL');
                return;
              }
              setSaving(true);
              setError('');
              try {
                await onSubmit({ url: url.trim() });
                onClose();
              } catch (err: any) {
                setError(String(err?.message || (ru ? 'Ошибка добавления' : 'Failed to add source')));
              } finally {
                setSaving(false);
              }
            }}
            disabled={saving}
            className="px-5 py-2 rounded-lg text-sm text-white transition-all disabled:opacity-40 disabled:cursor-not-allowed"
            style={{ fontWeight: 500, background: 'linear-gradient(135deg, #2563eb, #1d4ed8)' }}
          >
            <span className="flex items-center gap-1.5">
              {saving ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Plus className="w-4 h-4" />}
              {ru ? 'Добавить источник' : 'Add Source'}
            </span>
          </button>
        </div>
      </div>
    </div>
  );
}

function SocialRowActions({
  item,
  ru,
  disabled,
  onToggleActive,
}: {
  item: SocialSourceRow;
  ru: boolean;
  disabled: boolean;
  onToggleActive: (id: string, isActive: boolean) => Promise<void>;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handle(event: MouseEvent) {
      if (ref.current && !ref.current.contains(event.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener('mousedown', handle);
    return () => document.removeEventListener('mousedown', handle);
  }, []);

  return (
    <div className="relative" ref={ref}>
      <button
        onClick={() => setOpen(!open)}
        disabled={disabled}
        className={`p-1.5 rounded-lg transition-colors ${open ? 'bg-gray-100' : 'hover:bg-gray-100'} disabled:opacity-50`}
      >
        <MoreVertical className="w-4 h-4 text-gray-400" />
      </button>
      {open && (
        <div className="absolute right-0 top-full mt-1 w-48 bg-white border border-gray-200 rounded-xl shadow-xl z-30 py-1 overflow-hidden">
          <button
            onClick={async () => {
              await onToggleActive(item.id, !item.is_active);
              setOpen(false);
            }}
            className="w-full flex items-center gap-2.5 px-3 py-2 text-xs text-gray-700 hover:bg-gray-50 transition-colors"
          >
            {item.is_active ? (
              <>
                <Clock className="w-3.5 h-3.5 text-amber-500" />
                {ru ? 'Остановить источник' : 'Pause source'}
              </>
            ) : (
              <>
                <CheckCircle2 className="w-3.5 h-3.5 text-emerald-500" />
                {ru ? 'Активировать источник' : 'Activate source'}
              </>
            )}
          </button>
        </div>
      )}
    </div>
  );
}

export function SocialSourcesSection({
  ru,
  addModalOpen,
  onCloseAddModal,
}: {
  ru: boolean;
  addModalOpen: boolean;
  onCloseAddModal: () => void;
}) {
  const [items, setItems] = useState<SocialSourceRow[]>([]);
  const [search, setSearch] = useState('');
  const [statusFilter, setStatusFilter] = useState<'all' | SocialSourceStatus>('all');
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [runtime, setRuntime] = useState<SocialRuntimeStatus | null>(null);
  const [runtimeLoading, setRuntimeLoading] = useState(true);
  const [runtimeBusy, setRuntimeBusy] = useState(false);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);
  const [intervalInput, setIntervalInput] = useState('360');

  const loadSources = async (quiet = false) => {
    if (!quiet) setLoading(true);
    setError(null);
    try {
      const response = await requestJson<SocialSourceListResponse>('/api/sources/social');
      setItems(response.items || []);
    } catch (err: any) {
      setError(String(err?.message || 'Failed to load social sources'));
    } finally {
      if (!quiet) setLoading(false);
    }
  };

  const loadRuntime = async (quiet = false) => {
    if (!quiet) setRuntimeLoading(true);
    try {
      const response = await requestJson<SocialRuntimeStatus>('/api/social/runtime/status');
      setRuntime(response);
      setIntervalInput(String(response.interval_minutes));
      setRuntimeError(null);
    } catch (err: any) {
      setRuntimeError(String(err?.message || 'Failed to load social runtime'));
    } finally {
      if (!quiet) setRuntimeLoading(false);
    }
  };

  useEffect(() => {
    void loadSources();
    void loadRuntime();
    const timer = window.setInterval(() => {
      void loadSources(true);
      void loadRuntime(true);
    }, 10000);
    return () => window.clearInterval(timer);
  }, []);

  const filtered = useMemo(() => {
    return items.filter((item) => {
      const query = search.trim().toLowerCase();
      const itemStatus = socialRowStatus(item);
      const matchesSearch =
        !query ||
        item.company_name.toLowerCase().includes(query) ||
        (item.display_url || '').toLowerCase().includes(query) ||
        (item.account_external_id || '').toLowerCase().includes(query);
      const matchesStatus = statusFilter === 'all' || itemStatus === statusFilter;
      return matchesSearch && matchesStatus;
    });
  }, [items, search, statusFilter]);

  const activeCount = items.filter((item) => item.is_active).length;
  const healthyCount = items.filter((item) => item.health_status === 'healthy').length;
  const failingCount = items.filter((item) => socialRowStatus(item) === 'error').length;
  const lastResult = runtime?.last_result;

  const saveSchedulerInterval = async () => {
    const parsed = Number(intervalInput);
    if (!Number.isFinite(parsed) || parsed < 15) {
      setRuntimeError(ru ? 'Интервал должен быть не меньше 15 минут' : 'Interval must be at least 15 minutes');
      return;
    }

    setRuntimeBusy(true);
    setRuntimeError(null);
    try {
      const response = await requestJson<SocialRuntimeStatus>('/api/social/runtime', {
        method: 'PATCH',
        body: JSON.stringify({ interval_minutes: Math.floor(parsed) }),
      });
      setRuntime(response);
      setIntervalInput(String(response.interval_minutes));
    } catch (err: any) {
      setRuntimeError(String(err?.message || 'Failed to update social scheduler interval'));
    } finally {
      setRuntimeBusy(false);
    }
  };

  const startRuntime = async () => {
    setRuntimeBusy(true);
    setRuntimeError(null);
    try {
      const response = await requestJson<SocialRuntimeStatus>('/api/social/runtime/start', { method: 'POST' });
      setRuntime(response);
    } catch (err: any) {
      setRuntimeError(String(err?.message || 'Failed to start social runtime'));
    } finally {
      setRuntimeBusy(false);
    }
  };

  const stopRuntime = async () => {
    setRuntimeBusy(true);
    setRuntimeError(null);
    try {
      const response = await requestJson<SocialRuntimeStatus>('/api/social/runtime/stop', { method: 'POST' });
      setRuntime(response);
    } catch (err: any) {
      setRuntimeError(String(err?.message || 'Failed to stop social runtime'));
    } finally {
      setRuntimeBusy(false);
    }
  };

  const runNow = async () => {
    setRuntimeBusy(true);
    setRuntimeError(null);
    try {
      const response = await requestJson<SocialRuntimeStatus>('/api/social/runtime/run-once', { method: 'POST' });
      setRuntime(response);
      window.setTimeout(() => {
        void loadRuntime(true);
        void loadSources(true);
      }, 1200);
    } catch (err: any) {
      setRuntimeError(String(err?.message || 'Failed to run social runtime'));
    } finally {
      setRuntimeBusy(false);
    }
  };

  const handleAddSource = async (payload: { url: string }) => {
    setBusy(true);
    setError(null);
    try {
      await requestJson<SocialSourceCreateResponse>('/api/sources/social/facebook', {
        method: 'POST',
        body: JSON.stringify(payload),
      });
      await loadSources(true);
    } finally {
      setBusy(false);
    }
  };

  const setSourceActive = async (id: string, isActive: boolean) => {
    setBusy(true);
    setError(null);
    try {
      await requestJson<SocialSourceUpdateResponse>(`/api/sources/social/${id}`, {
        method: 'PATCH',
        body: JSON.stringify({ is_active: isActive }),
      });
      await loadSources(true);
    } catch (err: any) {
      setError(String(err?.message || 'Update failed'));
    } finally {
      setBusy(false);
    }
  };

  return (
    <>
      {error && (
        <div className="mb-4 flex items-center gap-2 rounded-xl border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
          <AlertCircle className="w-4 h-4" />
          <span>{error}</span>
        </div>
      )}

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        <div className="bg-white rounded-xl border border-gray-200 p-4">
          <div className="flex items-center gap-2 mb-1">
            <Database className="w-4 h-4 text-blue-600" />
            <span className="text-xs text-gray-500">{ru ? 'Всего источников' : 'Total Sources'}</span>
          </div>
          <span className="text-xl text-gray-900" style={{ fontWeight: 600 }}>{items.length}</span>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-4">
          <div className="flex items-center gap-2 mb-1">
            <CheckCircle2 className="w-4 h-4 text-emerald-600" />
            <span className="text-xs text-gray-500">{ru ? 'Активных' : 'Active'}</span>
          </div>
          <span className="text-xl text-gray-900" style={{ fontWeight: 600 }}>{activeCount}</span>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-4">
          <div className="flex items-center gap-2 mb-1">
            <Brain className="w-4 h-4 text-violet-600" />
            <span className="text-xs text-gray-500">{ru ? 'Здоровых' : 'Healthy'}</span>
          </div>
          <span className="text-xl text-gray-900" style={{ fontWeight: 600 }}>{healthyCount}</span>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-4">
          <div className="flex items-center gap-2 mb-1">
            <AlertCircle className="w-4 h-4 text-amber-600" />
            <span className="text-xs text-gray-500">{ru ? 'С ошибками' : 'Failing'}</span>
          </div>
          <span className="text-xl text-gray-900" style={{ fontWeight: 600 }}>{failingCount}</span>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 p-4 md:p-5 mb-6">
        <div className="flex items-start justify-between gap-4 flex-wrap">
          <div>
            <h2 className="text-gray-900" style={{ fontSize: '1rem', fontWeight: 600 }}>
              {ru ? 'Social scheduler' : 'Social Scheduler'}
            </h2>
            <p className="text-xs text-gray-500 mt-1">
              {ru
                ? 'Управление отдельным social worker для сбора, AI-анализа и графовой синхронизации'
                : 'Control the dedicated social worker for collection, AI analysis, and graph sync'}
            </p>
          </div>
          <div className={`inline-flex items-center gap-2 px-2.5 py-1 rounded-full text-xs ${runtime?.is_active ? 'bg-emerald-50 text-emerald-700' : 'bg-gray-100 text-gray-600'}`} style={{ fontWeight: 500 }}>
            <span className={`w-2 h-2 rounded-full ${runtime?.is_active ? 'bg-emerald-500' : 'bg-gray-400'}`} />
            {runtime?.is_active ? (ru ? 'Активен' : 'Active') : (ru ? 'Остановлен' : 'Stopped')}
            {runtime?.running_now ? ` · ${ru ? 'идет запуск' : 'running'}` : ''}
          </div>
        </div>

        {runtimeError && (
          <div className="mt-3 flex items-center gap-2 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
            <AlertCircle className="w-3.5 h-3.5" />
            <span>{runtimeError}</span>
          </div>
        )}

        <div className="mt-4 grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div className="border border-gray-100 rounded-lg p-3 bg-gray-50/50">
            <label className="text-xs text-gray-500 block mb-1.5" style={{ fontWeight: 500 }}>
              {ru ? 'Интервал запуска (минуты)' : 'Run interval (minutes)'}
            </label>
            <div className="flex items-center gap-2">
              <input
                type="number"
                min={15}
                value={intervalInput}
                onChange={(event) => setIntervalInput(event.target.value)}
                className="w-28 px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <button
                onClick={saveSchedulerInterval}
                disabled={busy || runtimeBusy || runtimeLoading}
                className="px-3 py-2 rounded-lg text-xs text-white bg-slate-800 hover:bg-slate-700 disabled:opacity-50 transition-colors"
                style={{ fontWeight: 500 }}
              >
                {ru ? 'Сохранить' : 'Save'}
              </button>
            </div>
            <p className="mt-2 text-xs text-gray-400">
              {runtime?.next_run_at
                ? (ru ? `Следующий запуск: ${formatDateTime(runtime.next_run_at, ru)}` : `Next run: ${formatDateTime(runtime.next_run_at, ru)}`)
                : (ru ? 'Следующий запуск появится после активации' : 'Next run appears after activation')}
            </p>
          </div>

          <div className="border border-gray-100 rounded-lg p-3 bg-gray-50/50">
            <span className="text-xs text-gray-500 block mb-1.5" style={{ fontWeight: 500 }}>
              {ru ? 'Управление' : 'Controls'}
            </span>
            <div className="flex flex-wrap items-center gap-2">
              <button
                onClick={runtime?.is_active ? stopRuntime : startRuntime}
                disabled={busy || runtimeBusy || runtimeLoading}
                className={`px-3 py-2 rounded-lg text-xs transition-colors disabled:opacity-50 ${
                  runtime?.is_active ? 'bg-amber-50 text-amber-700 hover:bg-amber-100' : 'bg-emerald-50 text-emerald-700 hover:bg-emerald-100'
                }`}
                style={{ fontWeight: 500 }}
              >
                {runtime?.is_active ? (ru ? 'Остановить' : 'Stop') : (ru ? 'Запустить' : 'Start')}
              </button>
              <button
                onClick={runNow}
                disabled={busy || runtimeBusy || runtimeLoading}
                className="px-3 py-2 rounded-lg text-xs text-blue-700 bg-blue-50 hover:bg-blue-100 disabled:opacity-50 transition-colors"
                style={{ fontWeight: 500 }}
              >
                {ru ? 'Запустить сейчас' : 'Run now'}
              </button>
            </div>
            <p className="mt-2 text-xs text-gray-400">
              {lastResultLabel(lastResult, runtime, ru)}
            </p>
          </div>
        </div>

        <div className="mt-4 grid grid-cols-1 md:grid-cols-3 gap-3">
          <div className="rounded-lg border border-gray-100 bg-gray-50/70 p-3">
            <div className="flex items-center gap-2 text-xs text-gray-500">
              <Database className="w-3.5 h-3.5 text-blue-500" />
              {ru ? 'Сбор' : 'Collect'}
            </div>
            <div className="mt-2 text-lg text-gray-900" style={{ fontWeight: 600 }}>
              {lastResult?.activities_collected ?? 0}
            </div>
            <div className="text-xs text-gray-400">{ru ? 'Активностей за последний цикл' : 'Activities in the last cycle'}</div>
          </div>
          <div className="rounded-lg border border-gray-100 bg-gray-50/70 p-3">
            <div className="flex items-center gap-2 text-xs text-gray-500">
              <Brain className="w-3.5 h-3.5 text-violet-500" />
              {ru ? 'AI анализ' : 'AI Analysis'}
            </div>
            <div className="mt-2 text-lg text-gray-900" style={{ fontWeight: 600 }}>
              {lastResult?.activities_analyzed ?? 0}
            </div>
            <div className="text-xs text-gray-400">{ru ? 'Проанализировано за цикл' : 'Analyzed in the last cycle'}</div>
          </div>
          <div className="rounded-lg border border-gray-100 bg-gray-50/70 p-3">
            <div className="flex items-center gap-2 text-xs text-gray-500">
              <Network className="w-3.5 h-3.5 text-emerald-500" />
              {ru ? 'Граф' : 'Graph'}
            </div>
            <div className="mt-2 text-lg text-gray-900" style={{ fontWeight: 600 }}>
              {lastResult?.activities_graph_synced ?? 0}
            </div>
            <div className="text-xs text-gray-400">{ru ? 'Синхронизировано в Neo4j' : 'Synced to Neo4j in the last cycle'}</div>
          </div>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <div className="p-4 border-b border-gray-100">
          <div className="flex items-center justify-between flex-wrap gap-3">
            <div className="relative flex-1 min-w-[220px]">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
              <input
                type="text"
                placeholder={ru ? 'Поиск по компании или ссылке' : 'Search by company or URL'}
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                className="w-full pl-10 pr-4 py-2.5 border border-gray-200 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
              />
            </div>
            <div className="flex items-center gap-2">
              {(['all', 'active', 'paused', 'error'] as const).map((value) => (
                <button
                  key={value}
                  onClick={() => setStatusFilter(value)}
                  className={`px-3 py-1.5 rounded-lg text-xs transition-colors ${
                    statusFilter === value ? 'bg-blue-50 text-blue-700' : 'bg-gray-100 text-gray-500 hover:text-gray-700'
                  }`}
                  style={{ fontWeight: 500 }}
                >
                  {value === 'all'
                    ? (ru ? 'Все' : 'All')
                    : value === 'active'
                      ? (ru ? 'Активные' : 'Active')
                      : value === 'paused'
                        ? (ru ? 'Пауза' : 'Paused')
                        : (ru ? 'Ошибка' : 'Error')}
                </button>
              ))}
            </div>
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full min-w-[720px]">
            <thead>
              <tr className="border-b border-gray-100 bg-gray-50/50">
                <th className="text-left text-xs text-gray-500 px-4 py-3" style={{ fontWeight: 500 }}>
                  {ru ? 'Источник' : 'Source'}
                </th>
                <th className="text-left text-xs text-gray-500 px-3 py-3" style={{ fontWeight: 500 }}>
                  {ru ? 'Платформа' : 'Platform'}
                </th>
                <th className="text-center text-xs text-gray-500 px-3 py-3" style={{ fontWeight: 500 }}>
                  {ru ? 'Статус' : 'Status'}
                </th>
                <th className="text-right text-xs text-gray-500 px-3 py-3" style={{ fontWeight: 500 }}>
                  {ru ? 'Собрано' : 'Collected'}
                </th>
                <th className="text-right text-xs text-gray-500 px-3 py-3 hidden lg:table-cell" style={{ fontWeight: 500 }}>
                  {ru ? 'Последняя ошибка' : 'Last error'}
                </th>
                <th className="w-10 px-3 py-3" />
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={6} className="text-center py-16">
                    <RefreshCw className="w-6 h-6 text-gray-300 mx-auto mb-2 animate-spin" />
                    <p className="text-sm text-gray-500">{ru ? 'Загрузка social источников...' : 'Loading social sources...'}</p>
                  </td>
                </tr>
              ) : filtered.length === 0 ? (
                <tr>
                  <td colSpan={6} className="text-center py-16">
                    <Database className="w-8 h-8 text-gray-200 mx-auto mb-2" />
                    <p className="text-sm text-gray-500">
                      {search
                        ? (ru ? 'Ничего не найдено' : 'No sources found')
                        : (ru ? 'Нет добавленных social источников' : 'No social sources added yet')}
                    </p>
                  </td>
                </tr>
              ) : (
                filtered.map((item) => (
                  <tr key={item.id} className="border-b border-gray-50 transition-colors hover:bg-gray-50">
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-3">
                        <PlatformBadge platform={item.platform} />
                        <div className="min-w-0">
                          <span className="text-sm text-gray-900 block truncate" style={{ fontWeight: 500 }}>
                            {item.company_name}
                          </span>
                          <span className="text-xs text-gray-400 block truncate">
                            {item.display_url || item.account_external_id || '—'}
                          </span>
                        </div>
                      </div>
                    </td>
                    <td className="px-3 py-3">
                      <span className="text-sm text-gray-700 capitalize">{item.platform}</span>
                    </td>
                    <td className="px-3 py-3 text-center">
                      <SocialStatusBadge status={socialRowStatus(item)} ru={ru} />
                    </td>
                    <td className="px-3 py-3 text-right">
                      <div className="flex items-center justify-end gap-1.5 text-xs text-gray-400">
                        <RefreshCw className={`w-3 h-3 ${item.last_collected_at ? 'text-emerald-400' : 'text-gray-300'}`} />
                        <span>{relativeTime(item.last_collected_at, ru)}</span>
                      </div>
                    </td>
                    <td className="px-3 py-3 text-right hidden lg:table-cell">
                      <span className="text-xs text-gray-400 block max-w-[240px] ml-auto truncate">
                        {item.last_error || '—'}
                      </span>
                    </td>
                    <td className="px-3 py-3">
                      <SocialRowActions item={item} ru={ru} disabled={busy} onToggleActive={setSourceActive} />
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        <div className="flex items-center justify-between px-4 py-3 border-t border-gray-100 bg-gray-50/50">
          <span className="text-xs text-gray-400">
            {ru ? `Показано ${filtered.length} из ${items.length} social источников` : `Showing ${filtered.length} of ${items.length} social sources`}
          </span>
          <span className="text-xs text-gray-400 flex items-center gap-1.5">
            <RefreshCw className="w-3 h-3" />
            {ru ? 'Обновление каждые 10 сек' : 'Refresh every 10 sec'}
          </span>
        </div>
      </div>

      <AddFacebookSourceModal
        open={addModalOpen}
        ru={ru}
        onClose={onCloseAddModal}
        onSubmit={handleAddSource}
      />
    </>
  );
}
