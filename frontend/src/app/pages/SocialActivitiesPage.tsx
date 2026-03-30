import { useEffect, useMemo, useState } from 'react';
import {
  AlertTriangle,
  CheckCircle2,
  Clock3,
  Facebook,
  Globe2,
  Instagram,
  PauseCircle,
  RefreshCw,
  Sparkles,
} from 'lucide-react';
import { useLanguage } from '../contexts/LanguageContext';
import { apiFetch } from '../services/api';

type SocialAccount = {
  id?: string;
  platform: 'facebook' | 'instagram' | 'google' | 'tiktok';
  account_handle: string | null;
  account_external_id: string | null;
  domain: string | null;
  is_active: boolean;
  health_status?: string | null;
  last_health_error?: string | null;
  last_health_checked_at?: string | null;
  last_collected_at?: string | null;
};

type SocialEntity = {
  id: string;
  legacy_company_id: string;
  name: string;
  industry: string | null;
  website: string | null;
  is_active: boolean;
  platform_accounts: Record<string, SocialAccount | null>;
  accounts: SocialAccount[];
};

type SocialAnalysis = {
  summary?: string | null;
  marketing_intent?: string | null;
  sentiment?: string | null;
  sentiment_score?: number | null;
  analysis_payload?: {
    summary?: string | null;
    topics?: Array<string | { name?: string }>;
  } | null;
};

type SocialActivity = {
  id: string;
  activity_uid: string;
  platform: string;
  source_kind: string;
  source_url: string;
  text_content: string | null;
  published_at: string | null;
  author_handle: string | null;
  cta_type: string | null;
  content_format: string | null;
  region_name: string | null;
  analysis_status: string;
  graph_status: string;
  entity?: {
    id: string;
    name: string;
  } | null;
  analysis?: SocialAnalysis | null;
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
    activities_collected?: number;
    activities_analyzed?: number;
    activities_graph_synced?: number;
  } | null;
  runtime_enabled?: boolean;
  postgres_worker_enabled?: boolean;
};

type SocialOverview = {
  entities_total: number;
  entities_active: number;
  activities_total: number;
  platform_counts: Record<string, number>;
  analysis_status_counts: Record<string, number>;
  account_health_counts?: Record<string, number>;
  queue_depth?: {
    analysis?: number;
    graph?: number;
  };
  dead_letter_failures: number;
  stale_entities: Array<{ entity_id: string; name: string; reason: string; age_hours?: number }>;
  runtime: SocialRuntimeStatus;
};

type SocialFailure = {
  id: string;
  stage: string;
  scope_key: string;
  last_error: string | null;
  is_dead_letter: boolean;
  last_failed_at: string;
};

function platformLabel(platform: string, ru: boolean) {
  if (platform === 'facebook') return 'Facebook Ads';
  if (platform === 'instagram') return 'Instagram';
  if (platform === 'google') return 'Google Ads';
  if (platform === 'tiktok') return 'TikTok';
  return ru ? 'Другое' : 'Other';
}

function platformIcon(platform: string) {
  if (platform === 'facebook') return Facebook;
  if (platform === 'instagram') return Instagram;
  return Globe2;
}

function relativeTime(iso: string | null, ru: boolean): string {
  if (!iso) return ru ? 'Никогда' : 'Never';
  const date = new Date(iso);
  const diffMs = Date.now() - date.getTime();
  if (!Number.isFinite(diffMs)) return '—';
  const minutes = Math.floor(diffMs / 60000);
  if (minutes < 1) return ru ? 'Только что' : 'Just now';
  if (minutes < 60) return ru ? `${minutes} мин назад` : `${minutes} min ago`;
  const hours = Math.floor(minutes / 60);
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

function topicNames(activity: SocialActivity): string[] {
  const topics = activity.analysis?.analysis_payload?.topics || [];
  return topics
    .map((topic) => (typeof topic === 'string' ? topic : topic?.name || ''))
    .filter(Boolean)
    .slice(0, 4);
}

function accountHealthLabel(status: string | null | undefined, ru: boolean): string {
  switch (status) {
    case 'healthy':
      return ru ? 'Исправен' : 'Healthy';
    case 'invalid_identifier':
      return ru ? 'Неверный ID' : 'Invalid identifier';
    case 'provider_404':
      return ru ? 'Источник не найден' : 'Provider 404';
    case 'rate_limited':
      return 'Rate limited';
    case 'auth_error':
      return ru ? 'Ошибка доступа' : 'Auth error';
    case 'network_error':
      return ru ? 'Сетевая ошибка' : 'Network error';
    default:
      return ru ? 'Не проверено' : 'Unchecked';
  }
}

function accountHealthTone(status: string | null | undefined): string {
  if (status === 'healthy') return 'bg-emerald-100 text-emerald-700';
  if (status === 'unknown' || !status) return 'bg-slate-200 text-slate-600';
  return 'bg-amber-100 text-amber-700';
}

function describeAvailabilityError(message: string, ru: boolean): string {
  if (message.includes('503')) {
    return ru
      ? 'Social data не подключены. Проверьте SOCIAL_* переменные и доступ к отдельным Social Supabase/Neo4j.'
      : 'Social data is not connected. Check the SOCIAL_* variables and connectivity to the separate Social Supabase/Neo4j stores.';
  }
  return message;
}

export function SocialActivitiesPage() {
  const { lang } = useLanguage();
  const ru = lang === 'ru';
  const [overview, setOverview] = useState<SocialOverview | null>(null);
  const [entities, setEntities] = useState<SocialEntity[]>([]);
  const [activities, setActivities] = useState<SocialActivity[]>([]);
  const [failures, setFailures] = useState<SocialFailure[]>([]);
  const [selectedActivityId, setSelectedActivityId] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [isUnavailable, setIsUnavailable] = useState(false);

  async function loadData(showSpinner: boolean) {
    if (showSpinner) {
      setLoading(true);
    } else {
      setRefreshing(true);
    }
    setError(null);
    setIsUnavailable(false);
    try {
      const [overviewPayload, entitiesPayload, activitiesPayload, failuresPayload] = await Promise.all([
        apiFetch<SocialOverview>('/social/overview'),
        apiFetch<{ items: SocialEntity[] }>('/social/entities'),
        apiFetch<{ items: SocialActivity[] }>('/social/activities?limit=80'),
        apiFetch<{ items: SocialFailure[] }>('/social/runtime/failures?dead_letter_only=true&limit=20'),
      ]);
      setOverview(overviewPayload);
      setEntities(entitiesPayload.items);
      setActivities(activitiesPayload.items);
      setFailures(failuresPayload.items);
      setSelectedActivityId((current) => current || activitiesPayload.items[0]?.id || null);
    } catch (err: any) {
      const message = err?.message || (ru ? 'Не удалось загрузить Social Media.' : 'Failed to load Social Media.');
      setIsUnavailable(message.includes('503'));
      setError(describeAvailabilityError(message, ru));
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }

  useEffect(() => {
    void loadData(true);
  }, []);

  const selectedActivity = useMemo(
    () => activities.find((item) => item.id === selectedActivityId) || activities[0] || null,
    [activities, selectedActivityId],
  );
  const hasNoData = !isUnavailable && !loading && entities.length === 0 && activities.length === 0;

  if (loading) {
    return (
      <div className="min-h-[calc(100dvh-6rem)] bg-slate-50 p-6">
        <div className="mx-auto max-w-7xl animate-pulse space-y-4">
          <div className="h-14 rounded-2xl bg-white shadow-sm" />
          <div className="grid gap-4 md:grid-cols-4">
            {Array.from({ length: 4 }).map((_, index) => (
              <div key={index} className="h-28 rounded-2xl bg-white shadow-sm" />
            ))}
          </div>
          <div className="grid gap-4 lg:grid-cols-[1.05fr_0.95fr]">
            <div className="h-[34rem] rounded-2xl bg-white shadow-sm" />
            <div className="h-[34rem] rounded-2xl bg-white shadow-sm" />
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-[calc(100dvh-6rem)] bg-slate-50 p-4 md:p-6">
      <div className="mx-auto max-w-7xl space-y-6">
        <section className="overflow-hidden rounded-[28px] border border-slate-200 bg-white shadow-sm">
          <div className="flex flex-col gap-5 border-b border-slate-200 px-6 py-6 md:flex-row md:items-center md:justify-between">
            <div className="space-y-2">
              <div className="inline-flex items-center gap-2 rounded-full border border-blue-100 bg-blue-50 px-3 py-1 text-xs font-medium text-blue-700">
                <Sparkles className="h-3.5 w-3.5" />
                {ru ? 'Social Media' : 'Social Media'}
              </div>
              <div>
                <h1 className="text-2xl font-semibold tracking-tight text-slate-900">
                  {ru ? 'Социальные медиа' : 'Social Media intelligence'}
                </h1>
                <p className="mt-1 max-w-3xl text-sm text-slate-600">
                  {ru
                    ? 'Отдельный read-only контур для Social Supabase и Social Neo4j, встроенный в текущую платформу без изменения Release A логики.'
                    : 'A separate read-only Social Supabase and Social Neo4j subsystem, integrated into the current platform without changing Release A behavior.'}
                </p>
              </div>
            </div>
            <div className="flex flex-wrap items-center gap-3">
              <button
                type="button"
                onClick={() => void loadData(false)}
                disabled={refreshing}
                className="inline-flex items-center gap-2 rounded-xl border border-slate-200 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-60"
              >
                <RefreshCw className={`h-4 w-4 ${refreshing ? 'animate-spin' : ''}`} />
                {ru ? 'Обновить' : 'Refresh'}
              </button>
              <button
                type="button"
                disabled
                className="inline-flex cursor-not-allowed items-center gap-2 rounded-xl bg-slate-200 px-4 py-2 text-sm font-medium text-slate-500"
                title={ru ? 'Runtime отключен для первого read-only релиза.' : 'Runtime is disabled for the first read-only rollout.'}
              >
                <PauseCircle className="h-4 w-4" />
                {ru ? 'Runtime отключен' : 'Runtime disabled'}
              </button>
            </div>
          </div>

          {error ? (
            <div className={`mx-6 mt-4 flex items-start gap-3 rounded-2xl px-4 py-3 text-sm ${isUnavailable ? 'border border-amber-200 bg-amber-50 text-amber-800' : 'border border-red-200 bg-red-50 text-red-700'}`}>
              <AlertTriangle className="mt-0.5 h-4 w-4 flex-shrink-0" />
              <span>{error}</span>
            </div>
          ) : null}

          {hasNoData ? (
            <div className="mx-6 mt-4 rounded-2xl border border-slate-200 bg-slate-50 px-5 py-4 text-sm text-slate-700">
              {ru
                ? 'Пока нет Social data. Сначала настройте social entities и platform identifiers, чтобы начать tracking.'
                : 'No social data yet. Configure entities and platform identifiers to begin tracking.'}
            </div>
          ) : null}

          <div className="grid gap-4 px-6 py-6 md:grid-cols-4">
            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <p className="text-xs uppercase tracking-[0.2em] text-slate-500">{ru ? 'Компании' : 'Entities'}</p>
              <p className="mt-3 text-3xl font-semibold text-slate-900">{overview?.entities_active ?? 0}</p>
              <p className="mt-1 text-sm text-slate-600">
                {ru ? `Активно из ${overview?.entities_total ?? 0}` : `Active out of ${overview?.entities_total ?? 0}`}
              </p>
            </div>
            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <p className="text-xs uppercase tracking-[0.2em] text-slate-500">{ru ? 'Активности' : 'Activities'}</p>
              <p className="mt-3 text-3xl font-semibold text-slate-900">{overview?.activities_total ?? 0}</p>
              <p className="mt-1 text-sm text-slate-600">
                {ru ? 'Отдельный Social evidence поток' : 'Separate Social evidence stream'}
              </p>
            </div>
            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <p className="text-xs uppercase tracking-[0.2em] text-slate-500">{ru ? 'Runtime' : 'Runtime'}</p>
              <div className="mt-3 flex items-center gap-2 text-slate-900">
                {overview?.runtime?.runtime_enabled ? (
                  overview?.runtime?.last_error ? (
                    <AlertTriangle className="h-4 w-4 text-red-600" />
                  ) : (
                    <CheckCircle2 className="h-4 w-4 text-emerald-600" />
                  )
                ) : (
                  <PauseCircle className="h-4 w-4 text-slate-500" />
                )}
                <span className="text-lg font-semibold">
                  {!overview?.runtime?.runtime_enabled
                    ? (ru ? 'Отключен' : 'Disabled')
                    : overview?.runtime?.last_error
                      ? (ru ? 'Ошибка' : 'Attention')
                      : (ru ? 'Готов' : 'Healthy')}
                </span>
              </div>
              <p className="mt-1 text-sm text-slate-600">
                {ru ? 'Последний успех:' : 'Last success:'} {relativeTime(overview?.runtime?.last_success_at || null, ru)}
              </p>
            </div>
            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <p className="text-xs uppercase tracking-[0.2em] text-slate-500">{ru ? 'Dead letters' : 'Dead letters'}</p>
              <p className="mt-3 text-3xl font-semibold text-slate-900">{overview?.dead_letter_failures ?? 0}</p>
              <p className="mt-1 text-sm text-slate-600">
                {ru ? 'Требуют ручной проверки' : 'Require manual review'}
              </p>
            </div>
          </div>
        </section>

        <section className="grid gap-6 lg:grid-cols-[1.05fr_0.95fr]">
          <div className="space-y-6">
            <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
              <div>
                <h2 className="text-lg font-semibold text-slate-900">
                  {ru ? 'Компании и платформы' : 'Tracked entities and platform identifiers'}
                </h2>
                <p className="mt-1 text-sm text-slate-600">
                  {ru
                    ? 'На первом этапе это read-only просмотр отдельного Social data plane. Управление runtime и запись будут включаться позже.'
                    : 'This first phase is a read-only view into the separate Social data plane. Runtime controls and writes will be enabled later.'}
                </p>
              </div>

              <div className="mt-5 space-y-4">
                {entities.map((entity) => (
                  <div key={entity.id} className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                    <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                      <div>
                        <div className="flex items-center gap-2">
                          <h3 className="text-base font-semibold text-slate-900">{entity.name}</h3>
                          <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${entity.is_active ? 'bg-emerald-100 text-emerald-700' : 'bg-slate-200 text-slate-600'}`}>
                            {entity.is_active ? (ru ? 'Активен' : 'Active') : (ru ? 'Выключен' : 'Inactive')}
                          </span>
                        </div>
                        <p className="mt-1 text-sm text-slate-600">
                          {[entity.industry, entity.website].filter(Boolean).join(' • ') || (ru ? 'Нет дополнительной информации' : 'No extra metadata')}
                        </p>
                      </div>
                      <div className="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-medium text-slate-600">
                        {ru ? 'Read-only' : 'Read-only'}
                      </div>
                    </div>

                    <div className="mt-4 grid gap-3 md:grid-cols-3">
                      {(['facebook', 'instagram', 'google'] as const).map((platform) => {
                        const Icon = platformIcon(platform);
                        const account = entity.platform_accounts[platform];
                        const value = account?.account_external_id || account?.account_handle || account?.domain || '—';
                        return (
                          <div key={platform} className="rounded-2xl border border-slate-200 bg-white p-3">
                            <div className="mb-2 flex items-start justify-between gap-2">
                              <div className="flex items-center gap-2 text-sm font-medium text-slate-800">
                                <Icon className="h-4 w-4 text-slate-500" />
                                {platformLabel(platform, ru)}
                              </div>
                              <span className={`inline-flex rounded-full px-2 py-0.5 text-[11px] font-medium ${accountHealthTone(account?.health_status)}`}>
                                {accountHealthLabel(account?.health_status, ru)}
                              </span>
                            </div>
                            <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-800">
                              {value}
                            </div>
                            <div className="mt-2 space-y-1 text-xs text-slate-500">
                              <div>{ru ? 'Last collected:' : 'Last collected:'} {relativeTime(account?.last_collected_at || null, ru)}</div>
                              {account?.last_health_error ? (
                                <div className="line-clamp-2 text-amber-700">{account.last_health_error}</div>
                              ) : null}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
              <div className="flex items-center gap-2">
                <Clock3 className="h-5 w-5 text-slate-500" />
                <h2 className="text-lg font-semibold text-slate-900">
                  {ru ? 'Runtime и риски' : 'Runtime and risk summary'}
                </h2>
              </div>
              <div className="mt-4 grid gap-4 md:grid-cols-2">
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <p className="text-sm font-medium text-slate-900">{ru ? 'Текущее состояние runtime' : 'Runtime posture'}</p>
                  <dl className="mt-3 space-y-2 text-sm text-slate-600">
                    <div className="flex items-center justify-between">
                      <dt>{ru ? 'Режим' : 'Mode'}</dt>
                      <dd>{overview?.runtime?.runtime_enabled ? (ru ? 'Включен' : 'Enabled') : (ru ? 'Отключен' : 'Disabled')}</dd>
                    </div>
                    <div className="flex items-center justify-between">
                      <dt>{ru ? 'Следующий run' : 'Next run'}</dt>
                      <dd>{formatDateTime(overview?.runtime?.next_run_at || null, ru)}</dd>
                    </div>
                    <div className="flex items-center justify-between">
                      <dt>{ru ? 'Последний success' : 'Last success'}</dt>
                      <dd>{formatDateTime(overview?.runtime?.last_success_at || null, ru)}</dd>
                    </div>
                    <div className="flex items-center justify-between">
                      <dt>{ru ? 'Postgres worker' : 'Postgres worker'}</dt>
                      <dd>{overview?.runtime?.postgres_worker_enabled ? 'ON' : 'OFF'}</dd>
                    </div>
                  </dl>
                </div>
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <p className="text-sm font-medium text-slate-900">{ru ? 'Риски и сигналы' : 'Risk and warning summary'}</p>
                  <div className="mt-3 space-y-3">
                    {overview?.queue_depth ? (
                      <div className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700">
                        {ru ? 'Очереди:' : 'Queues:'} {overview.queue_depth.analysis ?? 0} analysis / {overview.queue_depth.graph ?? 0} graph
                      </div>
                    ) : null}
                    {(overview?.stale_entities || []).slice(0, 3).map((item) => (
                      <div key={item.entity_id} className="rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
                        <div className="font-medium">{item.name}</div>
                        <div className="text-xs">
                          {item.reason === 'never_collected'
                            ? (ru ? 'Ни разу не собирался' : 'No successful collection yet')
                            : (ru ? `Не обновлялся ${item.age_hours} ч` : `Not refreshed for ${item.age_hours}h`)}
                        </div>
                      </div>
                    ))}
                    {failures.slice(0, 2).map((failure) => (
                      <div key={failure.id} className="rounded-xl border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800">
                        <div className="font-medium">{failure.stage}</div>
                        <div className="mt-1 line-clamp-2 text-xs">{failure.last_error || failure.scope_key}</div>
                      </div>
                    ))}
                    {!overview?.stale_entities?.length && !failures.length ? (
                      <div className="rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800">
                        {ru ? 'Критичных social runtime сигналов сейчас нет.' : 'No critical social runtime warnings right now.'}
                      </div>
                    ) : null}
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div className="space-y-6">
            <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
              <div className="flex items-center gap-2">
                <Sparkles className="h-5 w-5 text-slate-500" />
                <h2 className="text-lg font-semibold text-slate-900">
                  {ru ? 'Evidence feed' : 'Evidence feed'}
                </h2>
              </div>
              <div className="mt-4 space-y-3">
                {activities.map((activity) => {
                  const active = selectedActivity?.id === activity.id;
                  const Icon = platformIcon(activity.platform);
                  return (
                    <button
                      key={activity.id}
                      type="button"
                      onClick={() => setSelectedActivityId(activity.id)}
                      className={`w-full rounded-2xl border p-4 text-left transition ${active ? 'border-slate-900 bg-slate-900 text-white' : 'border-slate-200 bg-slate-50 hover:bg-white'}`}
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <div className={`inline-flex items-center gap-2 text-xs font-medium ${active ? 'text-slate-200' : 'text-slate-500'}`}>
                            <Icon className="h-3.5 w-3.5" />
                            {platformLabel(activity.platform, ru)}
                          </div>
                          <h3 className={`mt-2 text-sm font-semibold ${active ? 'text-white' : 'text-slate-900'}`}>
                            {activity.entity?.name || activity.author_handle || activity.activity_uid}
                          </h3>
                        </div>
                        <div className={`rounded-full px-2 py-0.5 text-[11px] font-medium ${active ? 'bg-white/15 text-white' : 'bg-white text-slate-600 ring-1 ring-slate-200'}`}>
                          {activity.analysis_status}
                        </div>
                      </div>
                      <p className={`mt-3 line-clamp-3 text-sm ${active ? 'text-slate-100' : 'text-slate-600'}`}>
                        {activity.analysis?.summary || activity.text_content || activity.source_url}
                      </p>
                      <div className={`mt-3 flex flex-wrap items-center gap-2 text-xs ${active ? 'text-slate-200' : 'text-slate-500'}`}>
                        <span>{relativeTime(activity.published_at, ru)}</span>
                        <span>•</span>
                        <span>{activity.graph_status}</span>
                        {activity.cta_type ? (
                          <>
                            <span>•</span>
                            <span>{activity.cta_type}</span>
                          </>
                        ) : null}
                      </div>
                    </button>
                  );
                })}
                {!activities.length ? (
                  <div className="rounded-2xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-500">
                    {ru ? 'Нет social activities для показа.' : 'No social activities to display yet.'}
                  </div>
                ) : null}
              </div>
            </div>

            <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
              <h2 className="text-lg font-semibold text-slate-900">
                {ru ? 'Выбранный сигнал' : 'Selected evidence'}
              </h2>
              {selectedActivity ? (
                <div className="mt-4 space-y-4">
                  <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <div className="text-xs uppercase tracking-[0.18em] text-slate-500">
                          {platformLabel(selectedActivity.platform, ru)}
                        </div>
                        <h3 className="mt-2 text-lg font-semibold text-slate-900">
                          {selectedActivity.entity?.name || selectedActivity.author_handle || selectedActivity.activity_uid}
                        </h3>
                        <p className="mt-2 text-sm text-slate-600">
                          {selectedActivity.analysis?.summary || selectedActivity.text_content || selectedActivity.source_url}
                        </p>
                      </div>
                      <a
                        href={selectedActivity.source_url}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-2 rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-100"
                      >
                        <Globe2 className="h-4 w-4" />
                        {ru ? 'Открыть источник' : 'Open source'}
                      </a>
                    </div>
                  </div>

                  <div className="grid gap-4 md:grid-cols-2">
                    <div className="rounded-2xl border border-slate-200 bg-white p-4">
                      <p className="text-xs uppercase tracking-[0.18em] text-slate-500">{ru ? 'Статусы' : 'Statuses'}</p>
                      <dl className="mt-3 space-y-2 text-sm text-slate-700">
                        <div className="flex items-center justify-between">
                          <dt>{ru ? 'Analysis' : 'Analysis'}</dt>
                          <dd>{selectedActivity.analysis_status}</dd>
                        </div>
                        <div className="flex items-center justify-between">
                          <dt>{ru ? 'Graph' : 'Graph'}</dt>
                          <dd>{selectedActivity.graph_status}</dd>
                        </div>
                        <div className="flex items-center justify-between">
                          <dt>{ru ? 'Опубликовано' : 'Published'}</dt>
                          <dd>{formatDateTime(selectedActivity.published_at, ru)}</dd>
                        </div>
                      </dl>
                    </div>
                    <div className="rounded-2xl border border-slate-200 bg-white p-4">
                      <p className="text-xs uppercase tracking-[0.18em] text-slate-500">{ru ? 'Темы' : 'Topics'}</p>
                      <div className="mt-3 flex flex-wrap gap-2">
                        {topicNames(selectedActivity).map((topic) => (
                          <span key={topic} className="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-700">
                            {topic}
                          </span>
                        ))}
                        {!topicNames(selectedActivity).length ? (
                          <span className="text-sm text-slate-500">{ru ? 'Темы пока не определены' : 'No topics detected yet'}</span>
                        ) : null}
                      </div>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="mt-4 rounded-2xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-500">
                  {ru ? 'Выберите activity слева, чтобы увидеть детали.' : 'Select an activity from the feed to inspect it here.'}
                </div>
              )}
            </div>
          </div>
        </section>
      </div>
    </div>
  );
}
