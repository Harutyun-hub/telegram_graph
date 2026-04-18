import { useEffect, useState } from 'react';
import {
  AlertTriangle,
  CheckCircle2,
  Clock3,
  Facebook,
  Globe2,
  Instagram,
  Play,
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
    marketing_intent?: string | null;
    topics?: Array<string | { name?: string }>;
    audience_segments?: Array<string | { name?: string }>;
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
  ingest_status: string;
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
    accounts_processed?: number;
    activities_collected?: number;
    activities_analyzed?: number;
    activities_graph_synced?: number;
    collect_failures?: number;
    analysis_failures?: number;
    graph_failures?: number;
  } | null;
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

type EntityDraft = {
  is_active: boolean;
  facebook_page_id: string;
  instagram_username: string;
  google_ads_domain: string;
};

const PLATFORM_ORDER: Array<'facebook' | 'instagram' | 'google'> = ['facebook', 'instagram', 'google'];

function platformLabel(platform: string, ru: boolean) {
  if (platform === 'facebook') return ru ? 'Facebook Ads' : 'Facebook Ads';
  if (platform === 'instagram') return ru ? 'Instagram' : 'Instagram';
  if (platform === 'google') return ru ? 'Google Ads' : 'Google Ads';
  if (platform === 'tiktok') return ru ? 'TikTok' : 'TikTok';
  return platform;
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
  if (!Number.isFinite(diffMs)) return ru ? '—' : '—';
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

function activitySummary(activity: SocialActivity) {
  return (
    activity.analysis?.analysis_payload?.summary ||
    activity.analysis?.summary ||
    activity.text_content ||
    ''
  );
}

function topicNames(activity: SocialActivity): string[] {
  const topics = activity.analysis?.analysis_payload?.topics || [];
  return topics
    .map((topic) => (typeof topic === 'string' ? topic : topic?.name || ''))
    .filter(Boolean)
    .slice(0, 4);
}

function buildDraft(entity: SocialEntity): EntityDraft {
  return {
    is_active: entity.is_active,
    facebook_page_id: entity.platform_accounts.facebook?.account_external_id || '',
    instagram_username: entity.platform_accounts.instagram?.account_handle || '',
    google_ads_domain: entity.platform_accounts.google?.domain || '',
  };
}

function accountHealthLabel(status: string | null | undefined, ru: boolean): string {
  switch (status) {
    case 'healthy':
      return ru ? 'Healthy' : 'Healthy';
    case 'invalid_identifier':
      return ru ? 'Неверный идентификатор' : 'Invalid identifier';
    case 'provider_404':
      return ru ? 'Источник не найден' : 'Provider 404';
    case 'rate_limited':
      return ru ? 'Rate limited' : 'Rate limited';
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

export function SocialActivitiesPage() {
  const { lang } = useLanguage();
  const ru = lang === 'ru';
  const [overview, setOverview] = useState<SocialOverview | null>(null);
  const [entities, setEntities] = useState<SocialEntity[]>([]);
  const [activities, setActivities] = useState<SocialActivity[]>([]);
  const [failures, setFailures] = useState<SocialFailure[]>([]);
  const [drafts, setDrafts] = useState<Record<string, EntityDraft>>({});
  const [selectedActivityId, setSelectedActivityId] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [running, setRunning] = useState(false);
  const [savingEntityId, setSavingEntityId] = useState<string | null>(null);
  const [retryingFailureId, setRetryingFailureId] = useState<string | null>(null);
  const [replayingStage, setReplayingStage] = useState<'analysis' | 'graph' | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function loadData(showSpinner: boolean) {
    if (showSpinner) {
      setLoading(true);
    } else {
      setRefreshing(true);
    }
    setError(null);
    try {
      const [overviewPayload, entitiesPayload, activitiesPayload, failuresPayload] = await Promise.all([
        apiFetch<SocialOverview>('/social/overview', { includeUserAuth: true }),
        apiFetch<{ items: SocialEntity[] }>('/social/entities', { includeUserAuth: true }),
        apiFetch<{ items: SocialActivity[] }>('/social/activities?limit=80', { includeUserAuth: true }),
        apiFetch<{ items: SocialFailure[] }>('/social/runtime/failures?dead_letter_only=true&limit=20', { includeUserAuth: true }),
      ]);
      setOverview(overviewPayload);
      setEntities(entitiesPayload.items);
      setActivities(activitiesPayload.items);
      setFailures(failuresPayload.items);
      setDrafts(
        Object.fromEntries(entitiesPayload.items.map((entity) => [entity.id, buildDraft(entity)])),
      );
      setSelectedActivityId((current) => current || activitiesPayload.items[0]?.id || null);
    } catch (err: any) {
      setError(err?.message || (ru ? 'Не удалось загрузить страницу.' : 'Failed to load social activities.'));
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }

  useEffect(() => {
    void loadData(true);
  }, []);

  const selectedActivity = activities.find((item) => item.id === selectedActivityId) || activities[0] || null;

  async function handleRunOnce() {
    setRunning(true);
    setError(null);
    try {
      await apiFetch('/social/runtime/run-once', {
        method: 'POST',
        includeUserAuth: true,
      });
      window.setTimeout(() => {
        void loadData(false);
      }, 1500);
    } catch (err: any) {
      setError(err?.message || (ru ? 'Не удалось запустить pipeline.' : 'Failed to trigger social runtime.'));
    } finally {
      setRunning(false);
    }
  }

  async function handleSaveEntity(entity: SocialEntity) {
    const draft = drafts[entity.id];
    if (!draft) return;
    setSavingEntityId(entity.id);
    setError(null);
    try {
      await apiFetch(`/social/entities/${entity.id}`, {
        method: 'PATCH',
        includeUserAuth: true,
        body: JSON.stringify({
          is_active: draft.is_active,
          accounts: [
            {
              platform: 'facebook',
              account_external_id: draft.facebook_page_id || null,
              is_active: draft.is_active && Boolean(draft.facebook_page_id),
            },
            {
              platform: 'instagram',
              account_handle: draft.instagram_username || null,
              is_active: draft.is_active && Boolean(draft.instagram_username),
            },
            {
              platform: 'google',
              domain: draft.google_ads_domain || null,
              is_active: draft.is_active && Boolean(draft.google_ads_domain),
            },
          ],
        }),
      });
      await loadData(false);
    } catch (err: any) {
      setError(err?.message || (ru ? 'Не удалось сохранить источник.' : 'Failed to save social source.'));
    } finally {
      setSavingEntityId(null);
    }
  }

  async function handleRetryFailure(failure: SocialFailure) {
    setRetryingFailureId(failure.id);
    setError(null);
    try {
      await apiFetch('/social/runtime/retry', {
        method: 'POST',
        includeUserAuth: true,
        body: JSON.stringify({
          stage: failure.stage,
          scope_key: failure.scope_key,
        }),
      });
      await loadData(false);
    } catch (err: any) {
      setError(err?.message || (ru ? 'Не удалось повторить ошибку runtime.' : 'Failed to retry runtime failure.'));
    } finally {
      setRetryingFailureId(null);
    }
  }

  async function handleReplaySelected(stage: 'analysis' | 'graph') {
    if (!selectedActivity) return;
    setReplayingStage(stage);
    setError(null);
    try {
      await apiFetch('/social/runtime/replay', {
        method: 'POST',
        includeUserAuth: true,
        body: JSON.stringify({
          stage,
          activity_uids: [selectedActivity.activity_uid],
        }),
      });
      await loadData(false);
    } catch (err: any) {
      setError(err?.message || (ru ? 'Не удалось запустить replay.' : 'Failed to replay selected activity.'));
    } finally {
      setReplayingStage(null);
    }
  }

  function updateDraft(entityId: string, patch: Partial<EntityDraft>) {
    setDrafts((current) => ({
      ...current,
      [entityId]: {
        ...(current[entityId] || {
          is_active: true,
          facebook_page_id: '',
          instagram_username: '',
          google_ads_domain: '',
        }),
        ...patch,
      },
    }));
  }

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
          <div className="grid gap-4 lg:grid-cols-[1.1fr_0.9fr]">
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
                {ru ? 'Social ops' : 'Social ops'}
              </div>
              <div>
                <h1 className="text-2xl font-semibold tracking-tight text-slate-900">
                  {ru ? 'Операторская панель Social' : 'Social operator panel'}
                </h1>
                <p className="mt-1 max-w-3xl text-sm text-slate-600">
                  {ru
                    ? 'Операторская панель для отдельного Social workflow: runtime, platform accounts, dead letters и evidence stream.'
                    : 'Operator view for the separate Social workflow: runtime, platform accounts, dead letters, and the evidence stream.'}
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
                onClick={handleRunOnce}
                disabled={running}
                className="inline-flex items-center gap-2 rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
              >
                <Play className="h-4 w-4" />
                {running ? (ru ? 'Запуск...' : 'Running...') : (ru ? 'Запустить один цикл' : 'Run once')}
              </button>
            </div>
          </div>

          {error ? (
            <div className="mx-6 mt-4 flex items-start gap-3 rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
              <AlertTriangle className="mt-0.5 h-4 w-4 flex-shrink-0" />
              <span>{error}</span>
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
                {ru ? 'Последние canonical rows' : 'Recent canonical rows'}
              </p>
            </div>
            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <p className="text-xs uppercase tracking-[0.2em] text-slate-500">{ru ? 'Runtime' : 'Runtime'}</p>
              <div className="mt-3 flex items-center gap-2 text-slate-900">
                {overview?.runtime?.running_now ? (
                  <RefreshCw className="h-4 w-4 animate-spin text-blue-600" />
                ) : overview?.runtime?.last_error ? (
                  <AlertTriangle className="h-4 w-4 text-red-600" />
                ) : (
                  <CheckCircle2 className="h-4 w-4 text-emerald-600" />
                )}
                <span className="text-lg font-semibold">
                  {overview?.runtime?.running_now
                    ? (ru ? 'Выполняется' : 'Running')
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
                {ru ? 'Элементы, требующие ручной проверки' : 'Items that need operator review'}
              </p>
            </div>
          </div>
        </section>

        <section className="grid gap-6 lg:grid-cols-[1.05fr_0.95fr]">
          <div className="space-y-6">
            <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
              <div className="flex items-center justify-between">
                <div>
                  <h2 className="text-lg font-semibold text-slate-900">
                    {ru ? 'Компании и платформы' : 'Tracked entities and platform identifiers'}
                  </h2>
                  <p className="mt-1 text-sm text-slate-600">
                    {ru
                      ? 'Компании синхронизируются из master registry, здесь вы управляете только social platform аккаунтами.'
                      : 'Companies are synced from the master registry; this page manages only social platform accounts.'}
                  </p>
                </div>
              </div>

              <div className="mt-5 space-y-4">
                {entities.map((entity) => {
                  const draft = drafts[entity.id] || buildDraft(entity);
                  return (
                    <div key={entity.id} className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                        <div>
                          <div className="flex items-center gap-2">
                            <h3 className="text-base font-semibold text-slate-900">{entity.name}</h3>
                            <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${draft.is_active ? 'bg-emerald-100 text-emerald-700' : 'bg-slate-200 text-slate-600'}`}>
                              {draft.is_active ? (ru ? 'Активен' : 'Active') : (ru ? 'Выключен' : 'Inactive')}
                            </span>
                          </div>
                          <p className="mt-1 text-sm text-slate-600">
                            {[entity.industry, entity.website].filter(Boolean).join(' • ') || (ru ? 'Нет дополнительной информации' : 'No extra metadata')}
                          </p>
                        </div>
                        <label className="inline-flex items-center gap-2 text-sm text-slate-700">
                          <input
                            type="checkbox"
                            checked={draft.is_active}
                            onChange={(event) => updateDraft(entity.id, { is_active: event.target.checked })}
                            className="h-4 w-4 rounded border-slate-300 text-slate-900 focus:ring-slate-500"
                          />
                          {ru ? 'Entity active' : 'Entity active'}
                        </label>
                      </div>

                      <div className="mt-4 grid gap-3 md:grid-cols-3">
                        {PLATFORM_ORDER.map((platform) => {
                          const Icon = platformIcon(platform);
                          const account = entity.platform_accounts[platform];
                          const value = platform === 'facebook'
                            ? draft.facebook_page_id
                            : platform === 'instagram'
                              ? draft.instagram_username
                              : draft.google_ads_domain;
                          const label = platform === 'facebook'
                            ? (ru ? 'Facebook page ID' : 'Facebook page ID')
                            : platform === 'instagram'
                              ? (ru ? 'Instagram handle' : 'Instagram handle')
                              : (ru ? 'Google Ads domain' : 'Google Ads domain');
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
                              <label className="block text-xs uppercase tracking-[0.18em] text-slate-500">{label}</label>
                              <input
                                value={value}
                                onChange={(event) => {
                                  if (platform === 'facebook') {
                                    updateDraft(entity.id, { facebook_page_id: event.target.value });
                                  } else if (platform === 'instagram') {
                                    updateDraft(entity.id, { instagram_username: event.target.value });
                                  } else {
                                    updateDraft(entity.id, { google_ads_domain: event.target.value });
                                  }
                                }}
                                placeholder={platform === 'facebook' ? '196765077044445' : platform === 'instagram' ? 'brand_handle' : 'brand.com'}
                                className="mt-2 w-full rounded-xl border border-slate-200 px-3 py-2 text-sm text-slate-900 outline-none transition focus:border-slate-400"
                              />
                              <div className="mt-2 space-y-1 text-xs text-slate-500">
                                <div>
                                  {ru ? 'Last collected:' : 'Last collected:'} {relativeTime(account?.last_collected_at || null, ru)}
                                </div>
                                {account?.last_health_error ? (
                                  <div className="line-clamp-2 text-amber-700">
                                    {account.last_health_error}
                                  </div>
                                ) : null}
                              </div>
                            </div>
                          );
                        })}
                      </div>

                      <div className="mt-4 flex justify-end">
                        <button
                          type="button"
                          onClick={() => void handleSaveEntity(entity)}
                          disabled={savingEntityId === entity.id}
                          className="inline-flex items-center gap-2 rounded-xl bg-white px-4 py-2 text-sm font-medium text-slate-800 ring-1 ring-slate-200 transition hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-60"
                        >
                          <RefreshCw className={`h-4 w-4 ${savingEntityId === entity.id ? 'animate-spin' : ''}`} />
                          {savingEntityId === entity.id ? (ru ? 'Сохранение...' : 'Saving...') : (ru ? 'Сохранить' : 'Save')}
                        </button>
                      </div>
                    </div>
                  );
                })}
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
                  <p className="text-sm font-medium text-slate-900">{ru ? 'Последний runtime результат' : 'Latest runtime result'}</p>
                  <dl className="mt-3 space-y-2 text-sm text-slate-600">
                    <div className="flex items-center justify-between">
                      <dt>{ru ? 'Собрано' : 'Collected'}</dt>
                      <dd>{overview?.runtime?.last_result?.activities_collected ?? 0}</dd>
                    </div>
                    <div className="flex items-center justify-between">
                      <dt>{ru ? 'Проанализировано' : 'Analyzed'}</dt>
                      <dd>{overview?.runtime?.last_result?.activities_analyzed ?? 0}</dd>
                    </div>
                    <div className="flex items-center justify-between">
                      <dt>{ru ? 'В графе' : 'Graph synced'}</dt>
                      <dd>{overview?.runtime?.last_result?.activities_graph_synced ?? 0}</dd>
                    </div>
                    <div className="flex items-center justify-between">
                      <dt>{ru ? 'Следующий run' : 'Next run'}</dt>
                      <dd>{formatDateTime(overview?.runtime?.next_run_at || null, ru)}</dd>
                    </div>
                    <div className="flex items-center justify-between">
                      <dt>{ru ? 'Postgres worker' : 'Postgres worker'}</dt>
                      <dd>{overview?.runtime?.postgres_worker_enabled ? 'ON' : 'OFF'}</dd>
                    </div>
                  </dl>
                </div>
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <p className="text-sm font-medium text-slate-900">{ru ? 'Stale / dead-letter сигналы' : 'Stale / dead-letter signals'}</p>
                  <div className="mt-3 space-y-3">
                    {(overview?.account_health_counts?.healthy || 0) || (overview?.account_health_counts?.provider_404 || 0) || (overview?.account_health_counts?.invalid_identifier || 0) ? (
                      <div className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700">
                        {ru ? 'Аккаунты:' : 'Accounts:'} {overview?.account_health_counts?.healthy ?? 0} healthy, {overview?.account_health_counts?.provider_404 ?? 0} 404, {overview?.account_health_counts?.invalid_identifier ?? 0} invalid
                      </div>
                    ) : null}
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
                    {(failures || []).slice(0, 2).map((failure) => (
                      <div key={failure.id} className="rounded-xl border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800">
                        <div className="flex items-center justify-between gap-3">
                          <div className="font-medium">{failure.stage}</div>
                          <button
                            type="button"
                            onClick={() => void handleRetryFailure(failure)}
                            disabled={retryingFailureId === failure.id}
                            className="inline-flex items-center gap-1 rounded-lg bg-white px-2.5 py-1 text-xs font-medium text-red-700 ring-1 ring-red-200 transition hover:bg-red-100 disabled:cursor-not-allowed disabled:opacity-60"
                          >
                            <RefreshCw className={`h-3.5 w-3.5 ${retryingFailureId === failure.id ? 'animate-spin' : ''}`} />
                            {ru ? 'Retry' : 'Retry'}
                          </button>
                        </div>
                        <div className="mt-1 line-clamp-2 text-xs">{failure.last_error || failure.scope_key}</div>
                      </div>
                    ))}
                    {!overview?.stale_entities?.length && !failures.length ? (
                      <div className="rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800">
                        {ru ? 'Критичных social runtime сигналов нет.' : 'No critical social runtime warnings right now.'}
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
                      className={`w-full rounded-2xl border p-4 text-left transition ${active ? 'border-slate-900 bg-slate-900 text-white' : 'border-slate-200 bg-slate-50 text-slate-900 hover:border-slate-300 hover:bg-white'}`}
                    >
                      <div className="flex items-start justify-between gap-4">
                        <div className="min-w-0">
                          <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] opacity-70">
                            <Icon className="h-3.5 w-3.5" />
                            {platformLabel(activity.platform, ru)}
                            <span>•</span>
                            <span>{activity.entity?.name || 'Unknown entity'}</span>
                          </div>
                          <p className="mt-2 line-clamp-3 text-sm leading-6">
                            {activitySummary(activity) || (ru ? 'Нет текста, сохранён только raw evidence.' : 'No extracted text, raw evidence only.')}
                          </p>
                        </div>
                        <span className={`rounded-full px-2 py-1 text-xs font-medium ${active ? 'bg-white/15 text-white' : 'bg-slate-200 text-slate-700'}`}>
                          {relativeTime(activity.published_at, ru)}
                        </span>
                      </div>
                    </button>
                  );
                })}
              </div>
            </div>

            <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
              <div className="flex items-center justify-between">
                <h2 className="text-lg font-semibold text-slate-900">
                  {ru ? 'Детали evidence' : 'Evidence detail'}
                </h2>
                <div className="flex flex-wrap items-center gap-2">
                  {selectedActivity ? (
                    <>
                      <button
                        type="button"
                        onClick={() => void handleReplaySelected('analysis')}
                        disabled={replayingStage !== null}
                        className="inline-flex items-center gap-2 rounded-xl border border-slate-200 px-3 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-60"
                      >
                        <RefreshCw className={`h-4 w-4 ${replayingStage === 'analysis' ? 'animate-spin' : ''}`} />
                        {ru ? 'Replay AI' : 'Replay AI'}
                      </button>
                      <button
                        type="button"
                        onClick={() => void handleReplaySelected('graph')}
                        disabled={replayingStage !== null}
                        className="inline-flex items-center gap-2 rounded-xl border border-slate-200 px-3 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-60"
                      >
                        <RefreshCw className={`h-4 w-4 ${replayingStage === 'graph' ? 'animate-spin' : ''}`} />
                        {ru ? 'Replay graph' : 'Replay graph'}
                      </button>
                    </>
                  ) : null}
                  {selectedActivity?.source_url ? (
                    <a
                      href={selectedActivity.source_url}
                      target="_blank"
                      rel="noreferrer"
                      className="inline-flex items-center gap-2 rounded-xl border border-slate-200 px-3 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-100"
                    >
                      <Globe2 className="h-4 w-4" />
                      {ru ? 'Открыть источник' : 'Open source'}
                    </a>
                  ) : null}
                </div>
              </div>

              {selectedActivity ? (
                <div className="mt-4 space-y-5">
                  <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                    <div className="grid gap-3 text-sm text-slate-700 md:grid-cols-2">
                      <div>
                        <div className="text-xs uppercase tracking-[0.18em] text-slate-500">{ru ? 'Компания' : 'Entity'}</div>
                        <div className="mt-1 font-medium text-slate-900">{selectedActivity.entity?.name || '—'}</div>
                      </div>
                      <div>
                        <div className="text-xs uppercase tracking-[0.18em] text-slate-500">{ru ? 'Опубликовано' : 'Published'}</div>
                        <div className="mt-1 font-medium text-slate-900">{formatDateTime(selectedActivity.published_at, ru)}</div>
                      </div>
                      <div>
                        <div className="text-xs uppercase tracking-[0.18em] text-slate-500">{ru ? 'CTA' : 'CTA'}</div>
                        <div className="mt-1 font-medium text-slate-900">{selectedActivity.cta_type || '—'}</div>
                      </div>
                      <div>
                        <div className="text-xs uppercase tracking-[0.18em] text-slate-500">{ru ? 'Формат' : 'Format'}</div>
                        <div className="mt-1 font-medium text-slate-900">{selectedActivity.content_format || '—'}</div>
                      </div>
                    </div>
                  </div>

                  <div>
                    <h3 className="text-sm font-semibold uppercase tracking-[0.18em] text-slate-500">
                      {ru ? 'AI summary' : 'AI summary'}
                    </h3>
                    <p className="mt-2 text-sm leading-7 text-slate-700">
                      {activitySummary(selectedActivity) || (ru ? 'Сводка пока не готова.' : 'Summary is not ready yet.')}
                    </p>
                  </div>

                  <div className="grid gap-4 md:grid-cols-2">
                    <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                      <div className="text-xs uppercase tracking-[0.18em] text-slate-500">{ru ? 'Маркетинговый intent' : 'Marketing intent'}</div>
                      <div className="mt-2 text-sm text-slate-800">
                        {selectedActivity.analysis?.analysis_payload?.marketing_intent ||
                          selectedActivity.analysis?.marketing_intent ||
                          '—'}
                      </div>
                    </div>
                    <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                      <div className="text-xs uppercase tracking-[0.18em] text-slate-500">{ru ? 'Темы' : 'Topics'}</div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {topicNames(selectedActivity).length ? topicNames(selectedActivity).map((topic) => (
                          <span key={topic} className="rounded-full bg-white px-2.5 py-1 text-xs font-medium text-slate-700 ring-1 ring-slate-200">
                            {topic}
                          </span>
                        )) : <span className="text-sm text-slate-600">—</span>}
                      </div>
                    </div>
                  </div>

                  <div>
                    <h3 className="text-sm font-semibold uppercase tracking-[0.18em] text-slate-500">
                      {ru ? 'Evidence text' : 'Evidence text'}
                    </h3>
                    <div className="mt-2 rounded-2xl border border-slate-200 bg-slate-50 p-4 text-sm leading-7 text-slate-700">
                      {selectedActivity.text_content || (ru ? 'Полный текст не был извлечён, но raw payload сохранён.' : 'Full text was not extracted, but the raw payload is preserved.')}
                    </div>
                  </div>
                </div>
              ) : (
                <div className="mt-4 rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-4 py-8 text-center text-sm text-slate-600">
                  {ru ? 'Когда появятся social activities, детали evidence отобразятся здесь.' : 'Evidence details will appear here once social activities are collected.'}
                </div>
              )}
            </div>
          </div>
        </section>
      </div>
    </div>
  );
}
