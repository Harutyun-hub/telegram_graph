import { useEffect, useMemo, useRef, useState, type ElementType } from 'react';
import {
  Plus,
  Search,
  Trash2,
  X,
  MoreVertical,
  Radio,
  Users,
  MessageCircle,
  RefreshCw,
  AlertCircle,
  CheckCircle2,
  Clock,
  Database,
  Brain,
  Network,
} from 'lucide-react';
import { useLanguage } from '../contexts/LanguageContext';
import { apiFetch } from '../services/api';
import type { TrackedChannel, ChannelStatus } from '../types/data';
import { SocialSourcesSection } from './sources/SocialSourcesSection';

type SourceApiItem = {
  id: string;
  channel_username: string;
  channel_title: string | null;
  description: string | null;
  member_count: number | null;
  is_active: boolean;
  scrape_depth_days: number;
  scrape_comments: boolean;
  source_type?: 'channel' | 'supergroup' | 'pending' | null;
  resolution_status?: 'pending' | 'resolved' | 'error' | null;
  last_resolution_error?: string | null;
  last_scraped_at: string | null;
  created_at: string;
  updated_at: string;
};

type SourceListResponse = {
  count: number;
  items: SourceApiItem[];
};

type SourceCreateResponse = {
  action: 'created' | 'reactivated' | 'exists';
  item: SourceApiItem;
};

type SourceUpdateResponse = {
  item: SourceApiItem;
};

type ScraperSchedulerStatus = {
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
    channels_total: number;
    channels_processed: number;
    posts_found: number;
    comments_found: number;
    scrape_skipped?: boolean;
    scrape_skipped_reason?: string | null;
    peer_ref_channels?: number;
    username_fallback_channels?: number;
    pending_resolution_channels?: number;
    resolve_flood_wait_count?: number;
    ai_analysis_saved?: number;
    posts_processed?: number;
    posts_pending_sync?: number;
    posts_synced?: number;
    sync_errors?: number;
    sync_batch_chunks?: number;
    sync_fallback_posts?: number;
    mode?: string;
  } | null;
  last_mode?: 'normal' | 'catchup' | string;
  catchup_limits?: {
    comment_limit: number;
    post_limit: number;
    sync_limit: number;
  };
  normal_limits?: {
    comment_limit: number;
    post_limit: number;
    sync_limit: number;
  };
};

type PipelineFreshnessSnapshot = {
  generated_at: string;
  operational?: {
    status?: 'healthy' | 'critical' | string;
    label?: string;
    reason?: string | null;
  };
  pipeline?: {
    scrape?: {
      status?: 'healthy' | 'warning' | 'stale' | 'unknown' | 'paused_by_backpressure' | string;
      reason?: string | null;
      last_scrape_at?: string | null;
      age_minutes?: number | null;
    };
    process?: {
      status?: 'healthy' | 'warning' | 'stale' | 'unknown' | string;
      last_process_at?: string | null;
      age_minutes?: number | null;
    };
    sync?: {
      status?: 'healthy' | 'warning' | 'stale' | 'unknown' | 'caught_up' | 'idle' | string;
      reason?: string | null;
      last_graph_sync_at?: string | null;
      age_minutes?: number | null;
      estimated?: boolean;
    };
  };
  backlog?: {
    unprocessed_posts?: number;
    unprocessed_comments?: number;
    unsynced_posts?: number;
    unsynced_analysis?: number;
  };
  health?: {
    status?: string;
    score?: number;
    notes?: string[];
  };
  pulse?: {
    queue?: {
      ai_items?: number;
      graph_posts?: number;
    };
    processed?: {
      scraped_items_last_run?: number;
      ai_items_last_run?: number;
      neo4j_posts_last_run?: number;
      ai_rate_per_hour?: number;
      neo4j_rate_per_hour?: number;
      scrape_rate_per_hour?: number;
    };
    eta?: {
      ai_queue_minutes?: number | null;
      graph_queue_minutes?: number | null;
      total_minutes?: number | null;
      confidence?: 'high' | 'medium' | 'low' | string;
      assumption?: string;
    };
  };
};

const stageStyle: Record<string, { badge: string; dot: string; text: string }> = {
  healthy: { badge: 'bg-emerald-50 text-emerald-700', dot: 'bg-emerald-500', text: 'text-emerald-700' },
  caught_up: { badge: 'bg-emerald-50 text-emerald-700', dot: 'bg-emerald-500', text: 'text-emerald-700' },
  idle: { badge: 'bg-slate-100 text-slate-600', dot: 'bg-slate-400', text: 'text-slate-600' },
  warning: { badge: 'bg-amber-50 text-amber-700', dot: 'bg-amber-500', text: 'text-amber-700' },
  paused_by_backpressure: { badge: 'bg-amber-50 text-amber-700', dot: 'bg-amber-500', text: 'text-amber-700' },
  stale: { badge: 'bg-red-50 text-red-700', dot: 'bg-red-500', text: 'text-red-700' },
  unknown: { badge: 'bg-slate-100 text-slate-600', dot: 'bg-slate-400', text: 'text-slate-600' },
};

const operationalStyle: Record<string, { badge: string; dot: string; text: string }> = {
  healthy: { badge: 'bg-emerald-50 text-emerald-700', dot: 'bg-emerald-500', text: 'text-emerald-700' },
  critical: { badge: 'bg-red-50 text-red-700', dot: 'bg-red-500', text: 'text-red-700' },
  unknown: { badge: 'bg-slate-100 text-slate-600', dot: 'bg-slate-400', text: 'text-slate-600' },
};

function toneFor(status?: string) {
  return stageStyle[(status || 'unknown').toLowerCase()] || stageStyle.unknown;
}

function operationalToneFor(status?: string) {
  return operationalStyle[(status || 'unknown').toLowerCase()] || operationalStyle.unknown;
}

function operationalLabel(status: string | undefined, ru: boolean) {
  const normalized = (status || 'unknown').toLowerCase();
  if (normalized === 'healthy') return ru ? 'Система работает' : 'System operational';
  if (normalized === 'critical') return ru ? 'Проблема системы' : 'System issue';
  return ru ? 'Статус системы неизвестен' : 'System status unknown';
}

function lastResultLabel(lastResult: ScraperSchedulerStatus['last_result'], scheduler: ScraperSchedulerStatus | null, ru: boolean) {
  if (lastResult?.scrape_skipped && lastResult.scrape_skipped_reason === 'backpressure') {
    return ru ? 'Пауза из-за backpressure' : 'Paused by backpressure';
  }
  if (lastResult) {
    return `${lastResult.posts_found} ${ru ? 'постов' : 'posts'}, ${lastResult.comments_found} ${ru ? 'комментариев' : 'comments'}`;
  }
  if (scheduler?.last_run_started_at || scheduler?.is_active) {
    return ru ? 'Ожидаем завершённый цикл' : 'Awaiting completed cycle';
  }
  return ru ? 'Нет данных' : 'No data';
}

function TelegramIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="currentColor">
      <path d="M11.944 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0a12 12 0 0 0-.056 0zm4.962 7.224c.1-.002.321.023.465.14a.506.506 0 0 1 .171.325c.016.093.036.306.02.472-.18 1.898-.962 6.502-1.36 8.627-.168.9-.499 1.201-.82 1.23-.696.065-1.225-.46-1.9-.902-1.056-.693-1.653-1.124-2.678-1.8-1.185-.78-.417-1.21.258-1.91.177-.184 3.247-2.977 3.307-3.23.007-.032.014-.15-.056-.212s-.174-.041-.249-.024c-.106.024-1.793 1.14-5.061 3.345-.48.33-.913.49-1.302.48-.428-.008-1.252-.241-1.865-.44-.752-.245-1.349-.374-1.297-.789.027-.216.325-.437.893-.663 3.498-1.524 5.83-2.529 6.998-3.014 3.332-1.386 4.025-1.627 4.476-1.635z" />
    </svg>
  );
}

const statusConfig: Record<ChannelStatus, { labelEn: string; labelRu: string; bg: string; text: string; dot: string }> = {
  active: { labelEn: 'Active', labelRu: 'Активен', bg: 'bg-emerald-50', text: 'text-emerald-700', dot: 'bg-emerald-500' },
  paused: { labelEn: 'Paused', labelRu: 'Пауза', bg: 'bg-amber-50', text: 'text-amber-700', dot: 'bg-amber-500' },
  error: { labelEn: 'Error', labelRu: 'Ошибка', bg: 'bg-red-50', text: 'text-red-700', dot: 'bg-red-500' },
  pending: { labelEn: 'Pending', labelRu: 'Ожидание', bg: 'bg-blue-50', text: 'text-blue-700', dot: 'bg-blue-500' },
};

function StatusBadge({ status, ru }: { status: ChannelStatus; ru: boolean }) {
  const cfg = statusConfig[status];
  return (
    <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs ${cfg.bg} ${cfg.text}`} style={{ fontWeight: 500 }}>
      <span className={`w-1.5 h-1.5 rounded-full ${cfg.dot}`} />
      {ru ? cfg.labelRu : cfg.labelEn}
    </span>
  );
}

const typeIcons: Record<string, ElementType> = {
  channel: Radio,
  group: Users,
  supergroup: MessageCircle,
};

const typeLabels: Record<string, { en: string; ru: string }> = {
  channel: { en: 'Channel', ru: 'Канал' },
  group: { en: 'Group', ru: 'Группа' },
  supergroup: { en: 'Supergroup', ru: 'Супергруппа' },
};

const TELEGRAM_USERNAME_RE = /^[a-z][a-z0-9_]{4,31}$/i;

function normalizeSourceInput(raw: string): string {
  let value = raw.trim();
  if (!value) return '';
  value = value.replace(/^https?:\/\//i, '');
  value = value.replace(/^www\./i, '');
  const lowered = value.toLowerCase();
  if (lowered.startsWith('t.me/')) value = value.slice(5);
  if (lowered.startsWith('telegram.me/')) value = value.slice(12);
  value = value.split('?')[0].split('#')[0].trim();
  if (value.startsWith('@')) value = value.slice(1);
  const segments = value.split('/').map((part) => part.trim()).filter(Boolean);
  if (!segments.length) return '';

  let candidate = segments[0];
  if (candidate.toLowerCase() === 'c') {
    candidate = segments[1] || '';
  }

  candidate = candidate.replace(/^@/, '').trim().toLowerCase();
  if (!TELEGRAM_USERNAME_RE.test(candidate)) return '';
  return `@${candidate}`;
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
  if (!iso) return ru ? '—' : '—';
  const date = new Date(iso);
  if (!Number.isFinite(date.getTime())) return ru ? '—' : '—';
  return date.toLocaleString(ru ? 'ru-RU' : 'en-US', {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatDuration(minutes: number | null | undefined, ru: boolean): string {
  if (minutes == null) return ru ? 'Расчет...' : 'Calculating...';
  if (minutes <= 0) return ru ? 'Готово' : 'Done';
  if (minutes < 60) return ru ? `~${minutes} мин` : `~${minutes} min`;
  const hours = Math.floor(minutes / 60);
  const rem = minutes % 60;
  if (hours < 24) return rem > 0 ? (ru ? `~${hours} ч ${rem} мин` : `~${hours}h ${rem}m`) : (ru ? `~${hours} ч` : `~${hours}h`);
  const days = Math.floor(hours / 24);
  const remH = hours % 24;
  return remH > 0 ? (ru ? `~${days} д ${remH} ч` : `~${days}d ${remH}h`) : (ru ? `~${days} д` : `~${days}d`);
}

function toTrackedChannel(item: SourceApiItem, ru: boolean): TrackedChannel {
  const username = normalizeSourceInput(item.channel_username || '');
  const title = (item.channel_title || '').trim() || username;
  const resolutionStatus = String(item.resolution_status || '').trim().toLowerCase();
  const description = (item.description || item.last_resolution_error || '').trim();
  const sourceType = String(item.source_type || '').trim().toLowerCase();
  const status: ChannelStatus = !item.is_active
    ? 'paused'
    : resolutionStatus === 'error'
      ? 'error'
      : resolutionStatus === 'pending'
        ? 'pending'
        : 'active';
  const type = sourceType === 'supergroup' ? 'supergroup' : 'channel';

  return {
    id: item.id,
    username,
    title,
    description: description || undefined,
    members: item.member_count ?? 0,
    dailyMessages: 0,
    status,
    addedDate: (item.created_at || '').split('T')[0] || new Date().toISOString().split('T')[0],
    lastSync: relativeTime(item.last_scraped_at, ru),
    type,
    language: 'RU/EN',
    growth: 0,
  };
}

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const normalizedPath = path.startsWith('/api/') ? path.slice(4) : path;
  return apiFetch<T>(normalizedPath, {
    ...init,
    includeUserAuth: true,
  });
}

function AddChannelModal({
  open,
  ru,
  onClose,
  onSubmit,
}: {
  open: boolean;
  ru: boolean;
  onClose: () => void;
  onSubmit: (payload: { channelUsername: string; channelTitle?: string }) => Promise<void>;
}) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [channelUsername, setChannelUsername] = useState('');
  const [channelTitle, setChannelTitle] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    if (!open) return;
    setChannelUsername('');
    setChannelTitle('');
    setSaving(false);
    setError('');
    setTimeout(() => inputRef.current?.focus(), 80);
  }, [open]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/50 backdrop-blur-sm" onClick={onClose} />
      <div className="relative bg-white rounded-2xl shadow-2xl w-full max-w-lg overflow-hidden">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl flex items-center justify-center" style={{ background: 'linear-gradient(135deg, #1a56db, #1e3a8a)' }}>
              <TelegramIcon className="w-5 h-5 text-white" />
            </div>
            <div>
              <h3 className="text-gray-900" style={{ fontSize: '1rem', fontWeight: 600 }}>
                {ru ? 'Добавить Telegram-источник' : 'Add Telegram Source'}
              </h3>
              <p className="text-xs text-gray-500 mt-0.5">
                {ru ? 'Username обязателен, название можно оставить пустым' : 'Username required, title optional'}
              </p>
            </div>
          </div>
          <button onClick={onClose} className="p-1.5 rounded-lg hover:bg-gray-100 transition-colors">
            <X className="w-5 h-5 text-gray-400" />
          </button>
        </div>

        <div className="px-6 py-5 space-y-4">
          <div>
            <label className="text-xs text-gray-600 block mb-1.5" style={{ fontWeight: 500 }}>
              {ru ? 'Username или ссылка' : 'Username or link'}
            </label>
            <div className="relative">
              <TelegramIcon className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
              <input
                ref={inputRef}
                type="text"
                value={channelUsername}
                onChange={(e) => {
                  setChannelUsername(e.target.value);
                  setError('');
                }}
                placeholder={
                  ru
                    ? '@channel_name, t.me/channel или t.me/c/public_name/123'
                    : '@channel_name, t.me/channel, or t.me/c/public_name/123'
                }
                className="w-full pl-10 pr-4 py-2.5 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-all"
              />
            </div>
          </div>

          <div>
            <label className="text-xs text-gray-600 block mb-1.5" style={{ fontWeight: 500 }}>
              {ru ? 'Название (опционально)' : 'Title (optional)'}
            </label>
            <input
              type="text"
              value={channelTitle}
              onChange={(e) => setChannelTitle(e.target.value)}
              placeholder={ru ? 'Например: Best Job in Armenia' : 'For example: Best Job in Armenia'}
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
              ? 'Новый источник сразу попадет в таблицу Supabase. Скрапинг запускается только по расписанию.'
              : 'The new source is saved in Supabase immediately. Scraping starts only on scheduler cycle.'}
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
              const normalized = normalizeSourceInput(channelUsername);
              if (!normalized) {
                setError(ru ? 'Введите корректный username' : 'Enter a valid username');
                return;
              }
              setSaving(true);
              setError('');
              try {
                await onSubmit({ channelUsername: normalized, channelTitle: channelTitle.trim() || undefined });
                onClose();
              } catch (err: any) {
                setError(String(err?.message || (ru ? 'Ошибка добавления' : 'Failed to add source')));
              } finally {
                setSaving(false);
              }
            }}
            disabled={saving}
            className="px-5 py-2 rounded-lg text-sm text-white transition-all disabled:opacity-40 disabled:cursor-not-allowed"
            style={{ fontWeight: 500, background: 'linear-gradient(135deg, #1a56db, #1e3a8a)' }}
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

function RowActions({
  channel,
  ru,
  onPauseResume,
  onDisable,
}: {
  channel: TrackedChannel;
  ru: boolean;
  onPauseResume: (id: string) => Promise<void>;
  onDisable: (id: string) => Promise<void>;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handle(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener('mousedown', handle);
    return () => document.removeEventListener('mousedown', handle);
  }, []);

  return (
    <div className="relative" ref={ref}>
      <button onClick={() => setOpen(!open)} className={`p-1.5 rounded-lg transition-colors ${open ? 'bg-gray-100' : 'hover:bg-gray-100'}`}>
        <MoreVertical className="w-4 h-4 text-gray-400" />
      </button>
      {open && (
        <div className="absolute right-0 top-full mt-1 w-48 bg-white border border-gray-200 rounded-xl shadow-xl z-30 py-1 overflow-hidden">
          <button
            onClick={async () => {
              await onPauseResume(channel.id);
              setOpen(false);
            }}
            className="w-full flex items-center gap-2.5 px-3 py-2 text-xs text-gray-700 hover:bg-gray-50 transition-colors"
          >
            {channel.status === 'paused' ? (
              <>
                <CheckCircle2 className="w-3.5 h-3.5 text-emerald-500" />
                {ru ? 'Возобновить' : 'Resume tracking'}
              </>
            ) : (
              <>
                <Clock className="w-3.5 h-3.5 text-amber-500" />
                {ru ? 'Поставить на паузу' : 'Pause tracking'}
              </>
            )}
          </button>
          <div className="border-t border-gray-100 my-1" />
          <button
            onClick={async () => {
              await onDisable(channel.id);
              setOpen(false);
            }}
            className="w-full flex items-center gap-2.5 px-3 py-2 text-xs text-red-600 hover:bg-red-50 transition-colors"
          >
            <Trash2 className="w-3.5 h-3.5" />
            {ru ? 'Отключить источник' : 'Disable source'}
          </button>
        </div>
      )}
    </div>
  );
}

export function SourcesPage() {
  const { lang } = useLanguage();
  const ru = lang === 'ru';
  const [mode, setMode] = useState<'telegram' | 'social'>('telegram');

  const [channels, setChannels] = useState<TrackedChannel[]>([]);
  const [search, setSearch] = useState('');
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [statusFilter, setStatusFilter] = useState<ChannelStatus | 'all'>('all');
  const [showAddModal, setShowAddModal] = useState(false);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [scheduler, setScheduler] = useState<ScraperSchedulerStatus | null>(null);
  const [schedulerLoading, setSchedulerLoading] = useState(true);
  const [schedulerBusy, setSchedulerBusy] = useState(false);
  const [schedulerError, setSchedulerError] = useState<string | null>(null);
  const [freshness, setFreshness] = useState<PipelineFreshnessSnapshot | null>(null);
  const [freshnessLoading, setFreshnessLoading] = useState(true);
  const [freshnessError, setFreshnessError] = useState<string | null>(null);
  const [intervalInput, setIntervalInput] = useState('15');

  useEffect(() => {
    setShowAddModal(false);
  }, [mode]);

  const loadSources = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await requestJson<SourceListResponse>('/api/sources/channels');
      setChannels((response.items || []).map((item) => toTrackedChannel(item, ru)));
    } catch (err: any) {
      setError(String(err?.message || 'Failed to load sources'));
    } finally {
      setLoading(false);
    }
  };

  const loadSchedulerStatus = async (quiet = false) => {
    if (!quiet) setSchedulerLoading(true);
    try {
      const response = await requestJson<ScraperSchedulerStatus>('/api/scraper/scheduler');
      setScheduler(response);
      setIntervalInput(String(response.interval_minutes));
      setSchedulerError(null);
    } catch (err: any) {
      setSchedulerError(String(err?.message || 'Failed to load scraper scheduler'));
    } finally {
      if (!quiet) setSchedulerLoading(false);
    }
  };

  const loadFreshnessStatus = async (quiet = false) => {
    if (!quiet) setFreshnessLoading(true);
    try {
      const response = await requestJson<PipelineFreshnessSnapshot>('/api/freshness?force=true');
      setFreshness(response);
      setFreshnessError(null);
    } catch (err: any) {
      setFreshnessError(String(err?.message || 'Failed to load pipeline freshness'));
    } finally {
      if (!quiet) setFreshnessLoading(false);
    }
  };

  useEffect(() => {
    if (mode !== 'telegram') return;
    void loadSources();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ru, mode]);

  useEffect(() => {
    if (mode !== 'telegram') return;
    void loadSchedulerStatus();
    void loadFreshnessStatus();
    const timer = window.setInterval(() => {
      void loadSchedulerStatus(true);
      void loadFreshnessStatus(true);
    }, 10000);
    return () => window.clearInterval(timer);
  }, [mode]);

  const saveSchedulerInterval = async () => {
    const parsed = Number(intervalInput);
    if (!Number.isFinite(parsed) || parsed < 1) {
      setSchedulerError(ru ? 'Интервал должен быть больше 0' : 'Interval must be greater than 0');
      return;
    }

    setSchedulerBusy(true);
    setSchedulerError(null);
    try {
      const response = await requestJson<ScraperSchedulerStatus>('/api/scraper/scheduler', {
        method: 'PATCH',
        body: JSON.stringify({ interval_minutes: Math.floor(parsed) }),
      });
      setScheduler(response);
      setIntervalInput(String(response.interval_minutes));
    } catch (err: any) {
      setSchedulerError(String(err?.message || 'Failed to update scheduler interval'));
    } finally {
      setSchedulerBusy(false);
    }
  };

  const startScheduler = async () => {
    setSchedulerBusy(true);
    setSchedulerError(null);
    try {
      const response = await requestJson<ScraperSchedulerStatus>('/api/scraper/scheduler/start', { method: 'POST' });
      setScheduler(response);
      void loadFreshnessStatus(true);
    } catch (err: any) {
      setSchedulerError(String(err?.message || 'Failed to start scheduler'));
    } finally {
      setSchedulerBusy(false);
    }
  };

  const stopScheduler = async () => {
    setSchedulerBusy(true);
    setSchedulerError(null);
    try {
      const response = await requestJson<ScraperSchedulerStatus>('/api/scraper/scheduler/stop', { method: 'POST' });
      setScheduler(response);
      void loadFreshnessStatus(true);
    } catch (err: any) {
      setSchedulerError(String(err?.message || 'Failed to stop scheduler'));
    } finally {
      setSchedulerBusy(false);
    }
  };

  const runSchedulerNow = async () => {
    setSchedulerBusy(true);
    setSchedulerError(null);
    try {
      const response = await requestJson<ScraperSchedulerStatus>('/api/scraper/scheduler/run-once', { method: 'POST' });
      setScheduler(response);
      setTimeout(() => {
        void loadSchedulerStatus(true);
        void loadFreshnessStatus(true);
      }, 1200);
    } catch (err: any) {
      setSchedulerError(String(err?.message || 'Failed to run scraper now'));
    } finally {
      setSchedulerBusy(false);
    }
  };

  const filtered = useMemo(() => {
    return channels.filter((ch) => {
      const query = search.trim().toLowerCase();
      const matchesSearch =
        !query ||
        ch.title.toLowerCase().includes(query) ||
        ch.username.toLowerCase().includes(query) ||
        (ch.description || '').toLowerCase().includes(query);
      const matchesStatus = statusFilter === 'all' || ch.status === statusFilter;
      return matchesSearch && matchesStatus;
    });
  }, [channels, search, statusFilter]);

  const allSelected = filtered.length > 0 && filtered.every((ch) => selectedIds.has(ch.id));
  const someSelected = filtered.some((ch) => selectedIds.has(ch.id));

  const toggleAll = () => {
    if (allSelected) {
      setSelectedIds(new Set());
      return;
    }
    setSelectedIds(new Set(filtered.map((ch) => ch.id)));
  };

  const toggleOne = (id: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const setChannelActive = async (id: string, isActive: boolean) => {
    setBusy(true);
    setError(null);
    try {
      await requestJson<SourceUpdateResponse>(`/api/sources/channels/${id}`, {
        method: 'PATCH',
        body: JSON.stringify({ is_active: isActive }),
      });
      await loadSources();
      setSelectedIds((prev) => {
        const next = new Set(prev);
        if (!isActive) next.delete(id);
        return next;
      });
    } catch (err: any) {
      setError(String(err?.message || 'Update failed'));
    } finally {
      setBusy(false);
    }
  };

  const handleAddSource = async (payload: { channelUsername: string; channelTitle?: string }) => {
    setBusy(true);
    setError(null);
    try {
      await requestJson<SourceCreateResponse>('/api/sources/channels', {
        method: 'POST',
        body: JSON.stringify({
          channel_username: payload.channelUsername,
          channel_title: payload.channelTitle,
          scrape_depth_days: 7,
          scrape_comments: true,
        }),
      });
      await loadSources();
    } catch (err) {
      throw err;
    } finally {
      setBusy(false);
    }
  };

  const handleBulkDisable = async () => {
    if (!selectedIds.size) return;
    setBusy(true);
    setError(null);
    try {
      await Promise.all(
        Array.from(selectedIds).map((id) =>
          requestJson<SourceUpdateResponse>(`/api/sources/channels/${id}`, {
            method: 'PATCH',
            body: JSON.stringify({ is_active: false }),
          }),
        ),
      );
      setSelectedIds(new Set());
      await loadSources();
    } catch (err: any) {
      setError(String(err?.message || 'Bulk update failed'));
    } finally {
      setBusy(false);
    }
  };

  const activeCount = channels.filter((ch) => ch.status === 'active').length;
  const totalMembers = channels.reduce((sum, ch) => sum + ch.members, 0);
  const totalMessages = 0;
  const lastResult = scheduler?.last_result;

  const scrapedInLastRun = {
    posts: lastResult?.posts_found ?? 0,
    comments: lastResult?.comments_found ?? 0,
  };

  const aiInLastRun = {
    commentBatches: lastResult?.ai_analysis_saved ?? 0,
    posts: lastResult?.posts_processed ?? 0,
  };

  const syncInLastRun = {
    posts: lastResult?.posts_synced ?? 0,
    pending: lastResult?.posts_pending_sync ?? 0,
    errors: lastResult?.sync_errors ?? 0,
  };

  const backlog = freshness?.backlog;
  const scrapeStage = freshness?.pipeline?.scrape;
  const processStage = freshness?.pipeline?.process;
  const syncStage = freshness?.pipeline?.sync;
  const operational = freshness?.operational;
  const operationalStatus = (operational?.status || 'unknown').toLowerCase();
  const operationalTone = operationalToneFor(operational?.status);

  const pulse = freshness?.pulse;
  const queueAiItems = pulse?.queue?.ai_items ?? ((backlog?.unprocessed_comments ?? 0) + (backlog?.unprocessed_posts ?? 0));
  const queueGraphPosts = pulse?.queue?.graph_posts ?? (backlog?.unsynced_posts ?? 0);
  const aiLastRun = pulse?.processed?.ai_items_last_run ?? (aiInLastRun.commentBatches + aiInLastRun.posts);
  const syncedLastRun = pulse?.processed?.neo4j_posts_last_run ?? syncInLastRun.posts;
  const scrapeLastRun = pulse?.processed?.scraped_items_last_run ?? (scrapedInLastRun.posts + scrapedInLastRun.comments);
  const aiRatePerHour = pulse?.processed?.ai_rate_per_hour;
  const syncRatePerHour = pulse?.processed?.neo4j_rate_per_hour;
  const etaTotal = pulse?.eta?.total_minutes;
  const etaConfidence = (pulse?.eta?.confidence || 'low').toUpperCase();
  const dataLooksDelayed = !scheduler?.running_now && (
    (scrapeStage?.status !== 'paused_by_backpressure' && scrapeStage?.age_minutes != null && scrapeStage.age_minutes >= 120)
    || (processStage?.age_minutes != null && processStage.age_minutes >= 360)
    || ((syncStage?.status !== 'caught_up' && syncStage?.status !== 'idle') && syncStage?.age_minutes != null && syncStage.age_minutes >= 360)
  );

  return (
    <div className="p-4 md:p-8 max-w-7xl mx-auto">
      <div className="mb-6">
        <div className="flex items-start justify-between flex-wrap gap-4">
          <div>
            <h1 className="text-gray-900" style={{ fontSize: '1.5rem', fontWeight: 600 }}>
              {ru ? 'Источники данных' : 'Data Sources'}
            </h1>
            <p className="text-sm text-gray-500 mt-1">
              {mode === 'telegram'
                ? (
                    ru
                      ? 'Управление Telegram-каналами и группами, которые отслеживает система'
                      : 'Manage Telegram channels and groups tracked by the system'
                  )
                : (
                    ru
                      ? 'Управление social media источниками, которые отслеживает система'
                      : 'Manage social media sources tracked by the system'
                  )}
            </p>
          </div>
          <div className="flex items-center gap-3 flex-wrap justify-end">
            <div className="inline-flex items-center rounded-xl bg-gray-100 p-1">
              {(['telegram', 'social'] as const).map((value) => (
                <button
                  key={value}
                  onClick={() => setMode(value)}
                  className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${
                    mode === value ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'
                  }`}
                  style={{ fontWeight: 500 }}
                >
                  {value === 'telegram' ? 'Telegram' : (ru ? 'Social' : 'Social')}
                </button>
              ))}
            </div>
            <button
              onClick={() => setShowAddModal(true)}
              className="flex items-center gap-2 px-4 py-2.5 rounded-xl text-sm text-white transition-all hover:shadow-lg hover:shadow-blue-500/25 active:scale-[0.98]"
              style={{ fontWeight: 500, background: 'linear-gradient(135deg, #1a56db, #1e3a8a)' }}
            >
              <Plus className="w-4 h-4" />
              {mode === 'telegram'
                ? (ru ? 'Добавить источник' : 'Add Source')
                : (ru ? 'Добавить источник' : 'Add Source')}
            </button>
          </div>
        </div>
      </div>

      {mode === 'social' ? (
        <SocialSourcesSection
          ru={ru}
          addModalOpen={showAddModal}
          onCloseAddModal={() => setShowAddModal(false)}
        />
      ) : (
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
            <Radio className="w-4 h-4 text-blue-600" />
            <span className="text-xs text-gray-500">{ru ? 'Всего источников' : 'Total Sources'}</span>
          </div>
          <span className="text-xl text-gray-900" style={{ fontWeight: 600 }}>{channels.length}</span>
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
            <Users className="w-4 h-4 text-violet-600" />
            <span className="text-xs text-gray-500">{ru ? 'Участников' : 'Total Members'}</span>
          </div>
          <span className="text-xl text-gray-900" style={{ fontWeight: 600 }}>
            {totalMembers >= 1000 ? `${(totalMembers / 1000).toFixed(1)}K` : totalMembers.toLocaleString()}
          </span>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-4">
          <div className="flex items-center gap-2 mb-1">
            <MessageCircle className="w-4 h-4 text-amber-600" />
            <span className="text-xs text-gray-500">{ru ? 'Сообщений/день' : 'Messages/day'}</span>
          </div>
          <span className="text-xl text-gray-900" style={{ fontWeight: 600 }}>{totalMessages.toLocaleString()}</span>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 p-4 md:p-5 mb-6">
        <div className="flex items-start justify-between gap-4 flex-wrap">
          <div>
            <h2 className="text-gray-900" style={{ fontSize: '1rem', fontWeight: 600 }}>
              {ru ? 'Планировщик скрапинга' : 'Scraper Scheduler'}
            </h2>
            <p className="text-xs text-gray-500 mt-1">
              {ru
                ? 'Управление реальным Python-скрапером для сбора источников в Supabase'
                : 'Control the real Python scraper that collects source data into Supabase'}
            </p>
          </div>
          <div className={`inline-flex items-center gap-2 px-2.5 py-1 rounded-full text-xs ${scheduler?.is_active ? 'bg-emerald-50 text-emerald-700' : 'bg-gray-100 text-gray-600'}`} style={{ fontWeight: 500 }}>
            <span className={`w-2 h-2 rounded-full ${scheduler?.is_active ? 'bg-emerald-500' : 'bg-gray-400'}`} />
            {scheduler?.is_active ? (ru ? 'Активен' : 'Active') : (ru ? 'Остановлен' : 'Stopped')}
            {scheduler?.running_now ? ` · ${ru ? 'идет запуск' : 'running'}` : ''}
          </div>
        </div>

        {schedulerError && (
          <div className="mt-3 flex items-center gap-2 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
            <AlertCircle className="w-3.5 h-3.5" />
            <span>{schedulerError}</span>
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
                min={1}
                value={intervalInput}
                onChange={(e) => setIntervalInput(e.target.value)}
                className="w-28 px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <button
                onClick={saveSchedulerInterval}
                disabled={schedulerBusy || schedulerLoading}
                className="px-3 py-2 rounded-lg text-xs text-white bg-slate-800 hover:bg-slate-700 disabled:opacity-50 transition-colors"
                style={{ fontWeight: 500 }}
              >
                {ru ? 'Сохранить' : 'Save'}
              </button>
              <button
                onClick={() => {
                  void loadSchedulerStatus();
                  void loadFreshnessStatus();
                }}
                disabled={schedulerBusy}
                className="px-2.5 py-2 rounded-lg text-xs text-gray-600 bg-gray-100 hover:bg-gray-200 disabled:opacity-50 transition-colors"
              >
                <RefreshCw className={`w-3.5 h-3.5 ${schedulerLoading ? 'animate-spin' : ''}`} />
              </button>
            </div>
          </div>

          <div className="border border-gray-100 rounded-lg p-3 bg-gray-50/50">
            <div className="flex items-center gap-2 flex-wrap">
              <button
                onClick={startScheduler}
                disabled={schedulerBusy || schedulerLoading || !!scheduler?.is_active}
                className="px-3 py-2 rounded-lg text-xs text-white bg-emerald-600 hover:bg-emerald-700 disabled:opacity-50 transition-colors"
                style={{ fontWeight: 500 }}
              >
                {ru ? 'Запустить' : 'Start'}
              </button>
              <button
                onClick={stopScheduler}
                disabled={schedulerBusy || schedulerLoading || !scheduler?.is_active}
                className="px-3 py-2 rounded-lg text-xs text-white bg-amber-500 hover:bg-amber-600 disabled:opacity-50 transition-colors"
                style={{ fontWeight: 500 }}
              >
                {ru ? 'Остановить' : 'Stop'}
              </button>
              <button
                onClick={runSchedulerNow}
                disabled={schedulerBusy || schedulerLoading || scheduler?.running_now}
                className="px-3 py-2 rounded-lg text-xs text-white bg-blue-600 hover:bg-blue-700 disabled:opacity-50 transition-colors"
                style={{ fontWeight: 500 }}
              >
                {scheduler?.running_now ? (ru ? 'Выполняется...' : 'Running...') : (ru ? 'Запустить сейчас' : 'Run now')}
              </button>
            </div>
          </div>
        </div>

        <div className="mt-3 grid grid-cols-1 md:grid-cols-3 gap-3 text-xs text-gray-500">
          <div className="border border-gray-100 rounded-lg px-3 py-2 bg-white">
            <span className="text-gray-400">{ru ? 'Последний запуск:' : 'Last run:'}</span>
            <div className="text-gray-700 mt-0.5" style={{ fontWeight: 500 }}>
              {formatDateTime(scheduler?.last_run_started_at || null, ru)}
            </div>
          </div>
          <div className="border border-gray-100 rounded-lg px-3 py-2 bg-white">
            <span className="text-gray-400">{ru ? 'Следующий запуск:' : 'Next run:'}</span>
            <div className="text-gray-700 mt-0.5" style={{ fontWeight: 500 }}>
              {formatDateTime(scheduler?.next_run_at || null, ru)}
            </div>
          </div>
          <div className="border border-gray-100 rounded-lg px-3 py-2 bg-white">
            <span className="text-gray-400">{ru ? 'Последний результат:' : 'Last result:'}</span>
            <div className="text-gray-700 mt-0.5" style={{ fontWeight: 500 }}>
              {lastResultLabel(scheduler?.last_result, scheduler, ru)}
            </div>
          </div>
        </div>

        <div className="mt-4 border border-gray-100 rounded-xl p-3 md:p-4 bg-gray-50/40">
          <div className="flex items-center justify-between gap-3 flex-wrap">
            <div>
              <h3 className="text-gray-900" style={{ fontSize: '0.95rem', fontWeight: 600 }}>
                {ru ? 'Пульс обработки данных' : 'Pipeline Pulse'}
              </h3>
              <p className="text-xs text-gray-500 mt-0.5">
                {ru ? 'Сколько данных собрано, обработано AI и синхронизировано в Neo4j' : 'How much data is scraped, AI-analyzed, and synced to Neo4j'}
              </p>
            </div>
            <div className="text-xs text-gray-500">
              {ru ? 'Обновлено:' : 'Updated:'} {formatDateTime(freshness?.generated_at || null, ru)}
            </div>
          </div>

          {freshnessError && (
            <div className="mt-3 flex items-center gap-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700">
              <AlertCircle className="w-3.5 h-3.5" />
              <span>{freshnessError}</span>
            </div>
          )}

          <div className="mt-3 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3">
            <div className="rounded-lg border border-gray-100 bg-white p-3">
              <div className="flex items-center gap-2">
                <Database className="w-4 h-4 text-blue-600" />
                <span className="text-xs text-gray-500">{ru ? 'В очереди' : 'In Queue'}</span>
              </div>
              <div className="mt-2 text-sm text-gray-900" style={{ fontWeight: 700 }}>
                {queueAiItems.toLocaleString()}
              </div>
              <div className="mt-1 text-[11px] text-gray-500">
                {ru ? 'AI задач' : 'AI items'} · {queueGraphPosts.toLocaleString()} {ru ? 'постов в Neo4j sync' : 'Neo4j posts pending'}
              </div>
            </div>

            <div className="rounded-lg border border-gray-100 bg-white p-3">
              <div className="flex items-center gap-2">
                <Brain className="w-4 h-4 text-violet-600" />
                <span className="text-xs text-gray-500">{ru ? 'AI обработка' : 'AI Processed'}</span>
              </div>
              <div className="mt-2 text-sm text-gray-900" style={{ fontWeight: 700 }}>
                {aiLastRun.toLocaleString()}
              </div>
              <div className="mt-1 text-[11px] text-gray-500">
                {ru ? 'за последний цикл' : 'in last run'} · {aiRatePerHour ? `${aiRatePerHour.toLocaleString()} ${ru ? 'в час' : '/hour'}` : (ru ? 'скорость считается...' : 'rate calculating...')}
              </div>
            </div>

            <div className="rounded-lg border border-gray-100 bg-white p-3">
              <div className="flex items-center gap-2">
                <Network className="w-4 h-4 text-cyan-600" />
                <span className="text-xs text-gray-500">{ru ? 'Neo4j синхронизация' : 'Neo4j Synced'}</span>
              </div>
              <div className="mt-2 text-sm text-gray-900" style={{ fontWeight: 700 }}>
                {syncedLastRun.toLocaleString()}
              </div>
              <div className="mt-1 text-[11px] text-gray-500">
                {ru ? 'за последний цикл' : 'in last run'} · {syncRatePerHour ? `${syncRatePerHour.toLocaleString()} ${ru ? 'в час' : '/hour'}` : (ru ? 'скорость считается...' : 'rate calculating...')}
              </div>
            </div>

            <div className="rounded-lg border border-gray-100 bg-white p-3">
              <div className="flex items-center gap-2">
                <Clock className="w-4 h-4 text-amber-600" />
                <span className="text-xs text-gray-500">{ru ? 'Оценка завершения' : 'Estimated Finish'}</span>
              </div>
              <div className="mt-2 text-sm text-gray-900" style={{ fontWeight: 700 }}>
                {formatDuration(etaTotal, ru)}
              </div>
              <div className="mt-1 text-[11px] text-gray-500">
                {ru ? 'Надежность оценки:' : 'Confidence:'} {etaConfidence}
              </div>
            </div>
          </div>

          <div className="mt-3 text-[11px] text-gray-500 flex items-center gap-3 flex-wrap">
            <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full ${operationalTone.badge}`}>
              <span className={`w-1.5 h-1.5 rounded-full ${operationalTone.dot}`} />
              {operationalLabel(operational?.status, ru)}
            </span>
            <span className="text-gray-500">
              {ru ? 'Последний scrape:' : 'Last scrape:'} {formatDateTime(scrapeStage?.last_scrape_at || null, ru)}
            </span>
            <span className="text-gray-500">
              {ru ? 'Последний AI:' : 'Last AI:'} {formatDateTime(processStage?.last_process_at || null, ru)}
            </span>
            <span className="text-gray-500">
              {ru ? 'Последний Neo4j sync:' : 'Last Neo4j sync:'} {formatDateTime(syncStage?.last_graph_sync_at || null, ru)}
            </span>
            {dataLooksDelayed && (operational?.status || 'healthy') !== 'critical' && (
              <span className="text-gray-500">
                {ru ? 'Данные могут быть не самыми свежими' : 'Data may be delayed'}
              </span>
            )}
            {scrapeStage?.status === 'paused_by_backpressure' && (
              <span className="text-amber-700" style={{ fontWeight: 500 }}>
                {ru ? 'Скрапинг на паузе из-за backpressure' : 'Scrape paused by backpressure'}
              </span>
            )}
            {(syncStage?.status === 'caught_up' || syncStage?.status === 'idle') && (
              <span className="text-emerald-700" style={{ fontWeight: 500 }}>
                {ru ? 'Neo4j синхронизация в актуальном состоянии' : 'Neo4j sync is caught up'}
              </span>
            )}
            {operationalStatus === 'critical' && operational?.reason && (
              <span className={`${operationalTone.text}`} style={{ fontWeight: 500 }}>
                {operational.reason}
              </span>
            )}
            <span className="text-gray-500">
              {ru ? 'Собрано за цикл:' : 'Scraped last run:'} {scrapeLastRun}
            </span>
            {scheduler?.last_mode && (
              <span className="text-gray-500">{ru ? 'Последний режим:' : 'Last mode:'} {scheduler.last_mode}</span>
            )}
            {scheduler?.running_now && (
              <span className="text-blue-600" style={{ fontWeight: 600 }}>
                {ru ? 'Выполняется активный цикл...' : 'Active cycle in progress...'}
              </span>
            )}
            {freshnessLoading && <RefreshCw className="w-3.5 h-3.5 animate-spin text-gray-400" />}
          </div>
        </div>

        {!!scheduler?.last_error && (
          <div className="mt-3 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
            <span style={{ fontWeight: 500 }}>{ru ? 'Ошибка скрапера:' : 'Scraper error:'}</span> {scheduler.last_error}
          </div>
        )}
      </div>

      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <div className="flex items-center justify-between gap-3 px-4 py-3 border-b border-gray-100 flex-wrap">
          <div className="relative flex-1 min-w-[200px] max-w-sm">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder={ru ? 'Поиск по имени или @username...' : 'Search by name or @username...'}
              className="w-full pl-9 pr-4 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
          </div>

          <div className="flex items-center gap-1">
            {(['all', 'active', 'paused', 'error'] as const).map((s) => (
              <button
                key={s}
                onClick={() => setStatusFilter(s)}
                className={`px-3 py-1.5 rounded-lg text-xs transition-colors ${
                  statusFilter === s ? 'bg-slate-800 text-white' : 'bg-gray-100 text-gray-500 hover:bg-gray-200'
                }`}
                style={{ fontWeight: 500 }}
              >
                {s === 'all'
                  ? ru
                    ? 'Все'
                    : 'All'
                  : s === 'active'
                    ? ru
                      ? 'Активные'
                      : 'Active'
                    : s === 'paused'
                      ? ru
                        ? 'На паузе'
                        : 'Paused'
                      : ru
                        ? 'Ошибки'
                        : 'Errors'}
              </button>
            ))}
          </div>

          <div className="flex items-center gap-2">
            <span className="text-xs text-gray-400">
              {filtered.length} {ru ? 'из' : 'of'} {channels.length}
            </span>
            {someSelected && (
              <button
                onClick={handleBulkDisable}
                disabled={busy}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs text-red-600 bg-red-50 hover:bg-red-100 transition-colors disabled:opacity-50"
                style={{ fontWeight: 500 }}
              >
                <Trash2 className="w-3.5 h-3.5" />
                {ru ? `Отключить (${selectedIds.size})` : `Disable (${selectedIds.size})`}
              </button>
            )}
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-gray-100">
                <th className="w-10 px-4 py-3">
                  <input
                    type="checkbox"
                    checked={allSelected}
                    ref={(el) => {
                      if (el) el.indeterminate = someSelected && !allSelected;
                    }}
                    onChange={toggleAll}
                    className="w-4 h-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500 cursor-pointer"
                  />
                </th>
                <th className="text-left text-xs text-gray-500 px-3 py-3" style={{ fontWeight: 500 }}>
                  {ru ? 'Источник' : 'Source'}
                </th>
                <th className="text-left text-xs text-gray-500 px-3 py-3 hidden md:table-cell" style={{ fontWeight: 500 }}>
                  {ru ? 'Тип' : 'Type'}
                </th>
                <th className="text-right text-xs text-gray-500 px-3 py-3" style={{ fontWeight: 500 }}>
                  {ru ? 'Участники' : 'Members'}
                </th>
                <th className="text-right text-xs text-gray-500 px-3 py-3 hidden sm:table-cell" style={{ fontWeight: 500 }}>
                  {ru ? 'Сообщ./день' : 'Msgs/day'}
                </th>
                <th className="text-right text-xs text-gray-500 px-3 py-3 hidden lg:table-cell" style={{ fontWeight: 500 }}>
                  {ru ? 'Рост' : 'Growth'}
                </th>
                <th className="text-center text-xs text-gray-500 px-3 py-3" style={{ fontWeight: 500 }}>
                  {ru ? 'Статус' : 'Status'}
                </th>
                <th className="text-right text-xs text-gray-500 px-3 py-3 hidden md:table-cell" style={{ fontWeight: 500 }}>
                  {ru ? 'Обновлено' : 'Updated'}
                </th>
                <th className="w-10 px-3 py-3" />
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={9} className="text-center py-16">
                    <RefreshCw className="w-6 h-6 text-gray-300 mx-auto mb-2 animate-spin" />
                    <p className="text-sm text-gray-500">{ru ? 'Загрузка источников...' : 'Loading sources...'}</p>
                  </td>
                </tr>
              ) : filtered.length === 0 ? (
                <tr>
                  <td colSpan={9} className="text-center py-16">
                    <Radio className="w-8 h-8 text-gray-200 mx-auto mb-2" />
                    <p className="text-sm text-gray-500">{search ? (ru ? 'Ничего не найдено' : 'No sources found') : (ru ? 'Нет добавленных источников' : 'No sources added yet')}</p>
                    {!search && (
                      <button
                        onClick={() => setShowAddModal(true)}
                        className="mt-3 text-xs text-blue-600 hover:text-blue-800 transition-colors"
                        style={{ fontWeight: 500 }}
                      >
                        {ru ? '+ Добавить первый источник' : '+ Add your first source'}
                      </button>
                    )}
                  </td>
                </tr>
              ) : (
                filtered.map((ch) => {
                  const TypeIcon = typeIcons[ch.type] ?? Radio;
                  const isSelected = selectedIds.has(ch.id);
                  return (
                    <tr key={ch.id} className={`border-b border-gray-50 transition-colors ${isSelected ? 'bg-blue-50/40' : 'hover:bg-gray-50'}`}>
                      <td className="px-4 py-3">
                        <input
                          type="checkbox"
                          checked={isSelected}
                          onChange={() => toggleOne(ch.id)}
                          className="w-4 h-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500 cursor-pointer"
                        />
                      </td>
                      <td className="px-3 py-3">
                        <div className="flex items-center gap-3">
                          <div className="w-9 h-9 rounded-full flex items-center justify-center flex-shrink-0" style={{ background: ch.status === 'error' ? '#fef2f2' : 'linear-gradient(135deg, #0ea5e9, #2563eb)' }}>
                            {ch.status === 'error' ? <AlertCircle className="w-4 h-4 text-red-500" /> : <TelegramIcon className="text-white" style={{ width: 18, height: 18 }} />}
                          </div>
                          <div className="min-w-0">
                            <span className="text-sm text-gray-900 block truncate" style={{ fontWeight: 500 }}>{ch.title}</span>
                            <span className="text-xs text-gray-400 block truncate">{ch.username}</span>
                            {ch.description && (
                              <span className="text-xs text-gray-500 block truncate max-w-[340px]">{ch.description}</span>
                            )}
                          </div>
                        </div>
                      </td>
                      <td className="px-3 py-3 hidden md:table-cell">
                        <div className="flex items-center gap-1.5 text-xs text-gray-500">
                          <TypeIcon className="w-3.5 h-3.5" />
                          {typeLabels[ch.type]?.[ru ? 'ru' : 'en'] ?? ch.type}
                        </div>
                      </td>
                      <td className="px-3 py-3 text-right">
                        <span className="text-sm text-gray-900" style={{ fontWeight: 500 }}>
                          {ch.members >= 1000 ? `${(ch.members / 1000).toFixed(1)}K` : ch.members}
                        </span>
                      </td>
                      <td className="px-3 py-3 text-right hidden sm:table-cell">
                        <span className="text-sm text-gray-700">{ch.dailyMessages > 0 ? ch.dailyMessages.toLocaleString() : '—'}</span>
                      </td>
                      <td className="px-3 py-3 text-right hidden lg:table-cell">
                        <span className="text-xs text-gray-400">—</span>
                      </td>
                      <td className="px-3 py-3 text-center">
                        <StatusBadge status={ch.status} ru={ru} />
                      </td>
                      <td className="px-3 py-3 text-right hidden md:table-cell">
                        <div className="flex items-center justify-end gap-1.5 text-xs text-gray-400">
                          {ch.status === 'active' && <RefreshCw className="w-3 h-3 text-emerald-400" />}
                          {ch.status === 'paused' && <Clock className="w-3 h-3 text-amber-400" />}
                          <span>{ch.lastSync}</span>
                        </div>
                      </td>
                      <td className="px-3 py-3">
                        <RowActions
                          channel={ch}
                          ru={ru}
                          onPauseResume={async (id) => {
                            const current = channels.find((it) => it.id === id);
                            if (!current) return;
                            await setChannelActive(id, current.status !== 'active');
                          }}
                          onDisable={async (id) => {
                            await setChannelActive(id, false);
                          }}
                        />
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>

        <div className="flex items-center justify-between px-4 py-3 border-t border-gray-100 bg-gray-50/50">
          <span className="text-xs text-gray-400">
            {ru ? `Показано ${filtered.length} из ${channels.length} источников` : `Showing ${filtered.length} of ${channels.length} sources`}
          </span>
          <span className="text-xs text-gray-400 flex items-center gap-1.5">
            <RefreshCw className="w-3 h-3" />
            {ru ? 'Автосинхронизация каждые 5 мин' : 'Auto-sync every 5 min'}
          </span>
        </div>
      </div>

      <AddChannelModal
        open={showAddModal}
        onClose={() => setShowAddModal(false)}
        ru={ru}
        onSubmit={handleAddSource}
      />
        </>
      )}
    </div>
  );
}
