import type { SocialAdCard, SocialEvidenceItem } from './socialIntelligence';

export const SOCIAL_ENTITY_COLORS = [
  '#2563eb',
  '#7c3aed',
  '#f59e0b',
  '#059669',
  '#dc2626',
  '#0f766e',
  '#ea580c',
  '#db2777',
];

export function socialEntityColor(index: number): string {
  return SOCIAL_ENTITY_COLORS[index % SOCIAL_ENTITY_COLORS.length];
}

export function formatSocialDateLabel(value: string | null, lang: 'en' | 'ru') {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleDateString(lang === 'ru' ? 'ru-RU' : 'en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  });
}

export function formatSocialDateTime(value: string | null, lang: 'en' | 'ru') {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString(lang === 'ru' ? 'ru-RU' : 'en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export function formatSocialRelativeTime(value: string | null, lang: 'en' | 'ru') {
  if (!value) return lang === 'ru' ? 'Никогда' : 'Never';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '—';
  const diffMs = Date.now() - date.getTime();
  const minutes = Math.floor(diffMs / 60_000);
  if (minutes < 1) return lang === 'ru' ? 'Только что' : 'Just now';
  if (minutes < 60) return lang === 'ru' ? `${minutes} мин назад` : `${minutes} min ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return lang === 'ru' ? `${hours} ч назад` : `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return lang === 'ru' ? `${days} д назад` : `${days}d ago`;
}

export function formatSocialBucket(value: string, lang: 'en' | 'ru') {
  const date = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(lang === 'ru' ? 'ru-RU' : 'en-US', {
    month: 'short',
    day: 'numeric',
    timeZone: 'UTC',
  }).format(date);
}

export function formatSocialPercent(value: number): string {
  return `${Math.round(value)}%`;
}

export function sentimentTone(score: number) {
  if (score >= 0.2) {
    return {
      badge: 'border-emerald-200 bg-emerald-50 text-emerald-700',
      text: 'text-emerald-700',
      fill: '#10b981',
    };
  }
  if (score <= -0.2) {
    return {
      badge: 'border-rose-200 bg-rose-50 text-rose-700',
      text: 'text-rose-700',
      fill: '#f43f5e',
    };
  }
  return {
    badge: 'border-slate-200 bg-slate-50 text-slate-700',
    text: 'text-slate-700',
    fill: '#94a3b8',
  };
}

export function sentimentFill(score: number): string {
  return sentimentTone(score).fill;
}

export function socialPlatformLabel(platform: string, _lang: 'en' | 'ru') {
  if (platform === 'facebook') return 'Facebook Ads';
  if (platform === 'instagram') return 'Instagram';
  if (platform === 'google') return 'Google Ads';
  if (platform === 'tiktok') return 'TikTok';
  return platform;
}

export function socialActivitySummary(item: SocialEvidenceItem | SocialAdCard) {
  const payload = item.analysis?.analysis_payload ?? {};
  return (typeof payload?.summary === 'string' && payload.summary) || item.analysis?.summary || item.text_content || '';
}

export function socialEvidenceText(item: SocialEvidenceItem | SocialAdCard) {
  return item.text_content?.trim() || socialActivitySummary(item);
}

export function socialPayloadList(payload: Record<string, unknown> | null | undefined, key: string): string[] {
  const value = payload?.[key];
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => {
      if (typeof item === 'string') return item.trim();
      if (item && typeof item === 'object') {
        const valueFromObject = (item as Record<string, unknown>).name
          || (item as Record<string, unknown>).claim
          || (item as Record<string, unknown>).label;
        return typeof valueFromObject === 'string' ? valueFromObject.trim() : '';
      }
      return '';
    })
    .filter(Boolean);
}
