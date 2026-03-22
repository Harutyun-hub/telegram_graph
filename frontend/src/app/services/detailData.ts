import { useCallback, useEffect, useRef, useState } from 'react';
import { apiFetch } from './api';
import type { AudienceMember, ChannelDetail, TopicDetail, TopicEvidence } from '../types/data';
import { useDashboardDateRange } from '../contexts/DashboardDateRangeContext';

const DOW_EN = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const SUMMARY_PAGE_SIZE = 1000;

function asNum(v: any, fallback = 0): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function asStr(v: any, fallback = ''): string {
  if (typeof v === 'string') return v;
  if (v === null || v === undefined) return fallback;
  return String(v);
}

function asArray<T = any>(v: any): T[] {
  return Array.isArray(v) ? (v as T[]) : [];
}

function clamp(n: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, n));
}

function slugify(input: string): string {
  return asStr(input, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'item';
}

function hashColor(seed: string): string {
  const palette = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#ec4899', '#64748b'];
  let hash = 0;
  const s = asStr(seed, 'general');
  for (let i = 0; i < s.length; i += 1) {
    hash = ((hash << 5) - hash) + s.charCodeAt(i);
    hash |= 0;
  }
  return palette[Math.abs(hash) % palette.length];
}

function mapTopicEvidence(rows: any[], prefix: string): TopicEvidence[] {
  return asArray(rows)
    .map((ev: any, idx: number) => {
      const rawType = asStr(ev.type, 'message').toLowerCase();
      const type = rawType === 'post' ? 'message' : rawType;
      return {
        id: asStr(ev.id, `${prefix}-${idx}`),
        type: (type === 'message' || type === 'reply' || type === 'reaction') ? type : 'message',
        author: asStr(ev.author, 'unknown'),
        channel: asStr(ev.channel, 'unknown'),
        text: asStr(ev.text, ''),
        timestamp: asStr(ev.timestamp, ''),
        reactions: Math.max(0, asNum(ev.reactions, 0)),
        replies: Math.max(0, asNum(ev.replies, 0)),
      };
    })
    .filter((ev) => ev.text.length > 0);
}

function toWeeklyTopicData(rows: any[]): { week: string; count: number }[] {
  return asArray(rows)
    .map((row: any) => ({
      year: asNum(row.year, 0),
      week: asNum(row.week, 0),
      count: Math.max(0, asNum(row.count, 0)),
    }))
    .filter((row) => row.week > 0)
    .sort((a, b) => `${a.year}-${a.week}`.localeCompare(`${b.year}-${b.week}`))
    .slice(-6)
    .map((row) => ({ week: `${row.year}-W${String(row.week).padStart(2, '0')}`, count: row.count }));
}

function adaptTopicRow(t: any, i: number): TopicDetail {
  const name = asStr(t.name, `Topic ${i + 1}`);
  const category = asStr(t.category, 'General');
  const evidence = mapTopicEvidence(t.evidence, `${slugify(name)}-ev`).slice(0, 6);
  const questionEvidence = mapTopicEvidence(t.questionEvidence, `${slugify(name)}-qev`).slice(0, 12);
  const topChannelsMap = new Map<string, number>();
  evidence.forEach((ev) => {
    if (!ev.channel) return;
    topChannelsMap.set(ev.channel, (topChannelsMap.get(ev.channel) || 0) + 1);
  });

  return {
    id: `${slugify(name)}-${slugify(category)}`,
    name,
    nameRu: name,
    sourceTopic: name,
    category,
    color: hashColor(category),
    mentions: Math.max(0, asNum(t.mentionCount, asNum(t.postCount, 0) + asNum(t.commentCount, 0))),
    growth: clamp(Math.round(asNum(t.growth7dPct, 0)), -200, 200),
    sentiment: {
      positive: clamp(Math.max(0, asNum(t.sentimentPositive, 0)), 0, 100),
      neutral: clamp(Math.max(0, asNum(t.sentimentNeutral, 0)), 0, 100),
      negative: clamp(Math.max(0, asNum(t.sentimentNegative, 0)), 0, 100),
    },
    weeklyData: toWeeklyTopicData(t.weeklyRows),
    topChannels: Array.from(topChannelsMap.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([channel]) => channel),
    description: '',
    descriptionRu: '',
    evidence,
    questionEvidence: questionEvidence.length > 0 ? questionEvidence : evidence.filter((ev) => ev.text.includes('?')),
  };
}

function adaptTopics(rows: any[]): TopicDetail[] {
  return asArray(rows).map((row: any, i: number) => adaptTopicRow(row, i));
}

function adaptChannelRow(c: any, i: number): ChannelDetail {
  const name = asStr(c.title || c.username, `Channel ${i + 1}`);
  const weeklyMap = new Map<number, number>();
  asArray(c.weeklyRows).forEach((row: any) => {
    const dow = asNum(row.dow, Number.NaN);
    if (!Number.isFinite(dow)) return;
    weeklyMap.set(Math.round(dow), Math.max(0, asNum(row.count, 0)));
  });
  const weeklyData = DOW_EN.map((day, idx) => ({ day, msgs: weeklyMap.get(idx + 1) || 0 }));

  const hourlyMap = new Map<number, number>();
  asArray(c.hourlyRows).forEach((row: any) => {
    const hour = asNum(row.hour, Number.NaN);
    if (!Number.isFinite(hour)) return;
    hourlyMap.set(Math.round(hour), Math.max(0, asNum(row.count, 0)));
  });
  const hourlyData = Array.from({ length: 24 }).map((_, h) => ({ hour: `${String(h).padStart(2, '0')}:00`, msgs: hourlyMap.get(h) || 0 }));

  const topTopics = asArray(c.topTopics)
    .map((t: any) => ({
      name: asStr(t.name, ''),
      mentions: Math.max(0, asNum(t.mentions, 0)),
      pct: clamp(Math.max(0, asNum(t.pct, 0)), 0, 100),
    }))
    .filter((t) => t.name.length > 0)
    .slice(0, 6);

  const messageTypes = asArray(c.messageTypes)
    .map((m: any) => ({ type: asStr(m.type, 'text'), count: Math.max(0, asNum(m.count, 0)), pct: 0 }))
    .slice(0, 6);
  const totalMessageTypeCount = messageTypes.reduce((sum, m) => sum + m.count, 0);

  return {
    id: asStr(c.username, slugify(name)),
    name,
    type: 'General',
    members: Math.max(0, asNum(c.memberCount, 0)),
    dailyMessages: Math.max(0, asNum(c.dailyMessages, Math.round(asNum(c.postCount, 0) / 30))),
    engagement: clamp(Math.round(Math.max(0, asNum(c.avgViews, 0)) / 25), 0, 100),
    growth: clamp(Math.round(asNum(c.growth7dPct, 0)), -200, 300),
    topTopic: asStr(c.topTopic, topTopics[0]?.name || ''),
    description: asStr(c.description, ''),
    weeklyData,
    hourlyData,
    topTopics,
    sentimentBreakdown: {
      positive: clamp(Math.max(0, asNum(c.sentimentPositive, 0)), 0, 100),
      neutral: clamp(Math.max(0, asNum(c.sentimentNeutral, 0)), 0, 100),
      negative: clamp(Math.max(0, asNum(c.sentimentNegative, 0)), 0, 100),
    },
    messageTypes: messageTypes.map((m) => ({
      ...m,
      pct: totalMessageTypeCount > 0 ? clamp(Math.round((m.count / totalMessageTypeCount) * 100), 0, 100) : 0,
    })),
    topVoices: asArray(c.topVoices)
      .map((v: any) => ({ name: asStr(v.name, ''), posts: Math.max(0, asNum(v.posts, 0)), helpScore: clamp(Math.max(0, asNum(v.helpScore, 0)), 0, 100) }))
      .filter((v) => v.name.length > 0)
      .slice(0, 4),
    recentPosts: asArray(c.recentPosts)
      .map((p: any, idx: number) => ({
        id: asStr(p.id, `${slugify(name)}-${idx}`),
        author: asStr(p.author, asStr(c.username, name)),
        text: asStr(p.text, '').slice(0, 220),
        timestamp: asStr(p.timestamp, ''),
        reactions: Math.max(0, asNum(p.reactions, 0)),
        replies: Math.max(0, asNum(p.replies, 0)),
      }))
      .filter((p) => p.text.length > 0)
      .slice(0, 6),
  };
}

function adaptChannels(rows: any[]): ChannelDetail[] {
  return asArray(rows).map((row: any, i: number) => adaptChannelRow(row, i));
}

function adaptAudienceRow(u: any, i: number): AudienceMember {
  const genderRaw = asStr(u.gender, 'Unknown').toLowerCase();
  const gender = genderRaw.includes('female') ? 'Female' : genderRaw.includes('male') ? 'Male' : 'Unknown';
  const topTopics = asArray(u.topTopics)
    .map((t: any) => ({ name: asStr(t.name, ''), count: Math.max(0, asNum(t.count, 0)) }))
    .filter((t) => t.name.length > 0)
    .slice(0, 5);
  const fallbackTopics = topTopics.length > 0 ? topTopics : asArray<string>(u.topics).slice(0, 5).map((name) => ({ name, count: 0 }));
  const userId = asStr(u.userId, `u-${i + 1}`);

  return {
    id: userId,
    username: userId.startsWith('@') ? userId : `@${userId}`,
    displayName: userId,
    gender,
    age: asStr(u.age, 'Unknown'),
    origin: asStr(u.language, 'unknown').toUpperCase(),
    location: '',
    joinedDate: '',
    lastActive: asStr(u.lastSeen, '').slice(0, 10),
    totalMessages: Math.max(0, asNum(u.commentCount, 0)),
    totalReactions: 0,
    helpScore: clamp(Math.round(Math.max(0, asNum(u.commentCount, 0)) * 3), 0, 100),
    interests: fallbackTopics.map((t) => t.name),
    channels: asArray(u.channels)
      .map((ch: any) => ({
        name: asStr(ch.name, ''),
        type: asStr(ch.type, 'General'),
        role: asStr(ch.role, 'Member'),
        messageCount: Math.max(0, asNum(ch.messageCount, 0)),
      }))
      .filter((ch) => ch.name.length > 0)
      .slice(0, 3),
    topTopics: fallbackTopics,
    sentiment: {
      positive: clamp(Math.max(0, asNum(u.sentimentPositive, 0)), 0, 100),
      neutral: clamp(Math.max(0, asNum(u.sentimentNeutral, 0)), 0, 100),
      negative: clamp(Math.max(0, asNum(u.sentimentNegative, 0)), 0, 100),
    },
    activityData: asArray(u.activityData)
      .map((point: any) => ({ week: asStr(point.week, ''), msgs: Math.max(0, asNum(point.msgs, 0)) }))
      .filter((point) => point.week.length > 0)
      .slice(-6),
    recentMessages: asArray(u.recentMessages)
      .map((m: any) => ({
        text: asStr(m.text, '').slice(0, 220),
        channel: asStr(m.channel, ''),
        timestamp: asStr(m.timestamp, ''),
        reactions: Math.max(0, asNum(m.reactions, 0)),
        replies: Math.max(0, asNum(m.replies, 0)),
      }))
      .filter((m) => m.text.length > 0)
      .slice(0, 4),
    persona: asStr(u.role, 'Member'),
    integrationLevel: '',
  };
}

function adaptAudience(rows: any[]): AudienceMember[] {
  return asArray(rows).map((row: any, i: number) => adaptAudienceRow(row, i));
}

function loadSnapshot<T>(key: string): T | null {
  if (typeof window === 'undefined') return null;
  try {
    const raw = window.sessionStorage.getItem(key);
    if (!raw) return null;
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

function saveSnapshot<T>(key: string, value: T): void {
  if (typeof window === 'undefined') return;
  try {
    window.sessionStorage.setItem(key, JSON.stringify(value));
  } catch {
    // ignore storage errors
  }
}

function useCachedResource<T>(
  key: string | null,
  fetcher: () => Promise<T>,
  fallback: T,
  enabled = true,
): { data: T; loading: boolean; hasLiveData: boolean; error: string | null; refresh: () => void } {
  const fallbackRef = useRef(fallback);
  const initial = key ? loadSnapshot<T>(key) : null;
  const [data, setData] = useState<T>(initial ?? fallbackRef.current);
  const [hasLiveData, setHasLiveData] = useState(initial !== null);
  const [loading, setLoading] = useState(enabled && initial === null);
  const [error, setError] = useState<string | null>(null);
  const requestIdRef = useRef(0);

  const refresh = useCallback(() => {
    if (!enabled || !key) {
      setLoading(false);
      setError(null);
      return;
    }
    requestIdRef.current += 1;
    const id = requestIdRef.current;
    setLoading(true);
    setError(null);
    fetcher()
      .then((next) => {
        if (requestIdRef.current !== id) return;
        setData(next);
        setHasLiveData(true);
        saveSnapshot(key, next);
        setLoading(false);
      })
      .catch((err: any) => {
        if (requestIdRef.current !== id) return;
        setError(err?.message ?? 'Failed to load data');
        setLoading(false);
      });
  }, [enabled, fetcher, key]);

  useEffect(() => {
    if (!enabled || !key) {
      setData(fallbackRef.current);
      setHasLiveData(false);
      setLoading(false);
      setError(null);
      return;
    }
    const snapshot = loadSnapshot<T>(key);
    setData(snapshot ?? fallbackRef.current);
    setHasLiveData(snapshot !== null);
    setLoading(snapshot === null);
    setError(null);
  }, [enabled, key]);

  useEffect(() => {
    if (!enabled || !key) return;
    refresh();
  }, [enabled, key, refresh]);

  return { data, loading, hasLiveData, error, refresh };
}

function withDateRange(path: string, from: string, to: string, extra: Record<string, string> = {}): string {
  const params = new URLSearchParams({ from, to, ...extra });
  return `${path}?${params.toString()}`;
}

async function fetchTopicSummaries(from: string, to: string): Promise<TopicDetail[]> {
  const rows = await apiFetch<any[]>(withDateRange('/topics', from, to, {
    page: '0',
    size: String(SUMMARY_PAGE_SIZE),
  }), { timeoutMs: 30_000 });
  return adaptTopics(rows);
}

async function fetchTopicDetailRow(topic: string, category: string, from: string, to: string): Promise<TopicDetail> {
  const row = await apiFetch<any>(withDateRange('/topics/detail', from, to, {
    topic,
    category,
  }), { timeoutMs: 30_000 });
  return adaptTopicRow(row, 0);
}

async function fetchChannelSummaries(from: string, to: string): Promise<ChannelDetail[]> {
  const rows = await apiFetch<any[]>(withDateRange('/channels', from, to), { timeoutMs: 25_000 });
  return adaptChannels(rows);
}

async function fetchChannelDetailRow(channel: string, from: string, to: string): Promise<ChannelDetail> {
  const row = await apiFetch<any>(withDateRange('/channels/detail', from, to, {
    channel,
  }), { timeoutMs: 25_000 });
  return adaptChannelRow(row, 0);
}

async function fetchAudienceSummaries(from: string, to: string): Promise<AudienceMember[]> {
  const rows = await apiFetch<any[]>(withDateRange('/audience', from, to, {
    page: '0',
    size: String(SUMMARY_PAGE_SIZE),
  }), { timeoutMs: 25_000 });
  return adaptAudience(rows);
}

async function fetchAudienceDetailRow(userId: string, from: string, to: string): Promise<AudienceMember> {
  const row = await apiFetch<any>(withDateRange('/audience/detail', from, to, {
    userId,
  }), { timeoutMs: 25_000 });
  return adaptAudienceRow(row, 0);
}

export function useTopicsDetailData() {
  const { range } = useDashboardDateRange();
  const fetcher = useCallback(() => fetchTopicSummaries(range.from, range.to), [range.from, range.to]);
  return useCachedResource<TopicDetail[]>(
    `radar.details.topics.summary.v3:${range.from}:${range.to}`,
    fetcher,
    [],
  );
}

export function useTopicDetail(topic: string | null, category: string | null) {
  const { range } = useDashboardDateRange();
  const enabled = Boolean(topic && category);
  const fetcher = useCallback(() => fetchTopicDetailRow(topic || '', category || '', range.from, range.to), [topic, category, range.from, range.to]);
  const key = enabled ? `radar.details.topic.v3:${range.from}:${range.to}:${topic}:${category}` : null;
  return useCachedResource<TopicDetail | null>(key, fetcher, null, enabled);
}

export function useChannelsDetailData() {
  const { range } = useDashboardDateRange();
  const fetcher = useCallback(() => fetchChannelSummaries(range.from, range.to), [range.from, range.to]);
  return useCachedResource<ChannelDetail[]>(
    `radar.details.channels.summary.v3:${range.from}:${range.to}`,
    fetcher,
    [],
  );
}

export function useChannelDetail(channel: string | null) {
  const { range } = useDashboardDateRange();
  const enabled = Boolean(channel);
  const fetcher = useCallback(() => fetchChannelDetailRow(channel || '', range.from, range.to), [channel, range.from, range.to]);
  const key = enabled ? `radar.details.channel.v3:${range.from}:${range.to}:${channel}` : null;
  return useCachedResource<ChannelDetail | null>(key, fetcher, null, enabled);
}

export function useAudienceDetailData() {
  const { range } = useDashboardDateRange();
  const fetcher = useCallback(() => fetchAudienceSummaries(range.from, range.to), [range.from, range.to]);
  return useCachedResource<AudienceMember[]>(
    `radar.details.audience.summary.v3:${range.from}:${range.to}`,
    fetcher,
    [],
  );
}

export function useAudienceMemberDetail(userId: string | null) {
  const { range } = useDashboardDateRange();
  const enabled = Boolean(userId);
  const fetcher = useCallback(() => fetchAudienceDetailRow(userId || '', range.from, range.to), [userId, range.from, range.to]);
  const key = enabled ? `radar.details.audience-member.v3:${range.from}:${range.to}:${userId}` : null;
  return useCachedResource<AudienceMember | null>(key, fetcher, null, enabled);
}
