import type { ReactNode } from 'react';
import { useEffect, useMemo, useState } from 'react';
import { AlertTriangle, BarChart3, CircleHelp, Layers3, Loader2, MessageSquareText, Radio, Sparkles, X } from 'lucide-react';
import { useChannelDetail, useChannelPostsFeed, useTopicDetail, useTopicEvidenceFeed } from '@/app/services/detailData';
import { getNodeDetails } from '@/app/graph/services/api';
import type { GraphData, GraphFilters, GraphNode, NodeDetails } from '@/app/graph/services/types';
import { getNodeColors } from '@/app/graph/utils/nodeColors';

interface NodeInspectorProps {
  node?: GraphNode | null;
  graphData?: GraphData | null;
  filters?: GraphFilters;
  onClose: () => void;
  embedded?: boolean;
}

function formatPct(value?: number | null): string {
  return `${Math.round(Number(value || 0))}%`;
}

function formatDate(value?: string | null): string {
  if (!value) return 'n/a';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date);
}

function SectionTitle({ icon, title }: { icon: ReactNode; title: string }) {
  return (
    <div className="flex items-center gap-2 text-white/70 text-[10px] uppercase tracking-[0.16em]">
      {icon}
      {title}
    </div>
  );
}

function StatCard({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-2xl bg-white/5 border border-white/10 px-3 py-2.5">
      <div className="text-white/45 text-[11px] uppercase tracking-[0.16em]">{label}</div>
      <div className="text-white/90 text-[17px] font-semibold mt-1">{value}</div>
    </div>
  );
}

function MiniTrend({ points }: { points: Array<{ count: number; week: string }> }) {
  const trimmed = points.slice(-10);
  const max = Math.max(1, ...trimmed.map((point) => point.count));
  return (
    <div className="rounded-2xl bg-white/5 border border-white/10 px-3 py-2.5">
      <div className="flex items-end gap-1 h-16">
        {trimmed.map((point) => (
          <div key={point.week} className="flex-1 flex flex-col items-center gap-1">
            <div
              className="w-full rounded-t-md bg-gradient-to-t from-cyan-400/75 to-orange-300/80"
              style={{ height: `${Math.max(8, Math.round((point.count / max) * 100))}%` }}
            />
          </div>
        ))}
      </div>
      <div className="text-white/40 text-[11px] mt-2">Recent trend in the current date window</div>
    </div>
  );
}

function EvidenceCard({
  item,
  compact = false,
}: {
  item: { id: string; channel?: string; author?: string; text: string; timestamp?: string; reactions?: number; replies?: number };
  compact?: boolean;
}) {
  return (
    <div className="rounded-2xl bg-white/5 border border-white/10 px-3 py-2.5">
      <div className="flex items-center justify-between gap-3">
        <div className="text-white/80 text-sm font-medium truncate">{item.channel || item.author || 'Community message'}</div>
        <div className="text-white/35 text-[11px] shrink-0">{formatDate(item.timestamp)}</div>
      </div>
      <p className={`text-white/65 mt-2 ${compact ? 'text-xs' : 'text-sm'} leading-relaxed`}>{item.text}</p>
      <div className="mt-2 flex items-center gap-3 text-[11px] text-white/40">
        <span>{item.reactions || 0} reactions</span>
        <span>{item.replies || 0} replies</span>
      </div>
    </div>
  );
}

export function NodeInspector({ node, graphData, filters, onClose, embedded = false }: NodeInspectorProps) {
  const [proofView, setProofView] = useState<'evidence' | 'questions'>('evidence');
  const [graphNodeDetails, setGraphNodeDetails] = useState<NodeDetails | null>(null);
  const [graphNodeDetailsLoading, setGraphNodeDetailsLoading] = useState(false);
  const [graphNodeDetailsError, setGraphNodeDetailsError] = useState<string | null>(null);

  useEffect(() => {
    setProofView('evidence');
  }, [node?.id]);

  useEffect(() => {
    let cancelled = false;

    if (!node) {
      setGraphNodeDetails(null);
      setGraphNodeDetailsLoading(false);
      setGraphNodeDetailsError(null);
      return () => {
        cancelled = true;
      };
    }

    setGraphNodeDetailsLoading(true);
    setGraphNodeDetailsError(null);

    void getNodeDetails(node.id, node.type, {
      from: filters?.from_date,
      to: filters?.to_date,
      channels: filters?.channels,
      sentiments: filters?.sentiments,
      category: filters?.category,
      signalFocus: filters?.signalFocus,
    })
      .then((details) => {
        if (cancelled) return;
        setGraphNodeDetails(details);
        setGraphNodeDetailsLoading(false);
      })
      .catch((error) => {
        if (cancelled) return;
        setGraphNodeDetails(null);
        setGraphNodeDetailsError(error instanceof Error ? error.message : 'Failed to load node details');
        setGraphNodeDetailsLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [
    filters?.category,
    filters?.channels,
    filters?.from_date,
    filters?.sentiments,
    filters?.signalFocus,
    filters?.to_date,
    node,
  ]);

  const topicActive = node?.type === 'topic';
  const categoryActive = node?.type === 'category';
  const channelActive = node?.type === 'channel';
  const hasGraphScopedFilters = Boolean(
    (filters?.channels?.length || 0) > 0
    || (filters?.sentiments?.length || 0) > 0
    || (filters?.category || '').trim()
    || (filters?.signalFocus && filters.signalFocus !== 'all')
  );

  const topicDetail = useTopicDetail(topicActive ? node?.name || null : null, topicActive ? node?.category || null : null);
  const topicEvidence = useTopicEvidenceFeed(
    topicActive ? node?.name || null : null,
    topicActive ? node?.category || null : null,
    proofView,
    null,
    topicActive && !hasGraphScopedFilters,
  );
  const channelDetail = useChannelDetail(channelActive ? node?.name || null : null);
  const channelPosts = useChannelPostsFeed(channelActive ? node?.name || null : null, channelActive);

  const color = getNodeColors(node?.type || 'topic');

  const categoryTopics = useMemo(() => {
    if (!categoryActive || !graphData || !node) return [];
    return graphData.nodes
      .filter((entry) => entry.type === 'topic' && entry.category === node.name)
      .sort((a, b) => (Number(b.mentionCount || 0) - Number(a.mentionCount || 0)) || a.name.localeCompare(b.name));
  }, [categoryActive, graphData, node]);

  const categoryChannels = useMemo(() => {
    if (!categoryActive) return [];
    if (Array.isArray(graphNodeDetails?.topChannels) && graphNodeDetails.topChannels.length > 0) {
      return graphNodeDetails.topChannels as Array<{ name: string; mentions: number }>;
    }
    const scores = new Map<string, number>();
    categoryTopics.forEach((topic) => {
      (topic.topChannels || []).forEach((channel) => {
        const key = channel.name;
        scores.set(key, (scores.get(key) || 0) + Number(channel.mentions || 0));
      });
    });
    return Array.from(scores.entries())
      .map(([name, mentions]) => ({ name, mentions }))
      .sort((a, b) => b.mentions - a.mentions || a.name.localeCompare(b.name))
      .slice(0, 8);
  }, [categoryActive, categoryTopics]);

  const siblingTopics = useMemo(() => {
    if (!topicActive || !graphData || !node?.category) return [];
    return graphData.nodes
      .filter((entry) => entry.type === 'topic' && entry.category === node.category && entry.id !== node.id)
      .sort((a, b) => (Number(b.mentionCount || 0) - Number(a.mentionCount || 0)) || a.name.localeCompare(b.name))
      .slice(0, 6);
  }, [graphData, node, topicActive]);

  const channelContextTopics = useMemo(() => {
    if (!channelActive) return [];
    if (Array.isArray(graphNodeDetails?.topics) && graphNodeDetails.topics.length > 0) {
      const mentionByName = new Map(
        (channelDetail.data?.topTopics || []).map((topic) => [topic.name, topic.mentions]),
      );
      return graphNodeDetails.topics.map((topic: any) => ({
        ...topic,
        mentions: mentionByName.get(topic.name) || 0,
      }));
    }
    return (channelDetail.data?.topTopics || []).map((topic) => ({
      name: topic.name,
      category: '',
      mentions: topic.mentions,
    }));
  }, [channelActive, channelDetail.data?.topTopics, graphNodeDetails?.topics]);

  if (!node) return null;

  const topicSummary = topicDetail.data;
  const channelSummary = channelDetail.data;
  const categorySummary = categoryActive ? graphNodeDetails : null;
  const categoryEvidence = Array.isArray(categorySummary?.evidence) ? categorySummary.evidence : [];
  const topicOverview = topicSummary?.overview || graphNodeDetails?.overview || null;
  const topicOverviewState = String(topicOverview?.status || 'unavailable').toLowerCase();
  const topicFallbackEvidence = proofView === 'questions'
    ? (
      Array.isArray(graphNodeDetails?.questionEvidence) && graphNodeDetails.questionEvidence.length > 0
        ? graphNodeDetails.questionEvidence
        : (topicSummary?.questionEvidence || [])
    )
    : (
      Array.isArray(graphNodeDetails?.evidence) && graphNodeDetails.evidence.length > 0
        ? graphNodeDetails.evidence
        : (topicSummary?.evidence || [])
    );
  const topicVisibleEvidence = hasGraphScopedFilters
    ? topicFallbackEvidence
    : (topicEvidence.data?.items && topicEvidence.data.items.length > 0)
      ? topicEvidence.data.items
      : topicFallbackEvidence;
  const topicEvidenceHasFallback = topicVisibleEvidence.length > 0;

  return (
    <div className={`h-full overflow-y-auto ${embedded ? '' : 'p-5'}`}>
      <div className="px-4 py-4 space-y-4">
        {!embedded && (
          <div className="flex justify-end">
            <button
              onClick={onClose}
              className="w-9 h-9 rounded-xl bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors"
            >
              <X className="w-4 h-4 text-white/70" />
            </button>
          </div>
        )}

        <div className="rounded-3xl border border-white/10 bg-white/5 px-4 py-3.5">
          <div className="flex items-start gap-3">
            <div
              className="w-11 h-11 rounded-2xl border flex items-center justify-center shrink-0"
              style={{
                backgroundColor: `${color.core}22`,
                borderColor: `${color.edge}66`,
                boxShadow: `0 0 28px ${color.glow}`,
              }}
            >
              {categoryActive ? <Layers3 className="w-5 h-5 text-cyan-100" /> : channelActive ? <Radio className="w-5 h-5 text-slate-200" /> : <Sparkles className="w-5 h-5 text-amber-100" />}
            </div>
            <div className="min-w-0 flex-1">
              <div className="text-white/45 text-[11px] uppercase tracking-[0.18em] capitalize">{node.type}</div>
              <h3 className="text-white text-[19px] font-semibold leading-tight mt-1">{node.name}</h3>
              {'category' in node && node.category && topicActive && (
                <div className="text-white/55 text-sm mt-1">{node.category}</div>
              )}
              {'lastSeen' in node && node.lastSeen && (
                <div className="text-white/35 text-xs mt-2">Last seen {formatDate(node.lastSeen)}</div>
              )}
            </div>
          </div>
        </div>

        {topicActive && (
          <>
            <div className="grid grid-cols-2 gap-3">
              <StatCard label="Mentions" value={node.mentionCount || 0} />
              <StatCard label="Growth" value={formatPct(node.trendPct)} />
              <StatCard label="Source channels" value={node.distinctChannels || 0} />
              <StatCard label="Evidence" value={node.evidenceCount || 0} />
            </div>

            <div className="rounded-2xl bg-cyan-500/10 border border-cyan-400/20 px-4 py-3.5">
              <SectionTitle icon={<MessageSquareText className="w-3.5 h-3.5 text-cyan-300" />} title="Why This Topic Matters" />
              {topicDetail.loading && !topicOverview ? (
                <div className="mt-3 flex items-center gap-3 text-white/60 text-sm">
                  <Loader2 className="w-4 h-4 animate-spin text-cyan-300" />
                  Loading AI topic overview...
                </div>
              ) : topicOverviewState === 'unavailable' ? (
                <div className="mt-3 rounded-2xl border border-white/10 bg-white/5 px-3 py-3 text-sm text-white/65 leading-relaxed">
                  AI overview is not ready yet for this topic in the current trusted window. Evidence and topic context are still available below.
                </div>
              ) : topicOverviewState === 'insufficient_evidence' ? (
                <div className="mt-3 rounded-2xl border border-dashed border-white/10 bg-white/5 px-3 py-3 text-sm text-white/65 leading-relaxed">
                  There is not enough recent grounded evidence yet for a reliable AI overview for this topic.
                </div>
              ) : (topicOverview?.summaryEn || topicOverview?.summaryRu) ? (
                <>
                  <p className="text-white/80 text-sm leading-relaxed mt-3">
                    {topicOverview.summaryEn || topicOverview.summaryRu}
                  </p>
                  {Array.isArray(topicOverview?.signalsEn) && topicOverview.signalsEn.length > 0 && (
                    <div className="mt-3 space-y-2">
                      {topicOverview.signalsEn.slice(0, 3).map((signal: string) => (
                        <div key={signal} className="rounded-xl border border-white/10 bg-white/5 px-3 py-2.5 text-xs text-white/70 leading-relaxed">
                          {signal}
                        </div>
                      ))}
                    </div>
                  )}
                </>
              ) : (
                <div className="mt-3 rounded-2xl border border-white/10 bg-white/5 px-3 py-3 text-sm text-white/65 leading-relaxed">
                  AI overview is temporarily unavailable, but the supporting topic evidence is still shown below.
                </div>
              )}
            </div>

              <div className="rounded-2xl bg-white/5 border border-white/10 px-4 py-3.5">
              <SectionTitle icon={<BarChart3 className="w-3.5 h-3.5 text-cyan-300" />} title="Signal Snapshot" />
              <div className="grid grid-cols-2 gap-2 mt-3">
                <StatCard label="Sentiment" value={node.dominantSentiment || 'Neutral'} />
                <StatCard label="Asks" value={node.askSignalCount || 0} />
                <StatCard label="Needs" value={node.needSignalCount || 0} />
              </div>
              <div className="grid grid-cols-2 gap-2 mt-2">
                <StatCard label="Fear/Urgency" value={node.fearSignalCount || 0} />
                <StatCard label="Sentiment Mix" value={`${node.sentimentPositive || 0}/${node.sentimentNeutral || 0}/${node.sentimentNegative || 0}`} />
              </div>
            </div>

            {topicSummary?.weeklyData?.length ? (
              <div className="space-y-3">
                <SectionTitle icon={<BarChart3 className="w-3.5 h-3.5 text-cyan-300" />} title="Trend" />
                <MiniTrend points={topicSummary.weeklyData} />
              </div>
            ) : null}

            <div className="space-y-3">
              <SectionTitle icon={<Radio className="w-3.5 h-3.5 text-cyan-300" />} title="Top Source Channels" />
              <div className="space-y-2">
                {(node.topChannels || []).slice(0, 6).map((channel) => (
                  <div key={channel.id} className="rounded-2xl bg-white/5 border border-white/10 px-3 py-3 flex items-center justify-between gap-3">
                    <div className="text-white/85 text-sm truncate">{channel.name}</div>
                    <div className="text-white/45 text-xs shrink-0">{channel.mentions} mentions</div>
                  </div>
                ))}
              </div>
            </div>

            {siblingTopics.length > 0 && (
              <div className="space-y-3">
                <SectionTitle icon={<CircleHelp className="w-3.5 h-3.5 text-cyan-300" />} title="Nearby Topics" />
                <div className="space-y-2">
                  {siblingTopics.map((topic) => (
                    <div key={topic.id} className="rounded-2xl bg-white/5 border border-white/10 px-3 py-3 flex items-center justify-between gap-3">
                      <div className="text-white/85 text-sm truncate">{topic.name}</div>
                      <div className="text-white/45 text-xs shrink-0">{topic.mentionCount || 0} mentions</div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <SectionTitle icon={<MessageSquareText className="w-3.5 h-3.5 text-cyan-300" />} title="Evidence" />
                <div className="flex items-center gap-2">
                  <button
                    onClick={() => setProofView('evidence')}
                    className={`rounded-xl px-3 py-1.5 text-xs border transition-colors ${
                      proofView === 'evidence' ? 'bg-white text-slate-950 border-white' : 'bg-white/5 border-white/10 text-white/70'
                    }`}
                  >
                    All
                  </button>
                  <button
                    onClick={() => setProofView('questions')}
                    className={`rounded-xl px-3 py-1.5 text-xs border transition-colors ${
                      proofView === 'questions' ? 'bg-white text-slate-950 border-white' : 'bg-white/5 border-white/10 text-white/70'
                    }`}
                  >
                    Questions
                  </button>
                </div>
              </div>

              {(topicDetail.loading || graphNodeDetailsLoading || (topicEvidence.loading && !hasGraphScopedFilters)) && !topicEvidenceHasFallback ? (
                <div className="rounded-2xl bg-white/5 border border-white/10 px-4 py-4 flex items-center gap-3 text-white/60 text-sm">
                  <Loader2 className="w-4 h-4 animate-spin text-cyan-300" />
                  Loading topic evidence...
                </div>
              ) : topicEvidence.error && !hasGraphScopedFilters && !topicEvidenceHasFallback ? (
                <div className="rounded-2xl bg-rose-500/10 border border-rose-400/20 px-4 py-4 text-rose-100 text-sm">
                  Failed to load evidence for this topic.
                </div>
              ) : !topicEvidenceHasFallback ? (
                <div className="rounded-2xl bg-white/5 border border-white/10 px-4 py-4 text-white/55 text-sm">
                  No grounded {proofView === 'questions' ? 'question evidence' : 'evidence'} is available for this topic in the selected window.
                </div>
              ) : (
                <div className="space-y-3">
                  {topicVisibleEvidence.slice(0, topicEvidence.data?.items?.length || 6).map((item) => (
                    <EvidenceCard key={item.id} item={item} />
                  ))}
                  {topicEvidence.error && !hasGraphScopedFilters && topicEvidenceHasFallback && (
                    <div className="rounded-2xl bg-amber-500/10 border border-amber-400/20 px-4 py-3 text-amber-100 text-xs">
                      Live evidence refresh failed, so the inspector is showing the latest available grounded topic evidence instead.
                    </div>
                  )}
                  {!hasGraphScopedFilters && topicEvidence.data?.hasMore && (
                    <button
                      onClick={() => topicEvidence.loadMore()}
                      disabled={topicEvidence.loadingMore}
                      className="w-full rounded-2xl border border-white/10 bg-white/5 hover:bg-white/10 px-4 py-3 text-sm text-white/80 transition-colors disabled:opacity-60"
                    >
                      {topicEvidence.loadingMore ? 'Loading more evidence...' : 'Load more evidence'}
                    </button>
                  )}
                </div>
              )}
            </div>
          </>
        )}

        {categoryActive && (
          <>
            <div className="grid grid-cols-2 gap-3">
              <StatCard label="Visible topics" value={categorySummary?.topicCount || categoryTopics.length} />
              <StatCard label="Mentions" value={categorySummary?.mentionCount || node.mentionCount || 0} />
              <StatCard label="Growth" value={formatPct(categorySummary?.trendPct ?? node.trendPct)} />
              <StatCard label="Dominant tone" value={String(categorySummary?.dominantSentiment || node.dominantSentiment || 'Neutral')} />
            </div>

            {(categorySummary?.overview?.summaryEn || categorySummary?.overview?.summaryRu) && (
              <div className="rounded-2xl bg-cyan-500/10 border border-cyan-400/20 px-4 py-3.5">
                <SectionTitle icon={<MessageSquareText className="w-3.5 h-3.5 text-cyan-300" />} title="Category Overview" />
                <p className="text-white/80 text-sm leading-relaxed mt-3">
                  {categorySummary?.overview?.summaryEn || categorySummary?.overview?.summaryRu}
                </p>
              </div>
            )}

            <div className="rounded-2xl bg-white/5 border border-white/10 px-4 py-3.5">
              <SectionTitle icon={<Layers3 className="w-3.5 h-3.5 text-cyan-300" />} title="Signal Summary" />
              <div className="grid grid-cols-2 gap-2 mt-3">
                <StatCard label="Asks" value={categorySummary?.askSignalCount || node.askSignalCount || 0} />
                <StatCard label="Needs" value={categorySummary?.needSignalCount || node.needSignalCount || 0} />
                <StatCard label="Fear/Urgency" value={categorySummary?.fearSignalCount || node.fearSignalCount || 0} />
              </div>
            </div>

            <div className="space-y-3">
              <SectionTitle icon={<Sparkles className="w-3.5 h-3.5 text-cyan-300" />} title="Top Topics In This Category" />
              <div className="space-y-2">
                {(Array.isArray(categorySummary?.topTopics) && categorySummary.topTopics.length > 0
                  ? categorySummary.topTopics
                  : categoryTopics.slice(0, 8)).map((topic: any) => (
                  <div key={topic.id || topic.name} className="rounded-2xl bg-white/5 border border-white/10 px-3 py-3 flex items-center justify-between gap-3">
                    <div className="min-w-0">
                      <div className="text-white/90 text-sm truncate">{topic.name}</div>
                      <div className="text-white/45 text-xs mt-1">{topic.dominantSentiment || 'Neutral'} sentiment</div>
                    </div>
                    <div className="text-right shrink-0">
                      <div className="text-white/85 text-sm font-medium">{topic.mentionCount || 0}</div>
                      <div className="text-white/40 text-xs">mentions</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div className="space-y-3">
              <SectionTitle icon={<Radio className="w-3.5 h-3.5 text-cyan-300" />} title="Strongest Channels In View" />
              <div className="space-y-2">
                {categoryChannels.length > 0 ? categoryChannels.map((channel) => (
                  <div key={channel.name} className="rounded-2xl bg-white/5 border border-white/10 px-3 py-3 flex items-center justify-between gap-3">
                    <div className="text-white/90 text-sm truncate">{channel.name}</div>
                    <div className="text-white/45 text-xs shrink-0">{channel.mentions} mentions</div>
                  </div>
                )) : (
                  <div className="rounded-2xl bg-white/5 border border-white/10 px-3 py-3 text-white/50 text-sm">
                    No channel context is visible for this category in the current source detail mode.
                  </div>
                )}
              </div>
            </div>

            <div className="space-y-3">
              <SectionTitle icon={<MessageSquareText className="w-3.5 h-3.5 text-cyan-300" />} title="Supporting Evidence" />
              {graphNodeDetailsLoading ? (
                <div className="rounded-2xl bg-white/5 border border-white/10 px-4 py-4 flex items-center gap-3 text-white/60 text-sm">
                  <Loader2 className="w-4 h-4 animate-spin text-cyan-300" />
                  Loading category evidence...
                </div>
              ) : graphNodeDetailsError ? (
                <div className="rounded-2xl bg-rose-500/10 border border-rose-400/20 px-4 py-4 text-rose-100 text-sm">
                  Failed to load category evidence.
                </div>
              ) : categoryEvidence.length > 0 ? (
                <div className="space-y-3">
                  {categoryEvidence.slice(0, 6).map((item: any) => (
                    <EvidenceCard key={item.id} item={item} compact />
                  ))}
                </div>
              ) : (
                <div className="rounded-2xl bg-white/5 border border-white/10 px-3 py-3 text-white/50 text-sm">
                  No evidence is available for this category in the current window.
                </div>
              )}
            </div>
          </>
        )}

        {channelActive && (
          <>
            <div className="grid grid-cols-2 gap-3">
              <StatCard label="Daily messages" value={channelSummary?.dailyMessages ?? 0} />
              <StatCard label="Growth" value={formatPct(channelSummary?.growth)} />
              <StatCard label="Visible topics" value={channelContextTopics.length} />
              <StatCard label="Members" value={channelSummary?.members ?? 0} />
            </div>

            <div className="space-y-3">
              <SectionTitle icon={<Sparkles className="w-3.5 h-3.5 text-cyan-300" />} title="Topics This Channel Supports" />
              <div className="space-y-2">
                {channelContextTopics.length > 0 ? channelContextTopics.map((topic) => (
                  <div key={topic.id || topic.name} className="rounded-2xl bg-white/5 border border-white/10 px-3 py-3 flex items-center justify-between gap-3">
                    <div className="min-w-0">
                      <div className="text-white/90 text-sm truncate">{topic.name}</div>
                      <div className="text-white/45 text-xs mt-1">{topic.category || 'Mapped through category context'}</div>
                    </div>
                    <div className="text-white/45 text-xs shrink-0">{topic.mentions || topic.mentionCount || 0} mentions</div>
                  </div>
                )) : (
                  <div className="rounded-2xl bg-white/5 border border-white/10 px-3 py-3 text-white/50 text-sm">
                    This channel is currently supporting the graph context quietly at this source detail level.
                  </div>
                )}
              </div>
            </div>

            {channelSummary?.topTopics?.length ? (
              <div className="space-y-3">
                <SectionTitle icon={<BarChart3 className="w-3.5 h-3.5 text-cyan-300" />} title="Top Topics For This Channel" />
                <div className="space-y-2">
                  {channelSummary.topTopics.slice(0, 6).map((topic) => (
                    <div key={topic.name} className="rounded-2xl bg-white/5 border border-white/10 px-3 py-3 flex items-center justify-between gap-3">
                      <div className="text-white/90 text-sm truncate">{topic.name}</div>
                      <div className="text-white/45 text-xs shrink-0">{topic.mentions} mentions</div>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}

            <div className="space-y-3">
              <SectionTitle icon={<MessageSquareText className="w-3.5 h-3.5 text-cyan-300" />} title="Recent Posts" />
              {channelDetail.loading || channelPosts.loading ? (
                <div className="rounded-2xl bg-white/5 border border-white/10 px-4 py-4 flex items-center gap-3 text-white/60 text-sm">
                  <Loader2 className="w-4 h-4 animate-spin text-cyan-300" />
                  Loading channel posts...
                </div>
              ) : channelPosts.error ? (
                <div className="rounded-2xl bg-rose-500/10 border border-rose-400/20 px-4 py-4 text-rose-100 text-sm">
                  Failed to load recent posts.
                </div>
              ) : (
                <div className="space-y-3">
                  {(channelPosts.data?.items || channelSummary?.recentPosts || []).slice(0, channelPosts.data?.items?.length || 6).map((item) => (
                    <EvidenceCard key={item.id} item={item} compact />
                  ))}
                  {channelPosts.data?.hasMore && (
                    <button
                      onClick={() => channelPosts.loadMore()}
                      disabled={channelPosts.loadingMore}
                      className="w-full rounded-2xl border border-white/10 bg-white/5 hover:bg-white/10 px-4 py-3 text-sm text-white/80 transition-colors disabled:opacity-60"
                    >
                      {channelPosts.loadingMore ? 'Loading more posts...' : 'Load more posts'}
                    </button>
                  )}
                </div>
              )}
            </div>
          </>
        )}

        {!topicActive && !categoryActive && !channelActive && (
          <div className="rounded-2xl bg-amber-500/10 border border-amber-400/20 px-4 py-4 text-amber-50 text-sm flex items-center gap-3">
            <AlertTriangle className="w-4 h-4 shrink-0" />
            This node type is not part of the conversation-map model.
          </div>
        )}
      </div>
    </div>
  );
}
