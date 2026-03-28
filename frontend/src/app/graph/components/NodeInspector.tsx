import type { ReactNode } from 'react';
import { useEffect, useMemo, useState } from 'react';
import { AlertTriangle, BarChart3, CircleHelp, Layers3, Loader2, MessageSquareText, Radio, Sparkles, X } from 'lucide-react';
import { useChannelDetail, useChannelPostsFeed, useTopicDetail, useTopicEvidenceFeed } from '@/app/services/detailData';
import type { GraphData, GraphFilters, GraphNode } from '@/app/graph/services/types';
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
    <div className="flex items-center gap-2 text-white/70 text-[11px] uppercase tracking-[0.18em]">
      {icon}
      {title}
    </div>
  );
}

function StatCard({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-2xl bg-white/5 border border-white/10 px-3 py-3">
      <div className="text-white/45 text-[11px] uppercase tracking-[0.16em]">{label}</div>
      <div className="text-white/90 text-lg font-semibold mt-1">{value}</div>
    </div>
  );
}

function MiniTrend({ points }: { points: Array<{ count: number; week: string }> }) {
  const trimmed = points.slice(-10);
  const max = Math.max(1, ...trimmed.map((point) => point.count));
  return (
    <div className="rounded-2xl bg-white/5 border border-white/10 px-3 py-3">
      <div className="flex items-end gap-1 h-20">
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
    <div className="rounded-2xl bg-white/5 border border-white/10 px-3 py-3">
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

  useEffect(() => {
    setProofView('evidence');
  }, [node?.id]);

  const topicActive = node?.type === 'topic';
  const categoryActive = node?.type === 'category';
  const channelActive = node?.type === 'channel';

  const topicDetail = useTopicDetail(topicActive ? node?.name || null : null, topicActive ? node?.category || null : null);
  const topicEvidence = useTopicEvidenceFeed(
    topicActive ? node?.name || null : null,
    topicActive ? node?.category || null : null,
    proofView,
    null,
    topicActive,
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
    if (!channelActive || !graphData || !node) return [];
    const topicIds = new Set(
      graphData.links
        .filter((link) => link.type === 'channel-topic')
        .flatMap((link) => {
          const sourceId = typeof link.source === 'string' ? link.source : link.source.id;
          const targetId = typeof link.target === 'string' ? link.target : link.target.id;
          if (sourceId === node.id) return [targetId];
          if (targetId === node.id) return [sourceId];
          return [];
        }),
    );
    return graphData.nodes
      .filter((entry) => entry.type === 'topic' && topicIds.has(entry.id))
      .sort((a, b) => (Number(b.mentionCount || 0) - Number(a.mentionCount || 0)) || a.name.localeCompare(b.name))
      .slice(0, 8);
  }, [channelActive, graphData, node]);

  if (!node) return null;

  const topicSummary = topicDetail.data;
  const channelSummary = channelDetail.data;

  return (
    <div className={`h-full overflow-y-auto ${embedded ? '' : 'p-5'}`}>
      <div className="px-5 py-5 space-y-5">
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

        <div className="rounded-3xl border border-white/10 bg-white/5 px-4 py-4">
          <div className="flex items-start gap-3">
            <div
              className="w-12 h-12 rounded-2xl border flex items-center justify-center shrink-0"
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
              <h3 className="text-white text-xl font-semibold leading-tight mt-1">{node.name}</h3>
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

            {(topicSummary?.overview?.summaryEn || topicSummary?.overview?.summaryRu) && (
              <div className="rounded-2xl bg-cyan-500/10 border border-cyan-400/20 px-4 py-4">
                <SectionTitle icon={<MessageSquareText className="w-3.5 h-3.5 text-cyan-300" />} title="Why This Topic Matters" />
                <p className="text-white/80 text-sm leading-relaxed mt-3">
                  {topicSummary.overview?.summaryEn || topicSummary.overview?.summaryRu}
                </p>
              </div>
            )}

            <div className="rounded-2xl bg-white/5 border border-white/10 px-4 py-4">
              <SectionTitle icon={<BarChart3 className="w-3.5 h-3.5 text-cyan-300" />} title="Signal Snapshot" />
              <div className="grid grid-cols-3 gap-2 mt-3">
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

              {topicDetail.loading || topicEvidence.loading ? (
                <div className="rounded-2xl bg-white/5 border border-white/10 px-4 py-5 flex items-center gap-3 text-white/60 text-sm">
                  <Loader2 className="w-4 h-4 animate-spin text-cyan-300" />
                  Loading topic evidence...
                </div>
              ) : topicEvidence.error ? (
                <div className="rounded-2xl bg-rose-500/10 border border-rose-400/20 px-4 py-4 text-rose-100 text-sm">
                  Failed to load evidence for this topic.
                </div>
              ) : (
                <div className="space-y-3">
                  {(topicEvidence.data?.items || topicSummary?.evidence || []).slice(0, topicEvidence.data?.items?.length || 6).map((item) => (
                    <EvidenceCard key={item.id} item={item} />
                  ))}
                  {topicEvidence.data?.hasMore && (
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
              <StatCard label="Visible topics" value={categoryTopics.length} />
              <StatCard label="Mentions" value={node.mentionCount || 0} />
              <StatCard label="Growth" value={formatPct(node.trendPct)} />
              <StatCard label="Dominant tone" value={node.dominantSentiment || 'Neutral'} />
            </div>

            <div className="rounded-2xl bg-white/5 border border-white/10 px-4 py-4">
              <SectionTitle icon={<Layers3 className="w-3.5 h-3.5 text-cyan-300" />} title="Signal Summary" />
              <div className="grid grid-cols-3 gap-2 mt-3">
                <StatCard label="Asks" value={node.askSignalCount || 0} />
                <StatCard label="Needs" value={node.needSignalCount || 0} />
                <StatCard label="Fear/Urgency" value={node.fearSignalCount || 0} />
              </div>
            </div>

            <div className="space-y-3">
              <SectionTitle icon={<Sparkles className="w-3.5 h-3.5 text-cyan-300" />} title="Top Topics In This Category" />
              <div className="space-y-2">
                {categoryTopics.slice(0, 8).map((topic) => (
                  <div key={topic.id} className="rounded-2xl bg-white/5 border border-white/10 px-3 py-3 flex items-center justify-between gap-3">
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
                  <div key={topic.id} className="rounded-2xl bg-white/5 border border-white/10 px-3 py-3 flex items-center justify-between gap-3">
                    <div className="min-w-0">
                      <div className="text-white/90 text-sm truncate">{topic.name}</div>
                      <div className="text-white/45 text-xs mt-1">{topic.category}</div>
                    </div>
                    <div className="text-white/45 text-xs shrink-0">{topic.mentionCount || 0} mentions</div>
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
                <div className="rounded-2xl bg-white/5 border border-white/10 px-4 py-5 flex items-center gap-3 text-white/60 text-sm">
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
