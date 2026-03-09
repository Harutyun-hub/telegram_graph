import { useEffect, useMemo, useRef, useState } from 'react';
import { X, TrendingUp, Loader2, Lightbulb, Network, Compass } from 'lucide-react';
import { getNodeDetails, NodeDetails } from '@/app/graph/services/api';
import { getNodeColors, getNodeLabel, NodeType } from '@/app/graph/utils/nodeColors';

interface NodeInspectorProps {
  node?: any;
  filters?: {
    channels?: string[];
    timeframe?: string;
    insightMode?: string;
    sourceProfile?: string;
    confidenceThreshold?: number;
  };
  onClose: () => void;
  embedded?: boolean;
}

export function NodeInspector({ node, filters, onClose, embedded = false }: NodeInspectorProps) {
  const [nodeDetails, setNodeDetails] = useState<NodeDetails | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const requestIdRef = useRef(0);

  useEffect(() => {
    if (!node) return;

    const requestId = ++requestIdRef.current;
    let active = true;

    const fetchNodeDetails = async () => {
      try {
        setLoading(true);
        setError(null);
        const details = await getNodeDetails(node.id, node.type, {
          timeframe: filters?.timeframe,
          channels: filters?.channels,
        });
        if (!active || requestId !== requestIdRef.current) return;
        setNodeDetails(details);
      } catch (err) {
        if (!active || requestId !== requestIdRef.current) return;
        console.error('Failed to load node details:', err);
        setError('Failed to load details');
      } finally {
        if (!active || requestId !== requestIdRef.current) return;
        setLoading(false);
      }
    };

    fetchNodeDetails();

    return () => {
      active = false;
    };
  }, [node, filters?.timeframe, filters?.channels]);

  const resolvedType = useMemo(() => {
    const detailType = nodeDetails?.type || node?.type || 'topic';
    return (detailType === 'brand' ? 'channel' : detailType) as NodeType;
  }, [nodeDetails?.type, node?.type]);

  const colors = getNodeColors(resolvedType);

  const insightModeLabel = useMemo(() => {
    switch (filters?.insightMode) {
      case 'ownership':
        return 'Who Owns What';
      case 'messageFit':
        return 'Message Fit';
      case 'competitorMoves':
        return 'Competitor Moves';
      case 'opportunities':
        return 'Hidden Opportunities';
      default:
        return 'Market Map';
    }
  }, [filters?.insightMode]);

  const sourceProfileLabel = useMemo(() => {
    switch (filters?.sourceProfile) {
      case 'performance':
        return 'Performance';
      case 'brandStrategy':
        return 'Channel Strategy';
      default:
        return 'Balanced';
    }
  }, [filters?.sourceProfile]);

  const formatPct = (value?: number | null) => {
    if (value == null || Number.isNaN(Number(value))) return null;
    return `${Math.round(Number(value))}%`;
  };

  const whyThisShown = useMemo(() => {
    if (!node) return '';

    const confidenceText = node?.confidence != null ? `Confidence ${Math.round(Number(node.confidence))}%` : 'Confidence not available';
    const scoreText = node?.insightScore != null ? `${Math.round(Number(node.insightScore))}` : 'n/a';

    if (resolvedType === 'topic' && filters?.insightMode === 'opportunities') {
      const opportunityScore = node?.opportunityScore != null ? Math.round(Number(node.opportunityScore)) : null;
      const evidenceCount = Number(node?.opportunityEvidenceCount || 0);
      const activeDays = Number(node?.opportunityActiveDays || 0);
      const needRate = node?.opportunityNeedRate != null ? Math.round(Number(node.opportunityNeedRate)) : null;
      const competitorRate = node?.opportunityCompetitorRate != null ? Math.round(Number(node.opportunityCompetitorRate)) : null;
      const specificity = node?.opportunitySpecificity != null ? Math.round(Number(node.opportunitySpecificity)) : null;

      return `${node.name} is ranked as a Hidden Opportunity with score ${opportunityScore ?? 'n/a'}. Evidence: ${evidenceCount} unique creatives across ${activeDays} active days, need-fit ${needRate ?? 'n/a'}%, competitor pressure ${competitorRate ?? 'n/a'}%, specificity ${specificity ?? 'n/a'}%. ${confidenceText}.`;
    }

    if (resolvedType === 'topic') {
      const evidenceCount = Number(node?.opportunityEvidenceCount || nodeDetails?.totalAds || 0);
      const activeDays = Number(node?.opportunityActiveDays || 0);
      const needRate = formatPct(node?.opportunityNeedRate);
      const competitorRate = formatPct(node?.opportunityCompetitorRate);
      const ownership = formatPct(node?.opportunityOwnershipRate);
      const channelCoverage = Number(node?.topicChannelCoverage || node?.topicBrandCoverage || nodeDetails?.channels?.length || nodeDetails?.brands?.length || 0);
      const momentum = node?.opportunityMomentum != null ? `${Math.round(Number(node.opportunityMomentum))}%` : null;

      if (filters?.insightMode === 'ownership') {
        return `${node.name} is shown because it has clear ownership signal in this period. ${channelCoverage} selected channels are active on this topic, ownership concentration is ${ownership || 'n/a'}, and ranking score is ${scoreText}. ${confidenceText}.`;
      }

      if (filters?.insightMode === 'messageFit') {
        return `${node.name} is shown because message-to-customer fit is strong. It has ${evidenceCount} supporting creatives${activeDays ? ` across ${activeDays} active days` : ''}, need-fit ${needRate || 'n/a'}, and ranking score ${scoreText}. ${confidenceText}.`;
      }

      if (filters?.insightMode === 'competitorMoves') {
        return `${node.name} is shown because competitor activity or momentum changed here. Competitor pressure is ${competitorRate || 'n/a'}${momentum ? `, momentum ${momentum}` : ''}, with ranking score ${scoreText}. ${confidenceText}.`;
      }

      return `${node.name} is shown because it is a high-signal market theme connecting ${channelCoverage} selected channels. Evidence depth: ${evidenceCount} creatives${activeDays ? ` across ${activeDays} active days` : ''}. Ranking score ${scoreText}. ${confidenceText}.`;
    }

    if (resolvedType === 'channel') {
      return `${node.name} is included because it is in the active channel scope for ${insightModeLabel}. ${confidenceText}.`;
    }

    return `${node.name} is visible because the active insight tool includes ${getNodeLabel(resolvedType)} in ${insightModeLabel}. ${confidenceText}.`;
  }, [node, resolvedType, insightModeLabel, sourceProfileLabel, filters?.insightMode, nodeDetails]);

  const normalizeStringList = (items: any): string[] => {
    if (!Array.isArray(items)) return [];
    return items
      .map((item) => {
        if (typeof item === 'string') return item;
        if (item && typeof item === 'object') return item.name || item.title || item.topic || item.product || item.channel || item.brand;
        return null;
      })
      .filter((item): item is string => Boolean(item && item.trim() && item !== '(unnamed)' && item !== 'unknown'));
  };

  const relatedChannels = useMemo(() => {
    if (!nodeDetails) return [];
    if (Array.isArray(nodeDetails.channels)) {
      return nodeDetails.channels
        .map((channel: any) => ({
          name: channel.channel || channel.brand || channel.name,
          score: Number(channel.adCount || channel.count || 0),
        }))
        .filter((channel: any) => channel.name);
    }
    if (Array.isArray(nodeDetails.brands)) {
      return nodeDetails.brands
        .map((channel: any) => ({
          name: channel.channel || channel.brand || channel.name,
          score: Number(channel.adCount || channel.count || 0),
        }))
        .filter((channel: any) => channel.name);
    }
    if (Array.isArray(nodeDetails.relatedChannels)) {
      return nodeDetails.relatedChannels
        .map((channel: any) => ({
          name: channel.name,
          score: Number(channel.score || 0),
        }))
        .filter((channel: any) => channel.name);
    }
    if (Array.isArray(nodeDetails.relatedBrands)) {
      return nodeDetails.relatedBrands
        .map((channel: any) => ({
          name: channel.name,
          score: Number(channel.score || 0),
        }))
        .filter((channel: any) => channel.name);
    }
    if (Array.isArray(nodeDetails.related)) {
      return nodeDetails.related
        .filter((item: any) => item.type === 'channel' || item.type === 'brand')
        .map((item: any) => ({ name: item.name, score: 0 }));
    }
    return [];
  }, [nodeDetails, node?.confidence]);

  const relatedTopics = useMemo(() => {
    if (!nodeDetails) return [];
    if (Array.isArray(nodeDetails.topics)) {
      return nodeDetails.topics
        .map((topic: any) => ({
          name: topic.topic || topic.name,
          score: Number(topic.adCount || topic.count || 0),
        }))
        .filter((topic: any) => topic.name);
    }
    if (Array.isArray(nodeDetails.relatedTopics)) {
      return nodeDetails.relatedTopics
        .map((topic: any) => ({
          name: topic.name,
          score: Number(topic.score || 0),
        }))
        .filter((topic: any) => topic.name);
    }
    if (Array.isArray(nodeDetails.related)) {
      return nodeDetails.related
        .filter((item: any) => item.type === 'topic')
        .map((item: any) => ({ name: item.name, score: 0 }));
    }
    return [];
  }, [nodeDetails]);

  const evidenceItems = useMemo(() => {
    if (!nodeDetails) return [];

    if (Array.isArray(nodeDetails.evidence) && nodeDetails.evidence.length > 0) {
      return nodeDetails.evidence.filter((entry: any) => entry?.text).slice(0, 3);
    }

    if (Array.isArray(nodeDetails.channels)) {
      return nodeDetails.channels
        .filter((entry: any) => entry?.adText)
        .map((entry: any) => ({ text: entry.adText, sentiment: entry.sentiment, channel: entry.channel || entry.brand }))
        .slice(0, 3);
    }

    if (Array.isArray(nodeDetails.brands)) {
      return nodeDetails.brands
        .filter((entry: any) => entry?.adText)
        .map((entry: any) => ({ text: entry.adText, sentiment: entry.sentiment, channel: entry.channel || entry.brand }))
        .slice(0, 3);
    }

    return [];
  }, [nodeDetails]);

  const keyMetrics = useMemo(() => {
    if (!nodeDetails) return [];
    const metrics = [] as Array<{ label: string; value: number | string }>;

    if (filters?.insightMode === 'opportunities' && node?.opportunityScore != null) {
      metrics.push({ label: 'Opp. Score', value: `${Math.round(Number(node.opportunityScore))}` });
    }
    if (node?.confidence != null) {
      metrics.push({ label: 'Confidence', value: `${Math.round(Number(node.confidence))}%` });
    }
    if (nodeDetails.totalAds != null) metrics.push({ label: 'Total Ads', value: nodeDetails.totalAds });
    if (nodeDetails.totalMentions != null) metrics.push({ label: 'Mentions', value: nodeDetails.totalMentions });
    if (nodeDetails.degree != null) metrics.push({ label: 'Connections', value: nodeDetails.degree });
    if (nodeDetails.channelCount != null) metrics.push({ label: 'Channels', value: nodeDetails.channelCount });
    else if (nodeDetails.brandCount != null) metrics.push({ label: 'Channels', value: nodeDetails.brandCount });
    if (nodeDetails.topicCount != null) metrics.push({ label: 'Topics', value: nodeDetails.topicCount });

    return metrics.slice(0, 3);
  }, [nodeDetails, node?.confidence, node?.opportunityScore, filters?.insightMode]);

  const opportunityDiagnostics = useMemo(() => {
    if (filters?.insightMode !== 'opportunities' || resolvedType !== 'topic') return null;

    const evidenceCount = Number(node?.opportunityEvidenceCount || 0);
    const activeDays = Number(node?.opportunityActiveDays || 0);
    const needRate = Number(node?.opportunityNeedRate || 0);
    const competitorRate = Number(node?.opportunityCompetitorRate || 0);
    const specificity = Number(node?.opportunitySpecificity || 0);
    const isEligible = Boolean(node?.opportunityEligible);

    return {
      evidenceCount,
      activeDays,
      needRate,
      competitorRate,
      specificity,
      isEligible,
    };
  }, [filters?.insightMode, resolvedType, node]);

  const topicInsightSummary = useMemo(() => {
    if (resolvedType !== 'topic') {
      return nodeDetails?.insight || '';
    }

    const evidenceCount = Number(node?.opportunityEvidenceCount || nodeDetails?.totalAds || 0);
    const activeDays = Number(node?.opportunityActiveDays || 0);
    const needRate = node?.opportunityNeedRate != null ? Math.round(Number(node.opportunityNeedRate)) : null;
    const competitorRate = node?.opportunityCompetitorRate != null ? Math.round(Number(node.opportunityCompetitorRate)) : null;
    const ownership = node?.opportunityOwnershipRate != null ? Math.round(Number(node.opportunityOwnershipRate)) : null;
    const coverage = Number(node?.topicChannelCoverage || node?.topicBrandCoverage || nodeDetails?.channels?.length || nodeDetails?.brands?.length || 0);
    const confidence = node?.confidence != null ? Math.round(Number(node.confidence)) : null;

    if (filters?.insightMode === 'opportunities') {
      if (opportunityDiagnostics?.isEligible) {
        return `${node.name} is a validated opportunity candidate with ${evidenceCount} unique creatives${activeDays ? ` across ${activeDays} days` : ''}, low competitor pressure (${competitorRate ?? 'n/a'}%), and strong customer-need signal (${needRate ?? 'n/a'}%).`;
      }
      return `${node.name} is visible for context, but current evidence is not strong enough to treat it as a reliable hidden opportunity.`;
    }

    if (filters?.insightMode === 'ownership') {
      return `${node.name} matters because ownership is concentrated (${ownership ?? 'n/a'}%) across ${coverage} active channels in the selected scope.`;
    }

    if (filters?.insightMode === 'messageFit') {
      return `${node.name} is retained because message-fit is strong: customer-need signal ${needRate ?? 'n/a'}% with ${evidenceCount} supporting creatives.`;
    }

    if (filters?.insightMode === 'competitorMoves') {
      return `${node.name} is retained because competitor activity is meaningful here (${competitorRate ?? 'n/a'}% pressure), making it relevant for movement tracking.`;
    }

    return `${node.name} is a core market theme with ${evidenceCount} supporting creatives across ${coverage} selected channels${confidence != null ? ` and ${confidence}% confidence` : ''}.`;
  }, [resolvedType, nodeDetails, node, filters?.insightMode, opportunityDiagnostics]);

  const recommendationText = useMemo(() => {
    if (!opportunityDiagnostics) {
      return nodeDetails?.recommendations || '';
    }

    if (!opportunityDiagnostics.isEligible) {
      return 'Do not treat this as a strategic opportunity yet. Collect more unique creatives across multiple days and validate against competitor activity before actioning.';
    }

    return 'Run a controlled test for this topic with a concrete product/value proposition angle, then monitor whether competitor pressure increases in the next cycle.';
  }, [opportunityDiagnostics, nodeDetails?.recommendations]);

  const layerSections = useMemo(() => {
    if (!nodeDetails) return [];
    return [
      { label: 'Products', values: normalizeStringList(nodeDetails.products), tone: 'emerald' },
      { label: 'Audiences', values: normalizeStringList(nodeDetails.audiences), tone: 'purple' },
      { label: 'Pain Points', values: normalizeStringList(nodeDetails.painPoints), tone: 'red' },
      { label: 'Value Props', values: normalizeStringList(nodeDetails.valueProps), tone: 'yellow' },
      { label: 'Intents', values: normalizeStringList(nodeDetails.intents), tone: 'indigo' },
      { label: 'Competitors', values: normalizeStringList(nodeDetails.competitors), tone: 'pink' },
    ].filter((section) => section.values.length > 0);
  }, [nodeDetails]);

  if (!node) return null;

  const containerClass = embedded
    ? 'h-full bg-transparent rounded-none shadow-none flex flex-col overflow-hidden'
    : 'absolute right-4 top-4 bottom-4 w-96 bg-slate-950/40 backdrop-blur-xl border border-white/10 rounded-2xl shadow-2xl flex flex-col z-40 overflow-hidden';

  return (
    <div className={containerClass}>
      <div className="px-6 py-4 border-b border-white/10 flex items-center justify-between">
        <div className="flex items-center gap-3 min-w-0">
          <div
            className="w-3 h-3 rounded-full shadow-lg flex-shrink-0"
            style={{ backgroundColor: colors.core, boxShadow: `0 0 14px ${colors.glow}` }}
          />
          <div className="min-w-0">
            <h2 className="text-white/90 font-semibold truncate">{node.name}</h2>
            <p className="text-white/45 text-xs uppercase tracking-wide">{getNodeLabel(resolvedType)}</p>
          </div>
        </div>
        <button
          onClick={onClose}
          className="w-8 h-8 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors"
        >
          <X className="w-4 h-4 text-white/70" />
        </button>
      </div>

      <div className="flex-1 overflow-y-auto px-6 py-4 space-y-6">
        {loading ? (
          <div className="flex flex-col items-center justify-center py-12 gap-3">
            <Loader2 className="w-8 h-8 text-cyan-500 animate-spin" />
            <p className="text-white/60 text-sm">Loading details...</p>
          </div>
        ) : error ? (
          <div className="flex flex-col items-center justify-center py-12 gap-3">
            <div className="w-12 h-12 rounded-full bg-red-500/10 flex items-center justify-center">
              <span className="text-2xl">⚠️</span>
            </div>
            <p className="text-white/60 text-sm">{error}</p>
          </div>
        ) : nodeDetails ? (
          <>
            {topicInsightSummary && (
              <div className="rounded-xl p-4 border" style={{ background: 'linear-gradient(135deg, rgba(6,182,212,0.12), rgba(15,23,42,0.25))', borderColor: 'rgba(34,211,238,0.3)' }}>
                <div className="flex items-center gap-2 mb-3">
                  <Lightbulb className="w-4 h-4 text-cyan-400" />
                  <span className="text-cyan-400 text-xs font-medium uppercase tracking-wider">Insight</span>
                </div>
                <p className="text-white/80 text-sm leading-relaxed">{topicInsightSummary}</p>
              </div>
            )}

            {opportunityDiagnostics && (
              <div className="bg-amber-500/10 border border-amber-400/30 rounded-xl p-4 space-y-3">
                <div className="text-amber-300 text-sm font-medium">Opportunity Evidence Quality</div>
                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div className="px-2.5 py-2 rounded-lg bg-white/5 border border-white/10 text-white/75">Unique Creatives: <span className="text-white">{opportunityDiagnostics.evidenceCount}</span></div>
                  <div className="px-2.5 py-2 rounded-lg bg-white/5 border border-white/10 text-white/75">Active Days: <span className="text-white">{opportunityDiagnostics.activeDays}</span></div>
                  <div className="px-2.5 py-2 rounded-lg bg-white/5 border border-white/10 text-white/75">Need Fit: <span className="text-white">{Math.round(opportunityDiagnostics.needRate)}%</span></div>
                  <div className="px-2.5 py-2 rounded-lg bg-white/5 border border-white/10 text-white/75">Competitor Pressure: <span className="text-white">{Math.round(opportunityDiagnostics.competitorRate)}%</span></div>
                  <div className="px-2.5 py-2 rounded-lg bg-white/5 border border-white/10 text-white/75 col-span-2">Specificity: <span className="text-white">{Math.round(opportunityDiagnostics.specificity)}%</span></div>
                </div>
                {!opportunityDiagnostics.isEligible && (
                  <p className="text-amber-200/90 text-xs leading-relaxed">
                    This topic is informational but not yet a high-confidence hidden opportunity.
                  </p>
                )}
              </div>
            )}

            <div className="bg-white/5 border border-white/10 rounded-xl p-4 space-y-3">
              <div className="text-white/70 text-sm font-medium">Why This Is Shown</div>
              <p className="text-white/80 text-sm leading-relaxed">{whyThisShown}</p>
              <div className="flex flex-wrap gap-2">
                <span className="px-2.5 py-1 rounded-full bg-cyan-500/20 border border-cyan-500/30 text-cyan-200 text-[11px]">
                  Mode: {insightModeLabel}
                </span>
                <span className="px-2.5 py-1 rounded-full bg-indigo-500/20 border border-indigo-500/30 text-indigo-200 text-[11px]">
                  Source: {sourceProfileLabel}
                </span>
                <span className="px-2.5 py-1 rounded-full bg-emerald-500/20 border border-emerald-500/30 text-emerald-200 text-[11px]">
                  Threshold: {filters?.confidenceThreshold ?? 35}%
                </span>
              </div>
            </div>

            {keyMetrics.length > 0 && (
              <div className="grid grid-cols-3 gap-2">
                {keyMetrics.map((metric) => (
                  <div key={metric.label} className="bg-white/5 border border-white/10 rounded-lg px-3 py-2">
                    <div className="text-white/45 text-[10px] uppercase tracking-wide">{metric.label}</div>
                    <div className="text-white text-lg font-semibold">{metric.value}</div>
                  </div>
                ))}
              </div>
            )}

            {(nodeDetails.category || nodeDetails.segmentType || nodeDetails.severity) && (
              <div className="bg-white/5 border border-white/10 rounded-xl p-4 space-y-2">
                <div className="text-white/70 text-sm font-medium flex items-center gap-2">
                  <Compass className="w-4 h-4 text-white/50" />
                  Node Metadata
                </div>
                {nodeDetails.category && <p className="text-white/75 text-sm">Category: <span className="text-white">{nodeDetails.category}</span></p>}
                {nodeDetails.segmentType && <p className="text-white/75 text-sm">Segment: <span className="text-white">{nodeDetails.segmentType}</span></p>}
                {nodeDetails.severity && <p className="text-white/75 text-sm">Severity: <span className="text-white capitalize">{nodeDetails.severity}</span></p>}
              </div>
            )}

            {nodeDetails.details && (
              <div className="bg-white/5 border border-white/10 rounded-xl p-4">
                <div className="text-white/70 text-sm font-medium mb-2">Description</div>
                <p className="text-white/80 text-sm leading-relaxed">{nodeDetails.details}</p>
              </div>
            )}

            {layerSections.length > 0 && (
              <div className="space-y-3">
                {layerSections.map((section) => (
                  <div key={section.label} className="space-y-2">
                    <div className="text-white/70 text-sm font-medium">{section.label}</div>
                    <div className="flex flex-wrap gap-2">
                      {section.values.slice(0, 10).map((value, index) => (
                        <span
                          key={`${section.label}-${index}`}
                          className={`px-3 py-1.5 rounded-full text-xs border ${
                            section.tone === 'emerald'
                              ? 'bg-emerald-500/20 border-emerald-500/30 text-emerald-300'
                              : section.tone === 'purple'
                                ? 'bg-purple-500/20 border-purple-500/30 text-purple-300'
                                : section.tone === 'red'
                                  ? 'bg-red-500/20 border-red-500/30 text-red-300'
                                  : section.tone === 'yellow'
                                    ? 'bg-yellow-500/20 border-yellow-500/30 text-yellow-300'
                                    : section.tone === 'indigo'
                                      ? 'bg-indigo-500/20 border-indigo-500/30 text-indigo-300'
                                      : 'bg-pink-500/20 border-pink-500/30 text-pink-300'
                          }`}
                        >
                          {value}
                        </span>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            )}

            {relatedChannels.length > 0 && (
              <div className="space-y-3">
                <div className="text-white/70 text-sm font-medium">Related Channels</div>
                <div className="flex flex-wrap gap-2">
                  {relatedChannels.slice(0, 8).map((channel, index) => (
                    <span
                      key={`${channel.name}-${index}`}
                      className="px-3 py-1.5 rounded-full bg-cyan-500/20 border border-cyan-500/30 text-cyan-300 text-xs"
                    >
                      {channel.name}{channel.score > 0 ? ` (${channel.score})` : ''}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {relatedTopics.length > 0 && (
              <div className="space-y-3">
                <div className="text-white/70 text-sm font-medium">Related Topics</div>
                <div className="flex flex-wrap gap-2">
                  {relatedTopics.slice(0, 10).map((topic, index) => (
                    <span
                      key={`${topic.name}-${index}`}
                      className="px-3 py-1.5 rounded-full bg-orange-500/20 border border-orange-500/30 text-orange-300 text-xs"
                    >
                      {topic.name}{topic.score > 0 ? ` (${topic.score})` : ''}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {Array.isArray(nodeDetails.related) && nodeDetails.related.length > 0 && (
              <div className="space-y-3">
                <div className="text-white/70 text-sm font-medium flex items-center gap-2">
                  <Network className="w-4 h-4 text-white/50" />
                  Related Entities
                </div>
                <div className="space-y-2 max-h-48 overflow-y-auto pr-1">
                  {nodeDetails.related.slice(0, 12).map((item: any, index: number) => (
                    <div key={`${item.id}-${index}`} className="bg-white/5 border border-white/10 rounded-lg px-3 py-2.5">
                      <div className="text-white/85 text-sm truncate">{item.name}</div>
                      <div className="text-white/45 text-[11px] uppercase tracking-wide">
                        {getNodeLabel((item.type || 'topic') as NodeType)}
                        {item.relation ? ` • ${item.relation}` : ''}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {recommendationText && (
              <div className="space-y-3">
                <div className="text-white/70 text-sm font-medium">Recommendations</div>
                <div className="bg-white/5 border border-white/10 rounded-xl p-4">
                  <div className="text-white/80 text-sm leading-relaxed whitespace-pre-wrap">{recommendationText}</div>
                </div>
              </div>
            )}

            {evidenceItems.length > 0 && (
              <div className="space-y-3">
                <div className="text-white/70 text-sm font-medium flex items-center gap-2">
                  <TrendingUp className="w-4 h-4 text-emerald-400" />
                  Evidence Samples
                </div>
                <div className="space-y-3">
                  {evidenceItems.map((entry: any, index: number) => (
                    <div key={index} className="bg-white/5 border border-white/10 rounded-xl p-4 hover:bg-white/10 transition-colors">
                      <p className="text-white/80 text-sm leading-relaxed mb-3">"{entry.text}"</p>
                      <div className="flex items-center justify-between gap-3">
                        <span className="text-white/45 text-xs truncate">{entry.channel || entry.publishedAt || 'Graph evidence'}</span>
                        {entry.sentiment && (
                          <span className={`text-xs px-2 py-1 rounded ${
                            entry.sentiment === 'positive'
                              ? 'bg-green-500/20 text-green-300'
                              : entry.sentiment === 'negative'
                                ? 'bg-red-500/20 text-red-300'
                                : 'bg-gray-500/20 text-gray-300'
                          }`}>
                            {entry.sentiment}
                          </span>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </>
        ) : null}
      </div>
    </div>
  );
}
