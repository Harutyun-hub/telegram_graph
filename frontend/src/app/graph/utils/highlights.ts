import type { GraphData, GraphNode } from '../services/types';

export type GraphHighlightKind = 'top' | 'growth' | 'urgent';

export interface GraphHighlight {
  kind: GraphHighlightKind;
  title: string;
  nodeId: string;
  name: string;
  metric: string;
}

function topicNodes(data?: GraphData | null): GraphNode[] {
  return (data?.nodes || []).filter((node) => node.type === 'topic');
}

function maxBy(topics: GraphNode[], score: (topic: GraphNode) => number): GraphNode | null {
  return topics.reduce<GraphNode | null>((best, topic) => {
    if (!best) return topic;
    const currentScore = score(topic);
    const bestScore = score(best);
    if (currentScore > bestScore) return topic;
    if (currentScore === bestScore && topic.name.localeCompare(best.name) < 0) return topic;
    return best;
  }, null);
}

function formatPct(value?: number): string {
  const rounded = Math.round(Number(value || 0));
  return rounded > 0 ? `+${rounded}%` : `${rounded}%`;
}

export function buildGraphHighlights(data?: GraphData | null): GraphHighlight[] {
  const topics = topicNodes(data);
  if (topics.length === 0) return [];

  const topTopic = maxBy(topics, (topic) => Number(topic.mentionCount || 0));
  const growthTopic = maxBy(topics, (topic) => Number(topic.trendPct || 0));
  const urgentTopic = maxBy(topics, (topic) => Number(topic.fearSignalCount || 0));

  return [
    topTopic && {
      kind: 'top' as const,
      title: 'Top Topic',
      nodeId: topTopic.id,
      name: topTopic.name,
      metric: `${Number(topTopic.mentionCount || 0)} mentions`,
    },
    growthTopic && {
      kind: 'growth' as const,
      title: 'Growing',
      nodeId: growthTopic.id,
      name: growthTopic.name,
      metric: `${formatPct(growthTopic.trendPct)} growth`,
    },
    urgentTopic && {
      kind: 'urgent' as const,
      title: 'Urgent',
      nodeId: urgentTopic.id,
      name: urgentTopic.name,
      metric: `${Number(urgentTopic.fearSignalCount || 0)} urgent`,
    },
  ].filter(Boolean) as GraphHighlight[];
}
