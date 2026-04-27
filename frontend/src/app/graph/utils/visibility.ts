import type { GraphData, GraphFilters, GraphNode } from '../services/types';

function sourceDetailTopicLinkLimit(sourceDetail?: string): number {
  switch ((sourceDetail || '').toLowerCase()) {
    case 'minimal':
      return 1;
    case 'expanded':
      return 3;
    default:
      return 2;
  }
}

function nodeId(value: string | GraphNode): string {
  return typeof value === 'string' ? value : value.id;
}

export function buildVisibleGraphData(
  data: GraphData | null,
  selectedNodeId?: string | null,
  filters?: GraphFilters,
): GraphData | null {
  if (!data) return null;

  const nodeById = new Map(data.nodes.map((node) => [node.id, node]));
  const selectedNode = selectedNodeId ? nodeById.get(selectedNodeId) || null : null;
  const activeCategory = selectedNode?.type === 'category'
    ? selectedNode.name
    : selectedNode?.type === 'topic'
      ? selectedNode.category || ''
      : (filters?.category || '').trim();

  const visibleNodes = data.nodes;
  const visibleNodeIds = new Set(visibleNodes.map((node) => node.id));
  const visibleLinks = data.links.filter((link) => visibleNodeIds.has(nodeId(link.source)) && visibleNodeIds.has(nodeId(link.target)));

  if (activeCategory) {
    const perTopicLimit = sourceDetailTopicLinkLimit(filters?.sourceDetail);
    const contextLinks = new Map<string, { source: string; target: string; value: number; type: string }>();
    const visibleChannelIds = new Set(visibleNodes.filter((node) => node.type === 'channel').map((node) => node.id));

    visibleNodes
      .filter((node) => node.type === 'topic' && node.category === activeCategory)
      .forEach((topic) => {
        (topic.topChannels || []).slice(0, perTopicLimit).forEach((channel) => {
          if (!visibleChannelIds.has(channel.id)) return;
          const key = `${channel.id}:${topic.id}`;
          contextLinks.set(key, {
            source: channel.id,
            target: topic.id,
            value: Math.max(1, Number(channel.mentions || 1)),
            type: 'channel-topic-context',
          });
        });
      });

    visibleLinks.push(...contextLinks.values());
  }

  return {
    nodes: visibleNodes,
    links: visibleLinks,
    meta: data.meta,
  };
}
