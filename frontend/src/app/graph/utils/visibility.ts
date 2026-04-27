import type { GraphData, GraphFilters, GraphNode } from '../services/types';

function nodeId(value: string | GraphNode): string {
  return typeof value === 'string' ? value : value.id;
}

export function buildVisibleGraphData(
  data: GraphData | null,
  selectedNodeId?: string | null,
  filters?: GraphFilters,
): GraphData | null {
  void selectedNodeId;
  void filters;

  if (!data) return null;

  const visibleNodes = data.nodes;
  const visibleNodeIds = new Set(visibleNodes.map((node) => node.id));
  const visibleLinks = data.links.filter((link) => visibleNodeIds.has(nodeId(link.source)) && visibleNodeIds.has(nodeId(link.target)));

  return {
    nodes: visibleNodes,
    links: visibleLinks,
    meta: data.meta,
  };
}
