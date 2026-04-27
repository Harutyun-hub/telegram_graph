export type NodeType = 'category' | 'topic' | 'channel';

export interface NodeColors {
  core: string;
  glow: string;
  edge: string;
  darkEdge: string;
}

const colorPalette: Record<NodeType, NodeColors> = {
  category: {
    core: '#21c56a',
    glow: 'rgba(33, 197, 106, 0.46)',
    edge: '#78f0aa',
    darkEdge: '#0f6d3b',
  },
  topic: {
    core: '#f47b1f',
    glow: 'rgba(244, 123, 31, 0.52)',
    edge: '#ffb160',
    darkEdge: '#913f0f',
  },
  channel: {
    core: '#4c91a7',
    glow: 'rgba(76, 145, 167, 0.22)',
    edge: '#8acbe0',
    darkEdge: '#284a56',
  },
};

export function getNodeColors(type: NodeType | string): NodeColors {
  const resolved = (['category', 'topic', 'channel'] as const).includes(type as NodeType)
    ? (type as NodeType)
    : 'topic';
  return colorPalette[resolved];
}

export function getNodeLabel(type: NodeType | string): string {
  if (type === 'category') return 'Categories';
  if (type === 'channel') return 'Channels';
  return 'Topics';
}

export function getNodeSize(type: NodeType | string, weight: number = 0): number {
  const safeWeight = Math.max(0, Number(weight) || 0);
  if (type === 'category') {
    return 18 + Math.min(Math.sqrt(safeWeight) * 0.72, 16);
  }
  if (type === 'channel') {
    return 8.8;
  }
  return 7 + Math.min(Math.sqrt(safeWeight) * 0.52, 8.5);
}
