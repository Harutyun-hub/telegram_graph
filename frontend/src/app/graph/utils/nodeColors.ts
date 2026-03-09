// Node color mapping for 15 node types
export type NodeType = 
  | 'channel'
  | 'brand' 
  | 'topic' 
  | 'product' 
  | 'audience' 
  | 'painpoint' 
  | 'valueprop' 
  | 'intent' 
  | 'competitor' 
  | 'cta' 
  | 'platform' 
  | 'format' 
  | 'engagement' 
  | 'sentiment' 
  | 'timeperiod';

export interface NodeColors {
  core: string;
  glow: string;
  edge: string;
  darkEdge: string;
}

// Professional color palette with cyber teal & coral theme
const colorPalette: Record<NodeType, NodeColors> = {
  channel: {
    core: '#06b6d4',
    glow: 'rgba(6, 182, 212, 0.6)',
    edge: '#22d3ee',
    darkEdge: '#0e7490',
  },
  brand: {
    core: '#06b6d4',
    glow: 'rgba(6, 182, 212, 0.6)',
    edge: '#22d3ee',
    darkEdge: '#0e7490',
  },
  topic: {
    core: '#f97316',      // Orange-500 - Topics (unchanged)
    glow: 'rgba(249, 115, 22, 0.5)',
    edge: '#fb923c',      // Orange-400
    darkEdge: '#9a3412',  // Orange-800
  },
  product: {
    core: '#10b981',      // Green-500 - Products/Services
    glow: 'rgba(16, 185, 129, 0.5)',
    edge: '#34d399',      // Green-400
    darkEdge: '#065f46',  // Green-800
  },
  audience: {
    core: '#a855f7',      // Purple-500 - Target audiences
    glow: 'rgba(168, 85, 247, 0.5)',
    edge: '#c084fc',      // Purple-400
    darkEdge: '#6b21a8',  // Purple-800
  },
  painpoint: {
    core: '#ef4444',      // Red-500 - Customer pain points
    glow: 'rgba(239, 68, 68, 0.6)',
    edge: '#f87171',      // Red-400
    darkEdge: '#991b1b',  // Red-800
  },
  valueprop: {
    core: '#eab308',      // Yellow-500 - Value propositions
    glow: 'rgba(234, 179, 8, 0.5)',
    edge: '#facc15',      // Yellow-400
    darkEdge: '#854d0e',  // Yellow-800
  },
  intent: {
    core: '#6366f1',      // Indigo-500 - User intent/behavior
    glow: 'rgba(99, 102, 241, 0.5)',
    edge: '#818cf8',      // Indigo-400
    darkEdge: '#3730a3',  // Indigo-800
  },
  competitor: {
    core: '#ec4899',      // Pink-500 - Competitor channels
    glow: 'rgba(236, 72, 153, 0.5)',
    edge: '#f472b6',      // Pink-400
    darkEdge: '#9f1239',  // Pink-800
  },
  cta: {
    core: '#8b5cf6',      // Violet-500 - Call to actions
    glow: 'rgba(139, 92, 246, 0.5)',
    edge: '#a78bfa',      // Violet-400
    darkEdge: '#5b21b6',  // Violet-800
  },
  platform: {
    core: '#06b6d4',      // Cyan-500 - Social platforms (same as channel for consistency)
    glow: 'rgba(6, 182, 212, 0.4)',
    edge: '#67e8f9',      // Cyan-300
    darkEdge: '#164e63',  // Cyan-900
  },
  format: {
    core: '#14b8a6',      // Teal-500 - Ad formats
    glow: 'rgba(20, 184, 166, 0.5)',
    edge: '#5eead4',      // Teal-300
    darkEdge: '#134e4a',  // Teal-900
  },
  engagement: {
    core: '#84cc16',      // Lime-500 - Engagement metrics
    glow: 'rgba(132, 204, 22, 0.5)',
    edge: '#a3e635',      // Lime-400
    darkEdge: '#3f6212',  // Lime-800
  },
  sentiment: {
    core: '#f59e0b',      // Amber-500 - Sentiment data
    glow: 'rgba(245, 158, 11, 0.5)',
    edge: '#fbbf24',      // Amber-400
    darkEdge: '#78350f',  // Amber-900
  },
  timeperiod: {
    core: '#71717a',      // Zinc-500 - Time periods
    glow: 'rgba(113, 113, 122, 0.4)',
    edge: '#a1a1aa',      // Zinc-400
    darkEdge: '#27272a',  // Zinc-800
  },
};

export function getNodeColors(type: NodeType | string): NodeColors {
  // Default to topic color for unknown types
  const validType = colorPalette[type as NodeType] ? (type as NodeType) : 'topic';
  return colorPalette[validType];
}

export function getNodeLabel(type: NodeType | string): string {
  const labels: Record<NodeType, string> = {
    channel: 'Channels',
    brand: 'Channels',
    topic: 'Topics',
    product: 'Products',
    audience: 'Audiences',
    painpoint: 'Pain Points',
    valueprop: 'Value Props',
    intent: 'Intents',
    competitor: 'Competitors',
    cta: 'CTAs',
    platform: 'Platforms',
    format: 'Formats',
    engagement: 'Engagement',
    sentiment: 'Sentiment',
    timeperiod: 'Time Periods',
  };
  return labels[type as NodeType] || type;
}

// Helper to check if a node type should be shown by default
export function isDefaultVisibleType(type: NodeType | string): boolean {
  const defaultVisible = new Set<NodeType>([
    'channel',
    'product',
    'audience',
    'painpoint',
  ]);
  return defaultVisible.has(type as NodeType);
}

// Get node size based on type (relative importance)
export function getNodeSize(type: NodeType | string, connections: number = 0): number {
  const baseSizes: Record<NodeType, number> = {
    channel: 18,
    brand: 18,
    competitor: 16,   // Large - important context
    product: 14,      // Medium-large
    audience: 12,     // Medium
    painpoint: 12,    // Medium
    valueprop: 10,    // Medium-small
    intent: 10,       // Medium-small
    cta: 10,          // Medium-small
    platform: 8,      // Small
    format: 8,        // Small
    engagement: 8,    // Small
    sentiment: 8,     // Small
    topic: 8,         // Small (legacy, less important now)
    timeperiod: 6,    // Smallest
  };
  
  const baseSize = baseSizes[type as NodeType] || 8;
  // Add connection-based scaling (but cap it)
  return baseSize + Math.min(connections * 0.3, 8);
}
