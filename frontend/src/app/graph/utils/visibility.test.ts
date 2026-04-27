import { describe, expect, it } from 'vitest';

import { buildVisibleGraphData } from './visibility';
import type { GraphData } from '../services/types';

const graphData: GraphData = {
  nodes: [
    { id: 'category:Security', name: 'Security', type: 'category', val: 20 },
    { id: 'topic:Border', name: 'Border', type: 'topic', category: 'Security', mentionCount: 20, val: 20 },
    { id: 'topic:Permits', name: 'Permits', type: 'topic', category: 'Services', mentionCount: 10, val: 10 },
    { id: 'channel:one', name: 'One', type: 'channel', val: 5 },
  ],
  links: [
    { source: 'category:Security', target: 'topic:Border', value: 20, type: 'category-topic' },
    { source: 'channel:one', target: 'category:Security', value: 5, type: 'channel-category' },
    { source: 'channel:one', target: 'topic:Border', value: 5, type: 'channel-topic' },
  ],
};

describe('buildVisibleGraphData', () => {
  it('keeps topic nodes visible in the initial graph', () => {
    const visible = buildVisibleGraphData(graphData);

    expect(visible?.nodes.map((node) => node.id)).toEqual([
      'category:Security',
      'topic:Border',
      'topic:Permits',
      'channel:one',
    ]);
    expect(visible?.links.some((link) => link.type === 'category-topic')).toBe(true);
  });

  it('preserves real channel-topic links without adding synthetic click links', () => {
    const visible = buildVisibleGraphData(graphData, 'category:Security', { sourceDetail: 'standard' });

    expect(visible?.nodes.some((node) => node.id === 'topic:Permits')).toBe(true);
    expect(visible?.links.some((link) => link.type === 'channel-topic')).toBe(true);
    expect(visible?.links.some((link) => link.type === 'channel-topic-context')).toBe(false);
  });
});
