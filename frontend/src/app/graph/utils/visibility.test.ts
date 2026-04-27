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

  it('adds focused channel-topic context without hiding other topics', () => {
    const visible = buildVisibleGraphData(
      {
        ...graphData,
        nodes: graphData.nodes.map((node) => (
          node.id === 'topic:Border'
            ? { ...node, topChannels: [{ id: 'channel:one', name: 'One', mentions: 5 }] }
            : node
        )),
      },
      'category:Security',
      { sourceDetail: 'standard' },
    );

    expect(visible?.nodes.some((node) => node.id === 'topic:Permits')).toBe(true);
    expect(visible?.links.some((link) => link.type === 'channel-topic-context')).toBe(true);
  });
});
