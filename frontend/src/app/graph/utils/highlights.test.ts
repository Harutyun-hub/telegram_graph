import { describe, expect, it } from 'vitest';

import { buildGraphHighlights } from './highlights';
import type { GraphData } from '../services/types';

const graphData: GraphData = {
  nodes: [
    { id: 'topic:Volume', name: 'Volume', type: 'topic', val: 20, mentionCount: 40, trendPct: 10, fearSignalCount: 1 },
    { id: 'topic:Growth', name: 'Growth', type: 'topic', val: 10, mentionCount: 20, trendPct: 80, fearSignalCount: 2 },
    { id: 'topic:Urgent', name: 'Urgent', type: 'topic', val: 8, mentionCount: 12, trendPct: 5, fearSignalCount: 9 },
    { id: 'channel:one', name: 'One', type: 'channel', val: 5 },
  ],
  links: [],
};

describe('buildGraphHighlights', () => {
  it('selects top, growing, and urgent topics from visible graph data', () => {
    const highlights = buildGraphHighlights(graphData);

    expect(highlights.map((highlight) => [highlight.kind, highlight.nodeId])).toEqual([
      ['top', 'topic:Volume'],
      ['growth', 'topic:Growth'],
      ['urgent', 'topic:Urgent'],
    ]);
  });
});
