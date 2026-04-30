import { describe, expect, it } from 'vitest';

import { getNodeSize } from './nodeColors';

describe('getNodeSize', () => {
  it('keeps topics smaller than categories while scaling by mentions', () => {
    const smallTopic = getNodeSize('topic', 4);
    const largeTopic = getNodeSize('topic', 100);
    const category = getNodeSize('category', 4);

    expect(largeTopic).toBeGreaterThan(smallTopic);
    expect(largeTopic).toBeLessThan(category);
  });
});
