import { describe, expect, it } from 'vitest';

import {
  isSnapshotMetaForRequestedRange,
  snapshotKeyForRange,
} from './DataContext';

describe('DataContext dashboard snapshot cache', () => {
  it('uses a new session cache namespace for exact-range dashboard snapshots', () => {
    expect(snapshotKeyForRange('2026-04-13', '2026-04-15')).toBe(
      'radar.dashboard.snapshot.v6:2026-04-13:2026-04-15',
    );
  });

  it('accepts snapshots whose meta matches the exact requested range', () => {
    expect(
      isSnapshotMetaForRequestedRange(
        {
          requestedFrom: '2026-04-13',
          requestedTo: '2026-04-15',
        },
        '2026-04-13',
        '2026-04-15',
      ),
    ).toBe(true);
  });

  it('rejects snapshots whose meta points at a different range', () => {
    expect(
      isSnapshotMetaForRequestedRange(
        {
          requestedFrom: '2026-04-14',
          requestedTo: '2026-04-15',
        },
        '2026-04-13',
        '2026-04-15',
      ),
    ).toBe(false);
  });
});
