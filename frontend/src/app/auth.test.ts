import { describe, expect, it } from 'vitest';

import { resolveAuthRedirectTarget } from './auth';

describe('resolveAuthRedirectTarget', () => {
  it('returns the preserved path when it is safe', () => {
    expect(
      resolveAuthRedirectTarget({
        from: {
          pathname: '/admin',
          search: '?tab=runtime',
          hash: '#models',
        },
      }),
    ).toBe('/admin?tab=runtime#models');
  });

  it('falls back to root for unsafe redirects', () => {
    expect(resolveAuthRedirectTarget({ from: { pathname: 'https://evil.example' } })).toBe('/');
    expect(resolveAuthRedirectTarget({ from: { pathname: '/login' } })).toBe('/');
  });
});
