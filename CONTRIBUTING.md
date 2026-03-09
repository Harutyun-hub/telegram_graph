# Contributing

## Branching

- Branch from `main`
- Use clear names, e.g. `feat/dashboard-cache-hardening`

## Before Opening PR

1. Run backend checks:

```bash
python3 -m compileall api buffer scraper processor ingester
```

2. Run frontend build:

```bash
npm --prefix frontend run build
```

3. Ensure no secrets or local artifacts are staged.

## Commit Style

- Keep commits focused and atomic
- Prefer imperative messages:
  - `Refactor detail page loading to dedicated endpoints`
  - `Add stale-safe fallback for dashboard tier cache`

## Code Standards

- Follow existing module boundaries (`api/queries`, `api/aggregator`, frontend services)
- Avoid adding synthetic/fake fallback data for production paths
- Preserve evidence-first semantics in UI and backend contracts
