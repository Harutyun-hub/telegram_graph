# Frontend (Radar Obshchiny)

React + Vite dashboard frontend for the Radar Obshchiny intelligence platform.

## Tech Stack

- React 18
- TypeScript
- Vite
- Recharts
- Radix UI
- Lucide

## Run

```bash
npm ci
npm run dev
```

Default local URL: `http://127.0.0.1:5173`

## Build

```bash
npm run build
```

## Environment

Copy the example env file:

```bash
cp .env.example .env
```

Key variable:

- `VITE_API_BASE_URL`
  - default: `/api`
  - local explicit example: `http://127.0.0.1:8001/api`

## Deployment Notes

- The production frontend is designed to work behind the Caddy config in [`/Users/harutnahapetyan/Documents/Gemini/Telegram/frontend/Caddyfile`](/Users/harutnahapetyan/Documents/Gemini/Telegram/frontend/Caddyfile).
- `/api/*` requests are still expected to be reverse-proxied to the backend via `BACKEND_URL`.
- This release does not change the frontend deploy contract used by Railway.

## Current Dashboard Behavior

- Dashboard data is bootstrapped through `src/app/contexts/DataContext.tsx`.
- Backend payload normalization lives in `src/app/services/dashboardAdapter.ts`.
- Strategic topic widgets now reflect direct-message mention semantics within the clean 15-day graph window.
- Service Gap Detector uses `serviceGapBriefs` only. It no longer renders fallback-derived service-gap bars.
- If no AI-backed service-gap cards are available, the widget shows a soft `No service gap detected.` state.

## Important Paths

- `src/app/contexts/DataContext.tsx` — dashboard bootstrap and refresh flow
- `src/app/services/dashboardAdapter.ts` — backend-to-UI transformation layer
- `src/app/services/detailData.ts` — topic/channel/audience detail loaders
- `src/app/components/widgets/` — dashboard widget implementations
- `src/app/graph/` — graph feature modules

## QA

```bash
npm run build
```

Keep frontend behavior aligned with backend query semantics and documented release behavior in the root [`/Users/harutnahapetyan/Documents/Gemini/Telegram/README.md`](/Users/harutnahapetyan/Documents/Gemini/Telegram/README.md).
