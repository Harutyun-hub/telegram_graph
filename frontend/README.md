# Frontend (Radar Obshchiny)

React + Vite dashboard app for community intelligence.

## Tech

- React 18
- TypeScript/TSX source
- Vite
- Recharts, Radix UI, Lucide

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

Copy example env file:

```bash
cp .env.example .env
```

Key variable:

- `VITE_API_BASE_URL` (defaults to `/api`)

Example:

```env
VITE_API_BASE_URL=http://127.0.0.1:8001/api
```

Railway example (frontend and backend on different domains):

```env
VITE_API_BASE_URL=https://your-backend-service.up.railway.app/api
```

## Important Paths

- `src/app/contexts/DataContext.tsx` — main dashboard data bootstrap
- `src/app/services/dashboardAdapter.ts` — backend payload adaptation
- `src/app/services/detailData.ts` — dedicated Topics/Channels/Audience fetch path
- `src/app/pages/` — route-level screens
- `src/app/graph/` — graph feature modules

## Notes

- This frontend is part of the monorepo; backend docs are in root `README.md`.
- Keep API contracts aligned with backend query/aggregator outputs.
