import type { GraphFreshnessMeta } from '@/app/graph/services/types';

interface FreshnessBadgeProps {
  freshness?: GraphFreshnessMeta;
}

function formatRelativeMinutes(value?: number | null): string {
  if (value == null || Number.isNaN(Number(value))) return 'n/a';
  const mins = Math.max(0, Number(value));
  if (mins < 60) return `${mins}m`;
  const hours = Math.floor(mins / 60);
  const rem = mins % 60;
  if (hours < 24) return rem > 0 ? `${hours}h ${rem}m` : `${hours}h`;
  const days = Math.floor(hours / 24);
  const remHours = hours % 24;
  return remHours > 0 ? `${days}d ${remHours}h` : `${days}d`;
}

export function FreshnessBadge({ freshness }: FreshnessBadgeProps) {
  if (!freshness) return null;

  const status = String(freshness.status || 'unknown').toLowerCase();
  const tone =
    status === 'healthy'
      ? {
          dot: 'bg-emerald-400',
          text: 'text-emerald-200',
          border: 'border-emerald-500/35',
          bg: 'bg-emerald-500/10',
        }
      : status === 'warning'
        ? {
            dot: 'bg-amber-400',
            text: 'text-amber-200',
            border: 'border-amber-500/35',
            bg: 'bg-amber-500/10',
          }
        : status === 'stale'
          ? {
              dot: 'bg-rose-400',
              text: 'text-rose-200',
              border: 'border-rose-500/35',
              bg: 'bg-rose-500/10',
            }
          : {
              dot: 'bg-slate-300',
              text: 'text-slate-200',
              border: 'border-slate-500/35',
              bg: 'bg-slate-500/10',
            };

  return (
    <div className={`rounded-xl border ${tone.border} ${tone.bg} backdrop-blur-xl px-3 py-2 shadow-xl min-w-64`}>
      <div className="flex items-center justify-between gap-3 text-xs">
        <div className="flex items-center gap-2">
          <span className={`inline-block w-2 h-2 rounded-full ${tone.dot}`} />
          <span className={`uppercase tracking-wide font-semibold ${tone.text}`}>Data Freshness: {status}</span>
        </div>
        <span className="text-white/75 font-medium">Score {freshness.score ?? 'n/a'}</span>
      </div>
      <div className="mt-2 grid grid-cols-2 gap-2 text-[11px] text-white/70">
        <div>Unsynced posts: <span className="text-white/90">{freshness.unsyncedPosts ?? 0}</span></div>
        <div>DB delta: <span className="text-white/90">{formatRelativeMinutes(freshness.latestPostDeltaMinutes)}</span></div>
      </div>
      {freshness.syncEstimated && (
        <p className="mt-1 text-[10px] text-white/50">Graph sync time is estimated from latest Neo4j content timestamp.</p>
      )}
    </div>
  );
}
