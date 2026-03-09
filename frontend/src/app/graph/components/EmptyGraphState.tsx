import { Filter, ArrowLeft } from 'lucide-react';

interface EmptyGraphStateProps {
  onQuickSelect?: (channel: string) => void;
  mode?: 'no_selection' | 'no_data';
  timeframe?: string;
  inactiveChannels?: string[];
}

export function EmptyGraphState({ onQuickSelect, mode = 'no_selection', timeframe, inactiveChannels = [] }: EmptyGraphStateProps) {
  const hasNoData = mode === 'no_data';

  const title = hasNoData
    ? 'No Activity in Selected Window'
    : 'Welcome to Your Intelligence Dashboard';

  const description = hasNoData
    ? `Selected channels currently have no matching posts in ${timeframe || 'the selected timeframe'}. Try another time window or adjust channel selection.`
    : 'To visualize community intelligence data and explore channel-topic relationships, please select at least one channel from the filters panel.';

  const hint = hasNoData
    ? inactiveChannels.length > 0
      ? `Inactive now: ${inactiveChannels.slice(0, 3).join(', ')}${inactiveChannels.length > 3 ? ` +${inactiveChannels.length - 3}` : ''}`
      : 'Try widening the timeframe to recover active topics.'
    : 'Select channels from the left sidebar';

  return (
    <div className="w-full h-full flex items-center justify-center" style={{ background: 'linear-gradient(135deg, #0a0e1a 0%, #1a1f2e 100%)' }}>
      <div className="max-w-xl mx-auto px-8 text-center">
        {/* Icon */}
        <div className="w-24 h-24 mx-auto mb-8 rounded-3xl bg-gradient-to-br from-cyan-500/20 to-cyan-600/10 border border-cyan-500/30 flex items-center justify-center backdrop-blur-xl shadow-lg shadow-cyan-500/20">
          <Filter className="w-12 h-12 text-cyan-400" />
        </div>

        {/* Heading */}
        <h2 className="text-4xl font-bold text-white mb-4">
          {title}
        </h2>

        {/* Description */}
        <p className="text-white/60 text-lg leading-relaxed mb-8">
          {description}
        </p>

        {/* Pointer to filters */}
        <div className="inline-flex items-center gap-3 px-6 py-4 rounded-2xl bg-cyan-500/10 border border-cyan-500/30 backdrop-blur-xl shadow-lg shadow-cyan-500/10">
          <ArrowLeft className="w-5 h-5 text-cyan-400 animate-pulse" />
          <div className="text-left">
            <p className="text-white font-medium text-sm">Get Started</p>
            <p className="text-white/60 text-xs">{hint}</p>
          </div>
        </div>
      </div>
    </div>
  );
}
