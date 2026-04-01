import { Compass } from 'lucide-react';

interface EmptyGraphStateProps {
  hasFilters?: boolean;
}

export function EmptyGraphState({ hasFilters = false }: EmptyGraphStateProps) {
  return (
    <div className="w-full h-full flex items-center justify-center" style={{ background: 'linear-gradient(135deg, #09101b 0%, #121c2b 100%)' }}>
      <div className="max-w-xl mx-auto px-8 text-center">
        <div className="w-24 h-24 mx-auto mb-8 rounded-3xl bg-gradient-to-br from-cyan-500/20 to-orange-500/10 border border-cyan-500/25 flex items-center justify-center backdrop-blur-xl shadow-lg shadow-cyan-500/15">
          <Compass className="w-12 h-12 text-cyan-300" />
        </div>

        <h2 className="text-4xl font-bold text-white mb-4">
          {hasFilters ? 'No matching conversation clusters' : 'Conversation map is waiting for data'}
        </h2>

        <p className="text-white/60 text-lg leading-relaxed mb-8">
          {hasFilters
            ? 'Try widening sentiment, source, or topic-size filters to bring more topics back into view.'
            : 'As soon as the graph has visible categories and topics for the selected window, they will appear here with source channels around them.'}
        </p>

        <div className="inline-flex items-center gap-3 px-6 py-4 rounded-2xl bg-cyan-500/10 border border-cyan-500/25 backdrop-blur-xl shadow-lg shadow-cyan-500/10">
          <div className="text-left">
            <p className="text-white font-medium text-sm">Graph reading order</p>
            <p className="text-white/60 text-xs">Category &rarr; Topic &rarr; Evidence &rarr; Source Channel</p>
          </div>
        </div>
      </div>
    </div>
  );
}
