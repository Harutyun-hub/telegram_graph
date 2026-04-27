import { ChevronDown, ChevronUp, Info } from 'lucide-react';
import { useState } from 'react';
import { getNodeColors, getNodeLabel, type NodeType } from '@/app/graph/utils/nodeColors';

const TYPES: NodeType[] = ['category', 'topic', 'channel'];

const sentimentLegend = [
  { label: 'Positive', color: '#4ade80' },
  { label: 'Neutral', color: '#cbd5e1' },
  { label: 'Negative', color: '#fb7185' },
  { label: 'Urgent', color: '#f97316' },
];

const signalLegend = [
  { label: 'Ask', color: '#60a5fa' },
  { label: 'Need', color: '#34d399' },
  { label: 'Fear', color: '#f87171' },
];

export function GraphLegend() {
  const [isCollapsed, setIsCollapsed] = useState(true);

  return (
    <div className="absolute bottom-4 left-1/2 -translate-x-1/2 bg-slate-950/55 backdrop-blur-xl border border-white/10 rounded-xl shadow-2xl z-40 overflow-hidden">
      <div
        className="px-3 py-1.5 border-b border-white/10 flex items-center justify-between cursor-pointer hover:bg-white/5 transition-colors"
        onClick={() => setIsCollapsed((value) => !value)}
      >
        <div className="flex items-center gap-2">
          <div className="text-white/90 text-[11px] font-medium">Legend</div>
          <Info className="w-3 h-3 text-white/40" />
        </div>
        <button className="w-4.5 h-4.5 rounded-md bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors">
          {isCollapsed ? (
            <ChevronUp className="w-2.5 h-2.5 text-white/70" />
          ) : (
            <ChevronDown className="w-2.5 h-2.5 text-white/70" />
          )}
        </button>
      </div>

      {!isCollapsed && (
        <div className="px-3 py-2.5 flex gap-4">
          <div className="space-y-2 min-w-[180px]">
            <div className="text-white/60 text-[10px] uppercase tracking-wider font-medium">Nodes</div>
            <div className="space-y-1.5">
              {TYPES.map((type) => {
                const color = getNodeColors(type);
                return (
                  <div key={type} className="flex items-center gap-2">
                    <div
                      className="w-3 h-3 rounded-full border"
                      style={{
                        backgroundColor: color.core,
                        borderColor: color.edge,
                        boxShadow: `0 0 10px ${color.glow}`,
                      }}
                    />
                    <span className="text-white/80 text-[11px]">{getNodeLabel(type)}</span>
                  </div>
                );
              })}
            </div>
          </div>

          <div className="w-px bg-white/10" />

          <div className="space-y-2 min-w-[160px]">
            <div className="text-white/60 text-[10px] uppercase tracking-wider font-medium">Topic Ring</div>
            <div className="space-y-1.5">
              {sentimentLegend.map((item) => (
                <div key={item.label} className="flex items-center gap-2">
                  <div className="w-3 h-3 rounded-full border-2 border-white/20" style={{ backgroundColor: item.color }} />
                  <span className="text-white/80 text-[11px]">{item.label}</span>
                </div>
              ))}
            </div>
          </div>

          <div className="w-px bg-white/10" />

          <div className="space-y-2 min-w-[150px]">
            <div className="text-white/60 text-[10px] uppercase tracking-wider font-medium">Topic Badges</div>
            <div className="space-y-1.5">
              {signalLegend.map((item) => (
                <div key={item.label} className="flex items-center gap-2">
                  <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: item.color }} />
                  <span className="text-white/80 text-[11px]">{item.label}</span>
                </div>
              ))}
            </div>
            <p className="text-white/40 text-[9px] leading-relaxed pt-1 border-t border-white/10">
              Topics and categories are sized by importance. Channels connect to the topics they share.
            </p>
          </div>
        </div>
      )}
    </div>
  );
}
