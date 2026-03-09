import { ChevronDown, ChevronUp, Info } from 'lucide-react';
import { useMemo, useState } from 'react';
import { getNodeColors, getNodeLabel, NodeType } from '@/app/graph/utils/nodeColors';

interface GraphLegendProps {
  activeLayers?: string[];
  insightMode?: string;
}

const DEFAULT_TYPES: NodeType[] = ['channel', 'topic'];

export function GraphLegend({ activeLayers = ['topic'], insightMode }: GraphLegendProps) {
  const [isCollapsed, setIsCollapsed] = useState(true);

  const visibleTypes = useMemo(() => {
    const requested = activeLayers
      .map((layer) => layer as NodeType)
      .filter(Boolean);

    return Array.from(new Set<NodeType>(['channel', ...DEFAULT_TYPES, ...requested]));
  }, [activeLayers]);

  return (
    <div className="absolute bottom-4 left-1/2 -translate-x-1/2 bg-slate-950/40 backdrop-blur-xl border border-white/10 rounded-xl shadow-2xl z-40 overflow-hidden">
      <div
        className="px-4 py-2 border-b border-white/10 flex items-center justify-between cursor-pointer hover:bg-white/5 transition-colors"
        onClick={() => setIsCollapsed(!isCollapsed)}
      >
        <div className="flex items-center gap-2">
          <div className="text-white/90 text-xs font-medium">Legend</div>
          <Info className="w-3 h-3 text-white/40" />
        </div>
        <button className="w-5 h-5 rounded-md bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors">
          {isCollapsed ? (
            <ChevronUp className="w-2.5 h-2.5 text-white/70" />
          ) : (
            <ChevronDown className="w-2.5 h-2.5 text-white/70" />
          )}
        </button>
      </div>

      {!isCollapsed && (
        <div className="px-4 py-3">
          <div className="flex gap-6">
            <div className="space-y-2 min-w-[220px]">
              <div className="text-white/60 text-[10px] uppercase tracking-wider font-medium">Visible Node Types</div>
              <div className="grid grid-cols-2 gap-x-4 gap-y-1.5">
                {visibleTypes.map((type) => {
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

            <div className="space-y-2">
              <div className="text-white/60 text-[10px] uppercase tracking-wider font-medium">Connection Volume</div>
              <div className="space-y-1.5">
                <div className="flex items-center gap-2">
                  <div className="w-8 h-0.5 bg-white/20 rounded" />
                  <span className="text-white/60 text-[11px]">Low</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-8 h-0.5 bg-white/40 rounded" />
                  <span className="text-white/60 text-[11px]">Medium</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-8 h-1 bg-white/60 rounded" />
                  <span className="text-white/60 text-[11px]">High</span>
                </div>
              </div>
              <div className="pt-1.5 border-t border-white/10">
                <p className="text-white/40 text-[9px] leading-relaxed max-w-[180px]">
                  Topic nodes highlight dominant themes and connector signals; layer nodes reveal deeper drivers behind channel dynamics.
                </p>
                {insightMode === 'opportunities' && (
                  <p className="text-amber-300/90 text-[9px] leading-relaxed mt-2 max-w-[180px]">
                    Diamond-gold nodes indicate hidden opportunities with stronger whitespace potential.
                  </p>
                )}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
