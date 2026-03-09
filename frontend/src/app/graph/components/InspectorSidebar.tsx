import { useState } from 'react';
import { ChevronRight, Info } from 'lucide-react';
import { NodeInspector } from '@/app/graph/components/NodeInspector';

interface InspectorSidebarProps {
  selectedNode?: any;
  filters?: {
    channels?: string[];
    timeframe?: string;
    sentiments?: string[];
    topics?: string[];
    connectionStrength?: number;
    layers?: string[];
    insightMode?: string;
    sourceProfile?: string;
    confidenceThreshold?: number;
  };
  onCloseInspector: () => void;
}

export function InspectorSidebar({ selectedNode, filters, onCloseInspector }: InspectorSidebarProps) {
  const [isCollapsed, setIsCollapsed] = useState(false);

  return (
    <div
      className={`absolute right-4 top-4 bottom-4 bg-slate-950/40 backdrop-blur-xl border border-white/10 rounded-2xl shadow-2xl flex flex-col z-40 overflow-hidden transition-all duration-300 ${
        isCollapsed ? 'w-12' : 'w-80'
      }`}
    >
      {!isCollapsed && (
        <button
          onClick={() => setIsCollapsed(true)}
          className="absolute top-4 right-4 w-8 h-8 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors z-50"
          title="Collapse inspector"
        >
          <ChevronRight className="w-4 h-4 text-white/70" />
        </button>
      )}

      {isCollapsed ? (
        <div className="flex flex-col items-center justify-center h-full gap-4 py-6">
          <button
            onClick={() => setIsCollapsed(false)}
            className="w-10 h-10 rounded-lg bg-cyan-500/20 hover:bg-cyan-500/30 border border-cyan-500/30 flex items-center justify-center transition-colors"
            title="Expand inspector"
          >
            <Info className="w-5 h-5 text-cyan-400" />
          </button>
        </div>
      ) : (
        <>
          <div className="px-6 py-4 border-b border-white/10">
            <div className="flex items-center gap-2">
              <Info className="w-5 h-5 text-cyan-400" />
              <h2 className="text-white/90 font-semibold">Inspector</h2>
            </div>
            <p className="text-white/50 text-xs mt-1">Evidence-backed node details for the current graph view</p>
          </div>

          <div className="flex-1 min-h-0">
            {selectedNode ? (
              <NodeInspector node={selectedNode} filters={filters} onClose={onCloseInspector} embedded />
            ) : (
              <div className="h-full px-5 py-5 text-white/60 text-sm leading-relaxed">
                Select a node in the graph to inspect timeframe-aware details.
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}
