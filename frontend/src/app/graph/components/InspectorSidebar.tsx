import { ChevronLeft, Info, PanelRightOpen } from 'lucide-react';
import { NodeInspector } from '@/app/graph/components/NodeInspector';
import type { GraphData, GraphFilters, GraphNode } from '@/app/graph/services/types';

interface InspectorSidebarProps {
  selectedNode?: GraphNode | null;
  filters?: GraphFilters;
  graphData?: GraphData | null;
  isCollapsed?: boolean;
  onCollapsedChange?: (collapsed: boolean) => void;
  onCloseInspector: () => void;
}

export function InspectorSidebar({
  selectedNode,
  filters,
  graphData,
  isCollapsed = true,
  onCollapsedChange,
  onCloseInspector,
}: InspectorSidebarProps) {
  const setCollapsed = (collapsed: boolean) => {
    onCollapsedChange?.(collapsed);
  };

  if (isCollapsed) {
    return (
      <aside className="absolute right-4 top-4 bottom-4 z-40 w-16 rounded-[24px] border border-white/10 bg-[#0d1524]/90 shadow-2xl backdrop-blur-xl">
        <div className="flex h-full flex-col items-center justify-start py-4">
          <button
            onClick={() => setCollapsed(false)}
            className="flex h-10 w-10 items-center justify-center rounded-2xl border border-cyan-400/20 bg-cyan-500/10 text-cyan-300 transition-colors hover:bg-cyan-500/18"
            title="Open inspector"
          >
            <PanelRightOpen className="h-4.5 w-4.5" />
          </button>
          <div className="mt-3 rounded-2xl border border-white/10 bg-white/5 px-2 py-3 text-center text-[9px] uppercase tracking-[0.18em] text-white/42 [writing-mode:vertical-rl] [text-orientation:mixed]">
            Inspector
          </div>
        </div>
      </aside>
    );
  }

  return (
    <div className="absolute right-4 top-4 bottom-4 w-[296px] bg-slate-950/55 backdrop-blur-xl border border-white/10 rounded-[28px] shadow-2xl flex flex-col z-40 overflow-hidden transition-all duration-300">
      <button
        onClick={() => setCollapsed(true)}
        className="absolute top-3.5 right-3.5 w-7 h-7 rounded-xl bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors z-50"
        title="Collapse inspector"
      >
        <ChevronLeft className="w-4 h-4 text-white/70" />
      </button>

      <div className="px-4 py-3 border-b border-white/10">
        <div className="flex items-center gap-2">
          <Info className="w-4 h-4 text-cyan-400" />
          <h2 className="text-white/90 font-semibold">Inspector</h2>
        </div>
        <p className="text-white/50 text-[11px] mt-1 leading-5">Select a node to inspect its evidence and context.</p>
      </div>

      <div className="flex-1 min-h-0">
        {selectedNode ? (
          <NodeInspector
            node={selectedNode}
            graphData={graphData}
            filters={filters}
            onClose={onCloseInspector}
            embedded
          />
        ) : (
          <div className="h-full px-4 py-4 text-white/60 text-[13px] leading-6">
            Select a node in the graph to inspect timeframe-aware details.
          </div>
        )}
      </div>
    </div>
  );
}
