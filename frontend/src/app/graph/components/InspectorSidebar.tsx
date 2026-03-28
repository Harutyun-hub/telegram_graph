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
      <div className="absolute right-4 top-4 z-40">
        <button
          onClick={() => setCollapsed(false)}
          className="flex h-12 items-center gap-2 rounded-2xl border border-white/10 bg-slate-950/70 px-3 text-white/75 shadow-2xl backdrop-blur-xl transition-colors hover:bg-white/10"
          title="Open inspector"
        >
          <PanelRightOpen className="h-4 w-4 text-cyan-300" />
          <span className="text-xs font-medium">Inspector</span>
        </button>
      </div>
    );
  }

  return (
    <div className="absolute right-4 top-4 bottom-4 w-[370px] bg-slate-950/55 backdrop-blur-xl border border-white/10 rounded-3xl shadow-2xl flex flex-col z-40 overflow-hidden transition-all duration-300">
      <button
        onClick={() => setCollapsed(true)}
        className="absolute top-4 right-4 w-8 h-8 rounded-xl bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors z-50"
        title="Collapse inspector"
      >
        <ChevronLeft className="w-4 h-4 text-white/70" />
      </button>

      <div className="px-6 py-4 border-b border-white/10">
        <div className="flex items-center gap-2">
          <Info className="w-5 h-5 text-cyan-400" />
          <h2 className="text-white/90 font-semibold">Inspector</h2>
        </div>
        <p className="text-white/50 text-xs mt-1">Evidence-backed node details for the current graph view</p>
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
          <div className="h-full px-5 py-5 text-white/60 text-sm leading-relaxed">
            Select a node in the graph to inspect timeframe-aware details.
          </div>
        )}
      </div>
    </div>
  );
}
