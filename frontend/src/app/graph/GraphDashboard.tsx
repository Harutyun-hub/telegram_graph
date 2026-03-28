import { useEffect, useMemo, useRef, useState } from 'react';
import { Loader2 } from 'lucide-react';
import { useDashboardDateRange } from '@/app/contexts/DashboardDateRangeContext';
import { mergeAIClientFiltersIntoGraphFilters, graphFiltersToAIClientFilters } from '@/app/graph/filterAdapters';
import { GraphVisualization } from '@/app/graph/components/GraphVisualization';
import { GlobalFilters } from '@/app/graph/components/GlobalFilters';
import { InspectorSidebar } from '@/app/graph/components/InspectorSidebar';
import { AISidebar } from '@/app/graph/components/AISidebar';
import { FloatingControls } from '@/app/graph/components/FloatingControls';
import { GraphLegend } from '@/app/graph/components/GraphLegend';
import { DotMatrixBackground } from '@/app/graph/components/DotMatrixBackground';
import { ExportButton } from '@/app/graph/components/ExportButton';
import { FreshnessBadge } from '@/app/graph/components/FreshnessBadge';
import type { GraphData, GraphFilters, GraphNode } from '@/app/graph/services/types';

const DEFAULT_FILTERS: GraphFilters = {
  channels: [],
  sentiments: [],
  category: '',
  signalFocus: 'all',
  sourceDetail: 'standard',
  rankingMode: 'volume',
  minMentions: 2,
  max_nodes: 20,
};

export function GraphDashboard() {
  const { range, ready } = useDashboardDateRange();
  const graphAiEnabled = String(import.meta.env.VITE_GRAPH_AI_ENABLED || '').trim().toLowerCase() === 'true';
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [filters, setFilters] = useState<GraphFilters>(DEFAULT_FILTERS);
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [filtersCollapsed, setFiltersCollapsed] = useState(false);
  const [inspectorCollapsed, setInspectorCollapsed] = useState(true);
  const graphRef = useRef<any>(null);

  const chromeOffsets = useMemo(
    () => ({
      left: filtersCollapsed ? 104 : 492,
      right: graphAiEnabled ? 96 : (inspectorCollapsed ? 92 : 402),
      top: 108,
      bottom: 44,
    }),
    [filtersCollapsed, graphAiEnabled, inspectorCollapsed],
  );

  const activeFilters = useMemo(
    () => ({ ...filters, from_date: range.from, to_date: range.to }),
    [filters, range.from, range.to],
  );

  const allNodes = useMemo(
    () => (graphData?.nodes || []).map((node) => ({ id: node.id, name: node.name, type: node.type })),
    [graphData],
  );

  const selectedNode = useMemo(
    () => graphData?.nodes.find((node) => node.id === selectedNodeId) || null,
    [graphData?.nodes, selectedNodeId],
  ) as GraphNode | null;

  useEffect(() => {
    if (!selectedNodeId) return;
    if (!graphData?.nodes.some((node) => node.id === selectedNodeId)) {
      setSelectedNodeId(null);
    }
  }, [graphData?.nodes, selectedNodeId]);

  useEffect(() => {
    if (graphAiEnabled) return;
    if (selectedNodeId) {
      setInspectorCollapsed(false);
    }
  }, [graphAiEnabled, selectedNodeId]);

  const handleSearchSelect = (nodeId: string) => {
    setSelectedNodeId(nodeId);
    if (graphRef.current?.focusNodeById) {
      graphRef.current.focusNodeById(nodeId);
    }
  };

  const handleExportImage = () => {
    const graphElement = document.querySelector('.graph-container');
    if (!graphElement) return;
    import('html2canvas')
      .then(({ default: html2canvas }) => html2canvas(graphElement as HTMLElement))
      .then((canvas) => {
        const link = document.createElement('a');
        link.download = `conversation-map-${new Date().toISOString().split('T')[0]}.png`;
        link.href = canvas.toDataURL();
        link.click();
      })
      .catch((error) => {
        console.error('Failed to export graph image:', error);
      });
  };

  if (!ready) {
    return (
      <div className="relative w-full h-full overflow-hidden bg-[#0b0e14] flex items-center justify-center">
        <DotMatrixBackground />
        <div className="relative z-10 flex flex-col items-center gap-3 text-white/75">
          <Loader2 className="w-8 h-8 animate-spin text-cyan-300" />
          <div className="text-sm">Preparing conversation map window...</div>
          <div className="text-xs text-white/45">Using the shared dashboard date range</div>
        </div>
      </div>
    );
  }

  return (
    <div className="relative w-full h-full overflow-hidden bg-[#0b0e14]">
      <DotMatrixBackground />

      <div className="absolute top-6 right-6 z-50">
        <ExportButton graphData={graphData} onExportImage={handleExportImage} />
      </div>

      <div className="absolute top-20 right-6 z-50">
        <FreshnessBadge freshness={graphData?.meta?.freshness} />
      </div>

      <GlobalFilters
        filters={activeFilters}
        availableCategories={graphData?.meta?.availableCategories || []}
        freshness={graphData?.meta?.freshness}
        showFreshnessBadge={false}
        isCollapsed={filtersCollapsed}
        onCollapsedChange={setFiltersCollapsed}
        onFiltersChange={setFilters}
        onSearchSelect={handleSearchSelect}
        allNodes={allNodes}
      />

      {graphAiEnabled ? (
        <AISidebar
          filters={graphFiltersToAIClientFilters(activeFilters)}
          onApplyFilters={(patch) => {
            setFilters((current) => mergeAIClientFiltersIntoGraphFilters(current, patch));
          }}
          selectedNode={selectedNode}
          onCloseInspector={() => setSelectedNodeId(null)}
        />
      ) : (
        <InspectorSidebar
          filters={activeFilters}
          selectedNode={selectedNode}
          graphData={graphData}
          isCollapsed={inspectorCollapsed}
          onCollapsedChange={setInspectorCollapsed}
          onCloseInspector={() => setSelectedNodeId(null)}
        />
      )}

      <div className="absolute inset-0 graph-container">
        <GraphVisualization
          ref={graphRef}
          filters={activeFilters}
          layoutInsets={chromeOffsets}
          selectedNodeId={selectedNodeId}
          onNodeClick={(node) => setSelectedNodeId(node.id)}
          onDataUpdate={setGraphData}
        />
      </div>

      <FloatingControls graphRef={graphRef} rightOffset={chromeOffsets.right} />
      <GraphLegend />
    </div>
  );
}
