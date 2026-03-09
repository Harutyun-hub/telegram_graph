import { useRef, useState } from 'react';
import { GraphVisualization } from '@/app/graph/components/GraphVisualization';
import { GlobalFilters } from '@/app/graph/components/GlobalFilters';
import { AISidebar } from '@/app/graph/components/AISidebar';
import { InspectorSidebar } from '@/app/graph/components/InspectorSidebar';
import { FloatingControls } from '@/app/graph/components/FloatingControls';
import { GraphLegend } from '@/app/graph/components/GraphLegend';
import { DotMatrixBackground } from '@/app/graph/components/DotMatrixBackground';
import { ExportButton } from '@/app/graph/components/ExportButton';
import { FreshnessBadge } from '@/app/graph/components/FreshnessBadge';

export function GraphDashboard() {
  const aiCopilotEnabled = String(import.meta.env.VITE_GRAPH_AI_COPILOT_ENABLED || '').toLowerCase() === 'true';
  const [selectedNode, setSelectedNode] = useState<any>(null);
  const [filters, setFilters] = useState<{
    channels?: string[];
    sentiments?: string[];
    timeframe?: string;
    topics?: string[];
    connectionStrength?: number;
    layers?: string[];
    insightMode?: string;
    sourceProfile?: string;
    confidenceThreshold?: number;
  }>({});
  const [graphData, setGraphData] = useState<any>(null);
  const [allNodes, setAllNodes] = useState<any[]>([]);
  const graphRef = useRef<any>(null);

  const handleNodeClick = (node: any) => {
    setSelectedNode(node);
  };

  const handleCloseInspector = () => {
    setSelectedNode(null);
  };

  const handleFiltersChange = (newFilters: any) => {
    setFilters(newFilters);
  };

  const handleApplyAIFilters = (patchFilters: any) => {
    setFilters((prev) => {
      const cleanedPatch = Object.fromEntries(
        Object.entries(patchFilters || {}).filter(([, value]) => value !== undefined),
      );

      return {
        ...prev,
        ...cleanedPatch,
        layers: cleanedPatch.layers || prev.layers || ['topic'],
      };
    });
  };

  const handleQuickSelectChannel = (channelName: string) => {
    setFilters((prev) => ({
      ...prev,
      channels: [channelName],
      layers: prev.layers && prev.layers.length > 0 ? prev.layers : ['topic'],
      insightMode: prev.insightMode || 'marketMap',
      sourceProfile: prev.sourceProfile || 'balanced',
      confidenceThreshold: prev.confidenceThreshold ?? 35,
    }));
  };

  const handleSearchSelect = (nodeId: string) => {
    const node = allNodes.find((n) => n.id === nodeId);
    if (node) {
      setSelectedNode(node);

      if (graphRef.current?.focusNodeById) {
        graphRef.current.focusNodeById(nodeId);
        return;
      }

      if (graphRef.current) {
        const graphInstance = graphRef.current;
        if (graphInstance.centerAt && typeof node.x === 'number' && typeof node.y === 'number') {
          graphInstance.centerAt(node.x, node.y, 1000);
          graphInstance.zoom(2, 1000);
        }
      }
    }
  };

  const handleGraphDataUpdate = (data: any) => {
    setGraphData(data);
    if (data?.nodes) {
      setAllNodes(
        data.nodes.map((n: any) => ({
          id: n.id,
          name: n.name,
          type: n.type,
        })),
      );
    }
  };

  const handleExportImage = () => {
    const graphElement = document.querySelector('.graph-container');
    if (graphElement) {
      import('html2canvas')
        .then(({ default: html2canvas }) => {
          html2canvas(graphElement as HTMLElement).then((canvas) => {
            const link = document.createElement('a');
            link.download = `graph-${new Date().toISOString().split('T')[0]}.png`;
            link.href = canvas.toDataURL();
            link.click();
          });
        })
        .catch((err) => {
          console.error('Failed to load html2canvas:', err);
          alert('Export feature requires html2canvas. Please try CSV/JSON export instead.');
        });
    }
  };

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
        onFiltersChange={handleFiltersChange}
        onQuickSelectChannel={handleQuickSelectChannel}
        onSearchSelect={handleSearchSelect}
        allNodes={allNodes}
      />

      {aiCopilotEnabled ? (
        <AISidebar
          filters={filters}
          onApplyFilters={handleApplyAIFilters}
          selectedNode={selectedNode}
          onCloseInspector={handleCloseInspector}
        />
      ) : (
        <InspectorSidebar
          filters={filters}
          selectedNode={selectedNode}
          onCloseInspector={handleCloseInspector}
        />
      )}

      <div className="absolute inset-0 graph-container">
        <GraphVisualization
          onNodeClick={handleNodeClick}
          selectedNodeId={selectedNode?.id}
          filters={filters}
          ref={graphRef}
          onDataUpdate={handleGraphDataUpdate}
        />
      </div>

      <FloatingControls graphRef={graphRef} />
      <GraphLegend activeLayers={filters.layers || ['topic']} insightMode={filters.insightMode} />
    </div>
  );
}
