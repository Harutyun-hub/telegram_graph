import { ZoomIn, ZoomOut, Maximize2, RotateCcw } from 'lucide-react';
import { ExportButton } from '@/app/graph/components/ExportButton';
import type { GraphData } from '@/app/graph/services/types';

interface FloatingControlsProps {
  graphRef: React.RefObject<any>;
  rightOffset?: number;
  graphData?: GraphData | null;
  onExportImage?: () => void;
}

export function FloatingControls({
  graphRef,
  rightOffset = 16,
  graphData,
  onExportImage,
}: FloatingControlsProps) {
  const handleZoomIn = () => {
    if (graphRef.current) {
      graphRef.current.zoomIn();
    }
  };

  const handleZoomOut = () => {
    if (graphRef.current) {
      graphRef.current.zoomOut();
    }
  };

  const handleZoomToFit = () => {
    if (graphRef.current) {
      graphRef.current.zoomToFit();
    }
  };

  const handleReset = () => {
    if (graphRef.current) {
      graphRef.current.centerGraph();
    }
  };

  return (
    <div className="absolute bottom-4 z-40" style={{ right: `${rightOffset}px` }}>
      <div className="bg-slate-950/40 backdrop-blur-xl border border-white/10 rounded-xl shadow-2xl p-1.5 flex flex-col gap-1.5">
        <button 
          onClick={handleZoomIn}
          className="w-9 h-9 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors group"
          title="Zoom In"
        >
          <ZoomIn className="w-4 h-4 text-white/70 group-hover:text-white" />
        </button>
        <button 
          onClick={handleZoomOut}
          className="w-9 h-9 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors group"
          title="Zoom Out"
        >
          <ZoomOut className="w-4 h-4 text-white/70 group-hover:text-white" />
        </button>
        <button 
          onClick={handleZoomToFit}
          className="w-9 h-9 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors group"
          title="Fit to Screen"
        >
          <Maximize2 className="w-4 h-4 text-white/70 group-hover:text-white" />
        </button>
        <button 
          onClick={handleReset}
          className="w-9 h-9 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors group"
          title="Reset View"
        >
          <RotateCcw className="w-4 h-4 text-white/70 group-hover:text-white" />
        </button>
        {graphData && onExportImage ? (
          <ExportButton
            graphData={graphData}
            onExportImage={onExportImage}
            compact
            menuPlacement="left-center"
          />
        ) : null}
      </div>
    </div>
  );
}
