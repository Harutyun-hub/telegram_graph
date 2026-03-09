import { ZoomIn, ZoomOut, Maximize2, RotateCcw } from 'lucide-react';

interface FloatingControlsProps {
  graphRef: React.RefObject<any>;
}

export function FloatingControls({ graphRef }: FloatingControlsProps) {
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
    <div className="absolute bottom-4 right-4 flex flex-col gap-2 z-40">
      <div className="bg-slate-950/40 backdrop-blur-xl border border-white/10 rounded-xl shadow-2xl p-2">
        <button 
          onClick={handleZoomIn}
          className="w-10 h-10 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors group"
          title="Zoom In"
        >
          <ZoomIn className="w-4 h-4 text-white/70 group-hover:text-white" />
        </button>
      </div>
      <div className="bg-slate-950/40 backdrop-blur-xl border border-white/10 rounded-xl shadow-2xl p-2">
        <button 
          onClick={handleZoomOut}
          className="w-10 h-10 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors group"
          title="Zoom Out"
        >
          <ZoomOut className="w-4 h-4 text-white/70 group-hover:text-white" />
        </button>
      </div>
      <div className="bg-slate-950/40 backdrop-blur-xl border border-white/10 rounded-xl shadow-2xl p-2">
        <button 
          onClick={handleZoomToFit}
          className="w-10 h-10 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors group"
          title="Fit to Screen"
        >
          <Maximize2 className="w-4 h-4 text-white/70 group-hover:text-white" />
        </button>
      </div>
      <div className="bg-slate-950/40 backdrop-blur-xl border border-white/10 rounded-xl shadow-2xl p-2">
        <button 
          onClick={handleReset}
          className="w-10 h-10 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors group"
          title="Reset View"
        >
          <RotateCcw className="w-4 h-4 text-white/70 group-hover:text-white" />
        </button>
      </div>
    </div>
  );
}
