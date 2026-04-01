import { Download, FileImage, Table, FileText } from 'lucide-react';
import { useState, useRef, useEffect } from 'react';

interface ExportButtonProps {
  graphData: any;
  onExportImage: () => void;
  compact?: boolean;
  menuPlacement?: 'bottom-right' | 'left-center';
}

export function ExportButton({
  graphData,
  onExportImage,
  compact = false,
  menuPlacement = 'bottom-right',
}: ExportButtonProps) {
  const [showMenu, setShowMenu] = useState(false);
  const [isExporting, setIsExporting] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        setShowMenu(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const exportCSV = () => {
    if (!graphData?.nodes || !graphData?.links) return;

    const nodesHeader = 'ID,Name,Type,Mentions,Connections\n';
    const nodesRows = graphData.nodes.map((node: any) => {
      const connections = graphData.links.filter((l: any) => 
        (typeof l.source === 'object' ? l.source.id : l.source) === node.id
          || (typeof l.target === 'object' ? l.target.id : l.target) === node.id
      ).length;
      return `"${node.id}","${node.name}","${node.type}",${node.mentionCount || 0},${connections}`;
    }).join('\n');
    const nodesCSV = nodesHeader + nodesRows;

    const linksHeader = 'Source,Target,Type,Weight\n';
    const linksRows = graphData.links.map((link: any) => {
      const sourceId = typeof link.source === 'object' ? link.source.id : link.source;
      const targetId = typeof link.target === 'object' ? link.target.id : link.target;
      return `"${sourceId}","${targetId}","${link.type || 'related'}",${link.value || 1}`;
    }).join('\n');
    const linksCSV = linksHeader + linksRows;

    const timestamp = new Date().toISOString().split('T')[0];
    const combined = `# Graph Data Export - ${timestamp}\n\n## Nodes\n${nodesCSV}\n\n## Links\n${linksCSV}`;

    const blob = new Blob([combined], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `graph-data-${timestamp}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    setShowMenu(false);
  };

  const exportJSON = () => {
    if (!graphData) return;

    const timestamp = new Date().toISOString().split('T')[0];
    const json = JSON.stringify(graphData, null, 2);
    const blob = new Blob([json], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `graph-data-${timestamp}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    setShowMenu(false);
  };

  const handleExportImage = async () => {
    setIsExporting(true);
    try {
      await onExportImage();
      setShowMenu(false);
    } finally {
      setTimeout(() => setIsExporting(false), 1000);
    }
  };

  const menuClasses = menuPlacement === 'left-center'
    ? 'absolute right-full top-1/2 mr-2 w-40 -translate-y-1/2'
    : 'absolute top-full right-0 mt-2 w-40';

  return (
    <div ref={menuRef} className="relative">
      <button
        onClick={() => setShowMenu(!showMenu)}
        className={`${compact ? 'w-9 h-9 rounded-xl' : 'w-10 h-10 rounded-lg'} bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-all group`}
        title="Export"
      >
        <Download className="w-4 h-4 text-white/70 group-hover:text-white/90" />
      </button>

      {showMenu && (
        <div className={`${menuClasses} bg-slate-950/95 backdrop-blur-xl border border-white/10 rounded-xl shadow-2xl overflow-hidden z-50`}>
          <div className="py-1">
            <button
              onClick={handleExportImage}
              disabled={isExporting}
              className="w-full px-4 py-2.5 flex items-center gap-3 text-white/80 hover:bg-white/5 transition-colors text-sm disabled:opacity-50"
            >
              <FileImage className="w-4 h-4 text-cyan-400" />
              PNG
            </button>
            <button
              onClick={exportCSV}
              className="w-full px-4 py-2.5 flex items-center gap-3 text-white/80 hover:bg-white/5 transition-colors text-sm"
            >
              <Table className="w-4 h-4 text-orange-400" />
              CSV
            </button>
            <button
              onClick={exportJSON}
              className="w-full px-4 py-2.5 flex items-center gap-3 text-white/80 hover:bg-white/5 transition-colors text-sm"
            >
              <FileText className="w-4 h-4 text-slate-300" />
              JSON
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
