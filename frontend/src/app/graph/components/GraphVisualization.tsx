import { useEffect, useState, useCallback, useRef, useMemo, forwardRef, useImperativeHandle } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import { getGraphData, GraphData } from '@/app/graph/services/api';
import { Loader2 } from 'lucide-react';
import { EmptyGraphState } from '@/app/graph/components/EmptyGraphState';
import { getNodeColors, getNodeSize, NodeType } from '@/app/graph/utils/nodeColors';

interface Node {
  id: string;
  name: string;
  color: string;
  val?: number;
  size?: number;
  type: NodeType | string;
  connections?: number;
  signal?: number;
  x?: number;
  y?: number;
  vx?: number;
  vy?: number;
  fx?: number | null;
  fy?: number | null;
  // New fields for 15-node-type system
  category?: string;
  details?: string;
  segmentType?: string;
  severity?: string;
  semanticRole?: 'opportunity';
  opportunityTier?: 'gold' | 'silver';
  opportunityScore?: number;
}

interface Link {
  source: string | Node;
  target: string | Node;
  value?: number;
}

interface GraphVisualizationProps {
  onNodeClick?: (node: Node) => void;
  selectedNodeId?: string | null;
  filters?: {
    channels?: string[];
    sentiments?: string[];
    timeframe?: string;
    topics?: string[];
    layers?: string[];
    connectionStrength?: number;
    insightMode?: string;
    sourceProfile?: string;
    confidenceThreshold?: number;
  };
  onQuickSelectChannel?: (channel: string) => void;
  onDataUpdate?: (data: GraphData) => void;
}

export const GraphVisualization = forwardRef<any, GraphVisualizationProps>(
  ({ onNodeClick, selectedNodeId, filters, onQuickSelectChannel, onDataUpdate }, ref) => {
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [filteredData, setFilteredData] = useState<GraphData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [dimensions, setDimensions] = useState({ width: 1200, height: 760 });
  const containerRef = useRef<HTMLDivElement | null>(null);
  const graphRef = useRef<any>();
  const [hoveredNode, setHoveredNode] = useState<string | null>(null);
  const connectionStrength = filters?.connectionStrength ?? 3;
  const isFocusedDetail = connectionStrength >= 4;
  const isExploreDetail = connectionStrength <= 2;
  const isOpportunitiesMode = filters?.insightMode === 'opportunities';

  const opportunityNodeIds = useMemo(
    () => new Set((filteredData?.nodes || []).filter((node: any) => node.semanticRole === 'opportunity').map((node: any) => node.id)),
    [filteredData],
  );

  const insightModeLabel = useMemo(() => {
    switch (filters?.insightMode) {
      case 'ownership':
        return 'Who Owns What';
      case 'messageFit':
        return 'Message Fit';
      case 'competitorMoves':
        return 'Competitor Moves';
      case 'opportunities':
        return 'Hidden Opportunities';
      default:
        return 'Market Map';
    }
  }, [filters?.insightMode]);

  const sourceProfileLabel = useMemo(() => {
    switch (filters?.sourceProfile) {
      case 'performance':
        return 'Performance';
      case 'brandStrategy':
        return 'Channel Strategy';
      default:
        return 'Balanced';
    }
  }, [filters?.sourceProfile]);

  const selectedChannels = filters?.channels || [];

  const activeChannelNames = useMemo(() => {
    const names = new Set<string>();
    (filteredData?.nodes || []).forEach((node: any) => {
      if (node.type === 'channel' || node.type === 'brand') {
        names.add(String(node.name || '').toLowerCase());
      }
    });
    return names;
  }, [filteredData]);

  const activeSelectedChannels = useMemo(
    () => selectedChannels.filter((channel) => activeChannelNames.has(String(channel || '').toLowerCase())),
    [selectedChannels, activeChannelNames],
  );

  const inactiveSelectedChannels = useMemo(
    () => selectedChannels.filter((channel) => !activeChannelNames.has(String(channel || '').toLowerCase())),
    [selectedChannels, activeChannelNames],
  );

  const selectedChannelLabel = useMemo(() => {
    if (selectedChannels.length === 0) return 'No channels selected';
    const first = selectedChannels.slice(0, 2).join(', ');
    const extra = selectedChannels.length - 2;
    return extra > 0 ? `${first} +${extra}` : first;
  }, [selectedChannels]);

  const topicMentions = useMemo(
    () => (filteredData?.links || []).reduce((sum, link) => sum + Number(link.value || 0), 0),
    [filteredData],
  );

  useEffect(() => {
    if (!filteredData || !graphRef.current) return;

    const fg = graphRef.current;
    const nodeTypeById = new Map(filteredData.nodes.map((node) => [node.id, node.type]));
    const resolveType = (endpoint: any): string => {
      if (endpoint && typeof endpoint === 'object') {
        return endpoint.type || nodeTypeById.get(endpoint.id) || 'topic';
      }
      return nodeTypeById.get(endpoint) || 'topic';
    };

    const linkForce: any = fg.d3Force?.('link');
    if (linkForce) {
      if (typeof linkForce.distance === 'function') {
        linkForce.distance((link: any) => {
          const sourceType = resolveType(link.source);
          const targetType = resolveType(link.target);
          const includesChannel = sourceType === 'brand' || sourceType === 'channel' || targetType === 'brand' || targetType === 'channel';
          const includesTopic = sourceType === 'topic' || targetType === 'topic';
          const includesLayer = !includesChannel && !includesTopic;

          if (includesChannel && includesTopic) {
            return isFocusedDetail ? 185 : 165;
          }
          if (includesTopic && !includesChannel) {
            return isFocusedDetail ? 150 : 135;
          }
          if (includesLayer) {
            return isFocusedDetail ? 140 : 125;
          }
          return isFocusedDetail ? 160 : 140;
        });
      }

      if (typeof linkForce.strength === 'function') {
        linkForce.strength((link: any) => {
          const sourceType = resolveType(link.source);
          const targetType = resolveType(link.target);
          const includesChannel = sourceType === 'brand' || sourceType === 'channel' || targetType === 'brand' || targetType === 'channel';
          return includesChannel ? 0.3 : 0.22;
        });
      }
    }

    const chargeForce: any = fg.d3Force?.('charge');
    if (chargeForce) {
      if (typeof chargeForce.strength === 'function') {
        chargeForce.strength(isFocusedDetail ? -1050 : isExploreDetail ? -760 : -900);
      }
      if (typeof chargeForce.distanceMax === 'function') {
        chargeForce.distanceMax(isFocusedDetail ? 460 : 400);
      }
    }

    fg.d3ReheatSimulation?.();
  }, [filteredData, isFocusedDetail, isExploreDetail]);

  // Expose methods to parent component
  useImperativeHandle(ref, () => ({
    zoomIn: () => {
      if (graphRef.current) {
        const currentZoom = graphRef.current.zoom();
        graphRef.current.zoom(currentZoom * 1.2, 400);
      }
    },
    zoomOut: () => {
      if (graphRef.current) {
        const currentZoom = graphRef.current.zoom();
        graphRef.current.zoom(currentZoom / 1.2, 400);
      }
    },
    zoomToFit: () => {
      if (graphRef.current) {
        graphRef.current.zoomToFit(400, 80);
      }
    },
    centerGraph: () => {
      if (graphRef.current) {
        graphRef.current.centerAt(0, 0, 400);
        graphRef.current.zoom(1.5, 400);
      }
    },
    focusNodeById: (nodeId: string) => {
      if (!graphRef.current || !filteredData?.nodes) return;
      const node = filteredData.nodes.find((entry) => entry.id === nodeId) as Node | undefined;
      if (!node || typeof node.x !== 'number' || typeof node.y !== 'number') return;
      graphRef.current.centerAt(node.x, node.y, 800);
      graphRef.current.zoom(2.2, 800);
    },
  }));

  // Fetch real graph data from Neo4j
  useEffect(() => {
    const fetchGraphData = async () => {
      if (!filters?.channels || filters.channels.length === 0) {
        setGraphData(null);
        setFilteredData(null);
        setLoading(false);
        setError(null);
        return;
      }

      try {
        setLoading(true);
        setError(null);
        
        // Pass filters to backend (including channel selection)
        const apiFilters = {
          channels: filters?.channels || [],
          timeframe: filters?.timeframe,
          sentiment: filters?.sentiments,
          topics: filters?.topics,
          layers: filters?.layers || ['topic'],
          connectionStrength: filters?.connectionStrength ?? 3,
          insightMode: filters?.insightMode || 'marketMap',
          sourceProfile: filters?.sourceProfile || 'balanced',
          confidenceThreshold: filters?.confidenceThreshold ?? 35,
        };
        
        console.log('📡 Fetching graph data with filters:', apiFilters);
        const data = await getGraphData(apiFilters);
        
        if (!data || !data.nodes || !data.links) {
          throw new Error('Invalid graph data structure');
        }

        console.log('✅ Raw graph data loaded:', {
          totalNodes: data.nodes.length,
          totalLinks: data.links.length,
          channels: data.nodes.filter(n => n.type === 'channel' || n.type === 'brand').length,
          topics: data.nodes.filter(n => n.type === 'topic').length,
          layers: Array.from(new Set(data.nodes.map((node) => node.type))).sort(),
        });

        // Validate data integrity
        const validNodeIds = new Set(data.nodes.map(node => node.id));
        const validLinks = data.links.filter(link => {
          const sourceId = typeof link.source === 'string' ? link.source : link.source.id;
          const targetId = typeof link.target === 'string' ? link.target : link.target.id;
          return validNodeIds.has(sourceId) && validNodeIds.has(targetId);
        });

        if (validLinks.length < data.links.length) {
          console.warn(`⚠️ Removed ${data.links.length - validLinks.length} invalid links`);
        }

        setGraphData({
          nodes: data.nodes,
          links: validLinks,
          meta: data.meta,
        });
      } catch (err) {
        console.error('❌ Failed to load graph data:', err);
        setError(err instanceof Error ? err.message : 'Failed to load graph data');
      } finally {
        setLoading(false);
      }
    };

    fetchGraphData();
  }, [filters]); // Re-fetch when filters change

  // Enrich graph with contextual sizing and link-aware signal
  useEffect(() => {
    if (!graphData) {
      setFilteredData(null);
      return;
    }

    // No channel selected = empty state
    if (!filters?.channels || filters.channels.length === 0) {
      console.log('📭 No channels selected - showing empty state');
      setFilteredData(null);
      return;
    }

    const nodeConnectionCounts = new Map<string, number>();
    const nodeSignalCounts = new Map<string, number>();

    graphData.links.forEach(link => {
      const sourceId = typeof link.source === 'string' ? link.source : link.source.id;
      const targetId = typeof link.target === 'string' ? link.target : link.target.id;
      const weight = Number(link.value || 1);

      nodeConnectionCounts.set(sourceId, (nodeConnectionCounts.get(sourceId) || 0) + 1);
      nodeConnectionCounts.set(targetId, (nodeConnectionCounts.get(targetId) || 0) + 1);
      nodeSignalCounts.set(sourceId, (nodeSignalCounts.get(sourceId) || 0) + weight);
      nodeSignalCounts.set(targetId, (nodeSignalCounts.get(targetId) || 0) + weight);
    });

    const enhancedNodes = graphData.nodes.map(node => {
      const connections = nodeConnectionCounts.get(node.id) || 0;
      const signal = nodeSignalCounts.get(node.id) || 0;
      const type = (node.type || 'topic') as NodeType;

      const baseSize = typeof node.size === 'number'
        ? node.size
        : getNodeSize(type, connections);

      const tunedSize = (type === 'channel' || type === 'brand')
        ? Math.max(18, Math.min(28, baseSize + Math.log(signal + 1) * 0.45))
        : type === 'topic'
          ? Math.max(10, Math.min(30, baseSize + Math.log(signal + 1) * 1.1))
          : Math.max(6, Math.min(14, baseSize + Math.log(signal + 1) * 0.35));

      return {
        ...node,
        connections,
        signal,
        size: tunedSize,
      };
    });

    const filtered = {
      nodes: enhancedNodes,
      links: graphData.links,
      meta: graphData.meta,
    };

    console.log('✅ Filtered graph:', {
      nodes: filtered.nodes.length,
      links: filtered.links.length,
      types: Array.from(new Set(filtered.nodes.map(node => node.type))).sort(),
    });

    setFilteredData(filtered);

    // Call onDataUpdate if provided
    if (onDataUpdate) {
      onDataUpdate(filtered);
    }
  }, [filters, graphData]);

  useEffect(() => {
    const updateDimensions = () => {
      const el = containerRef.current;
      if (!el) return;
      setDimensions({
        width: Math.max(320, el.clientWidth),
        height: Math.max(360, el.clientHeight),
      });
    };

    updateDimensions();

    let observer: ResizeObserver | null = null;
    if (typeof ResizeObserver !== 'undefined' && containerRef.current) {
      observer = new ResizeObserver(() => updateDimensions());
      observer.observe(containerRef.current);
    }

    window.addEventListener('resize', updateDimensions);
    return () => {
      window.removeEventListener('resize', updateDimensions);
      if (observer) observer.disconnect();
    };
  }, []);

  const handleNodeClick = useCallback((node: Node) => {
    if (onNodeClick) {
      onNodeClick(node);
    }
  }, [onNodeClick]);

  const handleNodeHover = useCallback((node: Node | null) => {
    setHoveredNode(node ? node.id : null);
  }, []);

  // Handle node drag end - pin node in place
  const handleNodeDragEnd = useCallback((node: Node) => {
    // Pin the node at its current position
    node.fx = node.x;
    node.fy = node.y;
  }, []);

  // Loading state
  if (loading) {
    return (
      <div className="w-full h-full flex items-center justify-center bg-[#0B0E14]">
        <div className="flex flex-col items-center gap-4">
          <div className="relative">
            <Loader2 className="w-12 h-12 text-cyan-500 animate-spin" />
            <div className="absolute inset-0 w-12 h-12 border-4 border-cyan-500/20 rounded-full animate-ping" />
          </div>
          <div className="text-center">
            <p className="text-white/90 text-sm font-medium mb-1">Loading Graph Data</p>
            <p className="text-white/50 text-xs">Querying Neo4j database...</p>
          </div>
          {/* Progress Steps */}
          <div className="flex items-center gap-2 mt-2">
            <div className="w-2 h-2 rounded-full bg-cyan-500 animate-pulse" />
            <div className="w-2 h-2 rounded-full bg-cyan-500/40 animate-pulse delay-75" />
            <div className="w-2 h-2 rounded-full bg-cyan-500/20 animate-pulse delay-150" />
          </div>
        </div>
      </div>
    );
  }

  // Error state
  if (error || !graphData) {
    return (
      <div className="w-full h-full flex items-center justify-center bg-[#0B0E14]">
        <div className="flex flex-col items-center gap-4 max-w-md text-center">
          <div className="w-16 h-16 rounded-full bg-red-500/10 flex items-center justify-center">
            <span className="text-3xl">⚠️</span>
          </div>
          <p className="text-white/90 font-medium">{error || 'No data available'}</p>
          <button 
            onClick={() => window.location.reload()}
            className="px-4 py-2 bg-cyan-500/20 hover:bg-cyan-500/30 text-cyan-300 rounded-lg transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  // Empty state - no channel selected
  if (!filters?.channels || filters.channels.length === 0) {
    return <EmptyGraphState onQuickSelect={onQuickSelectChannel} mode="no_selection" />;
  }

  // Empty state - selected channels have no activity in the selected timeframe
  if (!filteredData || filteredData.nodes.length === 0) {
    return (
      <EmptyGraphState
        onQuickSelect={onQuickSelectChannel}
        mode="no_data"
        timeframe={filters?.timeframe}
        inactiveChannels={inactiveSelectedChannels}
      />
    );
  }

  // Graph visualization
  return (
    <div ref={containerRef} className="w-full h-full relative" style={{ background: 'linear-gradient(135deg, #0a0e1a 0%, #1a1f2e 100%)' }}>
      {/* Animated Grid Background */}
      <div className="absolute inset-0 opacity-20 pointer-events-none">
        <svg width="100%" height="100%" xmlns="http://www.w3.org/2000/svg">
          <defs>
            <pattern id="grid" width="40" height="40" patternUnits="userSpaceOnUse">
              <path d="M 40 0 L 0 0 0 40" fill="none" stroke="rgba(0, 212, 255, 0.15)" strokeWidth="0.5"/>
            </pattern>
            <linearGradient id="gridFade" x1="0%" y1="0%" x2="0%" y2="100%">
              <animate attributeName="y1" values="0%;100%;0%" dur="20s" repeatCount="indefinite" />
              <animate attributeName="y2" values="100%;0%;100%" dur="20s" repeatCount="indefinite" />
              <stop offset="0%" stopColor="rgba(0, 212, 255, 0.3)" />
              <stop offset="50%" stopColor="rgba(0, 212, 255, 0.05)" />
              <stop offset="100%" stopColor="rgba(0, 212, 255, 0.3)" />
            </linearGradient>
          </defs>
          <rect width="100%" height="100%" fill="url(#grid)" />
          <rect width="100%" height="100%" fill="url(#gridFade)" />
        </svg>
      </div>

      {/* Context indicator */}
      <div className="absolute top-24 left-1/2 -translate-x-1/2 z-10 bg-slate-950/60 backdrop-blur-lg border border-cyan-500/30 rounded-full px-6 py-3 shadow-lg shadow-cyan-500/20">
        <div className="flex items-center gap-4 text-sm">
          <div className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-cyan-400 shadow-lg shadow-cyan-400/50" />
            <span className="text-white/90 font-medium">
              {selectedChannelLabel}
            </span>
          </div>
          <div className="w-px h-4 bg-white/20" />
          <span className="text-cyan-200 font-medium">
            {activeSelectedChannels.length}/{selectedChannels.length} active
          </span>
          {inactiveSelectedChannels.length > 0 && (
            <>
              <div className="w-px h-4 bg-white/20" />
              <span className="text-amber-300 font-medium">
                {inactiveSelectedChannels.length} inactive in window
              </span>
            </>
          )}
          <div className="w-px h-4 bg-white/20" />
          <span className="text-orange-400 font-semibold">
            {filteredData.nodes.filter(n => n.type === 'topic').length} topics
          </span>
          <div className="w-px h-4 bg-white/20" />
          <span className="text-emerald-300 font-medium">
            {filteredData.nodes.filter(n => n.type !== 'channel' && n.type !== 'brand' && n.type !== 'topic').length} layer nodes
          </span>
          <div className="w-px h-4 bg-white/20" />
          <span className="text-white/60">
            {topicMentions} topic mentions
          </span>
          <div className="w-px h-4 bg-white/20" />
          <span className="text-cyan-300 capitalize">
            {insightModeLabel}
          </span>
          <div className="w-px h-4 bg-white/20" />
          <span className="text-indigo-300">
            {sourceProfileLabel}
          </span>
          {isOpportunitiesMode && (
            <>
              <div className="w-px h-4 bg-white/20" />
              <span className="text-amber-300 font-semibold">
                {opportunityNodeIds.size} opportunities
              </span>
            </>
          )}
        </div>
      </div>

      <ForceGraph2D
        ref={graphRef}
        graphData={filteredData}
        width={dimensions.width}
        height={dimensions.height}
        backgroundColor="transparent"
        nodeVal={(node: any) => {
          const size = typeof node.size === 'number'
            ? node.size
            : getNodeSize((node.type || 'topic') as NodeType, node.connections || 0);
          return Math.max(16, size * size);
        }}
        nodeRelSize={1}
        nodeLabel={(node: any) => `${node.name} (${node.connections || 0} connections)`}
        onNodeClick={handleNodeClick}
        onNodeHover={handleNodeHover}
        onNodeDragEnd={handleNodeDragEnd}
        nodeCanvasObject={(node: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
          // Safety check: Ensure node has valid coordinates
          if (!node || typeof node.x !== 'number' || typeof node.y !== 'number' || 
              !isFinite(node.x) || !isFinite(node.y)) {
            console.warn('Invalid node coordinates:', node);
            return;
          }

          const label = node.name;
          const fontSize = 12 / globalScale;
          const isChannel = node.type === 'channel' || node.type === 'brand';
          const isSelected = selectedNodeId === node.id;
          const isHovered = hoveredNode === node.id;
          const isOpportunity = node.semanticRole === 'opportunity';
          
          // Use new color system for 15 node types
          const nodeColors = getNodeColors(node.type as NodeType);
          const nodeSize = typeof node.size === 'number'
            ? node.size
            : getNodeSize(node.type as NodeType, node.connections || 0);
          const scaledRadius = nodeSize / globalScale;
          
          // Get colors
          let coreColor = nodeColors.core;
          let glowColor = nodeColors.glow;
          let edgeColor = nodeColors.edge;
          const darkEdge = nodeColors.darkEdge;
          
          if (isSelected) {
            // Gold for selection
            coreColor = '#fbbf24';
            glowColor = 'rgba(251, 191, 36, 0.8)';
            edgeColor = '#fcd34d';
          } else if (isOpportunity) {
            coreColor = '#f59e0b';
            glowColor = 'rgba(245, 158, 11, 0.72)';
            edgeColor = '#fde68a';
          } else if (isHovered) {
            // Brighten on hover
            glowColor = glowColor.replace(/[\d.]+(?=\))/, '0.8');
          }
          
          // === ENHANCED GLOWING ORB RENDERING ===
          
          // Layer 1: Outer glow (very soft, large radius)
          const outerGlowRadius = scaledRadius * (isFocusedDetail ? 2.6 : 3.5);
          const outerGradient = ctx.createRadialGradient(node.x, node.y, 0, node.x, node.y, outerGlowRadius);
          outerGradient.addColorStop(0, glowColor);
          outerGradient.addColorStop(0.4, glowColor.replace(/[^,]+(?=\))/, '0.3'));
          outerGradient.addColorStop(1, 'rgba(0, 0, 0, 0)');
          ctx.fillStyle = outerGradient;
          ctx.beginPath();
          ctx.arc(node.x, node.y, outerGlowRadius, 0, 2 * Math.PI);
          ctx.fill();
          
          // Layer 2: Middle glow (bright)
          const midGlowRadius = scaledRadius * (isFocusedDetail ? 1.7 : 2);
          const midGradient = ctx.createRadialGradient(node.x, node.y, 0, node.x, node.y, midGlowRadius);
          midGradient.addColorStop(0, glowColor);
          midGradient.addColorStop(0.5, glowColor.replace(/[^,]+(?=\))/, '0.4'));
          midGradient.addColorStop(1, 'rgba(0, 0, 0, 0)');
          ctx.fillStyle = midGradient;
          ctx.beginPath();
          ctx.arc(node.x, node.y, midGlowRadius, 0, 2 * Math.PI);
          ctx.fill();
          
          // Layer 3: Core sphere with 3D gradient
          const sphereGradient = ctx.createRadialGradient(
            node.x - scaledRadius * 0.35,
            node.y - scaledRadius * 0.35,
            0,
            node.x,
            node.y,
            scaledRadius
          );
          sphereGradient.addColorStop(0, '#ffffff'); // Bright highlight
          sphereGradient.addColorStop(0.3, coreColor);
          sphereGradient.addColorStop(1, isChannel ? '#0e7490' : '#9a3412'); // Darker edge for depth
          
          ctx.fillStyle = sphereGradient;
          ctx.beginPath();
          if (isOpportunity) {
            const diamondRadius = scaledRadius * 1.08;
            ctx.moveTo(node.x, node.y - diamondRadius);
            ctx.lineTo(node.x + diamondRadius, node.y);
            ctx.lineTo(node.x, node.y + diamondRadius);
            ctx.lineTo(node.x - diamondRadius, node.y);
            ctx.closePath();
          } else {
            ctx.arc(node.x, node.y, scaledRadius, 0, 2 * Math.PI);
          }
          ctx.fill();
          
          // Layer 4: Specular highlight (glossy effect)
          if (!isOpportunity) {
            ctx.fillStyle = 'rgba(255, 255, 255, 0.7)';
            ctx.beginPath();
            ctx.arc(
              node.x - scaledRadius * 0.4,
              node.y - scaledRadius * 0.4,
              scaledRadius * 0.3,
              0,
              2 * Math.PI
            );
            ctx.fill();
          }
          
          // Layer 5: Edge ring for definition
          ctx.strokeStyle = edgeColor;
          ctx.lineWidth = (isChannel ? 2 : 1.5) / globalScale;
          ctx.beginPath();
          if (isOpportunity) {
            const ringRadius = scaledRadius * 1.12;
            ctx.moveTo(node.x, node.y - ringRadius);
            ctx.lineTo(node.x + ringRadius, node.y);
            ctx.lineTo(node.x, node.y + ringRadius);
            ctx.lineTo(node.x - ringRadius, node.y);
            ctx.closePath();
          } else {
            ctx.arc(node.x, node.y, scaledRadius, 0, 2 * Math.PI);
          }
          ctx.stroke();
          
          // Layer 6: Selection pulse ring (animated)
          if (isSelected) {
            const pulseRadius = scaledRadius * (1.3 + Math.sin(Date.now() / 300) * 0.15);
            ctx.strokeStyle = 'rgba(251, 191, 36, 0.6)';
            ctx.lineWidth = 2.5 / globalScale;
            ctx.beginPath();
            ctx.arc(node.x, node.y, pulseRadius, 0, 2 * Math.PI);
            ctx.stroke();
            
            // Second pulse ring
            const pulseRadius2 = scaledRadius * (1.5 + Math.sin(Date.now() / 300) * 0.2);
            ctx.strokeStyle = 'rgba(251, 191, 36, 0.3)';
            ctx.lineWidth = 1.5 / globalScale;
            ctx.beginPath();
            ctx.arc(node.x, node.y, pulseRadius2, 0, 2 * Math.PI);
            ctx.stroke();
          }
          
          // === SMART LABEL RENDERING ===
          // ONLY show labels when they're useful:
          const shouldShowLabel = 
            isSelected ||                    // Always show selected
            isHovered ||                     // Always show hovered
            isChannel ||                     // Always show channels (they're the focus)
            globalScale > 1.8;              // Show all when zoomed in close
          
          if (shouldShowLabel) {
            ctx.font = `${fontSize}px -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", sans-serif`;
            ctx.textAlign = 'center';
            ctx.textBaseline = 'top';
            ctx.fontWeight = isChannel ? 'bold' : 'normal';
            
            // Measure text for background
            const textWidth = ctx.measureText(label).width;
            const padding = 6 / globalScale;
            const bgHeight = fontSize + padding * 2;
            const labelY = node.y + scaledRadius + 6 / globalScale;
            
            // Label background with stronger glow
            ctx.shadowColor = isChannel ? 'rgba(6, 182, 212, 0.6)' : 'rgba(249, 115, 22, 0.5)';
            ctx.shadowBlur = 15 / globalScale;
            ctx.fillStyle = 'rgba(10, 14, 26, 0.95)';
            ctx.fillRect(
              node.x - textWidth / 2 - padding,
              labelY - padding,
              textWidth + padding * 2,
              bgHeight
            );
            
            // Label border for definition
            ctx.strokeStyle = isChannel ? 'rgba(6, 182, 212, 0.4)' : 'rgba(249, 115, 22, 0.3)';
            ctx.lineWidth = 1 / globalScale;
            ctx.strokeRect(
              node.x - textWidth / 2 - padding,
              labelY - padding,
              textWidth + padding * 2,
              bgHeight
            );
            ctx.shadowBlur = 0;
            
            // Label text with glow
            ctx.shadowColor = coreColor;
            ctx.shadowBlur = 10 / globalScale;
            ctx.fillStyle = isSelected ? '#fbbf24' : (isOpportunity ? '#facc15' : (isChannel ? '#22d3ee' : '#fb923c'));
            ctx.fillText(label, node.x, labelY + 2 / globalScale);
            ctx.shadowBlur = 0;
          }
        }}
        nodeCanvasObjectMode={() => 'replace'}
        linkColor={(link: any) => {
          const sourceId = typeof link.source === 'object' ? link.source.id : link.source;
          const targetId = typeof link.target === 'object' ? link.target.id : link.target;
          
          if (selectedNodeId && (sourceId === selectedNodeId || targetId === selectedNodeId)) {
            return 'rgba(251, 191, 36, 0.6)'; // Gold
          }
          if (hoveredNode && (sourceId === hoveredNode || targetId === hoveredNode)) {
            return 'rgba(0, 212, 255, 0.5)'; // Teal
          }
          if (opportunityNodeIds.has(sourceId) || opportunityNodeIds.has(targetId)) {
            return 'rgba(245, 158, 11, 0.42)';
          }
          return isFocusedDetail ? 'rgba(120, 170, 220, 0.24)' : 'rgba(100, 150, 200, 0.15)';
        }}
        linkWidth={(link: any) => {
          const sourceId = typeof link.source === 'object' ? link.source.id : link.source;
          const targetId = typeof link.target === 'object' ? link.target.id : link.target;

          const signalWeight = Math.max(1, Number(link.value || 1));
          const baseWidth = isFocusedDetail
            ? Math.max(1.2, Math.min(6.8, 0.9 + Math.sqrt(signalWeight) * 0.58))
            : Math.max(0.7, Math.min(5.6, 0.5 + Math.sqrt(signalWeight) * 0.42));
          
          if (selectedNodeId && (sourceId === selectedNodeId || targetId === selectedNodeId)) {
            return baseWidth * 2;
          }
          if (hoveredNode && (sourceId === hoveredNode || targetId === hoveredNode)) {
            return baseWidth * 1.5;
          }
          if (opportunityNodeIds.has(sourceId) || opportunityNodeIds.has(targetId)) {
            return baseWidth * 1.25;
          }
          return baseWidth;
        }}
        linkDirectionalParticles={(link: any) => {
          const sourceId = typeof link.source === 'object' ? link.source.id : link.source;
          const targetId = typeof link.target === 'object' ? link.target.id : link.target;
          
          if (selectedNodeId && (sourceId === selectedNodeId || targetId === selectedNodeId)) {
            return 3;
          }
          return 0;
        }}
        linkDirectionalParticleWidth={2.5}
        linkDirectionalParticleSpeed={0.006}
        d3AlphaDecay={0.02}
        d3VelocityDecay={isFocusedDetail ? 0.27 : 0.3}
        cooldownTicks={isFocusedDetail ? 140 : 100}
        onEngineStop={() => {
          if (graphRef.current) {
            graphRef.current.zoomToFit(400, 100);
          }
        }}
        enableNodeDrag={true}
        enableZoomInteraction={true}
        enablePanInteraction={true}
      />
    </div>
  );
});

GraphVisualization.displayName = 'GraphVisualization';
