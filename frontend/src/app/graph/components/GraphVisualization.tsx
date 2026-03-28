import { forwardRef, useEffect, useImperativeHandle, useMemo, useRef, useState } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import { Loader2 } from 'lucide-react';
import { EmptyGraphState } from '@/app/graph/components/EmptyGraphState';
import { getGraphData, type GraphData, type GraphFilters, type GraphNode } from '@/app/graph/services/api';
import { getNodeColors, getNodeSize, type NodeType } from '@/app/graph/utils/nodeColors';

interface PreparedNode extends GraphNode {
  size: number;
  rank?: number;
  connections?: number;
  signal?: number;
  x?: number;
  y?: number;
}

interface GraphVisualizationProps {
  onNodeClick?: (node: GraphNode) => void;
  selectedNodeId?: string | null;
  filters?: GraphFilters;
  layoutInsets?: {
    left: number;
    right: number;
    top: number;
    bottom: number;
  };
  onDataUpdate?: (data: GraphData) => void;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function drawRoundedRect(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number,
  radius: number,
) {
  const safeRadius = Math.min(radius, width / 2, height / 2);
  ctx.beginPath();
  ctx.moveTo(x + safeRadius, y);
  ctx.arcTo(x + width, y, x + width, y + height, safeRadius);
  ctx.arcTo(x + width, y + height, x, y + height, safeRadius);
  ctx.arcTo(x, y + height, x, y, safeRadius);
  ctx.arcTo(x, y, x + width, y, safeRadius);
  ctx.closePath();
}

function sentimentRingColor(sentiment?: string): string {
  switch ((sentiment || '').toLowerCase()) {
    case 'positive':
      return '#4ade80';
    case 'negative':
      return '#fb7185';
    case 'urgent':
      return '#f97316';
    default:
      return '#cbd5e1';
  }
}

function topicBadgePalette(node: GraphNode): string[] {
  const badges: string[] = [];
  if ((node.askSignalCount || 0) > 0) badges.push('#60a5fa');
  if ((node.needSignalCount || 0) > 0) badges.push('#34d399');
  if ((node.fearSignalCount || 0) > 0) badges.push('#f87171');
  return badges;
}

function hashNumber(input: string): number {
  let hash = 0;
  for (let index = 0; index < input.length; index += 1) {
    hash = ((hash << 5) - hash) + input.charCodeAt(index);
    hash |= 0;
  }
  return Math.abs(hash);
}

function buildSeededGraph(
  data: GraphData | null,
  width: number,
  height: number,
  layoutInsets: { left: number; right: number; top: number; bottom: number },
): GraphData | null {
  if (!data) return null;

  const left = clamp(layoutInsets.left, 0, Math.max(0, width - 320));
  const right = clamp(layoutInsets.right, 0, Math.max(0, width - left - 220));
  const top = clamp(layoutInsets.top, 0, Math.max(0, height - 220));
  const bottom = clamp(layoutInsets.bottom, 0, Math.max(0, height - top - 180));
  const innerLeft = left;
  const innerRight = Math.max(innerLeft + 320, width - right);
  const innerTop = top;
  const innerBottom = Math.max(innerTop + 240, height - bottom);
  const innerWidth = Math.max(320, innerRight - innerLeft);
  const innerHeight = Math.max(240, innerBottom - innerTop);
  const centerX = innerLeft + (innerWidth / 2);
  const centerY = innerTop + (innerHeight / 2);

  const nodes = data.nodes.map((node) => ({
    ...node,
    size: getNodeSize(node.type as NodeType, Number(node.val || node.mentionCount || node.topicCount || 0)),
  })) as PreparedNode[];

  const nodeById = new Map(nodes.map((node) => [node.id, node]));
  const categories = nodes
    .filter((node) => node.type === 'category')
    .sort((a, b) => (Number(b.mentionCount || 0) - Number(a.mentionCount || 0)) || a.name.localeCompare(b.name));

  const innerCount = Math.min(categories.length, 6);
  const outerCount = Math.max(0, categories.length - innerCount);
  const innerRadiusX = Math.max(90, Math.min(182, innerWidth * 0.19));
  const innerRadiusY = Math.max(64, Math.min(112, innerHeight * 0.14));
  const outerRadiusX = Math.max(172, Math.min(320, innerWidth * 0.33));
  const outerRadiusY = Math.max(126, Math.min(232, innerHeight * 0.27));
  const clusterRadiusX = Math.max(110, Math.min(200, innerWidth * 0.22));
  const clusterRadiusY = Math.max(92, Math.min(166, innerHeight * 0.19));

  categories.forEach((node, index) => {
    const isInner = index < innerCount;
    const ringIndex = isInner ? index : index - innerCount;
    const ringSize = isInner ? innerCount : Math.max(1, outerCount);
    const angle = (-Math.PI / 2) + ((Math.PI * 2 * ringIndex) / Math.max(1, ringSize));
    const radiusX = isInner ? innerRadiusX : outerRadiusX;
    const radiusY = isInner ? innerRadiusY : outerRadiusY;
    const driftX = ((hashNumber(`${node.id}:x`) % 13) - 6) * (isInner ? 5 : 8);
    const driftY = ((hashNumber(`${node.id}:y`) % 11) - 5) * (isInner ? 4 : 7);
    node.x = clamp(centerX + (Math.cos(angle) * radiusX) + driftX, innerLeft + 38, innerRight - 38);
    node.y = clamp(centerY + (Math.sin(angle) * radiusY) + driftY, innerTop + 38, innerBottom - 38);
    node.rank = index;
  });

  const categoryAngles = new Map<string, number>();
  categories.forEach((node, index) => {
    categoryAngles.set(node.name, (hashNumber(`${node.name}:${index}`) % 360) * (Math.PI / 180));
  });

  const topicsByCategory = new Map<string, PreparedNode[]>();
  nodes
    .filter((node) => node.type === 'topic')
    .forEach((node) => {
      const bucket = topicsByCategory.get(node.category || 'Uncategorized') || [];
      bucket.push(node);
      topicsByCategory.set(node.category || 'Uncategorized', bucket);
    });

  topicsByCategory.forEach((topicNodes, categoryName) => {
    const categoryNode = categories.find((entry) => entry.name === categoryName);
    const baseX = categoryNode?.x ?? centerX;
    const baseY = categoryNode?.y ?? centerY;
    const baseAngle = categoryAngles.get(categoryName) ?? -Math.PI / 2;

    topicNodes
      .sort((a, b) => (Number(b.mentionCount || 0) - Number(a.mentionCount || 0)) || a.name.localeCompare(b.name))
      .forEach((node, index) => {
        const ringCapacities = [5, 8, 11, 14, 18];
        let remainingIndex = index;
        let ring = 0;
        while (ring < ringCapacities.length - 1 && remainingIndex >= ringCapacities[ring]) {
          remainingIndex -= ringCapacities[ring];
          ring += 1;
        }
        const slotCount = ringCapacities[ring] || (18 + (ring * 4));
        const slot = remainingIndex;
        const angle = baseAngle
          + ((Math.PI * 2 * slot) / Math.max(1, slotCount))
          + (((hashNumber(node.id) % 17) - 8) * 0.02);
        const orbitX = clusterRadiusX + (ring * 46);
        const orbitY = clusterRadiusY + (ring * 38);
        node.x = clamp(baseX + (Math.cos(angle) * orbitX), innerLeft + 24, innerRight - 24);
        node.y = clamp(baseY + (Math.sin(angle) * orbitY), innerTop + 24, innerBottom - 24);
        node.rank = index;
      });
  });

  const channelLinks = data.links.filter((link) => link.type === 'channel-topic');
  const channelTopicAngles = new Map<string, number[]>();
  channelLinks.forEach((link) => {
    const sourceId = typeof link.source === 'string' ? link.source : link.source.id;
    const targetId = typeof link.target === 'string' ? link.target : link.target.id;
    const channelId = nodeById.get(sourceId)?.type === 'channel' ? sourceId : targetId;
    const topicId = channelId === sourceId ? targetId : sourceId;
    const topicNode = nodeById.get(topicId);
    if (typeof topicNode?.x !== 'number' || typeof topicNode?.y !== 'number') return;
    const angle = Math.atan2(topicNode.y - centerY, topicNode.x - centerX);
    const bucket = channelTopicAngles.get(channelId) || [];
    bucket.push(angle);
    channelTopicAngles.set(channelId, bucket);
  });

  nodes
    .filter((node) => node.type === 'channel')
    .sort((a, b) => a.name.localeCompare(b.name))
    .forEach((node, index, list) => {
      const connectedAngles = channelTopicAngles.get(node.id);
      const fallbackAngle = (-Math.PI / 2) + ((Math.PI * 2 * index) / Math.max(1, list.length));
      const meanAngle = connectedAngles && connectedAngles.length > 0
        ? connectedAngles.reduce((sum, value) => sum + value, 0) / connectedAngles.length
        : fallbackAngle;
      const jitter = ((hashNumber(node.id) % 23) - 11) * 0.018;
      const laneSpread = ((index % 4) - 1.5) * 18;
      const ringX = (innerWidth / 2) + 34 + laneSpread;
      const ringY = (innerHeight / 2) + 28 + laneSpread;
      node.x = clamp(centerX + (Math.cos(meanAngle + jitter) * ringX), innerLeft + 14, innerRight - 14);
      node.y = clamp(centerY + (Math.sin(meanAngle + jitter) * ringY), innerTop + 14, innerBottom - 14);
    });

  return {
    nodes,
    links: data.links.map((link) => ({ ...link })),
    meta: data.meta,
  };
}

function enrichGraphData(
  data: GraphData | null,
  width: number,
  height: number,
  layoutInsets: { left: number; right: number; top: number; bottom: number },
): GraphData | null {
  const seededData = buildSeededGraph(data, width, height, layoutInsets);
  if (!seededData) return null;

  const connectionCount = new Map<string, number>();
  const signalTotals = new Map<string, number>();

  seededData.links.forEach((link) => {
    const sourceId = typeof link.source === 'string' ? link.source : link.source.id;
    const targetId = typeof link.target === 'string' ? link.target : link.target.id;
    const linkValue = Math.max(1, Number(link.value || 1));

    connectionCount.set(sourceId, (connectionCount.get(sourceId) || 0) + 1);
    connectionCount.set(targetId, (connectionCount.get(targetId) || 0) + 1);
    signalTotals.set(sourceId, (signalTotals.get(sourceId) || 0) + linkValue);
    signalTotals.set(targetId, (signalTotals.get(targetId) || 0) + linkValue);
  });

  const nodes = seededData.nodes.map((node) => {
    const type = (node.type || 'topic') as NodeType;
    const connections = connectionCount.get(node.id) || 0;
    const signal = signalTotals.get(node.id) || 0;
    const baseSize = typeof (node as PreparedNode).size === 'number'
      ? Number((node as PreparedNode).size)
      : getNodeSize(type, Number(node.val || node.mentionCount || node.topicCount || 0));

    const signalBoost = type === 'channel'
      ? Math.min(Math.log(signal + 1) * 0.45, 4)
      : type === 'topic'
        ? Math.min(Math.log(signal + 1) * 1.1, 10)
        : Math.min(Math.log(signal + 1) * 0.35, 3);

    return {
      ...node,
      connections,
      signal,
      size: baseSize + signalBoost,
    } as PreparedNode;
  });

  return {
    nodes,
    links: seededData.links,
    meta: seededData.meta,
  };
}

function getLinkNodeType(linkNode: unknown, typeById: Map<string, string>): string {
  if (linkNode && typeof linkNode === 'object' && 'type' in (linkNode as Record<string, unknown>)) {
    return String((linkNode as Record<string, unknown>).type || 'topic');
  }
  return typeById.get(String(linkNode || '')) || 'topic';
}

export const GraphVisualization = forwardRef<any, GraphVisualizationProps>(
  ({ onNodeClick, selectedNodeId, filters, layoutInsets, onDataUpdate }, ref) => {
    const [rawGraphData, setRawGraphData] = useState<GraphData | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [dimensions, setDimensions] = useState({ width: 1200, height: 760 });
    const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);
    const graphRef = useRef<any>();
    const containerRef = useRef<HTMLDivElement | null>(null);

    const sourceDetail = filters?.sourceDetail || 'standard';
    const denseView = sourceDetail === 'expanded';
    const lightView = sourceDetail === 'minimal';

    const preparedData = useMemo(
      () => enrichGraphData(
        rawGraphData,
        dimensions.width,
        dimensions.height,
        layoutInsets || {
          left: 392,
          right: 92,
          top: 108,
          bottom: 44,
        },
      ),
      [dimensions.height, dimensions.width, layoutInsets, rawGraphData],
    );

    const typeById = useMemo(
      () => new Map((preparedData?.nodes || []).map((node) => [node.id, node.type])),
      [preparedData?.nodes],
    );

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
        if (!graphRef.current || !preparedData?.nodes) return;
        const target = preparedData.nodes.find((node) => node.id === nodeId);
        if (!target || typeof target.x !== 'number' || typeof target.y !== 'number') return;
        graphRef.current.centerAt(target.x, target.y, 800);
        graphRef.current.zoom(2.2, 800);
      },
    }));

    useEffect(() => {
      const updateDimensions = () => {
        const element = containerRef.current;
        if (!element) return;
        setDimensions({
          width: Math.max(320, element.clientWidth),
          height: Math.max(360, element.clientHeight),
        });
      };

      updateDimensions();
      let observer: ResizeObserver | null = null;
      if (typeof ResizeObserver !== 'undefined' && containerRef.current) {
        observer = new ResizeObserver(updateDimensions);
        observer.observe(containerRef.current);
      }

      window.addEventListener('resize', updateDimensions);
      return () => {
        observer?.disconnect();
        window.removeEventListener('resize', updateDimensions);
      };
    }, []);

    useEffect(() => {
      let active = true;

      const fetchData = async () => {
        try {
          setLoading(true);
          setError(null);
          const data = await getGraphData(filters || {});
          if (!active) return;
          setRawGraphData(data);
        } catch (fetchError) {
          if (!active) return;
          console.error('Failed to load conversation map:', fetchError);
          setError(fetchError instanceof Error ? fetchError.message : 'Failed to load conversation map');
        } finally {
          if (active) {
            setLoading(false);
          }
        }
      };

      void fetchData();
      return () => {
        active = false;
      };
    }, [filters]);

    useEffect(() => {
      if (preparedData) {
        onDataUpdate?.(preparedData);
      }
    }, [onDataUpdate, preparedData]);

    useEffect(() => {
      if (!preparedData || !graphRef.current) return;

      const graphInstance = graphRef.current;
      const linkForce = graphInstance.d3Force?.('link');
      if (linkForce) {
        if (typeof linkForce.distance === 'function') {
          linkForce.distance((link: any) => {
            const sourceType = getLinkNodeType(link.source, typeById);
            const targetType = getLinkNodeType(link.target, typeById);
            const hasChannel = sourceType === 'channel' || targetType === 'channel';
            const hasTopic = sourceType === 'topic' || targetType === 'topic';
            const hasCategory = sourceType === 'category' || targetType === 'category';

            if (hasChannel && hasTopic) return denseView ? 185 : 165;
            if (hasCategory && hasTopic) return denseView ? 140 : 125;
            return denseView ? 160 : 140;
          });
        }

        if (typeof linkForce.strength === 'function') {
          linkForce.strength((link: any) => {
            const sourceType = getLinkNodeType(link.source, typeById);
            const targetType = getLinkNodeType(link.target, typeById);
            return sourceType === 'channel' || targetType === 'channel' ? 0.3 : 0.22;
          });
        }
      }

      const chargeForce = graphInstance.d3Force?.('charge');
      if (chargeForce) {
        if (typeof chargeForce.strength === 'function') {
          chargeForce.strength(denseView ? -1050 : lightView ? -760 : -900);
        }
        if (typeof chargeForce.distanceMax === 'function') {
          chargeForce.distanceMax(denseView ? 460 : 400);
        }
      }

      graphInstance.d3ReheatSimulation?.();
    }, [denseView, lightView, preparedData, typeById]);

    useEffect(() => {
      if (!graphRef.current || !preparedData?.nodes?.length || selectedNodeId) return;
      const timer = window.setTimeout(() => {
        graphRef.current?.zoomToFit(400, 100);
      }, 160);
      return () => window.clearTimeout(timer);
    }, [preparedData?.nodes?.length, selectedNodeId]);

    const hasFilters = Boolean(
      (filters?.channels?.length || 0) > 0
      || (filters?.sentiments?.length || 0) > 0
      || (filters?.category || '').trim()
      || (filters?.signalFocus && filters.signalFocus !== 'all')
      || Number(filters?.minMentions || 0) > 2
    );

    const connectedNodeIds = useMemo(() => {
      if (!selectedNodeId || !preparedData) return new Set<string>();
      const ids = new Set<string>([selectedNodeId]);
      preparedData.links.forEach((link) => {
        const sourceId = typeof link.source === 'string' ? link.source : link.source.id;
        const targetId = typeof link.target === 'string' ? link.target : link.target.id;
        if (sourceId === selectedNodeId) ids.add(targetId);
        if (targetId === selectedNodeId) ids.add(sourceId);
      });
      return ids;
    }, [preparedData, selectedNodeId]);

    const selectedNode = useMemo(
      () => preparedData?.nodes.find((node) => node.id === selectedNodeId) || null,
      [preparedData?.nodes, selectedNodeId],
    );

    const signalFocusLabel = useMemo(() => {
      switch (filters?.signalFocus) {
        case 'asks':
          return 'Questions & Asks';
        case 'needs':
          return 'Needs & Services';
        case 'fear':
          return 'Fear / Urgency';
        default:
          return 'Conversation Map';
      }
    }, [filters?.signalFocus]);

    const sourceDetailLabel = useMemo(() => {
      switch (sourceDetail) {
        case 'minimal':
          return 'Minimal';
        case 'expanded':
          return 'Expanded';
        default:
          return 'Balanced';
      }
    }, [sourceDetail]);

    const headlineLabel = useMemo(() => {
      if (selectedNode) return selectedNode.name;
      if ((filters?.category || '').trim()) return filters?.category || 'Conversation Map';
      return 'Conversation Landscape';
    }, [filters?.category, selectedNode]);

    const headlineType = selectedNode ? `${selectedNode.type}` : signalFocusLabel;
    const headlineTypeLabel = selectedNode
      ? `${headlineType.charAt(0).toUpperCase()}${headlineType.slice(1)}`
      : headlineType;

    if (loading) {
      return (
        <div className="w-full h-full flex items-center justify-center bg-[#0B0E14]">
          <div className="flex flex-col items-center gap-4">
            <Loader2 className="w-12 h-12 text-cyan-500 animate-spin" />
            <div className="text-center">
              <p className="text-white/90 text-sm font-medium mb-1">Building conversation map</p>
              <p className="text-white/50 text-xs">Loading categories, topics, and source context...</p>
            </div>
          </div>
        </div>
      );
    }

    if (error) {
      return (
        <div className="w-full h-full flex items-center justify-center bg-[#0B0E14]">
          <div className="rounded-3xl bg-rose-500/10 border border-rose-400/20 px-6 py-5 text-center max-w-md">
            <div className="text-rose-100 font-medium">Conversation map failed to load</div>
            <div className="text-rose-100/70 text-sm mt-2">{error}</div>
          </div>
        </div>
      );
    }

    if (!preparedData || preparedData.nodes.length === 0) {
      return <EmptyGraphState hasFilters={hasFilters} />;
    }

    return (
      <div
        ref={containerRef}
        className="w-full h-full relative"
        style={{
          background:
            'radial-gradient(circle at 50% 20%, rgba(39, 201, 214, 0.18), transparent 22%), linear-gradient(135deg, #08101a 0%, #121d2d 100%)',
        }}
      >
        <div
          className="absolute top-6 z-10 rounded-[40px] border border-cyan-400/22 bg-[linear-gradient(135deg,rgba(10,16,30,0.92),rgba(18,20,34,0.9))] px-7 py-5 shadow-[0_30px_60px_rgba(0,0,0,0.38)] backdrop-blur-lg"
          style={{
            left: `${(layoutInsets?.left || 392) + 8}px`,
            right: `${(layoutInsets?.right || 92) + 8}px`,
          }}
        >
          <div className="flex flex-wrap items-center gap-5">
            <div className="flex min-w-[250px] flex-1 items-start gap-4">
              <div className="mt-1 h-4 w-1 rounded-full bg-cyan-300 shadow-[0_0_20px_rgba(34,211,238,0.6)]" />
              <div className="min-w-0">
                <div className="text-[11px] uppercase tracking-[0.18em] text-cyan-200/70">{headlineTypeLabel}</div>
                <div className="mt-1 text-[22px] font-semibold leading-tight tracking-[-0.03em] text-white">
                  {headlineLabel}
                </div>
              </div>
            </div>

            <div className="h-10 w-px bg-white/12" />

            <div className="flex flex-wrap items-center gap-x-8 gap-y-3 text-sm">
              <div>
                <div className="text-[11px] uppercase tracking-[0.18em] text-white/35">Categories</div>
                <div className="mt-1 text-[16px] font-medium text-cyan-100">{preparedData.meta?.visibleCategoryCount || 0}</div>
              </div>
              <div>
                <div className="text-[11px] uppercase tracking-[0.18em] text-white/35">Topics</div>
                <div className="mt-1 text-[16px] font-medium text-orange-300">{preparedData.meta?.visibleTopicCount || 0}</div>
              </div>
              <div>
                <div className="text-[11px] uppercase tracking-[0.18em] text-white/35">Channels</div>
                <div className="mt-1 text-[16px] font-medium text-white/82">{preparedData.meta?.visibleChannelCount || 0}</div>
              </div>
              <div>
                <div className="text-[11px] uppercase tracking-[0.18em] text-white/35">Mentions</div>
                <div className="mt-1 text-[16px] font-medium text-white/68">{preparedData.meta?.totalMentions || 0}</div>
              </div>
              <div>
                <div className="text-[11px] uppercase tracking-[0.18em] text-white/35">View</div>
                <div className="mt-1 text-[16px] font-medium text-cyan-300">{signalFocusLabel}</div>
              </div>
              <div>
                <div className="text-[11px] uppercase tracking-[0.18em] text-white/35">Source Detail</div>
                <div className="mt-1 text-[16px] font-medium text-[#a6b9ff]">{sourceDetailLabel}</div>
              </div>
            </div>
          </div>
        </div>

        <ForceGraph2D
          ref={graphRef}
          graphData={preparedData}
          width={dimensions.width}
          height={dimensions.height}
          backgroundColor="transparent"
          nodeVal={(node: any) => {
            const type = (node.type || 'topic') as NodeType;
            const size = typeof node.size === 'number'
              ? Number(node.size)
              : getNodeSize(type, Number(node.val || node.mentionCount || node.topicCount || 0));
            return Math.max(16, size * size);
          }}
          nodeRelSize={1}
          nodeLabel={(node: any) => `${node.name} (${node.connections || 0} connections)`}
          onNodeClick={(node: any) => onNodeClick?.(node)}
          onNodeHover={(node: any) => setHoveredNodeId(node?.id || null)}
          onNodeDragEnd={(node: any) => {
            node.fx = node.x;
            node.fy = node.y;
          }}
          nodeCanvasObjectMode={() => 'replace'}
          nodeCanvasObject={(node: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
            if (!node || typeof node.x !== 'number' || typeof node.y !== 'number' || !Number.isFinite(node.x) || !Number.isFinite(node.y)) {
              return;
            }

            const type = (node.type || 'topic') as NodeType;
            const colors = getNodeColors(type);
            const isSelected = selectedNodeId === node.id;
            const isHovered = hoveredNodeId === node.id;
            const isConnected = connectedNodeIds.has(node.id);
            const nodeName = String(node.name || '');
            const radius = Math.max(
              type === 'channel' ? 6 : 8,
              Number(node.size || getNodeSize(type, Number(node.connections || node.val || 0))) / globalScale,
            );

            let coreColor = colors.core;
            let glowColor = colors.glow;
            let edgeColor = type === 'topic' ? sentimentRingColor(node.dominantSentiment) : colors.edge;

            if (isSelected) {
              coreColor = '#fbbf24';
              glowColor = 'rgba(251, 191, 36, 0.8)';
              edgeColor = '#fcd34d';
            } else if (isHovered) {
              glowColor = glowColor.replace(/[\d.]+\)$/u, '0.8)');
            }

            const outerGlowRadius = radius * (denseView ? 2.6 : 3.3);
            const outerGlow = ctx.createRadialGradient(node.x, node.y, 0, node.x, node.y, outerGlowRadius);
            outerGlow.addColorStop(0, glowColor);
            outerGlow.addColorStop(0.4, glowColor.replace(/[\d.]+\)$/u, '0.3)'));
            outerGlow.addColorStop(1, 'rgba(0, 0, 0, 0)');

            ctx.fillStyle = outerGlow;
            ctx.beginPath();
            ctx.arc(node.x, node.y, outerGlowRadius, 0, Math.PI * 2);
            ctx.fill();

            const innerGlowRadius = radius * 1.9;
            const innerGlow = ctx.createRadialGradient(node.x, node.y, 0, node.x, node.y, innerGlowRadius);
            innerGlow.addColorStop(0, glowColor);
            innerGlow.addColorStop(0.5, glowColor.replace(/[\d.]+\)$/u, '0.4)'));
            innerGlow.addColorStop(1, 'rgba(0, 0, 0, 0)');

            ctx.fillStyle = innerGlow;
            ctx.beginPath();
            ctx.arc(node.x, node.y, innerGlowRadius, 0, Math.PI * 2);
            ctx.fill();

            const shellGradient = ctx.createRadialGradient(
              node.x - (radius * 0.35),
              node.y - (radius * 0.35),
              0,
              node.x,
              node.y,
              radius,
            );
            shellGradient.addColorStop(0, '#ffffff');
            shellGradient.addColorStop(0.3, coreColor);
            shellGradient.addColorStop(1, colors.darkEdge);

            ctx.fillStyle = shellGradient;
            ctx.beginPath();
            ctx.arc(node.x, node.y, radius, 0, Math.PI * 2);
            ctx.fill();

            if (type !== 'channel') {
              ctx.fillStyle = 'rgba(255, 255, 255, 0.7)';
              ctx.beginPath();
              ctx.arc(node.x - (radius * 0.4), node.y - (radius * 0.4), radius * 0.3, 0, Math.PI * 2);
              ctx.fill();
            }

            ctx.strokeStyle = edgeColor;
            ctx.lineWidth = (type === 'channel' ? 2 : 1.5) / globalScale;
            ctx.beginPath();
            ctx.arc(node.x, node.y, radius, 0, Math.PI * 2);
            ctx.stroke();

            if (type === 'topic') {
              const badges = topicBadgePalette(node);
              badges.forEach((badgeColor, index) => {
                ctx.beginPath();
                ctx.fillStyle = badgeColor;
                ctx.arc(
                  node.x - radius + (index * (7 / globalScale)),
                  node.y + radius + (5 / globalScale),
                  2.2 / globalScale,
                  0,
                  Math.PI * 2,
                );
                ctx.fill();
              });
            }

            if (isSelected) {
              const pulse = Math.sin(Date.now() / 300);
              ctx.strokeStyle = 'rgba(251, 191, 36, 0.6)';
              ctx.lineWidth = 2.5 / globalScale;
              ctx.beginPath();
              ctx.arc(node.x, node.y, radius * (1.3 + (pulse * 0.15)), 0, Math.PI * 2);
              ctx.stroke();

              ctx.strokeStyle = 'rgba(251, 191, 36, 0.3)';
              ctx.lineWidth = 1.5 / globalScale;
              ctx.beginPath();
              ctx.arc(node.x, node.y, radius * (1.5 + (pulse * 0.2)), 0, Math.PI * 2);
              ctx.stroke();
            }

            const showLabel = isSelected
              || isHovered
              || isConnected
              || type === 'channel'
              || (type === 'category' && globalScale > 1.1)
              || (type === 'topic' && globalScale > 1.8);

            if (!showLabel) {
              return;
            }

            const fontSize = Math.max(11, 12 / globalScale);
            ctx.font = `${fontSize}px -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", sans-serif`;
            ctx.textAlign = 'center';
            ctx.textBaseline = 'top';

            const textWidth = ctx.measureText(nodeName).width;
            const labelPadding = 6 / globalScale;
            const labelHeight = fontSize + (labelPadding * 2);
            const labelTop = node.y + radius + (6 / globalScale);

            ctx.shadowColor = type === 'channel' ? 'rgba(6, 182, 212, 0.6)' : 'rgba(249, 115, 22, 0.5)';
            ctx.shadowBlur = 15 / globalScale;
            ctx.fillStyle = 'rgba(10, 14, 26, 0.95)';
            drawRoundedRect(
              ctx,
              node.x - (textWidth / 2) - labelPadding,
              labelTop - labelPadding,
              textWidth + (labelPadding * 2),
              labelHeight,
              8 / globalScale,
            );
            ctx.fill();
            ctx.shadowBlur = 0;

            ctx.strokeStyle = type === 'channel' ? 'rgba(6, 182, 212, 0.4)' : 'rgba(249, 115, 22, 0.3)';
            ctx.lineWidth = 1 / globalScale;
            drawRoundedRect(
              ctx,
              node.x - (textWidth / 2) - labelPadding,
              labelTop - labelPadding,
              textWidth + (labelPadding * 2),
              labelHeight,
              8 / globalScale,
            );
            ctx.stroke();

            ctx.shadowColor = coreColor;
            ctx.shadowBlur = 10 / globalScale;
            ctx.fillStyle = isSelected ? '#fbbf24' : type === 'channel' ? '#22d3ee' : '#fb923c';
            ctx.fillText(nodeName, node.x, labelTop + (2 / globalScale));
            ctx.shadowBlur = 0;
          }}
          linkCurvature={(link: any) => {
            const sourceType = getLinkNodeType(link.source, typeById);
            const targetType = getLinkNodeType(link.target, typeById);
            return sourceType === 'channel' || targetType === 'channel' ? 0.08 : 0.14;
          }}
          linkColor={(link: any) => {
            const sourceId = typeof link.source === 'string' ? link.source : link.source.id;
            const targetId = typeof link.target === 'string' ? link.target : link.target.id;

            if (selectedNodeId && (sourceId === selectedNodeId || targetId === selectedNodeId)) {
              return 'rgba(251, 191, 36, 0.6)';
            }

            if (hoveredNodeId && (sourceId === hoveredNodeId || targetId === hoveredNodeId)) {
              return 'rgba(0, 212, 255, 0.5)';
            }

            return denseView ? 'rgba(120, 170, 220, 0.24)' : 'rgba(100, 150, 200, 0.15)';
          }}
          linkWidth={(link: any) => {
            const sourceId = typeof link.source === 'string' ? link.source : link.source.id;
            const targetId = typeof link.target === 'string' ? link.target : link.target.id;
            const baseWeight = Math.max(1, Number(link.value || 1));
            const baseWidth = denseView
              ? Math.max(1.2, Math.min(6.8, 0.9 + (Math.sqrt(baseWeight) * 0.58)))
              : Math.max(0.7, Math.min(5.6, 0.5 + (Math.sqrt(baseWeight) * 0.42)));

            if (selectedNodeId && (sourceId === selectedNodeId || targetId === selectedNodeId)) {
              return baseWidth * 2;
            }

            if (hoveredNodeId && (sourceId === hoveredNodeId || targetId === hoveredNodeId)) {
              return baseWidth * 1.5;
            }

            return connectedNodeIds.has(sourceId) || connectedNodeIds.has(targetId)
              ? baseWidth * 1.25
              : baseWidth;
          }}
          linkDirectionalParticles={(link: any) => {
            const sourceId = typeof link.source === 'string' ? link.source : link.source.id;
            const targetId = typeof link.target === 'string' ? link.target : link.target.id;
            return selectedNodeId && (sourceId === selectedNodeId || targetId === selectedNodeId) ? 3 : 0;
          }}
          linkDirectionalParticleWidth={2.5}
          linkDirectionalParticleSpeed={0.006}
          d3AlphaDecay={0.02}
          d3VelocityDecay={denseView ? 0.27 : 0.3}
          cooldownTicks={denseView ? 140 : 100}
          onEngineStop={() => {
            if (!selectedNodeId) {
              graphRef.current?.zoomToFit(400, 100);
            }
          }}
          enableNodeDrag
          enableZoomInteraction
          enablePanInteraction
        />
      </div>
    );
  },
);

GraphVisualization.displayName = 'GraphVisualization';
