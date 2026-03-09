import { Calendar, Filter, RotateCcw, ChevronLeft, ChevronDown, Search, Layers, Target } from 'lucide-react';
import { Checkbox } from '@/app/components/ui/checkbox';
import { useState, useEffect, useRef } from 'react';
import { getAllChannels, getTopChannels, getTrendingTopics, TopChannel, TrendingTopic } from '@/app/graph/services/api';
import { getNodeColors, getNodeLabel, NodeType } from '@/app/graph/utils/nodeColors';

const TIMEFRAMES = ['Last 24h', 'Last 7 Days', 'Last Month', 'Last 3 Months'];
const DEFAULT_CONNECTION_STRENGTH = 3;

type InsightTool = 'products' | 'customerNeeds' | 'competitorIntel';
type InsightMode = 'marketMap' | 'ownership' | 'messageFit' | 'competitorMoves' | 'opportunities';
type SourceProfile = 'balanced' | 'performance' | 'brandStrategy';

const INSIGHT_MODE_OPTIONS: Array<{ value: InsightMode; label: string; description: string }> = [
  {
    value: 'marketMap',
    label: 'Market Map',
    description: 'Top themes connecting selected channels',
  },
  {
    value: 'ownership',
    label: 'Who Owns What',
    description: 'Topic ownership by channel',
  },
  {
    value: 'messageFit',
    label: 'Message Fit',
    description: 'What message works for whom',
  },
  {
    value: 'competitorMoves',
    label: 'Competitor Moves',
    description: 'Where competitors are shifting focus',
  },
  {
    value: 'opportunities',
    label: 'Hidden Opportunities',
    description: 'Underserved high-potential spaces',
  },
];

const SOURCE_PROFILE_OPTIONS: Array<{ value: SourceProfile; label: string; description: string }> = [
  {
    value: 'balanced',
    label: 'Balanced',
    description: 'Channel activity weighting',
  },
  {
    value: 'performance',
    label: 'Performance',
    description: 'Activity-heavy weighting for channel momentum',
  },
  {
    value: 'brandStrategy',
    label: 'Channel Strategy',
    description: 'Channel narrative weighting',
  },
];

const INSIGHT_TOOL_OPTIONS: Array<{
  value: InsightTool;
  title: string;
  description: string;
  colorType: NodeType;
}> = [
  {
    value: 'products',
    title: 'Products',
    description: 'Channel topic and intent signals',
    colorType: 'product',
  },
  {
    value: 'customerNeeds',
    title: 'Customer Needs',
    description: 'Audience, pain points, value props, intents',
    colorType: 'audience',
  },
  {
    value: 'competitorIntel',
    title: 'Competitor Intel',
    description: 'Competitive references and overlaps',
    colorType: 'competitor',
  },
];

const MODE_TOOL_PRESETS: Record<InsightMode, Record<InsightTool, boolean>> = {
  marketMap: {
    products: false,
    customerNeeds: false,
    competitorIntel: false,
  },
  ownership: {
    products: true,
    customerNeeds: false,
    competitorIntel: false,
  },
  messageFit: {
    products: false,
    customerNeeds: true,
    competitorIntel: false,
  },
  competitorMoves: {
    products: false,
    customerNeeds: false,
    competitorIntel: true,
  },
  opportunities: {
    products: false,
    customerNeeds: true,
    competitorIntel: true,
  },
};

function layersFromInsightTools(tools: Record<InsightTool, boolean>): string[] {
  const layers = ['topic'];
  if (tools.products) {
    layers.push('product');
  }
  if (tools.customerNeeds) {
    layers.push('audience', 'painpoint', 'valueprop', 'intent');
  }
  if (tools.competitorIntel) {
    layers.push('competitor');
  }
  return layers;
}

const SENTIMENTS = [
  { label: 'Positive', icon: '😊', color: 'text-green-400' },
  { label: 'Negative', icon: '😞', color: 'text-red-400' },
  { label: 'Neutral', icon: '😐', color: 'text-gray-400' },
  { label: 'Urgent', icon: '⚡', color: 'text-orange-400' },
];

interface GlobalFiltersProps {
  onFiltersChange?: (filters: {
    channels?: string[];
    timeframe?: string;
    sentiments?: string[];
    topics?: string[];
    connectionStrength?: number;
    layers?: string[];
    insightMode?: InsightMode;
    sourceProfile?: SourceProfile;
    confidenceThreshold?: number;
  }) => void;
  onQuickSelectChannel?: (channelName: string) => void;
  onDateRangeChange?: (startDate: string, endDate: string) => void;
  onSearchSelect?: (nodeId: string) => void;
  allNodes?: Array<{ id: string; name: string; type: string }>;
}

export function GlobalFilters({ onFiltersChange, onQuickSelectChannel, onDateRangeChange, onSearchSelect, allNodes = [] }: GlobalFiltersProps) {
  const [isCollapsed, setIsCollapsed] = useState(false);
  const [selectedTimeframe, setSelectedTimeframe] = useState('Last 7 Days');
  const [selectedChannels, setSelectedChannels] = useState<string[]>([]);
  const [selectedSentiments, setSelectedSentiments] = useState<string[]>([]);
  const [selectedTopics, setSelectedTopics] = useState<string[]>([]);
  const [selectedInsightTools, setSelectedInsightTools] = useState<Record<InsightTool, boolean>>({
    products: false,
    customerNeeds: false,
    competitorIntel: false,
  });
  const [selectedInsightMode, setSelectedInsightMode] = useState<InsightMode>('marketMap');
  const [selectedSourceProfile, setSelectedSourceProfile] = useState<SourceProfile>('balanced');
  const [confidenceThreshold, setConfidenceThreshold] = useState<number>(35);
  const [connectionStrength, setConnectionStrength] = useState<number>(DEFAULT_CONNECTION_STRENGTH);
  const [hasUnappliedChanges, setHasUnappliedChanges] = useState(false);
  const [lastModeAppliedAt, setLastModeAppliedAt] = useState<Date | null>(null);
  const [lastModeAppliedMessage, setLastModeAppliedMessage] = useState<string>('');
  const suppressDirtyRef = useRef(false);

  // Search state
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<typeof allNodes>([]);
  
  // Data from backend
  const [allChannels, setAllChannels] = useState<TopChannel[]>([]);
  const [allTopics, setAllTopics] = useState<TrendingTopic[]>([]);
  const [channelsLoading, setChannelsLoading] = useState(true);
  const [topicsLoading, setTopicsLoading] = useState(true);
  const isBootstrappingRef = useRef(false);
  
  // Pagination
  const [showAllChannels, setShowAllChannels] = useState(false);
  const [showAllTopics, setShowAllTopics] = useState(false);
  
  // Search - only for topics
  const [topicSearchQuery, setTopicSearchQuery] = useState('');

  // Handle global search
  useEffect(() => {
    if (searchQuery.trim().length < 2) {
      setSearchResults([]);
      return;
    }

    const filtered = allNodes.filter(node =>
      node.name.toLowerCase().includes(searchQuery.toLowerCase())
    ).slice(0, 5);

    setSearchResults(filtered);
  }, [searchQuery, allNodes]);

  const handleSearchSelect = (nodeId: string) => {
    if (onSearchSelect) {
      onSearchSelect(nodeId);
    }
    setSearchQuery('');
    setSearchResults([]);
  };

  // Track if filters have changed but not applied
  useEffect(() => {
    if (isBootstrappingRef.current) return;
    if (suppressDirtyRef.current) {
      suppressDirtyRef.current = false;
      setHasUnappliedChanges(false);
      return;
    }
    setHasUnappliedChanges(true);
  }, [
    selectedChannels,
    selectedTimeframe,
    selectedSentiments,
    selectedTopics,
    selectedInsightTools,
    selectedInsightMode,
    selectedSourceProfile,
    confidenceThreshold,
    connectionStrength,
  ]);

  const applyFilters = (overrides?: {
    insightMode?: InsightMode;
    sourceProfile?: SourceProfile;
    confidenceThreshold?: number;
    insightTools?: Record<InsightTool, boolean>;
    connectionStrength?: number;
  }) => {
    const nextInsightMode = overrides?.insightMode ?? selectedInsightMode;
    const nextSourceProfile = overrides?.sourceProfile ?? selectedSourceProfile;
    const nextConfidence = overrides?.confidenceThreshold ?? confidenceThreshold;
    const nextInsightTools = overrides?.insightTools ?? selectedInsightTools;
    const nextConnectionStrength = overrides?.connectionStrength ?? connectionStrength;

    if (onFiltersChange) {
      onFiltersChange({
        channels: selectedChannels,
        timeframe: selectedTimeframe,
        sentiments: selectedSentiments,
        topics: selectedTopics,
        connectionStrength: nextConnectionStrength,
        layers: layersFromInsightTools(nextInsightTools),
        insightMode: nextInsightMode,
        sourceProfile: nextSourceProfile,
        confidenceThreshold: nextConfidence,
      });
    }
    setHasUnappliedChanges(false);
  };

  const handleApply = () => {
    applyFilters();
  };

  const handleReset = () => {
    setSelectedChannels([]);
    setSelectedTimeframe('Last 7 Days');
    setSelectedSentiments([]);
    setSelectedTopics([]);
    setSelectedInsightTools({
      products: false,
      customerNeeds: false,
      competitorIntel: false,
    });
    setSelectedInsightMode('marketMap');
    setSelectedSourceProfile('balanced');
    setConfidenceThreshold(35);
    setConnectionStrength(DEFAULT_CONNECTION_STRENGTH);
    setLastModeAppliedAt(null);
    setLastModeAppliedMessage('');
    
    if (onFiltersChange) {
      onFiltersChange({
        channels: [],
        timeframe: 'Last 7 Days',
        sentiments: [],
        topics: [],
        connectionStrength: DEFAULT_CONNECTION_STRENGTH,
        layers: ['topic'],
        insightMode: 'marketMap',
        sourceProfile: 'balanced',
        confidenceThreshold: 35,
      });
    }
    setHasUnappliedChanges(false);
  };

  const handleTimeframeClick = (timeframe: string) => {
    setSelectedTimeframe(timeframe);
  };

  const handleChannelToggle = (channelName: string) => {
    setSelectedChannels(prev => 
      prev.includes(channelName)
        ? prev.filter(b => b !== channelName)
        : [...prev, channelName]
    );
  };

  const handleSentimentToggle = (sentiment: string) => {
    setSelectedSentiments(prev => 
      prev.includes(sentiment)
        ? prev.filter(s => s !== sentiment)
        : [...prev, sentiment]
    );
  };

  const handleTopicToggle = (topic: string) => {
    setSelectedTopics(prev => 
      prev.includes(topic)
        ? prev.filter(t => t !== topic)
        : [...prev, topic]
    );
  };

  const handleInsightToolToggle = (tool: InsightTool) => {
    setSelectedInsightTools((prev) => ({
      ...prev,
      [tool]: !prev[tool],
    }));
  };

  const handleInsightModeSelect = (mode: InsightMode) => {
    if (mode === selectedInsightMode) {
      return;
    }

    const presetTools = MODE_TOOL_PRESETS[mode];
    suppressDirtyRef.current = true;
    setSelectedInsightMode(mode);
    setSelectedInsightTools(presetTools);
    applyFilters({ insightMode: mode, insightTools: presetTools });
    setLastModeAppliedAt(new Date());
    setLastModeAppliedMessage(`Auto-applied ${INSIGHT_MODE_OPTIONS.find((item) => item.value === mode)?.label || mode}`);
  };

  const handleSourceProfileSelect = (profile: SourceProfile) => {
    if (profile === selectedSourceProfile) {
      return;
    }

    suppressDirtyRef.current = true;
    setSelectedSourceProfile(profile);
    applyFilters({ sourceProfile: profile });
    setLastModeAppliedAt(new Date());
    setLastModeAppliedMessage(`Source weighting switched to ${SOURCE_PROFILE_OPTIONS.find((item) => item.value === profile)?.label || profile}`);
  };

  const handleConnectionStrengthChange = (nextValue: number) => {
    const safeValue = Math.max(1, Math.min(5, nextValue));
    if (safeValue === connectionStrength) return;

    suppressDirtyRef.current = true;
    setConnectionStrength(safeValue);
    applyFilters({ connectionStrength: safeValue });
    setLastModeAppliedAt(new Date());
    setLastModeAppliedMessage(`Detail level switched to ${safeValue <= 2 ? 'Explore' : safeValue >= 4 ? 'Focused' : 'Balanced'}`);
  };

  const selectedToolCount = Object.values(selectedInsightTools).filter(Boolean).length;

  const changeCount =
    selectedChannels.length +
    (selectedTimeframe !== 'Last 7 Days' ? 1 : 0) +
    selectedSentiments.length +
    selectedTopics.length +
    (selectedInsightMode !== 'marketMap' ? 1 : 0) +
    (selectedSourceProfile !== 'balanced' ? 1 : 0) +
    (confidenceThreshold !== 35 ? 1 : 0) +
    (connectionStrength !== DEFAULT_CONNECTION_STRENGTH ? 1 : 0) +
    selectedToolCount;

  // Fetch channels and topics from backend
  useEffect(() => {
    let cancelled = false;
    setChannelsLoading(true);
    setTopicsLoading(true);

    const fetchChannels = async () => {
      try {
        console.log('🔍 Fetching channels from Neo4j...');
        const [channels, topChannelsInWindow] = await Promise.all([
          getAllChannels(),
          getTopChannels(3, selectedTimeframe),
        ]);
        if (cancelled) return;

        console.log('✅ Channels fetched:', channels);
        setAllChannels(channels);

        if (selectedChannels.length === 0) {
          const defaults = topChannelsInWindow
            .slice(0, 3)
            .map((channel) => channel.name)
            .filter(Boolean);

          if (defaults.length > 0) {
            isBootstrappingRef.current = true;
            setSelectedChannels(defaults);
            if (onFiltersChange) {
              onFiltersChange({
                channels: defaults,
                timeframe: selectedTimeframe,
                sentiments: selectedSentiments,
                topics: selectedTopics,
                connectionStrength,
                layers: ['topic'],
                insightMode: 'marketMap',
                sourceProfile: 'balanced',
                confidenceThreshold: 35,
              });
            }
            setHasUnappliedChanges(false);

            queueMicrotask(() => {
              isBootstrappingRef.current = false;
            });
          }
        }

        setChannelsLoading(false);
      } catch (error) {
        console.error('❌ Failed to fetch channels:', error);
        if (!cancelled) {
          setChannelsLoading(false);
        }
      }
    };

    const fetchTopics = async () => {
      try {
        console.log('🔍 Fetching trending topics from Neo4j...');
        const topics = await getTrendingTopics(10, selectedTimeframe);
        if (cancelled) return;
        console.log('✅ Topics fetched:', topics);
        setAllTopics(topics);
        setTopicsLoading(false);
      } catch (error) {
        console.error('❌ Failed to fetch topics:', error);
        if (!cancelled) {
          setTopicsLoading(false);
        }
      }
    };

    fetchChannels();
    fetchTopics();

    return () => {
      cancelled = true;
    };
  }, [selectedTimeframe]);

  return (
    <div 
      className={`absolute left-4 top-4 bottom-4 bg-slate-950/40 backdrop-blur-xl border border-white/10 rounded-2xl shadow-2xl flex flex-col z-40 overflow-hidden transition-all duration-300 ${
        isCollapsed ? 'w-12' : 'w-80'
      }`}
    >
      {/* Collapse Button */}
      {!isCollapsed && (
        <button
          onClick={() => setIsCollapsed(true)}
          className="absolute top-4 right-4 w-8 h-8 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 flex items-center justify-center transition-colors z-50"
          title="Collapse filters"
        >
          <ChevronLeft className="w-4 h-4 text-white/70" />
        </button>
      )}

      {isCollapsed ? (
        /* Collapsed State */
        <div className="flex flex-col items-center justify-center h-full gap-4 py-6">
          <button
            onClick={() => setIsCollapsed(false)}
            className="w-10 h-10 rounded-lg bg-cyan-500/20 hover:bg-cyan-500/30 border border-cyan-500/30 flex items-center justify-center transition-colors"
            title="Expand filters"
          >
            <Filter className="w-5 h-5 text-cyan-400" />
          </button>
          {changeCount > 0 && (
            <div className="w-6 h-6 rounded-full bg-cyan-500 flex items-center justify-center">
              <span className="text-white text-xs font-bold">{changeCount}</span>
            </div>
          )}
        </div>
      ) : (
        /* Expanded State */
        <>
          {/* Header */}
          <div className="px-6 py-4 border-b border-white/10">
            <div className="flex items-center gap-2">
              <Filter className="w-5 h-5 text-cyan-400" />
              <h2 className="text-white/90 font-semibold">Filters</h2>
            </div>
            <p className="text-white/50 text-xs mt-1">
              Configure your community channel intelligence view
            </p>
          </div>

          {/* Scrollable Content */}
          <div className="flex-1 overflow-y-auto px-6 py-4 space-y-6">
            
            {/* Search Bar - Global */}
            <div className="space-y-3">
              <div className="relative">
                <input
                  type="text"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  placeholder="Search channels or topics..."
                  className="w-full px-4 py-3 pl-10 rounded-lg bg-white/5 border border-white/10 text-white/90 placeholder:text-white/40 focus:outline-none focus:border-cyan-500/50 focus:ring-2 focus:ring-cyan-500/20 transition-all"
                />
                <Search className="absolute left-3 top-3.5 w-4 h-4 text-white/40" />
              </div>
              
              {/* Search Results Dropdown */}
              {searchResults.length > 0 && (
                <div className="bg-white/5 border border-white/10 rounded-lg overflow-hidden">
                  {searchResults.map((result) => (
                    <button
                      key={result.id}
                      onClick={() => handleSearchSelect(result.id)}
                      className="w-full px-3 py-2.5 flex items-center gap-3 hover:bg-white/10 transition-colors text-left"
                    >
                      {(() => {
                        const palette = getNodeColors(result.type as NodeType);
                        return (
                          <div
                            className="w-2.5 h-2.5 rounded-full"
                            style={{ backgroundColor: palette.core, boxShadow: `0 0 10px ${palette.glow}` }}
                          />
                        );
                      })()}
                      <div className="flex-1">
                        <div className="text-white/90 text-sm">{result.name}</div>
                        <div className="text-white/40 text-xs capitalize">{getNodeLabel(result.type as NodeType)}</div>
                      </div>
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Divider */}
            <div className="h-px bg-white/10" />
            
            {/* Timeframe Section */}
            <div className="space-y-3">
              <div className="flex items-center gap-2 text-white/70 text-sm">
                <Calendar className="w-4 h-4" />
                <span>Time Period</span>
              </div>
              <div className="grid grid-cols-2 gap-2">
                {TIMEFRAMES.map((timeframe) => (
                  <button
                    key={timeframe}
                    onClick={() => handleTimeframeClick(timeframe)}
                    className={`px-3 py-2 rounded-lg text-xs font-medium transition-all ${
                      selectedTimeframe === timeframe
                        ? 'bg-cyan-500/30 border border-cyan-500/50 text-cyan-300 shadow-lg shadow-cyan-500/20'
                        : 'bg-white/5 border border-white/10 text-white/60 hover:bg-white/10'
                    }`}
                  >
                    {timeframe}
                  </button>
                ))}
              </div>
            </div>

            {/* Divider */}
            <div className="h-px bg-white/10" />

            {/* Insight Question Section */}
            <div className="space-y-3">
              <div className="flex items-center gap-2 text-white/70 text-sm font-medium">
                <Target className="w-4 h-4" />
                <span>Insight Question</span>
              </div>
              <div className="px-3 py-2 rounded-lg bg-cyan-500/10 border border-cyan-500/30">
                <p className="text-cyan-300 text-[11px] leading-relaxed">
                  Selecting a question auto-applies and updates the graph immediately.
                </p>
              </div>
              <div className="space-y-2">
                {INSIGHT_MODE_OPTIONS.map((mode) => (
                  <button
                    key={mode.value}
                    onClick={() => handleInsightModeSelect(mode.value)}
                    className={`w-full text-left px-3 py-2.5 rounded-lg border transition-colors ${
                      selectedInsightMode === mode.value
                        ? 'bg-cyan-500/20 border-cyan-500/40'
                        : 'bg-white/5 border-white/10 hover:bg-white/10'
                    }`}
                  >
                    <div className="text-white/90 text-sm font-medium">{mode.label}</div>
                    <div className="text-white/45 text-[11px] mt-0.5">{mode.description}</div>
                  </button>
                ))}
              </div>
              {lastModeAppliedAt && (
                <div className="px-3 py-2 rounded-lg bg-emerald-500/10 border border-emerald-500/30">
                  <p className="text-emerald-300 text-[11px] leading-relaxed">
                    {lastModeAppliedMessage} at {lastModeAppliedAt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                  </p>
                </div>
              )}
            </div>

            {/* Divider */}
            <div className="h-px bg-white/10" />

            {/* Source Profile */}
            <div className="space-y-3">
              <div className="text-white/70 text-sm font-medium">Source Weighting</div>
              <div className="space-y-2">
                {SOURCE_PROFILE_OPTIONS.map((profile) => (
                  <button
                    key={profile.value}
                    onClick={() => handleSourceProfileSelect(profile.value)}
                    className={`w-full text-left px-3 py-2.5 rounded-lg border transition-colors ${
                      selectedSourceProfile === profile.value
                        ? 'bg-cyan-500/20 border-cyan-500/40'
                        : 'bg-white/5 border-white/10 hover:bg-white/10'
                    }`}
                  >
                    <div className="text-white/90 text-sm font-medium">{profile.label}</div>
                    <div className="text-white/45 text-[11px] mt-0.5">{profile.description}</div>
                  </button>
                ))}
              </div>
            </div>

            {/* Channel Source Section */}
            <div className="space-y-3">
              <div className="text-white/70 text-sm font-medium">Select Channels</div>
              <div className="space-y-2">
                {channelsLoading ? (
                  <div className="text-white/50 text-xs">Loading channels...</div>
                ) : (
                  <>
                    {allChannels
                      .slice(0, showAllChannels ? allChannels.length : 5)
                      .map((channel) => (
                        <label
                          key={channel.name}
                          className="flex items-center gap-3 px-3 py-2.5 rounded-lg bg-white/5 border border-white/10 hover:bg-white/10 cursor-pointer transition-colors group"
                        >
                          <Checkbox
                            checked={selectedChannels.includes(channel.name)}
                            onCheckedChange={() => handleChannelToggle(channel.name)}
                            className="border-white/30 data-[state=checked]:bg-cyan-500 data-[state=checked]:border-cyan-500"
                          />
                          <div className="flex-1 flex items-center justify-between">
                            <span className="text-white/80 text-sm group-hover:text-white transition-colors">
                              {channel.name}
                            </span>
                            <span className="text-white/40 text-xs font-mono">
                              {channel.adCount} posts
                            </span>
                          </div>
                        </label>
                      ))}
                    {allChannels.length > 5 && !showAllChannels && (
                      <button
                        onClick={() => setShowAllChannels(true)}
                        className="w-full px-3 py-2 rounded-lg bg-cyan-500/10 border border-cyan-500/30 hover:bg-cyan-500/20 text-cyan-400 text-xs font-medium transition-colors flex items-center justify-center gap-2"
                      >
                        <ChevronDown className="w-3 h-3" />
                        Show all {allChannels.length} channels
                      </button>
                    )}
                  </>
                )}
              </div>
              
              {selectedChannels.length > 0 && (
                <div className="mt-2 px-3 py-2 rounded-lg bg-cyan-500/10 border border-cyan-500/30">
                  <p className="text-cyan-400 text-xs">
                    ✓ {selectedChannels.length} {selectedChannels.length === 1 ? 'channel' : 'channels'} selected
                  </p>
                </div>
              )}
            </div>

            {/* Divider */}
            <div className="h-px bg-white/10" />

            {/* Sentiment Section */}
            <div className="space-y-3">
              <div className="text-white/70 text-sm font-medium">Select Sentiments</div>
              <div className="space-y-2">
                {SENTIMENTS.map((sentiment) => (
                  <label
                    key={sentiment.label}
                    className="flex items-center gap-3 px-3 py-2.5 rounded-lg bg-white/5 border border-white/10 hover:bg-white/10 cursor-pointer transition-colors group"
                  >
                    <Checkbox
                      checked={selectedSentiments.includes(sentiment.label)}
                      onCheckedChange={() => handleSentimentToggle(sentiment.label)}
                      className="border-white/30 data-[state=checked]:bg-cyan-500 data-[state=checked]:border-cyan-500"
                    />
                    <div className="flex-1 flex items-center justify-between">
                      <span className="text-white/80 text-sm group-hover:text-white transition-colors">
                        {sentiment.label}
                      </span>
                      <span className={`${sentiment.color} text-xs font-mono`}>
                        {sentiment.icon}
                      </span>
                    </div>
                  </label>
                ))}
              </div>
              
              {selectedSentiments.length > 0 && (
                <div className="mt-2 px-3 py-2 rounded-lg bg-cyan-500/10 border border-cyan-500/30">
                  <p className="text-cyan-400 text-xs">
                    ✓ {selectedSentiments.length} {selectedSentiments.length === 1 ? 'sentiment' : 'sentiments'} selected
                  </p>
                </div>
              )}
            </div>

            {/* Divider */}
            <div className="h-px bg-white/10" />

            {/* Topic Section */}
            <div className="space-y-3">
              <div className="text-white/70 text-sm font-medium">Select Topics</div>
              <div className="space-y-2">
                <div className="relative">
                  <input
                    type="text"
                    value={topicSearchQuery}
                    onChange={(e) => setTopicSearchQuery(e.target.value)}
                    placeholder="Search topics..."
                    className="w-full px-3 py-2.5 rounded-lg bg-white/5 border border-white/10 hover:bg-white/10 text-white/90 placeholder:text-white/40 focus:outline-none focus:border-cyan-500/50 transition-colors"
                  />
                  <Search className="absolute right-3 top-3 w-4 h-4 text-white/60 pointer-events-none" />
                </div>
                {topicsLoading ? (
                  <div className="text-white/50 text-xs">Loading topics...</div>
                ) : (
                  allTopics
                    .filter(topic => topic.name.toLowerCase().includes(topicSearchQuery.toLowerCase()))
                    .slice(0, showAllTopics ? allTopics.length : 5)
                    .map((topic) => (
                      <label
                        key={topic.name}
                        className="flex items-center gap-3 px-3 py-2.5 rounded-lg bg-white/5 border border-white/10 hover:bg-white/10 cursor-pointer transition-colors group"
                      >
                        <Checkbox
                          checked={selectedTopics.includes(topic.name)}
                          onCheckedChange={() => handleTopicToggle(topic.name)}
                          className="border-white/30 data-[state=checked]:bg-cyan-500 data-[state=checked]:border-cyan-500"
                        />
                        <div className="flex-1 flex items-center justify-between">
                          <span className="text-white/80 text-sm group-hover:text-white transition-colors">
                            {topic.name}
                          </span>
                          <span className="text-white/40 text-xs font-mono">
                            {topic.adCount} posts
                          </span>
                        </div>
                      </label>
                    ))
                )}
                {allTopics.length > 5 && !showAllTopics && (
                  <button
                    onClick={() => setShowAllTopics(true)}
                    className="w-full px-3 py-2 rounded-lg bg-cyan-500/10 border border-cyan-500/30 hover:bg-cyan-500/20 text-cyan-400 text-xs font-medium transition-colors flex items-center justify-center gap-2"
                  >
                    <ChevronDown className="w-3 h-3" />
                    Show all {allTopics.length} topics
                  </button>
                )}
              </div>
              
              {selectedTopics.length > 0 && (
                <div className="mt-2 px-3 py-2 rounded-lg bg-cyan-500/10 border border-cyan-500/30">
                  <p className="text-cyan-400 text-xs">
                    ✓ {selectedTopics.length} {selectedTopics.length === 1 ? 'topic' : 'topics'} selected
                  </p>
                </div>
              )}
            </div>

            {/* Divider */}
            <div className="h-px bg-white/10" />

            {/* Data Layers Section */}
            <div className="space-y-3">
              <div className="flex items-center gap-2 text-white/70 text-sm font-medium">
                <Layers className="w-4 h-4" />
                <span>Insight Tools</span>
              </div>
              <div className="space-y-2">
                <div className="px-3 py-2 rounded-lg bg-cyan-500/10 border border-cyan-500/30">
                  <p className="text-cyan-300 text-xs leading-relaxed">
                    Core map is always active: Topics with top 3 channel connections.
                  </p>
                </div>

                {INSIGHT_TOOL_OPTIONS.map((tool) => {
                  const palette = getNodeColors(tool.colorType);
                  const checked = selectedInsightTools[tool.value];

                  return (
                    <label
                      key={tool.value}
                      className={`flex items-center gap-3 px-3 py-2.5 rounded-lg border transition-colors cursor-pointer ${
                        checked
                          ? 'bg-white/8 border-white/20'
                          : 'bg-white/5 border-white/10 hover:bg-white/10'
                      }`}
                    >
                      <Checkbox
                        checked={checked}
                        onCheckedChange={() => handleInsightToolToggle(tool.value)}
                        className="border-white/30 data-[state=checked]:bg-cyan-500 data-[state=checked]:border-cyan-500"
                      />
                      <div
                        className="w-2.5 h-2.5 rounded-full"
                        style={{ backgroundColor: palette.core, boxShadow: `0 0 10px ${palette.glow}` }}
                      />
                      <div className="flex-1 min-w-0">
                        <div className="text-white/90 text-sm truncate">{tool.title}</div>
                        <div className="text-white/40 text-[11px] truncate">{tool.description}</div>
                      </div>
                    </label>
                  );
                })}
              </div>

              <div className="mt-2 px-3 py-2 rounded-lg bg-cyan-500/10 border border-cyan-500/30">
                <p className="text-cyan-400 text-xs">
                  ✓ {selectedToolCount} {selectedToolCount === 1 ? 'tool' : 'tools'} enabled
                </p>
              </div>
            </div>

            {/* Divider */}
            <div className="h-px bg-white/10" />

            {/* Confidence Threshold Section */}
            <div className="space-y-3">
              <div className="text-white/70 text-sm font-medium">Confidence Threshold</div>
              <div className="space-y-2">
                <input
                  type="range"
                  min="10"
                  max="90"
                  step="5"
                  value={confidenceThreshold}
                  onChange={(e) => setConfidenceThreshold(parseInt(e.target.value, 10))}
                  className="w-full h-2 bg-white/5 border border-white/10 hover:bg-white/10 cursor-pointer transition-colors"
                />
                <div className="flex items-center justify-between text-white/50 text-xs">
                  <span>Inclusive</span>
                  <span>{confidenceThreshold}%</span>
                  <span>Strict</span>
                </div>
              </div>
            </div>

            {/* Divider */}
            <div className="h-px bg-white/10" />

            {/* Connection Strength Section */}
            <div className="space-y-3">
              <div className="text-white/70 text-sm font-medium">Connection Strength</div>
              <div className="space-y-2">
                <input
                  type="range"
                  min="1"
                  max="5"
                  value={connectionStrength}
                  onChange={(e) => handleConnectionStrengthChange(parseInt(e.target.value, 10))}
                  className="w-full h-2 bg-white/5 border border-white/10 hover:bg-white/10 cursor-pointer transition-colors group"
                />
                <div className="flex items-center justify-between text-white/50 text-xs">
                  <span>Explore</span>
                  <span>
                    {connectionStrength <= 2 ? 'Explore' : connectionStrength >= 4 ? 'Focused' : 'Balanced'}
                  </span>
                  <span>Focused</span>
                </div>
              </div>
              
              {connectionStrength !== DEFAULT_CONNECTION_STRENGTH && (
                <div className="mt-2 px-3 py-2 rounded-lg bg-cyan-500/10 border border-cyan-500/30">
                  <p className="text-cyan-400 text-xs">
                    ✓ Detail level set to {connectionStrength <= 2 ? 'Explore' : connectionStrength >= 4 ? 'Focused' : 'Balanced'} ({connectionStrength}/5)
                  </p>
                </div>
              )}
            </div>

            {/* Info Box */}
            <div className="bg-cyan-500/10 border border-cyan-500/30 rounded-xl p-4">
              <div className="flex items-start gap-2">
                <div className="w-5 h-5 rounded-full bg-cyan-500/20 flex items-center justify-center flex-shrink-0 mt-0.5">
                  <span className="text-cyan-400 text-xs">💡</span>
                </div>
                <div>
                  <p className="text-cyan-400 text-xs font-medium mb-1">Pro Tip</p>
                  <p className="text-white/70 text-xs leading-relaxed">
                    Default view starts clean: 3 channels plus key connector topics. Choose an insight question first, then enable tools only when needed.
                  </p>
                </div>
              </div>
            </div>
          </div>

          {/* Footer Actions */}
          <div className="px-6 py-4 border-t border-white/10 space-y-2">
            <button
              onClick={handleApply}
              disabled={!hasUnappliedChanges && selectedChannels.length === 0}
              className={`w-full px-4 py-3 rounded-xl font-medium text-sm transition-all ${
                hasUnappliedChanges || selectedChannels.length > 0
                  ? 'bg-cyan-500 hover:bg-cyan-600 text-white shadow-lg shadow-cyan-500/30'
                  : 'bg-white/5 text-white/40 cursor-not-allowed'
              }`}
            >
              {hasUnappliedChanges ? (
                <span className="flex items-center justify-center gap-2">
                  <span className="w-2 h-2 rounded-full bg-white animate-pulse" />
                  Apply {changeCount > 0 ? `${changeCount} ${changeCount === 1 ? 'Filter' : 'Filters'}` : 'Changes'}
                </span>
              ) : (
                'Apply Filters'
              )}
            </button>
            
            <button
              onClick={handleReset}
              className="w-full px-4 py-2.5 rounded-xl bg-white/5 hover:bg-white/10 border border-white/10 text-white/70 hover:text-white font-medium text-sm transition-all flex items-center justify-center gap-2"
            >
              <RotateCcw className="w-3.5 h-3.5" />
              Reset All
            </button>
          </div>
        </>
      )}
    </div>
  );
}
