import { ChevronLeft, Filter, Radio, RotateCcw, Search, SlidersHorizontal } from 'lucide-react';
import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { Checkbox } from '@/app/components/ui/checkbox';
import { FreshnessBadge } from '@/app/graph/components/FreshnessBadge';
import { getAllChannels, type TopChannel } from '@/app/graph/services/api';
import type { GraphFilters, GraphFreshnessMeta, RankingMode, SignalFocus, SourceDetail } from '@/app/graph/services/types';

const SENTIMENT_OPTIONS = [
  { value: 'Positive', label: 'Positive', accent: 'border-emerald-400/30 bg-emerald-400/15 text-emerald-100' },
  { value: 'Neutral', label: 'Neutral', accent: 'border-slate-400/25 bg-slate-300/10 text-slate-100' },
  { value: 'Negative', label: 'Negative', accent: 'border-rose-400/30 bg-rose-400/15 text-rose-100' },
  { value: 'Urgent', label: 'Urgent', accent: 'border-orange-400/30 bg-orange-400/15 text-orange-100' },
] as const;

const SOURCE_OPTIONS: Array<{ value: SourceDetail; label: string }> = [
  { value: 'minimal', label: 'Minimal' },
  { value: 'standard', label: 'Balanced' },
  { value: 'expanded', label: 'Expanded' },
];

const SORT_OPTIONS: Array<{ value: RankingMode; label: string }> = [
  { value: 'volume', label: 'Volume' },
  { value: 'momentum', label: 'Momentum' },
  { value: 'spread', label: 'Spread' },
];

const SIGNAL_OPTIONS: Array<{ value: SignalFocus; label: string }> = [
  { value: 'all', label: 'All' },
  { value: 'asks', label: 'Asks' },
  { value: 'needs', label: 'Needs' },
  { value: 'fear', label: 'Urgent' },
];

interface GlobalFiltersProps {
  filters: GraphFilters;
  availableCategories?: string[];
  freshness?: GraphFreshnessMeta;
  showFreshnessBadge?: boolean;
  isCollapsed?: boolean;
  onCollapsedChange?: (collapsed: boolean) => void;
  onFiltersChange: (filters: GraphFilters) => void;
  onSearchSelect?: (nodeId: string) => void;
  allNodes?: Array<{ id: string; name: string; type: string }>;
}

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

function SectionTitle({ icon, title }: { icon: ReactNode; title: string }) {
  return (
    <div className="flex items-center gap-2 text-[12px] font-medium text-white/72">
      {icon}
      <span>{title}</span>
    </div>
  );
}

export function GlobalFilters({
  filters,
  availableCategories = [],
  freshness,
  showFreshnessBadge = true,
  isCollapsed = false,
  onCollapsedChange,
  onFiltersChange,
  onSearchSelect,
  allNodes = [],
}: GlobalFiltersProps) {
  const [selectedChannels, setSelectedChannels] = useState<string[]>(filters.channels || []);
  const [selectedSentiments, setSelectedSentiments] = useState<string[]>(filters.sentiments || []);
  const [selectedCategory, setSelectedCategory] = useState(filters.category || '');
  const [signalFocus, setSignalFocus] = useState<SignalFocus>(filters.signalFocus || 'all');
  const [sourceDetail, setSourceDetail] = useState<SourceDetail>(filters.sourceDetail || 'standard');
  const [rankingMode, setRankingMode] = useState<RankingMode>(filters.rankingMode || 'volume');
  const [minMentions, setMinMentions] = useState<number>(filters.minMentions || 2);
  const [searchQuery, setSearchQuery] = useState('');
  const [channelQuery, setChannelQuery] = useState('');
  const [allChannels, setAllChannels] = useState<TopChannel[]>([]);
  const [loadingChannels, setLoadingChannels] = useState(true);

  useEffect(() => {
    setSelectedChannels(filters.channels || []);
    setSelectedSentiments(filters.sentiments || []);
    setSelectedCategory(filters.category || '');
    setSignalFocus(filters.signalFocus || 'all');
    setSourceDetail(filters.sourceDetail || 'standard');
    setRankingMode(filters.rankingMode || 'volume');
    setMinMentions(filters.minMentions || 2);
  }, [
    filters.category,
    filters.channels,
    filters.minMentions,
    filters.rankingMode,
    filters.sentiments,
    filters.signalFocus,
    filters.sourceDetail,
  ]);

  useEffect(() => {
    let cancelled = false;
    const loadChannels = async () => {
      try {
        setLoadingChannels(true);
        const rows = await getAllChannels();
        if (!cancelled) {
          setAllChannels(rows);
        }
      } catch (error) {
        console.error('Failed to load graph channels:', error);
      } finally {
        if (!cancelled) {
          setLoadingChannels(false);
        }
      }
    };
    void loadChannels();
    return () => {
      cancelled = true;
    };
  }, []);

  const searchResults = useMemo(() => {
    if (searchQuery.trim().length < 2) return [];
    const query = searchQuery.trim().toLowerCase();
    return allNodes
      .filter((node) => node.name.toLowerCase().includes(query))
      .slice(0, 6);
  }, [allNodes, searchQuery]);

  const filteredChannels = useMemo(() => {
    const query = channelQuery.trim().toLowerCase();
    return !query
      ? allChannels
      : allChannels.filter((channel) => channel.name.toLowerCase().includes(query));
  }, [allChannels, channelQuery]);

  const activeFilterCount = useMemo(() => {
    let count = 0;
    if (selectedChannels.length > 0) count += 1;
    if (selectedSentiments.length > 0) count += 1;
    if (selectedCategory) count += 1;
    if (signalFocus !== 'all') count += 1;
    if (sourceDetail !== 'standard') count += 1;
    if (rankingMode !== 'volume') count += 1;
    if (minMentions > 2) count += 1;
    return count;
  }, [minMentions, rankingMode, selectedCategory, selectedChannels.length, selectedSentiments.length, signalFocus, sourceDetail]);

  const toggleSentiment = (value: string) => {
    setSelectedSentiments((prev) => (
      prev.includes(value) ? prev.filter((item) => item !== value) : [...prev, value]
    ));
  };

  const toggleChannel = (value: string) => {
    setSelectedChannels((prev) => (
      prev.includes(value) ? prev.filter((item) => item !== value) : [...prev, value]
    ));
  };

  const handleApply = () => {
    onFiltersChange({
      channels: selectedChannels,
      sentiments: selectedSentiments,
      category: selectedCategory || undefined,
      signalFocus,
      sourceDetail,
      rankingMode,
      minMentions,
      max_nodes: filters.max_nodes || DEFAULT_FILTERS.max_nodes,
    });
  };

  const handleReset = () => {
    setSelectedChannels(DEFAULT_FILTERS.channels || []);
    setSelectedSentiments(DEFAULT_FILTERS.sentiments || []);
    setSelectedCategory(DEFAULT_FILTERS.category || '');
    setSignalFocus(DEFAULT_FILTERS.signalFocus || 'all');
    setSourceDetail(DEFAULT_FILTERS.sourceDetail || 'standard');
    setRankingMode(DEFAULT_FILTERS.rankingMode || 'volume');
    setMinMentions(DEFAULT_FILTERS.minMentions || 2);
    setSearchQuery('');
    setChannelQuery('');
    onFiltersChange(DEFAULT_FILTERS);
  };

  if (isCollapsed) {
    return (
      <aside className="absolute left-4 top-4 bottom-4 z-40 w-16 rounded-[24px] border border-white/10 bg-[#0d1524]/90 shadow-2xl backdrop-blur-xl">
        <div className="flex h-full flex-col items-center justify-between py-4">
          <button
            onClick={() => onCollapsedChange?.(false)}
            className="flex h-10 w-10 items-center justify-center rounded-2xl border border-cyan-400/20 bg-cyan-500/10 text-cyan-300 transition-colors hover:bg-cyan-500/18"
            title="Open filters"
          >
            <Filter className="h-4.5 w-4.5" />
          </button>
          <div className="rounded-2xl border border-white/10 bg-white/5 px-2 py-2.5 text-center text-[10px] text-white/55">
            {activeFilterCount}
            <div className="mt-1 text-[9px] uppercase tracking-[0.18em] text-white/35">active</div>
          </div>
        </div>
      </aside>
    );
  }

  return (
    <aside className="absolute left-4 top-4 bottom-4 z-40 w-[320px] overflow-hidden rounded-[28px] border border-white/10 bg-[#0c1422]/92 shadow-[0_26px_80px_rgba(0,0,0,0.48)] backdrop-blur-xl">
      <div className="flex h-full flex-col">
        <div className="border-b border-white/10 px-5 py-5">
          <div className="flex items-start justify-between gap-4">
            <div className="min-w-0">
              <div className="flex items-center gap-2.5">
                <div className="flex h-10 w-10 items-center justify-center rounded-2xl border border-cyan-400/25 bg-cyan-500/12">
                  <Filter className="h-5 w-5 text-cyan-300" />
                </div>
                <h2 className="text-[28px] font-semibold leading-none tracking-[-0.04em] text-white">Filters</h2>
              </div>
              <p className="mt-3 max-w-[190px] text-[13px] leading-6 text-white/52">
                Narrow the map without taking space away from it.
              </p>
              {showFreshnessBadge ? (
                <div className="mt-4">
                  <FreshnessBadge freshness={freshness} compact />
                </div>
              ) : null}
            </div>

            <button
              onClick={() => onCollapsedChange?.(true)}
              className="flex h-11 w-11 shrink-0 items-center justify-center rounded-[16px] border border-white/10 bg-white/5 text-white/70 transition-colors hover:bg-white/10"
              title="Collapse filters"
            >
              <ChevronLeft className="h-5 w-5" />
            </button>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4">
          <div className="space-y-5">
            <section className="border-b border-white/10 pb-5">
              <div className="rounded-[20px] border border-white/10 bg-white/5 px-4 py-3.5">
                <div className="flex items-center gap-3">
                  <Search className="h-5 w-5 text-white/35" />
                  <input
                    value={searchQuery}
                    onChange={(event) => setSearchQuery(event.target.value)}
                    placeholder="Search channels or topics..."
                    className="w-full bg-transparent text-[15px] text-white placeholder:text-white/35 outline-none"
                  />
                </div>
              </div>
              {searchResults.length > 0 && (
                <div className="mt-2 overflow-hidden rounded-[18px] border border-white/10 bg-slate-950/75">
                  {searchResults.map((result) => (
                    <button
                      key={result.id}
                      onClick={() => {
                        setSearchQuery('');
                        onSearchSelect?.(result.id);
                      }}
                      className="w-full border-b border-white/5 px-3.5 py-2.5 text-left transition-colors last:border-b-0 hover:bg-white/5"
                    >
                      <div className="text-[13px] text-white/90">{result.name}</div>
                      <div className="mt-1 text-[11px] capitalize text-white/45">{result.type}</div>
                    </button>
                  ))}
                </div>
              )}
            </section>

            <section className="border-b border-white/10 pb-5">
              <SectionTitle icon={<Radio className="h-4 w-4 text-white/55" />} title="Sentiment" />
              <div className="mt-3 grid grid-cols-2 gap-2">
                {SENTIMENT_OPTIONS.map((option) => {
                  const active = selectedSentiments.includes(option.value);
                  return (
                    <button
                      key={option.value}
                      onClick={() => toggleSentiment(option.value)}
                      className={`rounded-[16px] border px-3 py-2.5 text-[13px] transition-colors ${
                        active ? option.accent : 'border-white/10 bg-white/5 text-white/75 hover:bg-white/8'
                      }`}
                    >
                      {option.label}
                    </button>
                  );
                })}
              </div>
            </section>

            <section className="border-b border-white/10 pb-5">
              <SectionTitle icon={<SlidersHorizontal className="h-4 w-4 text-white/55" />} title="Signals" />
              <div className="mt-3 grid grid-cols-4 gap-1.5">
                {SIGNAL_OPTIONS.map((option) => (
                  <button
                    key={option.value}
                    onClick={() => setSignalFocus(option.value)}
                    className={`rounded-[14px] border px-2 py-2.5 text-[12px] transition-colors ${
                      signalFocus === option.value
                        ? 'border-cyan-400/35 bg-cyan-500/14 text-cyan-100'
                        : 'border-white/10 bg-white/5 text-white/70 hover:bg-white/8'
                    }`}
                  >
                    {option.label}
                  </button>
                ))}
              </div>
            </section>

            <section className="border-b border-white/10 pb-5">
              <SectionTitle icon={<Filter className="h-4 w-4 text-white/55" />} title="Categories" />
              <div className="mt-3">
                <select
                  value={selectedCategory}
                  onChange={(event) => setSelectedCategory(event.target.value)}
                  className="w-full rounded-[18px] border border-white/10 bg-white/5 px-3.5 py-3 text-[13px] text-white outline-none"
                >
                  <option value="">All categories</option>
                  {availableCategories.map((category) => (
                    <option key={category} value={category}>{category}</option>
                  ))}
                </select>
              </div>
            </section>

            <section className="border-b border-white/10 pb-5">
              <SectionTitle icon={<SlidersHorizontal className="h-4 w-4 text-white/55" />} title="Topic Filters" />
              <div className="mt-3 space-y-3.5">
                <div>
                  <div className="mb-3 flex items-center justify-between text-sm text-white/75">
                    <span>Topic size threshold</span>
                    <span className="font-semibold text-cyan-200">{minMentions}</span>
                  </div>
                  <div className="rounded-[18px] border border-white/10 bg-white/5 px-3.5 py-3.5">
                    <input
                      type="range"
                      min={1}
                      max={50}
                      step={1}
                      value={minMentions}
                      onChange={(event) => setMinMentions(Number(event.target.value))}
                      className="w-full accent-cyan-400"
                    />
                  </div>
                </div>

                <div className="grid grid-cols-3 gap-1.5">
                  {SOURCE_OPTIONS.map((option) => (
                    <button
                      key={option.value}
                      onClick={() => setSourceDetail(option.value)}
                      className={`rounded-[14px] border px-2.5 py-2.5 text-[12px] transition-colors ${
                        sourceDetail === option.value
                          ? 'border-cyan-400/35 bg-cyan-500/14 text-cyan-100'
                          : 'border-white/10 bg-white/5 text-white/70 hover:bg-white/8'
                      }`}
                    >
                      {option.label}
                    </button>
                  ))}
                </div>

                <div className="grid grid-cols-3 gap-1.5">
                  {SORT_OPTIONS.map((option) => (
                    <button
                      key={option.value}
                      onClick={() => setRankingMode(option.value)}
                      className={`rounded-[14px] border px-2.5 py-2.5 text-[12px] transition-colors ${
                        rankingMode === option.value
                          ? 'border-orange-400/35 bg-orange-400/14 text-orange-100'
                          : 'border-white/10 bg-white/5 text-white/70 hover:bg-white/8'
                      }`}
                    >
                      {option.label}
                    </button>
                  ))}
                </div>
              </div>
            </section>

            <section>
              <div className="flex items-center justify-between">
                <SectionTitle icon={<Filter className="h-4 w-4 text-white/55" />} title="Channels" />
                {selectedChannels.length > 0 && (
                  <div className="text-xs text-white/40">{selectedChannels.length} selected</div>
                )}
              </div>

              <div className="mt-3 rounded-[18px] border border-white/10 bg-white/5 px-3.5 py-2.5">
                <input
                  value={channelQuery}
                  onChange={(event) => setChannelQuery(event.target.value)}
                  placeholder="Search channels..."
                  className="w-full bg-transparent text-[13px] text-white placeholder:text-white/32 outline-none"
                />
              </div>

              <div className="mt-3 overflow-hidden rounded-[18px] border border-white/10 bg-white/5">
                <div className="max-h-[300px] overflow-y-auto divide-y divide-white/5">
                  {loadingChannels && (
                    <div className="px-3.5 py-3 text-sm text-white/45">Loading channels...</div>
                  )}
                  {!loadingChannels && filteredChannels.length === 0 && (
                    <div className="px-3.5 py-3 text-sm text-white/45">No channels match this search.</div>
                  )}
                  {filteredChannels.map((channel) => {
                    const active = selectedChannels.includes(channel.name);
                    return (
                      <label key={channel.id} className="flex cursor-pointer items-center gap-3 px-3.5 py-3 transition-colors hover:bg-white/5">
                        <Checkbox checked={active} onCheckedChange={() => toggleChannel(channel.name)} />
                        <div className="min-w-0 flex-1">
                          <div className="truncate text-[14px] text-white/90">{channel.name}</div>
                          <div className="mt-1 text-[12px] text-white/42">{channel.adCount} all-time posts</div>
                        </div>
                      </label>
                    );
                  })}
                </div>
              </div>
            </section>
          </div>
        </div>

        <div className="border-t border-white/10 px-5 py-4">
          <button
            onClick={handleApply}
            className="w-full rounded-[24px] bg-cyan-500 px-4 py-3.5 text-[15px] font-medium text-white shadow-[0_16px_30px_rgba(34,211,238,0.25)] transition-colors hover:bg-cyan-400"
          >
            {activeFilterCount > 0
              ? `Apply ${activeFilterCount} Filter${activeFilterCount === 1 ? '' : 's'}`
              : 'Apply Filters'}
          </button>
          <button
            onClick={handleReset}
            className="mt-3 flex w-full items-center justify-center gap-2.5 rounded-[20px] border border-white/10 bg-white/5 px-4 py-3 text-[14px] text-white/78 transition-colors hover:bg-white/8"
          >
            <RotateCcw className="h-4 w-4" />
            Reset All
          </button>
        </div>
      </div>
    </aside>
  );
}
