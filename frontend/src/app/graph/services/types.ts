export type GraphNodeType = 'category' | 'topic' | 'channel';

export type SignalFocus = 'all' | 'asks' | 'needs' | 'fear';
export type SourceDetail = 'minimal' | 'standard' | 'expanded';
export type RankingMode = 'volume' | 'momentum' | 'spread';

export interface GraphChannelRef {
  id: string;
  name: string;
  mentions: number;
}

export interface GraphNode {
  id: string;
  name: string;
  type: GraphNodeType;
  val: number;
  category?: string;
  mentionCount?: number;
  postCount?: number;
  commentCount?: number;
  evidenceCount?: number;
  distinctUsers?: number;
  distinctChannels?: number;
  topicCount?: number;
  trendPct?: number;
  dominantSentiment?: 'Positive' | 'Neutral' | 'Negative' | 'Urgent' | string;
  sentimentPositive?: number;
  sentimentNeutral?: number;
  sentimentNegative?: number;
  urgentSignals?: number;
  askSignalCount?: number;
  needSignalCount?: number;
  fearSignalCount?: number;
  topChannels?: GraphChannelRef[];
  sampleEvidenceId?: string | null;
  lastSeen?: string | null;
  x?: number;
  y?: number;
  vx?: number;
  vy?: number;
  fx?: number | null;
  fy?: number | null;
  size?: number;
  labelVisible?: boolean;
}

export interface GraphLink {
  source: string | GraphNode;
  target: string | GraphNode;
  value: number;
  type: 'category-topic' | 'channel-topic' | string;
}

export interface GraphFreshnessMeta {
  status?: 'healthy' | 'warning' | 'stale' | 'unknown' | string;
  score?: number;
  generatedAt?: string;
  lastScrapeAt?: string | null;
  lastProcessAt?: string | null;
  lastGraphSyncAt?: string | null;
  syncEstimated?: boolean;
  unsyncedPosts?: number;
  latestPostDeltaMinutes?: number | null;
}

export interface GraphMeta {
  from?: string;
  to?: string;
  days?: number;
  selectedChannels?: string[];
  selectedSentiments?: string[];
  selectedCategory?: string | null;
  signalFocus?: SignalFocus | string;
  sourceDetail?: SourceDetail | string;
  rankingMode?: RankingMode | string;
  minMentions?: number;
  availableCategories?: string[];
  visibleTopicCount?: number;
  visibleCategoryCount?: number;
  visibleChannelCount?: number;
  totalMentions?: number;
  generatedAt?: string;
  freshness?: GraphFreshnessMeta;
}

export interface GraphData {
  nodes: GraphNode[];
  links: GraphLink[];
  meta?: GraphMeta;
}

export interface GraphFilters {
  from_date?: string;
  to_date?: string;
  channels?: string[];
  sentiments?: string[];
  topics?: string[];
  category?: string;
  signalFocus?: SignalFocus;
  sourceDetail?: SourceDetail;
  rankingMode?: RankingMode;
  minMentions?: number;
  max_nodes?: number;
}

export interface NodeDetails {
  id: string;
  name: string;
  type?: string;
  [key: string]: any;
}

export interface DataFreshnessSnapshot {
  generated_at: string;
  scheduler?: {
    is_active?: boolean;
    interval_minutes?: number;
    running_now?: boolean;
    last_success_at?: string | null;
    next_run_at?: string | null;
    last_error?: string | null;
  };
  pipeline?: {
    scrape?: { status?: string; last_scrape_at?: string | null; age_minutes?: number | null };
    process?: { status?: string; last_process_at?: string | null; age_minutes?: number | null };
    sync?: {
      status?: string;
      last_graph_sync_at?: string | null;
      age_minutes?: number | null;
      source?: string;
      estimated?: boolean;
    };
  };
  backlog?: {
    unprocessed_posts?: number;
    unprocessed_comments?: number;
    unsynced_posts?: number;
    unsynced_analysis?: number;
  };
  drift?: {
    supabase_total_posts?: number;
    neo4j_total_posts?: number;
    post_count_gap?: number;
    latest_post_delta_minutes?: number | null;
  };
  health?: {
    status?: string;
    score?: number;
    notes?: string[];
  };
}
