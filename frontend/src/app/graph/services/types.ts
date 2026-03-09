export type GraphNodeType =
  | 'channel'
  | 'brand'
  | 'topic'
  | 'product'
  | 'audience'
  | 'painpoint'
  | 'valueprop'
  | 'intent'
  | 'competitor'
  | 'cta'
  | 'platform'
  | 'format'
  | 'engagement'
  | 'sentiment'
  | 'timeperiod';

export interface GraphNode {
  id: string;
  name: string;
  type: GraphNodeType | string;
  val: number;
  color?: string;
  size?: number;
  category?: string;
  segmentType?: string;
  severity?: string;
  confidence?: number;
  insightScore?: number;
  semanticRole?: 'opportunity';
  opportunityTier?: 'gold' | 'silver';
  opportunityScore?: number;
  opportunityEvidenceCount?: number;
  opportunityActiveDays?: number;
  opportunityNeedRate?: number;
  opportunityCompetitorRate?: number;
  opportunityOwnershipRate?: number;
  opportunityMomentum?: number;
  opportunitySpecificity?: number;
  opportunityEligible?: boolean;
  topicChannelCoverage?: number;
  topicBrandCoverage?: number;
}

export interface GraphLink {
  source: string;
  target: string;
  value: number;
  type?: string;
  adVolume?: number;
  avgSentiment?: number;
  sentimentLabel?: string;
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
  timeframe?: string;
  since?: string;
  insightMode?: string;
  sourceProfile?: string;
  confidenceThreshold?: number;
  connectionStrength?: number;
  layers?: string[];
  selectedChannels?: string[];
  topicCountConsidered?: number;
  topicCountReturned?: number;
  thresholdRelaxed?: boolean;
  generatedAt?: string;
  freshness?: GraphFreshnessMeta;
}

export interface GraphData {
  nodes: GraphNode[];
  links: GraphLink[];
  meta?: GraphMeta;
}

export interface NodeDetails {
  id: string;
  name: string;
  type?: string;
  insight?: string;
  recommendations?: string;
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
