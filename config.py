"""
config.py — Centralized configuration loader.
Reads all values from .env and exposes them as typed constants.
"""
import os
from dotenv import load_dotenv

load_dotenv()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_csv(name: str, default: str = "") -> list:
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_API_ID       = int(os.getenv("TELEGRAM_API_ID", 0))
TELEGRAM_API_HASH     = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_PHONE        = os.getenv("TELEGRAM_PHONE", "")
TELEGRAM_SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "telegram_scraper")
TELEGRAM_SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING", "")  # For Railway/cloud deployment

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL              = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

# ── Neo4j ─────────────────────────────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI", "")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

# ── OpenAI ────────────────────────────────────────────────────────────────────
# Prefer standard OPENAI_API_KEY, but keep backward compatibility with OpenAI_API.
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", os.getenv("OpenAI_API", ""))
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-5-nano")

# ── Runtime / Deployment ───────────────────────────────────────────────────────
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("PORT", os.getenv("APP_PORT", "8001")))
CORS_ALLOW_ORIGINS = _env_csv("CORS_ALLOW_ORIGINS", "*")

# ── Feature Flags ──────────────────────────────────────────────────────────────
FEATURE_TAXONOMY_V2 = _env_bool("FEATURE_TAXONOMY_V2", True)
FEATURE_ENTITY_GRAPH = _env_bool("FEATURE_ENTITY_GRAPH", True)
FEATURE_AI_ANALYST = _env_bool("FEATURE_AI_ANALYST", False)
FEATURE_AI_PRESENTER = _env_bool("FEATURE_AI_PRESENTER", False)
FEATURE_EXTRACTION_V2 = _env_bool("FEATURE_EXTRACTION_V2", True)
FEATURE_QUESTION_BRIEFS_AI = _env_bool("FEATURE_QUESTION_BRIEFS_AI", True)
FEATURE_BEHAVIORAL_BRIEFS_AI = _env_bool("FEATURE_BEHAVIORAL_BRIEFS_AI", True)

# ── AI Safety / Performance ────────────────────────────────────────────────────
AI_REQUEST_TIMEOUT_SECONDS = float(os.getenv("AI_REQUEST_TIMEOUT_SECONDS", "45"))
AI_REQUEST_MAX_RETRIES = int(os.getenv("AI_REQUEST_MAX_RETRIES", "2"))
AI_REQUEST_RETRY_BACKOFF_SECONDS = float(os.getenv("AI_REQUEST_RETRY_BACKOFF_SECONDS", "1.5"))
AI_COMMENT_MAX_TOKENS = int(os.getenv("AI_COMMENT_MAX_TOKENS", "1200"))
AI_POST_MAX_TOKENS = int(os.getenv("AI_POST_MAX_TOKENS", "900"))
AI_COMMENT_WORKERS = int(os.getenv("AI_COMMENT_WORKERS", "3"))
AI_POST_WORKERS = int(os.getenv("AI_POST_WORKERS", "2"))
AI_MAX_INFLIGHT_REQUESTS = int(os.getenv("AI_MAX_INFLIGHT_REQUESTS", "4"))
AI_FAILURE_MAX_RETRIES = int(os.getenv("AI_FAILURE_MAX_RETRIES", "5"))
AI_FAILURE_BACKOFF_SECONDS = int(os.getenv("AI_FAILURE_BACKOFF_SECONDS", "60"))
AI_FAILURE_BACKOFF_MAX_SECONDS = int(os.getenv("AI_FAILURE_BACKOFF_MAX_SECONDS", "3600"))
AI_POST_BATCH_SIZE = int(os.getenv("AI_POST_BATCH_SIZE", "5"))
AI_POST_BATCH_MAX_TOKENS = int(os.getenv("AI_POST_BATCH_MAX_TOKENS", "2600"))
AI_MESSAGE_CHAR_LIMIT = int(os.getenv("AI_MESSAGE_CHAR_LIMIT", "700"))
AI_PROCESS_STAGE_MAX_SECONDS = int(os.getenv("AI_PROCESS_STAGE_MAX_SECONDS", "1200"))
AI_SYNC_STAGE_MAX_SECONDS = int(os.getenv("AI_SYNC_STAGE_MAX_SECONDS", "900"))
AI_POST_PROMPT_STYLE = os.getenv("AI_POST_PROMPT_STYLE", "compact").strip().lower()
GRAPH_ANALYTICS_RETENTION_DAYS = int(os.getenv("GRAPH_ANALYTICS_RETENTION_DAYS", "15"))
QUESTION_BRIEFS_MODEL = os.getenv("QUESTION_BRIEFS_MODEL", OPENAI_MODEL)
QUESTION_BRIEFS_MAX_TOKENS = int(os.getenv("QUESTION_BRIEFS_MAX_TOKENS", "2600"))
QUESTION_BRIEFS_TRIAGE_MODEL = os.getenv("QUESTION_BRIEFS_TRIAGE_MODEL", "gpt-5-nano")
QUESTION_BRIEFS_TRIAGE_MAX_TOKENS = int(os.getenv("QUESTION_BRIEFS_TRIAGE_MAX_TOKENS", "1400"))
QUESTION_BRIEFS_USE_AI_TRIAGE = _env_bool("QUESTION_BRIEFS_USE_AI_TRIAGE", False)
QUESTION_BRIEFS_SYNTHESIS_MODEL = os.getenv("QUESTION_BRIEFS_SYNTHESIS_MODEL", "gpt-5-nano")
QUESTION_BRIEFS_SYNTHESIS_MAX_TOKENS = int(os.getenv("QUESTION_BRIEFS_SYNTHESIS_MAX_TOKENS", "2200"))
QUESTION_BRIEFS_CACHE_TTL_SECONDS = int(os.getenv("QUESTION_BRIEFS_CACHE_TTL_SECONDS", "7200"))
QUESTION_BRIEFS_WINDOW_DAYS = int(os.getenv("QUESTION_BRIEFS_WINDOW_DAYS", str(GRAPH_ANALYTICS_RETENTION_DAYS)))
QUESTION_BRIEFS_MAX_TOPICS = int(os.getenv("QUESTION_BRIEFS_MAX_TOPICS", "14"))
QUESTION_BRIEFS_MAX_BRIEFS = int(os.getenv("QUESTION_BRIEFS_MAX_BRIEFS", "8"))
QUESTION_BRIEFS_EVIDENCE_PER_TOPIC = int(os.getenv("QUESTION_BRIEFS_EVIDENCE_PER_TOPIC", "14"))
QUESTION_BRIEFS_MIN_CONFIDENCE = float(os.getenv("QUESTION_BRIEFS_MIN_CONFIDENCE", "0.60"))
QUESTION_BRIEFS_MESSAGE_CHAR_LIMIT = int(os.getenv("QUESTION_BRIEFS_MESSAGE_CHAR_LIMIT", "900"))
QUESTION_BRIEFS_CONTEXT_CHAR_LIMIT = int(os.getenv("QUESTION_BRIEFS_CONTEXT_CHAR_LIMIT", "520"))
QUESTION_BRIEFS_SYNTH_EVIDENCE_LIMIT = int(os.getenv("QUESTION_BRIEFS_SYNTH_EVIDENCE_LIMIT", "8"))
QUESTION_BRIEFS_CLUSTER_SIMILARITY = float(os.getenv("QUESTION_BRIEFS_CLUSTER_SIMILARITY", "0.42"))
QUESTION_BRIEFS_MIN_CLUSTER_MESSAGES = int(os.getenv("QUESTION_BRIEFS_MIN_CLUSTER_MESSAGES", "8"))
QUESTION_BRIEFS_MIN_CLUSTER_USERS = int(os.getenv("QUESTION_BRIEFS_MIN_CLUSTER_USERS", "3"))
QUESTION_BRIEFS_MIN_CLUSTER_CHANNELS = int(os.getenv("QUESTION_BRIEFS_MIN_CLUSTER_CHANNELS", "2"))
QUESTION_BRIEFS_MAX_CLUSTER_CANDIDATES = int(os.getenv("QUESTION_BRIEFS_MAX_CLUSTER_CANDIDATES", "24"))
QUESTION_BRIEFS_MIN_ACCEPTED_CLUSTERS = int(os.getenv("QUESTION_BRIEFS_MIN_ACCEPTED_CLUSTERS", "6"))
QUESTION_BRIEFS_REFRESH_MINUTES = int(os.getenv("QUESTION_BRIEFS_REFRESH_MINUTES", "120"))
QUESTION_BRIEFS_REFRESH_ON_STARTUP = _env_bool("QUESTION_BRIEFS_REFRESH_ON_STARTUP", True)
QUESTION_BRIEFS_PROMPT_VERSION = os.getenv("QUESTION_BRIEFS_PROMPT_VERSION", "qcards-v2")

BEHAVIORAL_BRIEFS_MODEL = os.getenv("BEHAVIORAL_BRIEFS_MODEL", "gpt-5-nano")
BEHAVIORAL_BRIEFS_MAX_TOKENS = int(os.getenv("BEHAVIORAL_BRIEFS_MAX_TOKENS", "1500"))
BEHAVIORAL_BRIEFS_CACHE_TTL_SECONDS = int(os.getenv("BEHAVIORAL_BRIEFS_CACHE_TTL_SECONDS", "7200"))
BEHAVIORAL_BRIEFS_WINDOW_DAYS = int(os.getenv("BEHAVIORAL_BRIEFS_WINDOW_DAYS", str(GRAPH_ANALYTICS_RETENTION_DAYS)))
BEHAVIORAL_BRIEFS_MAX_TOPICS = int(os.getenv("BEHAVIORAL_BRIEFS_MAX_TOPICS", "16"))
BEHAVIORAL_BRIEFS_MAX_CARDS = int(os.getenv("BEHAVIORAL_BRIEFS_MAX_CARDS", "8"))
BEHAVIORAL_BRIEFS_EVIDENCE_PER_TOPIC = int(os.getenv("BEHAVIORAL_BRIEFS_EVIDENCE_PER_TOPIC", "14"))
BEHAVIORAL_BRIEFS_MIN_CONFIDENCE = float(os.getenv("BEHAVIORAL_BRIEFS_MIN_CONFIDENCE", "0.58"))
BEHAVIORAL_BRIEFS_REFRESH_MINUTES = int(os.getenv("BEHAVIORAL_BRIEFS_REFRESH_MINUTES", "120"))
BEHAVIORAL_BRIEFS_REFRESH_ON_STARTUP = _env_bool("BEHAVIORAL_BRIEFS_REFRESH_ON_STARTUP", True)
BEHAVIORAL_BRIEFS_MIN_MESSAGES = int(os.getenv("BEHAVIORAL_BRIEFS_MIN_MESSAGES", "8"))
BEHAVIORAL_BRIEFS_MIN_USERS = int(os.getenv("BEHAVIORAL_BRIEFS_MIN_USERS", "3"))
BEHAVIORAL_BRIEFS_MIN_CHANNELS = int(os.getenv("BEHAVIORAL_BRIEFS_MIN_CHANNELS", "2"))
BEHAVIORAL_BRIEFS_PROMPT_VERSION = os.getenv("BEHAVIORAL_BRIEFS_PROMPT_VERSION", "behavior-v2")

# Scrape backpressure: when backlog is high, prioritize processing/sync before scraping more.
SCRAPE_SKIP_WHEN_BACKLOG = _env_bool("SCRAPE_SKIP_WHEN_BACKLOG", True)
SCRAPE_BACKPRESSURE_UNPROCESSED_POSTS = int(os.getenv("SCRAPE_BACKPRESSURE_UNPROCESSED_POSTS", "250"))
SCRAPE_BACKPRESSURE_UNPROCESSED_COMMENTS = int(os.getenv("SCRAPE_BACKPRESSURE_UNPROCESSED_COMMENTS", "120"))

# ── Scheduler ─────────────────────────────────────────────────────────────────
SCRAPER_INTERVAL_MINUTES   = 15    # How often to check for new posts
PROCESSOR_INTERVAL_MINUTES = 60    # How often to run AI analysis
NEO4J_SYNC_INTERVAL_MINUTES = 60  # How often to sync AI results to Neo4j

# ── AI Processing ─────────────────────────────────────────────────────────────
AI_BATCH_SIZE = 50   # Messages per user per AI call
AI_NORMAL_COMMENT_LIMIT = int(os.getenv("AI_NORMAL_COMMENT_LIMIT", "60"))
AI_NORMAL_POST_LIMIT = int(os.getenv("AI_NORMAL_POST_LIMIT", "25"))
AI_NORMAL_SYNC_LIMIT = int(os.getenv("AI_NORMAL_SYNC_LIMIT", "80"))

# Catch-up mode (processing/sync heavy, optionally without scraping)
AI_CATCHUP_COMMENT_LIMIT = int(os.getenv("AI_CATCHUP_COMMENT_LIMIT", "220"))
AI_CATCHUP_POST_LIMIT = int(os.getenv("AI_CATCHUP_POST_LIMIT", "120"))
AI_CATCHUP_SYNC_LIMIT = int(os.getenv("AI_CATCHUP_SYNC_LIMIT", "320"))

# ── Safety Checks ─────────────────────────────────────────────────────────────
def validate():
    missing = []
    if not TELEGRAM_API_ID:        missing.append("TELEGRAM_API_ID")
    if not TELEGRAM_API_HASH:      missing.append("TELEGRAM_API_HASH")
    if not TELEGRAM_PHONE:         missing.append("TELEGRAM_PHONE")
    if not SUPABASE_URL:           missing.append("SUPABASE_URL")
    if not SUPABASE_SERVICE_ROLE_KEY: missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if not NEO4J_URI:              missing.append("NEO4J_URI")
    if not NEO4J_PASSWORD:         missing.append("NEO4J_PASSWORD")
    if not OPENAI_API_KEY:         missing.append("OPENAI_API_KEY/OpenAI_API")
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")
