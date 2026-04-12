"""
config.py — Centralized configuration loader.
Reads all values from .env and exposes them as typed constants.
"""
import os
import sys
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


def _environment_name() -> str:
    return str(
        os.getenv("RAILWAY_ENVIRONMENT")
        or os.getenv("APP_ENV")
        or os.getenv("ENVIRONMENT")
        or ""
    ).strip().lower()


ENVIRONMENT_NAME = _environment_name()
IS_STAGING = ENVIRONMENT_NAME in {"stage", "staging"}
IS_PRODUCTION = ENVIRONMENT_NAME in {"prod", "production"}
IS_LOCKED_ENV = IS_PRODUCTION or IS_STAGING
STAGING_ENABLE_BACKGROUND_JOBS = _env_bool("STAGING_ENABLE_BACKGROUND_JOBS", False)


def should_validate_on_import() -> bool:
    return "pytest" not in sys.modules


def _normalize_app_role_for_validation(value=None) -> str:
    role = str(os.getenv("APP_ROLE") if value is None else value or "").strip().lower()
    if role not in {"web", "worker", "all"}:
        role = "all"
    if IS_STAGING:
        return "web"
    return role

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_API_ID       = int(os.getenv("TELEGRAM_API_ID", 0))
TELEGRAM_API_HASH     = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_PHONE        = os.getenv("TELEGRAM_PHONE", "")
TELEGRAM_SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "telegram_scraper")
TELEGRAM_SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING", "")  # For Railway/cloud deployment


def has_telegram_runtime_credentials() -> bool:
    return bool(TELEGRAM_API_ID and TELEGRAM_API_HASH)


def has_telegram_login_credentials() -> bool:
    return bool(TELEGRAM_API_ID and TELEGRAM_API_HASH and TELEGRAM_PHONE)


def needs_telegram_runtime_credentials() -> bool:
    return _normalize_app_role_for_validation() in {"worker", "all"}

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL              = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
PIPELINE_DATABASE_URL = os.getenv("PIPELINE_DATABASE_URL", os.getenv("SUPABASE_DB_URL", "")).strip()
SOCIAL_SUPABASE_URL = os.getenv("SOCIAL_SUPABASE_URL", SUPABASE_URL).strip()
SOCIAL_SUPABASE_SERVICE_ROLE_KEY = os.getenv(
    "SOCIAL_SUPABASE_SERVICE_ROLE_KEY",
    SUPABASE_SERVICE_ROLE_KEY,
).strip()
SOCIAL_DATABASE_URL = os.getenv("SOCIAL_DATABASE_URL", "").strip()

# ── Neo4j ─────────────────────────────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI", "")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")
SOCIAL_NEO4J_URI = os.getenv("SOCIAL_NEO4J_URI", NEO4J_URI).strip()
SOCIAL_NEO4J_USERNAME = os.getenv("SOCIAL_NEO4J_USERNAME", NEO4J_USERNAME).strip()
SOCIAL_NEO4J_PASSWORD = os.getenv("SOCIAL_NEO4J_PASSWORD", NEO4J_PASSWORD).strip()
SOCIAL_NEO4J_DATABASE = os.getenv("SOCIAL_NEO4J_DATABASE", NEO4J_DATABASE).strip()

# ── OpenAI ────────────────────────────────────────────────────────────────────
# Prefer standard OPENAI_API_KEY, but keep backward compatibility with OpenAI_API.
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", os.getenv("OpenAI_API", ""))
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-5.4-mini")

# ── Runtime / Deployment ───────────────────────────────────────────────────────
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("PORT", os.getenv("APP_PORT", "8001")))
CORS_ALLOW_ORIGINS = _env_csv("CORS_ALLOW_ORIGINS", "*")
ANALYTICS_API_REQUIRE_AUTH = _env_bool("ANALYTICS_API_REQUIRE_AUTH", False)
ANALYTICS_API_KEY_FRONTEND = os.getenv("ANALYTICS_API_KEY_FRONTEND", "").strip()
ANALYTICS_API_KEY_OPENCLAW = os.getenv("ANALYTICS_API_KEY_OPENCLAW", "").strip()
ANALYTICS_RATE_LIMIT_ENABLED = _env_bool("ANALYTICS_RATE_LIMIT_ENABLED", True)
ANALYTICS_RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("ANALYTICS_RATE_LIMIT_WINDOW_SECONDS", "60"))
ANALYTICS_RATE_LIMIT_MAX_REQUESTS = int(os.getenv("ANALYTICS_RATE_LIMIT_MAX_REQUESTS", "120"))
ANALYTICS_RATE_LIMIT_TRUST_PROXY = _env_bool("ANALYTICS_RATE_LIMIT_TRUST_PROXY", True)
REDIS_URL = os.getenv("REDIS_URL", "").strip()
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "").strip()
SIMPLE_AUTH_USERNAME = os.getenv("SIMPLE_AUTH_USERNAME", "").strip()
SIMPLE_AUTH_PASSWORD = os.getenv("SIMPLE_AUTH_PASSWORD", "").strip()
ENABLE_DEBUG_ENDPOINTS = _env_bool("ENABLE_DEBUG_ENDPOINTS", not IS_LOCKED_ENV)
OPENCLAW_GATEWAY_BASE_URL = os.getenv("OPENCLAW_GATEWAY_BASE_URL", "").strip().rstrip("/")
OPENCLAW_GATEWAY_TOKEN = os.getenv("OPENCLAW_GATEWAY_TOKEN", "").strip()
OPENCLAW_GATEWAY_TRANSPORT = os.getenv(
    "OPENCLAW_GATEWAY_TRANSPORT",
    "openai_compatible" if IS_STAGING else "auto",
).strip().lower()
OPENCLAW_GATEWAY_MODEL = os.getenv("OPENCLAW_GATEWAY_MODEL", "").strip()
OPENCLAW_ANALYTICS_AGENT_ID = os.getenv("OPENCLAW_ANALYTICS_AGENT_ID", "").strip()
OPENCLAW_BRIDGE_BASE_URL = os.getenv("OPENCLAW_BRIDGE_BASE_URL", "").strip().rstrip("/")
OPENCLAW_BRIDGE_TOKEN = os.getenv("OPENCLAW_BRIDGE_TOKEN", "").strip()
OPENCLAW_BRIDGE_AGENT_ID = os.getenv("OPENCLAW_BRIDGE_AGENT_ID", "web-api-assistant").strip()
OPENCLAW_WEB_SESSION_KEY = os.getenv(
    "OPENCLAW_WEB_SESSION_KEY",
    "" if IS_PRODUCTION else "tg-analyst-ru-web-admin",
).strip()
OPENCLAW_KB_SESSION_KEY = os.getenv(
    "OPENCLAW_KB_SESSION_KEY",
    "" if IS_PRODUCTION else "tg-analyst-ru-web-kb",
).strip()
OPENCLAW_HELPER_TIMEOUT_SECONDS = float(os.getenv("OPENCLAW_HELPER_TIMEOUT_SECONDS", "30"))
OPENCLAW_HELPER_CONNECT_TIMEOUT_SECONDS = float(
    os.getenv("OPENCLAW_HELPER_CONNECT_TIMEOUT_SECONDS", str(OPENCLAW_HELPER_TIMEOUT_SECONDS))
)
OPENCLAW_HELPER_READ_TIMEOUT_SECONDS = float(
    os.getenv("OPENCLAW_HELPER_READ_TIMEOUT_SECONDS", str(OPENCLAW_HELPER_TIMEOUT_SECONDS))
)
OPENCLAW_HELPER_RETRY_ATTEMPTS = max(0, int(os.getenv("OPENCLAW_HELPER_RETRY_ATTEMPTS", "1")))
OPENCLAW_HELPER_HISTORY_MAX_MESSAGES = max(
    4,
    int(os.getenv("OPENCLAW_HELPER_HISTORY_MAX_MESSAGES", "40")),
)
OPENCLAW_HELPER_HISTORY_MAX_CHARS = max(
    1000,
    int(os.getenv("OPENCLAW_HELPER_HISTORY_MAX_CHARS", "12000")),
)
OPENCLAW_HELPER_REPLAY_MAX_MESSAGES = max(
    2,
    int(os.getenv("OPENCLAW_HELPER_REPLAY_MAX_MESSAGES", "20")),
)
OPENCLAW_HELPER_REPLAY_MAX_CHARS = max(
    1000,
    int(os.getenv("OPENCLAW_HELPER_REPLAY_MAX_CHARS", "8000")),
)
OPENCLAW_HELPER_TRANSCRIPT_TTL_SECONDS = max(
    300,
    int(os.getenv("OPENCLAW_HELPER_TRANSCRIPT_TTL_SECONDS", "604800")),
)
OPENCLAW_HELPER_HTTP_MAX_BODY_BYTES = max(
    1024,
    int(os.getenv("OPENCLAW_HELPER_HTTP_MAX_BODY_BYTES", "8192")),
)
AI_HELPER_ADMIN_SUPABASE_USER_ID = os.getenv("AI_HELPER_ADMIN_SUPABASE_USER_ID", "").strip()
AI_HELPER_ADMIN_EMAIL = os.getenv("AI_HELPER_ADMIN_EMAIL", "").strip().lower()
SCRAPECREATORS_API_KEY = os.getenv("SCRAPECREATORS_API_KEY", "").strip()

# ── Feature Flags ──────────────────────────────────────────────────────────────
FEATURE_TAXONOMY_V2 = _env_bool("FEATURE_TAXONOMY_V2", True)
FEATURE_ENTITY_GRAPH = _env_bool("FEATURE_ENTITY_GRAPH", True)
FEATURE_AI_ANALYST = _env_bool("FEATURE_AI_ANALYST", False)
FEATURE_AI_PRESENTER = _env_bool("FEATURE_AI_PRESENTER", False)
FEATURE_EXTRACTION_V2 = _env_bool("FEATURE_EXTRACTION_V2", True)
FEATURE_QUESTION_BRIEFS_AI = _env_bool("FEATURE_QUESTION_BRIEFS_AI", True)
FEATURE_BEHAVIORAL_BRIEFS_AI = _env_bool("FEATURE_BEHAVIORAL_BRIEFS_AI", True)
FEATURE_OPPORTUNITY_BRIEFS_AI = _env_bool("FEATURE_OPPORTUNITY_BRIEFS_AI", True)
FEATURE_TOPIC_OVERVIEWS_AI = _env_bool("FEATURE_TOPIC_OVERVIEWS_AI", True)
PIPELINE_QUEUE_ENABLED = _env_bool("PIPELINE_QUEUE_ENABLED", False)
FEATURE_SOURCE_RESOLUTION_QUEUE = _env_bool("FEATURE_SOURCE_RESOLUTION_QUEUE", False)
FEATURE_SOURCE_RESOLUTION_WORKER = _env_bool("FEATURE_SOURCE_RESOLUTION_WORKER", False)
FEATURE_SOURCE_PEER_REF_LOOKUP = _env_bool("FEATURE_SOURCE_PEER_REF_LOOKUP", False)

# ── AI Safety / Performance ────────────────────────────────────────────────────
AI_REQUEST_TIMEOUT_SECONDS = float(os.getenv("AI_REQUEST_TIMEOUT_SECONDS", "45"))
AI_REQUEST_MAX_RETRIES = int(os.getenv("AI_REQUEST_MAX_RETRIES", "2"))
AI_REQUEST_RETRY_BACKOFF_SECONDS = float(os.getenv("AI_REQUEST_RETRY_BACKOFF_SECONDS", "1.5"))
OPENAI_CIRCUIT_BREAKER_ENABLED = _env_bool("OPENAI_CIRCUIT_BREAKER_ENABLED", True)
OPENAI_CIRCUIT_RATE_LIMIT_THRESHOLD = max(1, int(os.getenv("OPENAI_CIRCUIT_RATE_LIMIT_THRESHOLD", "3")))
OPENAI_CIRCUIT_RATE_LIMIT_WINDOW_SECONDS = max(
    10,
    int(os.getenv("OPENAI_CIRCUIT_RATE_LIMIT_WINDOW_SECONDS", "60")),
)
OPENAI_CIRCUIT_PROVIDER_ERROR_THRESHOLD = max(1, int(os.getenv("OPENAI_CIRCUIT_PROVIDER_ERROR_THRESHOLD", "3")))
OPENAI_CIRCUIT_PROVIDER_ERROR_WINDOW_SECONDS = max(
    10,
    int(os.getenv("OPENAI_CIRCUIT_PROVIDER_ERROR_WINDOW_SECONDS", "60")),
)
OPENAI_CIRCUIT_RATE_LIMIT_OPEN_SECONDS = max(30, int(os.getenv("OPENAI_CIRCUIT_RATE_LIMIT_OPEN_SECONDS", "300")))
OPENAI_CIRCUIT_PROVIDER_ERROR_OPEN_SECONDS = max(
    30,
    int(os.getenv("OPENAI_CIRCUIT_PROVIDER_ERROR_OPEN_SECONDS", "120")),
)
OPENAI_CIRCUIT_QUOTA_OPEN_SECONDS = max(300, int(os.getenv("OPENAI_CIRCUIT_QUOTA_OPEN_SECONDS", "1800")))
OPENAI_CIRCUIT_HALF_OPEN_TTL_SECONDS = max(5, int(os.getenv("OPENAI_CIRCUIT_HALF_OPEN_TTL_SECONDS", "30")))
OPENAI_CIRCUIT_REOPEN_MULTIPLIER = max(1.0, float(os.getenv("OPENAI_CIRCUIT_REOPEN_MULTIPLIER", "2.0")))
OPENAI_CIRCUIT_MAX_OPEN_SECONDS = max(300, int(os.getenv("OPENAI_CIRCUIT_MAX_OPEN_SECONDS", "7200")))
AI_USAGE_TELEMETRY_ENABLED = _env_bool("AI_USAGE_TELEMETRY_ENABLED", True)
AI_COMMENT_MAX_TOKENS = int(os.getenv("AI_COMMENT_MAX_TOKENS", "2200"))
AI_POST_MAX_TOKENS = int(os.getenv("AI_POST_MAX_TOKENS", "1800"))
AI_COMMENT_WORKERS = int(os.getenv("AI_COMMENT_WORKERS", "3"))
AI_POST_WORKERS = int(os.getenv("AI_POST_WORKERS", "2"))
AI_MAX_INFLIGHT_REQUESTS = int(os.getenv("AI_MAX_INFLIGHT_REQUESTS", "4"))
AI_FAILURE_MAX_RETRIES = int(os.getenv("AI_FAILURE_MAX_RETRIES", "5"))
AI_FAILURE_BACKOFF_SECONDS = int(os.getenv("AI_FAILURE_BACKOFF_SECONDS", "60"))
AI_FAILURE_BACKOFF_MAX_SECONDS = int(os.getenv("AI_FAILURE_BACKOFF_MAX_SECONDS", "3600"))
AI_TRANSIENT_RECOVERY_ENABLED = _env_bool("AI_TRANSIENT_RECOVERY_ENABLED", True)
AI_TRANSIENT_RECOVERY_CANARY_LIMIT = int(os.getenv("AI_TRANSIENT_RECOVERY_CANARY_LIMIT", "10"))
AI_TRANSIENT_RECOVERY_BATCH_LIMIT = int(os.getenv("AI_TRANSIENT_RECOVERY_BATCH_LIMIT", "50"))
AI_TRANSIENT_RECOVERY_COOLDOWN_MINUTES = int(os.getenv("AI_TRANSIENT_RECOVERY_COOLDOWN_MINUTES", "60"))
AI_TRANSIENT_RECOVERY_SUCCESS_WINDOW_MINUTES = int(os.getenv("AI_TRANSIENT_RECOVERY_SUCCESS_WINDOW_MINUTES", "120"))
AI_TRANSIENT_RECOVERY_MAX_ATTEMPTS = int(os.getenv("AI_TRANSIENT_RECOVERY_MAX_ATTEMPTS", "3"))
AI_POST_BATCH_SIZE = int(os.getenv("AI_POST_BATCH_SIZE", "5"))
AI_POST_BATCH_MAX_TOKENS = int(os.getenv("AI_POST_BATCH_MAX_TOKENS", "2600"))
AI_MESSAGE_CHAR_LIMIT = int(os.getenv("AI_MESSAGE_CHAR_LIMIT", "700"))
AI_MIN_COMMENT_LENGTH = max(0, int(os.getenv("AI_MIN_COMMENT_LENGTH", "15")))
AI_SKIP_BOT_COMMENTS = _env_bool("AI_SKIP_BOT_COMMENTS", True)
AI_FILTER_DUPLICATE_COMMENTS = _env_bool("AI_FILTER_DUPLICATE_COMMENTS", True)
AI_THREAD_SUMMARY_CONTEXT_MESSAGES = int(os.getenv("AI_THREAD_SUMMARY_CONTEXT_MESSAGES", "12"))
AI_PROCESS_STAGE_MAX_SECONDS = int(os.getenv("AI_PROCESS_STAGE_MAX_SECONDS", "1200"))
AI_SYNC_STAGE_MAX_SECONDS = int(os.getenv("AI_SYNC_STAGE_MAX_SECONDS", "900"))
SCRAPER_CONTROL_POLL_SECONDS = max(2, int(os.getenv("SCRAPER_CONTROL_POLL_SECONDS", "5")))
NEO4J_SYNC_BATCH_CHUNK_SIZE = max(1, int(os.getenv("NEO4J_SYNC_BATCH_CHUNK_SIZE", "20")))
AI_POST_PROMPT_STYLE = os.getenv("AI_POST_PROMPT_STYLE", "compact").strip().lower()
PIPELINE_QUEUE_REPAIR_BATCH_SIZE = max(1, int(os.getenv("PIPELINE_QUEUE_REPAIR_BATCH_SIZE", "500")))
PIPELINE_QUEUE_CLAIM_BATCH_SIZE = max(1, int(os.getenv("PIPELINE_QUEUE_CLAIM_BATCH_SIZE", "100")))
PIPELINE_QUEUE_LEASE_SECONDS = max(30, int(os.getenv("PIPELINE_QUEUE_LEASE_SECONDS", "900")))
PIPELINE_QUEUE_MAX_ATTEMPTS = max(1, int(os.getenv("PIPELINE_QUEUE_MAX_ATTEMPTS", "5")))
PIPELINE_QUEUE_BACKOFF_SECONDS = max(5, int(os.getenv("PIPELINE_QUEUE_BACKOFF_SECONDS", "60")))
PIPELINE_QUEUE_BACKOFF_MAX_SECONDS = max(
    PIPELINE_QUEUE_BACKOFF_SECONDS,
    int(os.getenv("PIPELINE_QUEUE_BACKOFF_MAX_SECONDS", "3600")),
)
GRAPH_ANALYTICS_RETENTION_DAYS = int(os.getenv("GRAPH_ANALYTICS_RETENTION_DAYS", "15"))
QUESTION_BRIEFS_MODEL = os.getenv("QUESTION_BRIEFS_MODEL", OPENAI_MODEL)
QUESTION_BRIEFS_MAX_TOKENS = int(os.getenv("QUESTION_BRIEFS_MAX_TOKENS", "2600"))
QUESTION_BRIEFS_TRIAGE_MODEL = os.getenv("QUESTION_BRIEFS_TRIAGE_MODEL", QUESTION_BRIEFS_MODEL)
QUESTION_BRIEFS_TRIAGE_MAX_TOKENS = int(os.getenv("QUESTION_BRIEFS_TRIAGE_MAX_TOKENS", "1400"))
QUESTION_BRIEFS_USE_AI_TRIAGE = _env_bool("QUESTION_BRIEFS_USE_AI_TRIAGE", False)
QUESTION_BRIEFS_SYNTHESIS_MODEL = os.getenv("QUESTION_BRIEFS_SYNTHESIS_MODEL", QUESTION_BRIEFS_MODEL)
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

BEHAVIORAL_BRIEFS_MODEL = os.getenv("BEHAVIORAL_BRIEFS_MODEL", OPENAI_MODEL)
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

OPPORTUNITY_BRIEFS_MODEL = os.getenv("OPPORTUNITY_BRIEFS_MODEL", OPENAI_MODEL)
OPPORTUNITY_BRIEFS_TRIAGE_MODEL = os.getenv("OPPORTUNITY_BRIEFS_TRIAGE_MODEL", OPPORTUNITY_BRIEFS_MODEL)
OPPORTUNITY_BRIEFS_TRIAGE_MAX_TOKENS = int(os.getenv("OPPORTUNITY_BRIEFS_TRIAGE_MAX_TOKENS", "1600"))
OPPORTUNITY_BRIEFS_SYNTHESIS_MODEL = os.getenv("OPPORTUNITY_BRIEFS_SYNTHESIS_MODEL", OPPORTUNITY_BRIEFS_MODEL)
OPPORTUNITY_BRIEFS_SYNTHESIS_MAX_TOKENS = int(os.getenv("OPPORTUNITY_BRIEFS_SYNTHESIS_MAX_TOKENS", "2200"))
OPPORTUNITY_BRIEFS_CACHE_TTL_SECONDS = int(os.getenv("OPPORTUNITY_BRIEFS_CACHE_TTL_SECONDS", "7200"))
OPPORTUNITY_BRIEFS_WINDOW_DAYS = int(os.getenv("OPPORTUNITY_BRIEFS_WINDOW_DAYS", "30"))
OPPORTUNITY_BRIEFS_MAX_TOPICS = int(os.getenv("OPPORTUNITY_BRIEFS_MAX_TOPICS", "16"))
OPPORTUNITY_BRIEFS_MAX_BRIEFS = int(os.getenv("OPPORTUNITY_BRIEFS_MAX_BRIEFS", "8"))
OPPORTUNITY_BRIEFS_EVIDENCE_PER_TOPIC = int(os.getenv("OPPORTUNITY_BRIEFS_EVIDENCE_PER_TOPIC", "14"))
OPPORTUNITY_BRIEFS_SYNTH_EVIDENCE_LIMIT = int(os.getenv("OPPORTUNITY_BRIEFS_SYNTH_EVIDENCE_LIMIT", "6"))
OPPORTUNITY_BRIEFS_MIN_CONFIDENCE = float(os.getenv("OPPORTUNITY_BRIEFS_MIN_CONFIDENCE", "0.60"))
OPPORTUNITY_BRIEFS_MIN_MESSAGES = int(os.getenv("OPPORTUNITY_BRIEFS_MIN_MESSAGES", "3"))
OPPORTUNITY_BRIEFS_MIN_USERS = int(os.getenv("OPPORTUNITY_BRIEFS_MIN_USERS", "2"))
OPPORTUNITY_BRIEFS_MESSAGE_CHAR_LIMIT = int(os.getenv("OPPORTUNITY_BRIEFS_MESSAGE_CHAR_LIMIT", "900"))
OPPORTUNITY_BRIEFS_CONTEXT_CHAR_LIMIT = int(os.getenv("OPPORTUNITY_BRIEFS_CONTEXT_CHAR_LIMIT", "520"))
OPPORTUNITY_BRIEFS_MIN_ACCEPTED_CLUSTERS = int(os.getenv("OPPORTUNITY_BRIEFS_MIN_ACCEPTED_CLUSTERS", "3"))
OPPORTUNITY_BRIEFS_REFRESH_MINUTES = int(os.getenv("OPPORTUNITY_BRIEFS_REFRESH_MINUTES", "120"))
OPPORTUNITY_BRIEFS_REFRESH_ON_STARTUP = _env_bool("OPPORTUNITY_BRIEFS_REFRESH_ON_STARTUP", True)
OPPORTUNITY_BRIEFS_PROMPT_VERSION = os.getenv("OPPORTUNITY_BRIEFS_PROMPT_VERSION", "opportunity-v1")

TOPIC_OVERVIEWS_MODEL = os.getenv("TOPIC_OVERVIEWS_MODEL", OPENAI_MODEL)
TOPIC_OVERVIEWS_MAX_TOKENS = int(os.getenv("TOPIC_OVERVIEWS_MAX_TOKENS", "1200"))
TOPIC_OVERVIEWS_CACHE_TTL_SECONDS = int(os.getenv("TOPIC_OVERVIEWS_CACHE_TTL_SECONDS", "7200"))
TOPIC_OVERVIEWS_WINDOW_DAYS = int(os.getenv("TOPIC_OVERVIEWS_WINDOW_DAYS", str(GRAPH_ANALYTICS_RETENTION_DAYS)))
TOPIC_OVERVIEWS_MAX_TOPICS = int(os.getenv("TOPIC_OVERVIEWS_MAX_TOPICS", "24"))
TOPIC_OVERVIEWS_EVIDENCE_PER_TOPIC = int(os.getenv("TOPIC_OVERVIEWS_EVIDENCE_PER_TOPIC", "5"))
TOPIC_OVERVIEWS_QUESTION_LIMIT = int(os.getenv("TOPIC_OVERVIEWS_QUESTION_LIMIT", "3"))
TOPIC_OVERVIEWS_MIN_EVIDENCE = int(os.getenv("TOPIC_OVERVIEWS_MIN_EVIDENCE", "8"))
TOPIC_OVERVIEWS_MIN_USERS = int(os.getenv("TOPIC_OVERVIEWS_MIN_USERS", "3"))
TOPIC_OVERVIEWS_MIN_CHANNELS = int(os.getenv("TOPIC_OVERVIEWS_MIN_CHANNELS", "2"))
TOPIC_OVERVIEWS_MAX_CONCURRENCY = max(1, int(os.getenv("TOPIC_OVERVIEWS_MAX_CONCURRENCY", "2")))
TOPIC_OVERVIEWS_REFRESH_MINUTES = int(os.getenv("TOPIC_OVERVIEWS_REFRESH_MINUTES", "120"))
TOPIC_OVERVIEWS_REFRESH_ON_STARTUP = _env_bool("TOPIC_OVERVIEWS_REFRESH_ON_STARTUP", True)
TOPIC_OVERVIEWS_PROMPT_VERSION = os.getenv("TOPIC_OVERVIEWS_PROMPT_VERSION", "topic-overview-v2")

# Scrape backpressure: when backlog is high, prioritize processing/sync before scraping more.
SCRAPE_SKIP_WHEN_BACKLOG = _env_bool("SCRAPE_SKIP_WHEN_BACKLOG", True)
SCRAPE_BACKPRESSURE_UNPROCESSED_POSTS = int(os.getenv("SCRAPE_BACKPRESSURE_UNPROCESSED_POSTS", "250"))
SCRAPE_BACKPRESSURE_UNPROCESSED_COMMENTS = int(os.getenv("SCRAPE_BACKPRESSURE_UNPROCESSED_COMMENTS", "120"))
GROUP_MAX_MESSAGES_PER_SOURCE_PER_CYCLE = int(os.getenv("GROUP_MAX_MESSAGES_PER_SOURCE_PER_CYCLE", "400"))
GROUP_MAX_THREAD_ANCHORS_PER_SOURCE_PER_CYCLE = int(os.getenv("GROUP_MAX_THREAD_ANCHORS_PER_SOURCE_PER_CYCLE", "120"))
SOURCE_RESOLUTION_INTERVAL_MINUTES = max(1, int(os.getenv("SOURCE_RESOLUTION_INTERVAL_MINUTES", "1")))
SOURCE_RESOLUTION_MIN_INTERVAL_SECONDS = max(1, int(os.getenv("SOURCE_RESOLUTION_MIN_INTERVAL_SECONDS", "5")))
SOURCE_RESOLUTION_MAX_JOBS_PER_RUN = max(1, int(os.getenv("SOURCE_RESOLUTION_MAX_JOBS_PER_RUN", "10")))
SOURCE_RESOLUTION_LEASE_SECONDS = max(30, int(os.getenv("SOURCE_RESOLUTION_LEASE_SECONDS", "180")))
SOURCE_RESOLUTION_RETRY_MAX_SECONDS = max(300, int(os.getenv("SOURCE_RESOLUTION_RETRY_MAX_SECONDS", "21600")))
TELEGRAM_SESSION_SLOTS = _env_csv("TELEGRAM_SESSION_SLOTS", "primary")

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

# ── Knowledge Base (RAG) ──────────────────────────────────────────────────────
GEMINI_API_KEY         = os.getenv("GEMINI_API_KEY", "")
KB_STORAGE_PATH        = os.getenv("KB_STORAGE_PATH", "/data/kb")
KB_EMBED_DIM           = int(os.getenv("KB_EMBED_DIM", "768"))
KB_TOP_K               = int(os.getenv("KB_TOP_K", "8"))
KB_CHUNK_SIZE          = int(os.getenv("KB_CHUNK_SIZE", "1500"))
KB_CHUNK_OVERLAP       = int(os.getenv("KB_CHUNK_OVERLAP", "200"))
KB_GENERATION_MODEL    = os.getenv("KB_GENERATION_MODEL", "")  # Defaults to OPENAI_MODEL at runtime
KB_UPLOAD_MAX_MB       = int(os.getenv("KB_UPLOAD_MAX_MB", "50"))

# ── Social Media Activities ───────────────────────────────────────────────────
SOCIAL_FETCH_MAX_PAGES = max(1, int(os.getenv("SOCIAL_FETCH_MAX_PAGES", "3")))
SOCIAL_FETCH_PAGE_SIZE = max(1, int(os.getenv("SOCIAL_FETCH_PAGE_SIZE", "50")))
SOCIAL_ANALYSIS_BATCH_SIZE = max(1, min(int(os.getenv("SOCIAL_ANALYSIS_BATCH_SIZE", "8")), 8))
SOCIAL_ANALYSIS_MODEL = os.getenv("SOCIAL_ANALYSIS_MODEL", OPENAI_MODEL).strip() or OPENAI_MODEL
SOCIAL_ANALYSIS_PROMPT_VERSION = os.getenv("SOCIAL_ANALYSIS_PROMPT_VERSION", "social-v1").strip() or "social-v1"
SOCIAL_GRAPH_PROJECTION_VERSION = os.getenv("SOCIAL_GRAPH_PROJECTION_VERSION", "social-graph-v1").strip() or "social-graph-v1"
SOCIAL_TIKTOK_ENABLED = _env_bool("SOCIAL_TIKTOK_ENABLED", False)
SOCIAL_RUNTIME_ENABLED = _env_bool("SOCIAL_RUNTIME_ENABLED", True)
SOCIAL_ACTIVITY_RETENTION_DAYS = max(30, int(os.getenv("SOCIAL_ACTIVITY_RETENTION_DAYS", "365")))
SOCIAL_PAYLOAD_RETENTION_DAYS = max(7, int(os.getenv("SOCIAL_PAYLOAD_RETENTION_DAYS", "90")))
SOCIAL_STAGE_CLAIM_LIMIT = max(1, int(os.getenv("SOCIAL_STAGE_CLAIM_LIMIT", "120")))
SOCIAL_STAGE_LEASE_SECONDS = max(60, int(os.getenv("SOCIAL_STAGE_LEASE_SECONDS", "900")))
SOCIAL_RETRY_BASE_SECONDS = max(30, int(os.getenv("SOCIAL_RETRY_BASE_SECONDS", "120")))
SOCIAL_RETRY_MAX_SECONDS = max(
    SOCIAL_RETRY_BASE_SECONDS,
    int(os.getenv("SOCIAL_RETRY_MAX_SECONDS", "3600")),
)

# ── Safety Checks ─────────────────────────────────────────────────────────────
def validate():
    runtime_role = _normalize_app_role_for_validation()
    web_runtime = runtime_role in {"web", "all"}
    missing = []
    if needs_telegram_runtime_credentials():
        if not TELEGRAM_API_ID:
            missing.append("TELEGRAM_API_ID")
        if not TELEGRAM_API_HASH:
            missing.append("TELEGRAM_API_HASH")
    if not SUPABASE_URL:           missing.append("SUPABASE_URL")
    if not SUPABASE_SERVICE_ROLE_KEY: missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if not NEO4J_URI:              missing.append("NEO4J_URI")
    if not NEO4J_PASSWORD:         missing.append("NEO4J_PASSWORD")
    if not OPENAI_API_KEY:         missing.append("OPENAI_API_KEY/OpenAI_API")
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

    if IS_LOCKED_ENV:
        locked_env_errors: list[str] = []
        if not REDIS_URL:
            locked_env_errors.append("REDIS_URL")
        if web_runtime:
            if not ANALYTICS_API_REQUIRE_AUTH:
                locked_env_errors.append("ANALYTICS_API_REQUIRE_AUTH must be true")
            if not CORS_ALLOW_ORIGINS or "*" in CORS_ALLOW_ORIGINS:
                locked_env_errors.append("CORS_ALLOW_ORIGINS must not contain '*'")
            if not ADMIN_API_KEY:
                locked_env_errors.append("ADMIN_API_KEY")
            if not ANALYTICS_API_KEY_FRONTEND:
                locked_env_errors.append("ANALYTICS_API_KEY_FRONTEND")
            if not ANALYTICS_API_KEY_OPENCLAW:
                locked_env_errors.append("ANALYTICS_API_KEY_OPENCLAW")
        if locked_env_errors:
            raise EnvironmentError(
                "Missing or unsafe locked-environment configuration: "
                + ", ".join(locked_env_errors)
            )

    if IS_LOCKED_ENV and web_runtime:
        ai_helper_missing = []
        if not OPENCLAW_WEB_SESSION_KEY:
            ai_helper_missing.append("OPENCLAW_WEB_SESSION_KEY")
        transport = (
            OPENCLAW_GATEWAY_TRANSPORT
            if OPENCLAW_GATEWAY_TRANSPORT in {"openai_compatible", "legacy", "auto", "cli_bridge"}
            else "auto"
        )
        effective_transport = "openai_compatible" if transport == "auto" and OPENCLAW_GATEWAY_MODEL else transport
        if effective_transport == "cli_bridge":
            if not OPENCLAW_BRIDGE_BASE_URL:
                ai_helper_missing.append("OPENCLAW_BRIDGE_BASE_URL")
            if not OPENCLAW_BRIDGE_TOKEN:
                ai_helper_missing.append("OPENCLAW_BRIDGE_TOKEN")
            if not OPENCLAW_BRIDGE_AGENT_ID:
                ai_helper_missing.append("OPENCLAW_BRIDGE_AGENT_ID")
        else:
            if not OPENCLAW_GATEWAY_BASE_URL:
                ai_helper_missing.append("OPENCLAW_GATEWAY_BASE_URL")
            if not OPENCLAW_GATEWAY_TOKEN:
                ai_helper_missing.append("OPENCLAW_GATEWAY_TOKEN")
            if effective_transport == "openai_compatible":
                if not OPENCLAW_GATEWAY_MODEL:
                    ai_helper_missing.append("OPENCLAW_GATEWAY_MODEL")
            else:
                if not OPENCLAW_ANALYTICS_AGENT_ID:
                    ai_helper_missing.append("OPENCLAW_ANALYTICS_AGENT_ID")
        if not AI_HELPER_ADMIN_SUPABASE_USER_ID:
            ai_helper_missing.append("AI_HELPER_ADMIN_SUPABASE_USER_ID")
        if ai_helper_missing:
            raise EnvironmentError(
                "Missing required locked-environment AI helper environment variables: "
                + ", ".join(ai_helper_missing)
            )
