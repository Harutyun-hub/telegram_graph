"""
config.py — Centralized configuration loader.
Reads all values from .env and exposes them as typed constants.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_API_ID       = int(os.getenv("TELEGRAM_API_ID", 0))
TELEGRAM_API_HASH     = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_PHONE        = os.getenv("TELEGRAM_PHONE", "")
TELEGRAM_SESSION_NAME = "telegram_scraper"   # session file saved as telegram_scraper.session

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL              = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

# ── Neo4j ─────────────────────────────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI", "")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

# ── OpenAI ────────────────────────────────────────────────────────────────────
OPENAI_API_KEY = os.getenv("OpenAI_API", "")   # matches your .env key name
OPENAI_MODEL   = "gpt-4o-mini"

# ── Scheduler ─────────────────────────────────────────────────────────────────
SCRAPER_INTERVAL_MINUTES   = 15    # How often to check for new posts
PROCESSOR_INTERVAL_MINUTES = 60    # How often to run AI analysis
NEO4J_SYNC_INTERVAL_MINUTES = 60  # How often to sync AI results to Neo4j

# ── AI Processing ─────────────────────────────────────────────────────────────
AI_BATCH_SIZE = 50   # Messages per user per AI call

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
    if not OPENAI_API_KEY:         missing.append("OpenAI_API")
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")
