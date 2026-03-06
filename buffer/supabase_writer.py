"""
supabase_writer.py — All Supabase read/write operations.

Central data access layer. All other modules call this — never 
touch Supabase directly from scrapers or processors.
"""
from __future__ import annotations
from supabase import create_client, Client
from datetime import datetime, timezone
import json
from loguru import logger
import config


class SupabaseWriter:

    def __init__(self):
        self.client: Client = create_client(
            config.SUPABASE_URL,
            config.SUPABASE_SERVICE_ROLE_KEY
        )
        self._runtime_bucket_name = "runtime-config"
        self._scheduler_settings_path = "scraper/scheduler_settings.json"

    def _ensure_runtime_bucket(self):
        """Ensure runtime config bucket exists in Supabase Storage."""
        buckets = self.client.storage.list_buckets()
        names = []
        for bucket in buckets:
            if isinstance(bucket, dict):
                names.append(bucket.get("name"))
            else:
                names.append(getattr(bucket, "name", None))

        if self._runtime_bucket_name not in names:
            self.client.storage.create_bucket(
                self._runtime_bucket_name,
                self._runtime_bucket_name,
                {"public": False},
            )

    def get_scraper_scheduler_settings(self, default_interval_minutes: int = 15) -> dict:
        """Read persisted scraper scheduler config from Supabase Storage."""
        default = {
            "is_active": False,
            "interval_minutes": int(default_interval_minutes),
            "updated_at": None,
        }

        try:
            self._ensure_runtime_bucket()
            raw = self.client.storage.from_(self._runtime_bucket_name).download(self._scheduler_settings_path)
            if not raw:
                return default
            parsed = json.loads(raw.decode("utf-8"))
            interval = int(parsed.get("interval_minutes", default_interval_minutes))
            if interval < 1:
                interval = int(default_interval_minutes)
            return {
                "is_active": bool(parsed.get("is_active", False)),
                "interval_minutes": interval,
                "updated_at": parsed.get("updated_at"),
            }
        except Exception:
            return default

    def save_scraper_scheduler_settings(self, *, is_active: bool, interval_minutes: int) -> dict:
        """Persist scraper scheduler config to Supabase Storage."""
        self._ensure_runtime_bucket()
        payload = {
            "is_active": bool(is_active),
            "interval_minutes": int(interval_minutes),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self.client.storage.from_(self._runtime_bucket_name).upload(
            self._scheduler_settings_path,
            json.dumps(payload, ensure_ascii=True).encode("utf-8"),
            {"content-type": "application/json", "upsert": "true"},
        )
        return payload

    # ── Channels ─────────────────────────────────────────────────────────────

    def get_active_channels(self) -> list[dict]:
        """Return all channels with is_active=TRUE."""
        res = self.client.table("telegram_channels") \
            .select("*") \
            .eq("is_active", True) \
            .execute()
        return res.data or []

    def list_channels(self) -> list[dict]:
        """Return all channels ordered by newest first."""
        res = self.client.table("telegram_channels") \
            .select("*") \
            .order("created_at", desc=True) \
            .execute()
        return res.data or []

    def get_channel_by_id(self, channel_uuid: str) -> dict | None:
        """Return a single channel by UUID."""
        res = self.client.table("telegram_channels") \
            .select("*") \
            .eq("id", channel_uuid) \
            .limit(1) \
            .execute()
        return res.data[0] if res.data else None

    def get_channel_by_username(self, channel_username: str) -> dict | None:
        """Case-insensitive lookup by exact username string."""
        normalized = (channel_username or "").strip().lower()
        if not normalized:
            return None
        res = self.client.table("telegram_channels") \
            .select("*") \
            .ilike("channel_username", normalized) \
            .limit(1) \
            .execute()
        return res.data[0] if res.data else None

    def get_channel_by_handle(self, handle: str) -> dict | None:
        """
        Lookup by normalized handle (without @), matching rows with or without @ prefix.
        """
        normalized = (handle or "").strip().lower().lstrip("@")
        if not normalized:
            return None

        # Keep this robust against mixed historical data where usernames were
        # stored both with and without leading '@'.
        rows = self.list_channels()
        for row in rows:
            value = (row.get("channel_username") or "").strip().lower().lstrip("@")
            if value == normalized:
                return row
        return None

    def create_channel(self, payload: dict) -> dict:
        """Create a new channel source and return it."""
        res = self.client.table("telegram_channels") \
            .insert(payload) \
            .execute()
        if not res.data:
            raise RuntimeError("Failed to create telegram channel")
        return res.data[0]

    def update_channel(self, channel_uuid: str, payload: dict) -> dict | None:
        """Update channel source fields and return updated row."""
        if not payload:
            return self.get_channel_by_id(channel_uuid)
        res = self.client.table("telegram_channels") \
            .update(payload) \
            .eq("id", channel_uuid) \
            .execute()
        if res.data:
            return res.data[0]
        return self.get_channel_by_id(channel_uuid)

    def update_channel_metadata(self, channel_uuid: str, metadata: dict):
        """Update channel title, telegram_channel_id, member_count etc."""
        # Filter out None values so we don't overwrite useful data with NULLs
        payload = {k: v for k, v in metadata.items() if v is not None}
        if payload:
            self.client.table("telegram_channels") \
                .update(payload) \
                .eq("id", channel_uuid) \
                .execute()

    def update_channel_last_scraped(self, channel_uuid: str):
        """Set last_scraped_at to now."""
        self.client.table("telegram_channels") \
            .update({"last_scraped_at": datetime.now(timezone.utc).isoformat()}) \
            .eq("id", channel_uuid) \
            .execute()

    # ── Posts ─────────────────────────────────────────────────────────────────

    def upsert_posts(self, posts: list[dict]):
        """
        Insert or update posts. 
        UNIQUE constraint on (channel_id, telegram_message_id) prevents duplicates.
        """
        if not posts:
            return
        self.client.table("telegram_posts") \
            .upsert(posts, on_conflict="channel_id,telegram_message_id") \
            .execute()
        logger.debug(f"Upserted {len(posts)} posts")

    def get_unprocessed_posts(self, limit: int = 100) -> list[dict]:
        """Fetch posts not yet sent to AI."""
        res = self.client.table("telegram_posts") \
            .select("id, channel_id, telegram_message_id, text, posted_at") \
            .eq("is_processed", False) \
            .not_.is_("text", "null") \
            .order("posted_at", desc=False) \
            .limit(limit) \
            .execute()
        return res.data or []

    def get_posts_with_comments_pending(self, limit: int = 50) -> list[dict]:
        """Fetch posts that have comments but haven't had comments scraped yet."""
        res = self.client.table("telegram_posts") \
            .select("id, channel_id, telegram_message_id") \
            .eq("has_comments", True) \
            .is_("comments_scraped_at", "null") \
            .limit(limit) \
            .execute()
        return res.data or []

    def get_posts_with_comments_pending_for_channel(self, channel_uuid: str, limit: int = 50) -> list[dict]:
        """Fetch pending comment-scrape posts for a specific channel."""
        res = self.client.table("telegram_posts") \
            .select("id, channel_id, telegram_message_id") \
            .eq("channel_id", channel_uuid) \
            .eq("has_comments", True) \
            .is_("comments_scraped_at", "null") \
            .limit(limit) \
            .execute()
        return res.data or []

    def mark_post_processed(self, post_uuid: str):
        self.client.table("telegram_posts") \
            .update({"is_processed": True}) \
            .eq("id", post_uuid) \
            .execute()

    def mark_post_comments_scraped(self, post_uuid: str, comment_count: int):
        self.client.table("telegram_posts") \
            .update({
                "comment_count":        comment_count,
                "comments_scraped_at":  datetime.now(timezone.utc).isoformat(),
            }) \
            .eq("id", post_uuid) \
            .execute()

    # ── Comments ─────────────────────────────────────────────────────────────

    def upsert_comments(self, comments: list[dict]):
        """Insert or update comments. Dedup on (post_id, telegram_message_id)."""
        if not comments:
            return
        self.client.table("telegram_comments") \
            .upsert(comments, on_conflict="post_id,telegram_message_id") \
            .execute()
        logger.debug(f"Upserted {len(comments)} comments")

    def get_unprocessed_comments(self, limit: int = 200) -> list[dict]:
        """Fetch comments not yet sent to AI."""
        res = self.client.table("telegram_comments") \
            .select("id, post_id, channel_id, telegram_user_id, text, posted_at") \
            .eq("is_processed", False) \
            .not_.is_("text", "null") \
            .order("posted_at", desc=False) \
            .limit(limit) \
            .execute()
        return res.data or []

    def mark_comment_processed(self, comment_uuid: str):
        self.client.table("telegram_comments") \
            .update({"is_processed": True}) \
            .eq("id", comment_uuid) \
            .execute()

    # ── Users ─────────────────────────────────────────────────────────────────

    def upsert_user(self, user: dict) -> str | None:
        """
        Insert or update a user by telegram_user_id.
        Returns the internal UUID of the user.
        """
        if not user.get("telegram_user_id"):
            return None

        payload = {k: v for k, v in user.items() if v is not None}
        payload["last_seen_at"] = datetime.now(timezone.utc).isoformat()

        res = self.client.table("telegram_users") \
            .upsert(payload, on_conflict="telegram_user_id") \
            .execute()

        if res.data:
            return res.data[0]["id"]
        return None

    def get_user_by_telegram_id(self, telegram_user_id: int) -> dict | None:
        res = self.client.table("telegram_users") \
            .select("*") \
            .eq("telegram_user_id", telegram_user_id) \
            .limit(1) \
            .execute()
        return res.data[0] if res.data else None

    # ── AI Analysis ─────────────────────────────────────────────────────────

    def save_analysis(self, analysis: dict):
        """Save AI analysis result."""
        self.client.table("ai_analysis") \
            .insert(analysis) \
            .execute()

    def get_unsynced_analysis(self, limit: int = 100) -> list[dict]:
        """Fetch AI analysis not yet pushed to Neo4j."""
        res = self.client.table("ai_analysis") \
            .select("*") \
            .eq("neo4j_synced", False) \
            .limit(limit) \
            .execute()
        return res.data or []

    def mark_analysis_synced(self, analysis_uuid: str):
        self.client.table("ai_analysis") \
            .update({"neo4j_synced": True}) \
            .eq("id", analysis_uuid) \
            .execute()

    def mark_post_neo4j_synced(self, post_uuid: str):
        self.client.table("telegram_posts") \
            .update({"neo4j_synced": True}) \
            .eq("id", post_uuid) \
            .execute()

    # ── Neo4j Bundle Assembly ────────────────────────────────────────────────

    def get_unsynced_posts(self, limit: int = 100) -> list[dict]:
        """Fetch posts not yet fully synced to Neo4j graph."""
        res = self.client.table("telegram_posts") \
            .select("*") \
            .eq("neo4j_synced", False) \
            .order("posted_at", desc=False) \
            .limit(limit) \
            .execute()
        return res.data or []

    def get_post_bundle(self, post: dict) -> dict:
        """
        Assemble everything needed to build the Neo4j graph for one post.

        Returns:
          {
            "post":     post dict,
            "channel":  channel dict,
            "comments": [comment dict, ...],
            "analyses": { str(telegram_user_id): analysis_dict, ... }
          }
        """
        # Channel
        ch_res = self.client.table("telegram_channels") \
            .select("*") \
            .eq("id", post["channel_id"]) \
            .limit(1) \
            .execute()
        channel = (ch_res.data or [{}])[0]

        # Comments for this post
        cmt_res = self.client.table("telegram_comments") \
            .select("*") \
            .eq("post_id", post["id"]) \
            .execute()
        comments = cmt_res.data or []

        # Build set of telegram_user_ids appearing in comments
        user_ids = list({
            int(c["telegram_user_id"])
            for c in comments
            if c.get("telegram_user_id")
        })

        # Fetch AI analyses for those users in this channel
        analyses: dict[str, dict] = {}
        if user_ids:
            an_res = self.client.table("ai_analysis") \
                .select("*") \
                .eq("channel_id", post["channel_id"]) \
                .in_("telegram_user_id", user_ids) \
                .execute()
            for a in (an_res.data or []):
                uid = str(a["telegram_user_id"])
                # Keep the MOST RECENT analysis per user (by created_at)
                if uid not in analyses or (
                    (a.get("created_at") or "") > (analyses[uid].get("created_at") or "")
                ):
                    analyses[uid] = a

        return {
            "post":           post,
            "channel":        channel,
            "comments":       comments,
            "analyses":       analyses,
            # Maps telegram_message_id → telegram_user_id for User→User network
            # Used by neo4j_writer to resolve who was replied to
            "reply_user_map": {
                int(c["telegram_message_id"]): int(c["telegram_user_id"])
                for c in comments
                if c.get("telegram_message_id") and c.get("telegram_user_id")
            },
        }
