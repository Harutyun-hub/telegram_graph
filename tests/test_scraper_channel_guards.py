from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from scraper import channel_scraper
from scraper import scrape_orchestrator


class _FakeChannel:
    pass


class _FakeClient:
    def __init__(self, entity, messages=None):
        self._entity = entity
        self._messages = list(messages or [])
        self.entity_requests = []

    async def get_entity(self, username):
        self.entity_requests.append(username)
        return self._entity

    async def iter_messages(self, entity, wait_time=0, reverse=False):
        for message in self._messages:
            yield message

    async def get_messages(self, entity, ids):
        found = []
        wanted = {int(value) for value in ids}
        for message in self._messages:
            if int(message.id) in wanted:
                found.append(message)
        return found


class _FakeWriter:
    def __init__(self):
        self.updated = []
        self.last_scraped = []
        self.explicit_last_scraped = []
        self.rows = {}
        self.posts = {}
        self.comments = {}
        self.peer_refs = {}
        self.queued_resolution_jobs = []
        self.slots = {}

    def update_channel(self, channel_uuid, payload):
        existing = self.rows.get(channel_uuid, {"id": channel_uuid})
        existing.update(payload)
        self.rows[channel_uuid] = existing
        self.updated.append((channel_uuid, payload))
        return dict(existing)

    def get_channel_by_id(self, channel_uuid):
        return self.rows.get(channel_uuid, {"id": channel_uuid})

    def get_channel_peer_ref(self, channel_uuid, session_slot="primary"):
        return self.peer_refs.get((str(channel_uuid), str(session_slot)))

    def upsert_channel_peer_ref(self, channel_uuid, session_slot, payload):
        row = {
            "channel_id": str(channel_uuid),
            "session_slot": str(session_slot),
            **dict(payload),
        }
        self.peer_refs[(str(channel_uuid), str(session_slot))] = row
        return row

    def ensure_source_resolution_slot(self, slot_key="primary", **payload):
        row = {"slot_key": slot_key, **payload}
        self.slots[str(slot_key)] = row
        return row

    def enqueue_source_resolution_job(self, channel_uuid, **payload):
        job = {"channel_id": str(channel_uuid), **payload}
        self.queued_resolution_jobs.append(job)
        return job

    def update_channel_last_scraped(self, channel_uuid):
        self.last_scraped.append(channel_uuid)

    def update_channel_last_scraped_at(self, channel_uuid, when_iso):
        self.explicit_last_scraped.append((channel_uuid, when_iso))

    def upsert_posts(self, posts):
        for post in posts:
            key = (str(post["channel_id"]), int(post["telegram_message_id"]))
            existing = dict(self.posts.get(key, {}))
            existing.update(post)
            self.posts[key] = existing
        return None

    def get_posts_by_message_ids(self, channel_uuid, telegram_message_ids):
        result = {}
        for message_id in telegram_message_ids:
            row = self.posts.get((str(channel_uuid), int(message_id)))
            if row:
                result[int(message_id)] = dict(row)
        return result

    def upsert_user(self, sender):
        telegram_user_id = sender.get("telegram_user_id")
        return f"user-{telegram_user_id}" if telegram_user_id is not None else None

    def upsert_comments(self, comments):
        for comment in comments:
            key = (str(comment["post_id"]), int(comment["telegram_message_id"]))
            existing = dict(self.comments.get(key, {}))
            existing.update(comment)
            self.comments[key] = existing
        return None

    def get_comment_thread_stats(self, post_ids):
        stats = {}
        requested = {str(post_id) for post_id in post_ids}
        for comment in self.comments.values():
            post_id = str(comment.get("post_id") or "")
            if post_id not in requested:
                continue
            bucket = stats.setdefault(
                post_id,
                {
                    "message_count": 0,
                    "root_count": 0,
                    "participant_ids": set(),
                    "last_activity_at": None,
                },
            )
            bucket["message_count"] += 1
            if comment.get("is_thread_root"):
                bucket["root_count"] += 1
            telegram_user_id = comment.get("telegram_user_id")
            if telegram_user_id is not None:
                bucket["participant_ids"].add(int(telegram_user_id))
            posted_at = comment.get("posted_at")
            if posted_at and (
                bucket["last_activity_at"] is None or str(posted_at) > str(bucket["last_activity_at"])
            ):
                bucket["last_activity_at"] = posted_at

        for post_id, bucket in stats.items():
            participants = bucket.pop("participant_ids", set())
            bucket["thread_participant_count"] = len(participants)
            bucket["comment_count"] = max(0, int(bucket["message_count"]) - int(bucket["root_count"]))
        return stats


class _FakeReplyTo:
    def __init__(self, reply_to_msg_id, reply_to_top_id=None):
        self.reply_to_msg_id = reply_to_msg_id
        self.reply_to_top_id = reply_to_top_id


class _FakeMessage:
    def __init__(self, *, message_id, text, posted_at, sender_id, reply_to=None):
        self.id = message_id
        self.text = text
        self.message = text
        self.date = posted_at
        self.sender_id = sender_id
        self.reply_to = reply_to
        self.photo = None
        self.video = None
        self.document = None
        self.audio = None
        self.reactions = None
        self.views = None
        self.forwards = None
        self.replies = None


class ScraperChannelGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_non_channel_source_is_auto_paused(self) -> None:
        writer = _FakeWriter()
        client = _FakeClient(entity=object())
        channel_record = {
            "id": "chan-1",
            "channel_username": "@tovgeneralbot",
            "scrape_depth_days": 30,
        }

        result = await channel_scraper.scrape_channel(client, channel_record, writer)

        self.assertEqual(result, {"posts_found": 0, "comments_found": 0, "source_type": "pending"})
        self.assertEqual(
            writer.updated,
            [
                (
                    "chan-1",
                    {
                        "is_active": False,
                        "source_type": "pending",
                        "resolution_status": "error",
                        "last_resolution_error": "resolved peer is object, not a Telegram channel/supergroup",
                    },
                )
            ],
        )
        self.assertEqual(writer.last_scraped, [])

    async def test_real_channel_scrape_continues_without_auto_pause(self) -> None:
        writer = _FakeWriter()
        client = _FakeClient(entity=_FakeChannel())
        channel_record = {
            "id": "chan-2",
            "channel_username": "@realchannel",
            "scrape_depth_days": 30,
            "telegram_channel_id": 123,
            "member_count": 10,
            "description": "desc",
            "source_type": "channel",
        }

        with patch.object(channel_scraper, "Channel", _FakeChannel):
            result = await channel_scraper.scrape_channel(client, channel_record, writer, entity=_FakeChannel())

        self.assertEqual(result, {"posts_found": 0, "comments_found": 0, "source_type": "channel"})
        self.assertEqual(writer.updated, [])
        self.assertEqual(writer.last_scraped, ["chan-2"])

    async def test_broadcast_channel_cap_stops_at_limit_and_persists_last_scraped_post_timestamp(self) -> None:
        writer = _FakeWriter()
        first = _FakeMessage(
            message_id=201,
            text="Newest post",
            posted_at=datetime(2026, 4, 12, 9, 5, tzinfo=timezone.utc),
            sender_id=1,
        )
        second = _FakeMessage(
            message_id=200,
            text="Older post",
            posted_at=datetime(2026, 4, 12, 9, 0, tzinfo=timezone.utc),
            sender_id=1,
        )
        client = _FakeClient(entity=_FakeChannel(), messages=[first, second])
        channel_record = {
            "id": "chan-cap",
            "channel_username": "@tinycap",
            "scrape_depth_days": 30,
            "source_type": "channel",
        }

        with patch.object(channel_scraper, "Message", _FakeMessage), \
             patch.object(channel_scraper.config, "SCRAPE_MAX_POSTS_PER_SOURCE_PER_CYCLE", 1):
            result = await channel_scraper.scrape_channel(client, channel_record, writer, entity=_FakeChannel())

        self.assertEqual(result, {"posts_found": 1, "comments_found": 0, "source_type": "channel"})
        self.assertEqual(len(writer.posts), 1)
        self.assertEqual(writer.last_scraped, [])
        self.assertEqual(
            writer.explicit_last_scraped,
            [("chan-cap", "2026-04-12T09:05:00+00:00")],
        )

    async def test_broadcast_channel_without_cap_preserves_current_unlimited_behavior(self) -> None:
        writer = _FakeWriter()
        first = _FakeMessage(
            message_id=301,
            text="Newest post",
            posted_at=datetime(2026, 4, 12, 10, 5, tzinfo=timezone.utc),
            sender_id=1,
        )
        second = _FakeMessage(
            message_id=300,
            text="Older post",
            posted_at=datetime(2026, 4, 12, 10, 0, tzinfo=timezone.utc),
            sender_id=1,
        )
        client = _FakeClient(entity=_FakeChannel(), messages=[first, second])
        channel_record = {
            "id": "chan-unlimited",
            "channel_username": "@nocap",
            "scrape_depth_days": 30,
            "source_type": "channel",
        }

        with patch.object(channel_scraper, "Message", _FakeMessage), \
             patch.object(channel_scraper.config, "SCRAPE_MAX_POSTS_PER_SOURCE_PER_CYCLE", 0):
            result = await channel_scraper.scrape_channel(client, channel_record, writer, entity=_FakeChannel())

        self.assertEqual(result, {"posts_found": 2, "comments_found": 0, "source_type": "channel"})
        self.assertEqual(len(writer.posts), 2)
        self.assertEqual(writer.last_scraped, ["chan-unlimited"])
        self.assertEqual(writer.explicit_last_scraped, [])

    async def test_supergroup_scrape_creates_thread_anchor_and_group_messages(self) -> None:
        writer = _FakeWriter()
        root = _FakeMessage(
            message_id=101,
            text="Root conversation prompt",
            posted_at=datetime(2026, 3, 27, 12, 0, tzinfo=timezone.utc),
            sender_id=11,
        )
        reply = _FakeMessage(
            message_id=102,
            text="A follow-up reply from another member",
            posted_at=datetime(2026, 3, 27, 12, 5, tzinfo=timezone.utc),
            sender_id=22,
            reply_to=_FakeReplyTo(reply_to_msg_id=101),
        )
        client = _FakeClient(entity=_FakeChannel(), messages=[reply, root])
        channel_record = {
            "id": "chan-3",
            "channel_username": "@rusarmenia",
            "scrape_depth_days": 30,
            "source_type": "supergroup",
        }

        async def _fake_sender_info(_client, message):
            return {
                "telegram_user_id": int(message.sender_id),
                "username": f"user{message.sender_id}",
                "first_name": f"User{message.sender_id}",
                "last_name": None,
                "bio": None,
                "is_bot": False,
            }

        with patch.object(channel_scraper, "Message", _FakeMessage), patch.object(
            channel_scraper,
            "get_sender_info",
            side_effect=_fake_sender_info,
        ):
            result = await channel_scraper.scrape_channel(client, channel_record, writer, entity=_FakeChannel())

        self.assertEqual(result, {"posts_found": 1, "comments_found": 2, "source_type": "supergroup"})
        self.assertEqual(writer.last_scraped, ["chan-3"])


    async def test_peer_ref_mode_queues_resolution_when_peer_ref_missing(self) -> None:
        writer = _FakeWriter()
        client = _FakeClient(entity=_FakeChannel())
        diagnostics = {}
        channel_record = {
            "id": "chan-4",
            "channel_username": "@peerrefmissing",
            "channel_title": "Peer Ref Missing",
            "is_active": True,
        }

        with patch.object(channel_scraper.config, "FEATURE_SOURCE_PEER_REF_LOOKUP", True), patch.object(
            channel_scraper.config, "FEATURE_SOURCE_RESOLUTION_QUEUE", True
        ):
            refreshed, entity = await channel_scraper.prepare_source_for_scrape(
                client,
                channel_record,
                writer,
                diagnostics=diagnostics,
            )

        self.assertIsNone(entity)
        self.assertEqual(client.entity_requests, [])
        self.assertEqual(refreshed["id"], "chan-4")
        self.assertEqual(len(writer.queued_resolution_jobs), 1)
        self.assertEqual(writer.queued_resolution_jobs[0]["channel_id"], "chan-4")
        self.assertEqual(diagnostics["pending_resolution_channels"], 1)
        self.assertNotIn("username_fallback_channels", diagnostics)

    async def test_peer_ref_mode_uses_cached_peer_ref_without_username_lookup(self) -> None:
        writer = _FakeWriter()
        diagnostics = {}
        writer.peer_refs[("chan-5", "primary")] = {
            "channel_id": "chan-5",
            "session_slot": "primary",
            "peer_id": 123,
            "access_hash": 456,
            "resolved_username": "@peerrefhit",
        }
        client = _FakeClient(entity=_FakeChannel())
        channel_record = {
            "id": "chan-5",
            "channel_username": "@peerrefhit",
            "channel_title": "Peer Ref Hit",
            "is_active": True,
        }

        async def _fake_resolve_source_metadata(_client, *, username=None, entity=None):
            return (
                {
                    "channel_username": username,
                    "channel_title": "Peer Ref Hit",
                    "source_type": "channel",
                    "resolution_status": "resolved",
                    "last_resolution_error": None,
                },
                entity,
            )

        with patch.object(channel_scraper.config, "FEATURE_SOURCE_PEER_REF_LOOKUP", True), patch.object(
            channel_scraper.config, "FEATURE_SOURCE_RESOLUTION_QUEUE", True
        ), patch.object(channel_scraper, "resolve_source_metadata", side_effect=_fake_resolve_source_metadata), patch.object(
            channel_scraper, "Channel", _FakeChannel
        ):
            refreshed, entity = await channel_scraper.prepare_source_for_scrape(
                client,
                channel_record,
                writer,
                diagnostics=diagnostics,
            )

        self.assertIsNotNone(entity)
        self.assertEqual(refreshed["source_type"], "channel")
        self.assertEqual(len(client.entity_requests), 1)
        self.assertNotEqual(client.entity_requests[0], "@peerrefhit")
        self.assertEqual(diagnostics["peer_ref_channels"], 1)
        self.assertNotIn("username_fallback_channels", diagnostics)

    async def test_prepare_source_tracks_resolve_flood_wait_diagnostics(self) -> None:
        writer = _FakeWriter()
        diagnostics = {}
        channel_record = {
            "id": "chan-7",
            "channel_username": "@floodwait",
            "channel_title": "Flood Wait",
            "is_active": True,
        }

        class _FakeFloodWaitError(Exception):
            pass

        class _FloodWaitClient:
            async def get_entity(self, lookup):
                del lookup
                raise _FakeFloodWaitError("wait")

        with patch.object(channel_scraper.config, "FEATURE_SOURCE_PEER_REF_LOOKUP", False), patch.object(
            channel_scraper, "FloodWaitError", _FakeFloodWaitError
        ):
            with self.assertRaises(_FakeFloodWaitError):
                await channel_scraper.prepare_source_for_scrape(
                    _FloodWaitClient(),
                    channel_record,
                    writer,
                    diagnostics=diagnostics,
                )

        self.assertEqual(diagnostics["username_fallback_channels"], 1)
        self.assertEqual(diagnostics["resolve_flood_wait_count"], 1)

    async def test_peer_ref_mode_requeues_when_cached_peer_ref_is_stale(self) -> None:
        writer = _FakeWriter()
        writer.peer_refs[("chan-6", "primary")] = {
            "channel_id": "chan-6",
            "session_slot": "primary",
            "peer_id": 123,
            "access_hash": 456,
            "resolved_username": "@stalepeer",
        }
        channel_record = {
            "id": "chan-6",
            "channel_username": "@stalepeer",
            "channel_title": "Stale Peer",
            "is_active": True,
        }

        class _StalePeerClient:
            def __init__(self):
                self.entity_requests = []

            async def get_entity(self, lookup):
                self.entity_requests.append(lookup)
                raise ValueError("stale peer ref")

        client = _StalePeerClient()

        with patch.object(channel_scraper.config, "FEATURE_SOURCE_PEER_REF_LOOKUP", True), patch.object(
            channel_scraper.config, "FEATURE_SOURCE_RESOLUTION_QUEUE", True
        ):
            refreshed, entity = await channel_scraper.prepare_source_for_scrape(client, channel_record, writer)

        self.assertIsNone(entity)
        self.assertEqual(len(client.entity_requests), 1)
        self.assertEqual(refreshed["resolution_status"], "pending")
        self.assertEqual(refreshed["last_resolution_error"], "stale peer ref")
        self.assertEqual(len(writer.queued_resolution_jobs), 1)
        self.assertEqual(writer.queued_resolution_jobs[0]["channel_id"], "chan-6")


class ScrapeOrchestratorCapTests(unittest.TestCase):
    def test_comment_post_cap_is_read_from_config(self) -> None:
        writer = SimpleNamespace(
            get_active_channels=lambda: [
                {
                    "id": "chan-1",
                    "channel_username": "@comments",
                    "source_type": "channel",
                    "scrape_comments": True,
                }
            ],
        )
        observed_limits: list[int] = []

        def _get_posts_with_comments_pending_for_channel(channel_uuid: str, limit: int = 50):
            observed_limits.append(limit)
            self.assertEqual(channel_uuid, "chan-1")
            return [{"id": "post-1", "channel_id": "chan-1", "telegram_message_id": 123}]

        writer.get_posts_with_comments_pending_for_channel = _get_posts_with_comments_pending_for_channel

        async def scenario() -> None:
            with patch.object(scrape_orchestrator, "prepare_source_for_scrape", AsyncMock(return_value=(
                {
                    "id": "chan-1",
                    "channel_username": "@comments",
                    "source_type": "channel",
                    "scrape_comments": True,
                },
                object(),
            ))), \
                 patch.object(scrape_orchestrator, "scrape_channel", AsyncMock(return_value={"posts_found": 0, "comments_found": 0})), \
                 patch.object(scrape_orchestrator, "scrape_comments_for_post", AsyncMock(return_value=1)), \
                 patch.object(scrape_orchestrator.config, "SCRAPE_MAX_COMMENT_POSTS_PER_SOURCE_PER_CYCLE", 3), \
                 patch.object(scrape_orchestrator.asyncio, "sleep", AsyncMock()):
                result = await scrape_orchestrator.run_scrape_cycle(object(), writer)
            self.assertEqual(result["comments_found"], 1)

        asyncio.run(scenario())
        self.assertEqual(observed_limits, [3])


if __name__ == "__main__":
    unittest.main()
