from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from scraper import channel_scraper


class _FakeChannel:
    pass


class _FakeClient:
    def __init__(self, entity, messages=None):
        self._entity = entity
        self._messages = list(messages or [])

    async def get_entity(self, username):
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
        self.rows = {}
        self.posts = {}
        self.comments = {}

    def update_channel(self, channel_uuid, payload):
        existing = self.rows.get(channel_uuid, {"id": channel_uuid})
        existing.update(payload)
        self.rows[channel_uuid] = existing
        self.updated.append((channel_uuid, payload))
        return dict(existing)

    def get_channel_by_id(self, channel_uuid):
        return self.rows.get(channel_uuid, {"id": channel_uuid})

    def update_channel_last_scraped(self, channel_uuid):
        self.last_scraped.append(channel_uuid)

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

        anchor = writer.posts[("chan-3", 101)]
        self.assertEqual(anchor["entry_kind"], "thread_anchor")
        self.assertEqual(anchor["thread_message_count"], 2)
        self.assertEqual(anchor["thread_participant_count"], 2)
        self.assertEqual(anchor["comment_count"], 1)
        self.assertTrue(anchor["has_comments"])

        comments = sorted(writer.comments.values(), key=lambda item: item["telegram_message_id"])
        self.assertEqual(len(comments), 2)
        self.assertEqual(comments[0]["message_kind"], "group_message")
        self.assertTrue(comments[0]["is_thread_root"])
        self.assertEqual(comments[0]["thread_top_message_id"], 101)
        self.assertFalse(comments[1]["is_thread_root"])
        self.assertEqual(comments[1]["reply_to_message_id"], 101)


if __name__ == "__main__":
    unittest.main()
