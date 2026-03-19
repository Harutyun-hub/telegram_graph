from __future__ import annotations

import unittest
from unittest.mock import patch

from scraper import channel_scraper


class _FakeChannel:
    pass


class _FakeClient:
    def __init__(self, entity):
        self._entity = entity

    async def get_entity(self, username):
        return self._entity

    async def iter_messages(self, entity, wait_time=0, reverse=False):
        if False:
            yield None
        return


class _FakeWriter:
    def __init__(self):
        self.updated = []
        self.last_scraped = []

    def update_channel(self, channel_uuid, payload):
        self.updated.append((channel_uuid, payload))
        return {"id": channel_uuid, **payload}

    def update_channel_last_scraped(self, channel_uuid):
        self.last_scraped.append(channel_uuid)

    def upsert_posts(self, posts):
        return None


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

        self.assertEqual(result, 0)
        self.assertEqual(writer.updated, [("chan-1", {"is_active": False})])
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
        }

        with patch.object(channel_scraper, "Channel", _FakeChannel):
            result = await channel_scraper.scrape_channel(client, channel_record, writer)

        self.assertEqual(result, 0)
        self.assertEqual(writer.updated, [])
        self.assertEqual(writer.last_scraped, ["chan-2"])


if __name__ == "__main__":
    unittest.main()
