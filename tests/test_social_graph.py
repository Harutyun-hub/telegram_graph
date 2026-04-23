from __future__ import annotations

import unittest

from social import graph


class _FakeResult:
    def __init__(self, updated: int = 0) -> None:
        self.updated = updated

    def consume(self):
        return None

    def single(self):
        return {"updated": self.updated}


class _FakeSession:
    def __init__(self, *, updated: int = 0) -> None:
        self.updated = updated
        self.calls: list[tuple[str, dict]] = []

    def run(self, query: str, params: dict):
        self.calls.append((query, dict(params)))
        return _FakeResult(self.updated)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeDriver:
    def __init__(self, *, updated: int = 0) -> None:
        self.updated = updated
        self.sessions: list[_FakeSession] = []

    def session(self, *, database: str):
        del database
        session = _FakeSession(updated=self.updated)
        self.sessions.append(session)
        return session


class SocialGraphTests(unittest.TestCase):
    def test_topic_items_prefer_normalized_raw_model_output(self) -> None:
        analysis = {
            "analysis_payload": {"topics": ["Tax Policy"]},
            "raw_model_output": {"topics": ["Tax Policy", "Media And News"]},
        }

        items = graph._topic_items_from_analysis(analysis)

        self.assertEqual(
            items,
            [
                {"name": "Tax Policy", "proposed": False},
                {"name": "Media And News", "proposed": True},
            ],
        )

    def test_topic_items_fall_back_to_analysis_payload_topics(self) -> None:
        analysis = {
            "analysis_payload": {"topics": ["Community Solidarity", "Tax Policy"]},
            "raw_model_output": {},
        }

        items = graph._topic_items_from_analysis(analysis)

        self.assertEqual(
            items,
            [
                {"name": "Community Solidarity", "proposed": True},
                {"name": "Tax Policy", "proposed": False},
            ],
        )

    def test_sync_activity_updates_topic_proposed_on_match_and_create(self) -> None:
        writer = object.__new__(graph.SocialGraphWriter)
        writer.driver = _FakeDriver()
        activity = {
            "activity_uid": "social:1",
            "platform": "facebook",
            "source_kind": "post",
            "source_url": "https://facebook.com/post/1",
            "text_content": "hello",
            "published_at": "2026-04-23T12:00:00+00:00",
            "author_handle": "page",
            "entity": {"id": "entity-1", "name": "Entity", "industry": "Politics"},
            "analysis": {
                "analysis_payload": {"summary": "sum", "topics": ["Tax Policy"]},
                "raw_model_output": {"topics": ["Media And News", "Tax Policy"]},
            },
        }

        writer.sync_activity(activity)

        session = writer.driver.sessions[0]
        query, params = session.calls[0]
        self.assertIn("SET t.proposed = coalesce(t.proposed, false) OR coalesce(topic.proposed, false)", query)
        self.assertEqual(
            params["topics"],
            [
                {"name": "Media And News", "proposed": True},
                {"name": "Tax Policy", "proposed": False},
            ],
        )

    def test_mark_non_issue_topics_proposed_targets_exact_taxonomy_set(self) -> None:
        writer = object.__new__(graph.SocialGraphWriter)
        writer.driver = _FakeDriver(updated=5)

        updated = writer.mark_non_issue_topics_proposed()

        self.assertEqual(updated, 5)
        session = writer.driver.sessions[0]
        query, params = session.calls[0]
        self.assertIn("SET t.proposed = true", query)
        self.assertEqual(
            set(params["non_issue_topics"]),
            {
                "Community Solidarity",
                "Media And News",
                "Social Media Trend",
                "Telegram Community",
            },
        )


if __name__ == "__main__":
    unittest.main()
