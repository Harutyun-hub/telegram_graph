from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from social.runtime import SocialRuntimeService


class _StoreStub:
    def __init__(self) -> None:
        self.settings = {
            "scheduler": {"is_active": False, "interval_minutes": 360},
        }
        self.pending_analysis = []
        self.thread_comments = {}
        self.not_needed_ids = []
        self.saved_analysis = []

    def get_runtime_setting(self, key: str, default: dict) -> dict:
        return dict(self.settings.get(key, default))

    def save_runtime_setting(self, key: str, value: dict) -> dict:
        self.settings[key] = dict(value)
        return dict(value)

    def create_ingest_run(self, **kwargs):
        return {"id": "run-1", **kwargs}

    def finish_ingest_run(self, *args, **kwargs):
        return None

    def record_failure(self, **kwargs):
        return {"is_dead_letter": False, **kwargs}

    def mark_activity_failure(self, **kwargs):
        return None

    def mark_graph_synced(self, **kwargs):
        return None

    def save_analysis(self, **kwargs):
        self.saved_analysis.append(kwargs)
        return kwargs

    def mark_analysis_not_needed(self, activity_ids):
        self.not_needed_ids.extend(activity_ids)
        return None

    def clear_failure(self, **kwargs):
        return None

    def get_failure(self, **kwargs):
        return None

    def prepare_activity_replay(self, *args, **kwargs):
        return []

    def get_account_by_scope_key(self, scope_key: str):
        return {"id": "account-1", "platform": "facebook", "entity_id": "entity-1", "source_key": scope_key}

    def mark_account_collect_success(self, account_id: str):
        return account_id

    def mark_account_collect_failure(self, account_id: str, **kwargs):
        return {"id": account_id, **kwargs}

    def list_active_accounts(self, enabled_platforms):
        return []

    def list_pending_analysis(self, limit: int):
        return list(self.pending_analysis)[:limit]

    def list_thread_comments(self, parent_activity_uids, *, limit_per_parent: int):
        return {
            uid: list(self.thread_comments.get(uid, []))[:limit_per_parent]
            for uid in parent_activity_uids
        }

    def list_pending_graph(self, limit: int):
        return []

    def upsert_activities(self, items):
        return items


class _ConnectorStub:
    def __init__(self) -> None:
        self.collect_kwargs = None

    def collect_account(self, account, **kwargs):
        self.collect_kwargs = kwargs
        return [{"items": []}]

    def normalize_payloads(self, account, payloads):
        return [{"id": "activity-1", "platform": account["platform"]}]


class SocialRuntimeTests(unittest.TestCase):
    def test_set_interval_persists_scheduler_state(self) -> None:
        async def scenario() -> None:
            service = SocialRuntimeService(_StoreStub())
            status = await service.set_interval(420)
            self.assertEqual(status["interval_minutes"], 420)
            self.assertEqual(service.store.get_runtime_setting("scheduler", {})["interval_minutes"], 420)

        asyncio.run(scenario())

    def test_run_once_schedules_background_cycle_when_idle(self) -> None:
        async def scenario() -> None:
            service = SocialRuntimeService(_StoreStub())
            run_cycle_mock = AsyncMock()

            with patch.object(service, "_run_cycle", run_cycle_mock):
                status = await service.run_once()
                await asyncio.sleep(0)

            self.assertFalse(status["running_now"])
            run_cycle_mock.assert_awaited_once_with()

        asyncio.run(scenario())

    def test_collect_stage_passes_facebook_page_limits_to_connector(self) -> None:
        store = _StoreStub()
        connector = _ConnectorStub()
        service = SocialRuntimeService(store)
        service.pg_store = type("_PgDisabled", (), {"enabled": False})()

        with patch.object(service, "_get_connector", return_value=connector):
            result = service._run_collect_stage_sync(
                enabled_platforms=["facebook"],
                max_pages=1,
                page_size=50,
                include_tiktok=False,
                facebook_page_post_limit=1,
                facebook_page_comment_limit=20,
                accounts=[
                    {
                        "id": "account-1",
                        "entity_id": "entity-1",
                        "platform": "facebook",
                        "source_kind": "facebook_page",
                    }
                ],
            )

        self.assertEqual(result["accounts_processed"], 1)
        self.assertEqual(connector.collect_kwargs["facebook_page_post_limit"], 1)
        self.assertEqual(connector.collect_kwargs["facebook_page_comment_limit"], 20)

    def test_analysis_stage_analyzes_parent_threads_and_marks_comments_not_needed(self) -> None:
        store = _StoreStub()
        store.pending_analysis = [
            {
                "id": "post-row",
                "entity_id": "entity-1",
                "platform": "facebook",
                "source_kind": "post",
                "activity_uid": "facebook:post:1",
                "entity": {"id": "entity-1", "name": "Nikol Pashinyan"},
            },
            {
                "id": "comment-row",
                "entity_id": "entity-1",
                "platform": "facebook",
                "source_kind": "comment",
                "parent_activity_uid": "facebook:post:1",
                "activity_uid": "facebook:comment:1",
            },
        ]
        store.thread_comments = {
            "facebook:post:1": [
                {"activity_uid": "facebook:comment:1", "text_content": "Comment text"},
            ]
        }

        class _Analyzer:
            def __init__(self) -> None:
                self.items = []

            def analyze_batch(self, items):
                self.items.append(items)
                return [
                    {
                        "activity_id": items[0]["id"],
                        "entity_id": items[0]["entity_id"],
                        "platform": items[0]["platform"],
                        "activity_uid": items[0]["activity_uid"],
                        "analysis_payload": {"summary": "ok", "topics": ["Tax Policy"]},
                        "raw_model_output": {"activity_uid": items[0]["activity_uid"]},
                        "model": "test-model",
                        "prompt_version": "social-thread-v1",
                        "analysis_version": "social-thread-v1",
                    }
                ]

        analyzer = _Analyzer()
        service = SocialRuntimeService(store)
        service.pg_store = type("_PgDisabled", (), {"enabled": False})()

        with patch.object(service, "_get_analyzer", return_value=analyzer):
            result = service._run_analysis_stage_sync()

        self.assertEqual(store.not_needed_ids, ["comment-row"])
        self.assertEqual(result["activities_analyzed"], 1)
        self.assertEqual(result["comments_marked_not_needed"], 1)
        self.assertEqual(result["thread_comments_included"], 1)
        self.assertEqual(analyzer.items[0][0]["thread_comments"][0]["activity_uid"], "facebook:comment:1")


if __name__ == "__main__":
    unittest.main()
