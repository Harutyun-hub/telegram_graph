from __future__ import annotations

import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

from api import freshness
from api import server
from buffer.supabase_writer import SupabaseWriter


class _FakeRuntimeBucket:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.download_overrides: dict[str, bytes | Exception] = {}
        self.fail_duplicate_upload = False
        self.signed_url_calls = 0

    def download(self, path: str) -> bytes:
        override = self.download_overrides.get(path)
        if isinstance(override, Exception):
            raise override
        if isinstance(override, bytes):
            return override
        if path not in self.files:
            raise RuntimeError("Object not found")
        return self.files[path]

    def update(self, path: str, body: bytes, _options: dict) -> None:
        self.files[path] = body

    def create_signed_url(self, _path: str, _expires_in: int) -> dict:
        self.signed_url_calls += 1
        raise RuntimeError("signed-url-should-not-be-used")

    def upload(self, path: str, body: bytes, options: dict) -> None:
        upsert = str(options.get("upsert", "")).strip().lower() in {"1", "true", "yes", "on"}
        if self.fail_duplicate_upload and path in self.files and not upsert:
            raise RuntimeError("Duplicate")
        self.files[path] = body

    def remove(self, paths: list[str]) -> list[dict]:
        removed: list[dict] = []
        for path in paths:
            if path in self.files:
                self.files.pop(path, None)
                removed.append({"name": path})
        return removed


class _FakeStorageClient:
    def __init__(self, bucket: _FakeRuntimeBucket) -> None:
        self._bucket = bucket
        self._buckets = [{"name": "runtime-config"}]

    def list_buckets(self) -> list[dict]:
        return list(self._buckets)

    def create_bucket(self, *_args, **_kwargs) -> None:
        self._buckets.append({"name": "runtime-config"})

    def from_(self, _name: str) -> _FakeRuntimeBucket:
        return self._bucket


class _FakeClient:
    def __init__(self, bucket: _FakeRuntimeBucket) -> None:
        self.storage = _FakeStorageClient(bucket)


def _make_writer(bucket: _FakeRuntimeBucket) -> SupabaseWriter:
    writer = object.__new__(SupabaseWriter)
    writer.client = _FakeClient(bucket)
    writer._runtime_bucket_name = "runtime-config"
    writer._scheduler_settings_path = "scraper/scheduler_settings.json"
    writer._scheduler_runtime_path = "scraper/scheduler_runtime.json"
    writer._scheduler_control_path = "scraper/scheduler_control.json"
    writer._freshness_snapshot_path = "pipeline/freshness_snapshot.json"
    writer._failure_table_warning_emitted = False
    writer._topic_review_warning_emitted = False
    writer._topic_promotion_warning_emitted = False
    return writer


class RuntimePersistenceTests(unittest.TestCase):
    def test_persist_freshness_snapshot_uses_valid_log_level(self) -> None:
        snapshot = {"generated_at": "2026-03-28T07:54:00+00:00", "health": {"status": "healthy"}}

        with patch.object(server, "get_supabase_writer") as writer_mock, \
             patch.object(server.logger, "log") as log_mock:
            writer_mock.return_value.save_runtime_blob.return_value = True

            result = server._persist_freshness_snapshot_sync(snapshot)

        self.assertTrue(result["ok"])
        self.assertEqual(log_mock.call_args[0][0], "INFO")

    def test_freshness_endpoint_uses_passive_shared_snapshot_on_web_role(self) -> None:
        snapshot = {"generated_at": "2026-03-30T07:00:00+00:00", "health": {"status": "healthy"}}

        with patch.object(server, "APP_ROLE", "web"), \
             patch.object(server, "get_supabase_writer", return_value=SimpleNamespace()), \
             patch.object(server, "get_current_scraper_scheduler_status", return_value={}), \
             patch.object(server, "get_passive_freshness_snapshot", return_value=snapshot) as passive_mock, \
             patch.object(server, "get_freshness_snapshot") as live_mock:
            payload = asyncio.run(server.freshness_snapshot(force=False))

        self.assertEqual(payload, snapshot)
        passive_mock.assert_called_once()
        live_mock.assert_not_called()

    def test_persist_dashboard_snapshot_schedules_default_topics_warm(self) -> None:
        snapshot = {"communityHealth": {"score": 72}}
        meta = {"snapshotBuiltAt": "2026-03-22T00:00:00+00:00", "cacheStatus": "refresh_success", "degradedTiers": []}
        ctx = server.build_dashboard_date_context("2026-03-08", "2026-03-22")

        with patch.object(server, "get_supabase_writer") as writer_mock, \
             patch.object(server, "prime_dashboard_snapshot") as prime_mock, \
             patch.object(server, "_schedule_default_topics_prewarm", return_value=True) as warm_mock:
            writer_mock.return_value.save_runtime_blob.return_value = True
            result = server._persist_dashboard_snapshot_sync(
                ctx,
                snapshot,
                meta,
                trusted_end_date="2026-03-22",
                write_default_alias=True,
            )

        self.assertTrue(result["ok"])
        prime_mock.assert_called_once()
        warm_mock.assert_called_once_with(ctx)

    def test_get_runtime_json_reads_via_authenticated_download(self) -> None:
        bucket = _FakeRuntimeBucket()
        bucket.files["admin/config.json"] = json.dumps({"widgets": {"w1": {"enabled": False}}}).encode("utf-8")
        writer = _make_writer(bucket)

        payload = writer.get_runtime_json("admin/config.json", default={"widgets": {}})

        self.assertEqual(payload["widgets"]["w1"]["enabled"], False)

    def test_save_runtime_json_fails_when_readback_mismatches(self) -> None:
        bucket = _FakeRuntimeBucket()
        bucket.download_overrides["admin/config.json"] = json.dumps({"widgets": {"w1": {"enabled": True}}}).encode("utf-8")
        writer = _make_writer(bucket)

        saved = writer.save_runtime_json("admin/config.json", {"widgets": {"w1": {"enabled": False}}})

        self.assertFalse(saved)

    def test_save_runtime_json_overwrites_duplicate_with_upsert(self) -> None:
        bucket = _FakeRuntimeBucket()
        bucket.fail_duplicate_upload = True
        bucket.files["admin/config.json"] = json.dumps({"widgets": {"w1": {"enabled": True}}}).encode("utf-8")
        writer = _make_writer(bucket)

        saved = writer.save_runtime_json("admin/config.json", {"widgets": {"w1": {"enabled": False}}})
        stored = writer.get_runtime_json("admin/config.json", default={})

        self.assertTrue(saved)
        self.assertEqual(stored["widgets"]["w1"]["enabled"], False)

    def test_save_runtime_blob_persists_binary_payload(self) -> None:
        bucket = _FakeRuntimeBucket()
        writer = _make_writer(bucket)

        saved = writer.save_runtime_blob("dashboard-cache/v1/test.json.gz", b"compressed-payload")

        self.assertTrue(saved)
        self.assertEqual(bucket.files["dashboard-cache/v1/test.json.gz"], b"compressed-payload")

    def test_save_runtime_blob_overwrites_duplicate_with_upsert(self) -> None:
        bucket = _FakeRuntimeBucket()
        bucket.fail_duplicate_upload = True
        bucket.files["dashboard-cache/v1/test.json.gz"] = b"old"
        writer = _make_writer(bucket)

        saved = writer.save_runtime_blob("dashboard-cache/v1/test.json.gz", b"new-payload")

        self.assertTrue(saved)
        self.assertEqual(bucket.files["dashboard-cache/v1/test.json.gz"], b"new-payload")

    def test_read_runtime_blob_returns_timeout_status(self) -> None:
        bucket = _FakeRuntimeBucket()
        writer = _make_writer(bucket)

        with patch.object(writer, "_read_runtime_blob_bytes", side_effect=TimeoutError("timed out")):
            result = writer.read_runtime_blob("dashboard-cache/v1/test.json.gz", timeout_seconds=0.5)

        self.assertEqual(result["status"], "timeout")
        self.assertEqual(result["body"], b"")

    def test_read_runtime_blob_prefers_authenticated_download(self) -> None:
        bucket = _FakeRuntimeBucket()
        bucket.files["dashboard-cache/v1/test.json.gz"] = b"cached"
        writer = _make_writer(bucket)

        result = writer.read_runtime_blob("dashboard-cache/v1/test.json.gz", timeout_seconds=0.5)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["body"], b"cached")
        self.assertEqual(bucket.signed_url_calls, 0)

    def test_update_admin_config_returns_500_when_persistence_cannot_be_verified(self) -> None:
        payload = server.AdminConfigPatchRequest(runtime={"openaiModel": "gpt-4o-mini"})

        with patch.object(server, "_load_admin_config", return_value=server._default_admin_config()), \
             patch.object(server, "save_admin_config_raw", return_value=False), \
             patch.object(server, "get_admin_config_runtime_warning", return_value="Admin config save failed because storage could not round-trip the new config."):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(server.update_admin_config(payload))

        self.assertEqual(ctx.exception.status_code, 500)
        self.assertIn("could not round-trip", str(ctx.exception.detail))

    def test_freshness_prefers_shared_snapshot_when_requested(self) -> None:
        writer = SimpleNamespace(
            get_shared_freshness_snapshot=lambda default=None, timeout_seconds=1.5: {
                "generated_at": "2026-04-11T14:15:15.876280+00:00",
                "pipeline": {"scrape": {"last_scrape_at": "2026-04-11T14:08:06.134560+00:00"}},
            },
        )
        old_cache = freshness._CACHE
        old_cache_ts = freshness._CACHE_TS
        try:
            freshness._CACHE = {"generated_at": "2026-04-10T13:13:18.510055+00:00"}
            freshness._CACHE_TS = None

            snapshot = freshness.get_freshness_snapshot(
                writer,
                scheduler_status={},
                force_refresh=False,
                prefer_shared_snapshot=True,
            )

            self.assertEqual(snapshot["generated_at"], "2026-04-11T14:15:15.876280+00:00")
        finally:
            freshness._CACHE = old_cache
            freshness._CACHE_TS = old_cache_ts

    def test_shared_freshness_snapshot_fast_read_skips_signed_url(self) -> None:
        bucket = _FakeRuntimeBucket()
        bucket.files["pipeline/freshness_snapshot.json"] = json.dumps(
            {"generated_at": "2026-04-11T10:00:00+00:00", "health": {"status": "healthy"}}
        ).encode("utf-8")
        writer = _make_writer(bucket)

        payload = writer.get_shared_freshness_snapshot(default={})

        self.assertEqual(payload["generated_at"], "2026-04-11T10:00:00+00:00")
        self.assertEqual(bucket.signed_url_calls, 0)

    def test_passive_freshness_rebuilds_when_shared_snapshot_is_stale(self) -> None:
        writer = SimpleNamespace(
            get_shared_freshness_snapshot=lambda default=None, timeout_seconds=1.5: {
                "generated_at": "2026-04-10T13:13:18.510055+00:00",
                "pipeline": {"scrape": {"last_scrape_at": "2026-04-08T19:13:16.187192+00:00"}},
            },
            get_pipeline_freshness_snapshot=lambda: {
                "active_channels": 157,
                "active_channels_never_scraped": 0,
                "last_scrape_at": "2026-04-11T18:40:00+00:00",
                "last_post_at": "2026-04-11T18:40:00+00:00",
                "last_process_at": "2026-04-11T18:55:00+00:00",
                "last_graph_sync_at": "2026-04-11T19:00:00+00:00",
                "unprocessed_posts": 12,
                "unprocessed_comments": 34,
                "unsynced_posts": 5,
                "unsynced_analysis": 8,
                "dead_letter_scopes": 0,
                "retry_blocked_scopes": 0,
                "transient_dead_letter_scopes": 0,
                "permanent_dead_letter_scopes": 0,
                "recent_transient_failures": 0,
                "recent_permanent_failures": 0,
                "runnable_posts": 12,
                "runnable_comment_groups": 20,
                "blocked_dead_letter_posts": 0,
                "blocked_dead_letter_comment_groups": 0,
                "blocked_retry_posts": 0,
                "blocked_retry_comment_groups": 0,
            },
            get_recent_pipeline_snapshot=lambda: {
                "window_days": 15,
                "window_start_at": "2026-03-27T00:00:00+00:00",
                "recent_posts": 222,
                "recent_comments": 333,
                "recent_unsynced_posts": 5,
                "recent_last_post_at": "2026-04-11T18:40:00+00:00",
                "recent_last_graph_sync_post_at": "2026-04-11T19:00:00+00:00",
            },
        )

        old_cache = freshness._CACHE
        old_cache_ts = freshness._CACHE_TS
        try:
            freshness._CACHE = None
            freshness._CACHE_TS = None
            snapshot = freshness.get_passive_freshness_snapshot(
                writer,
                scheduler_status={
                    "is_active": True,
                    "interval_minutes": 60,
                    "running_now": False,
                    "last_run_finished_at": "2026-04-11T19:05:15.841400+00:00",
                    "last_success_at": "2026-04-11T19:05:15.838513+00:00",
                    "next_run_at": "2026-04-11T20:05:15.838513+00:00",
                    "last_error": None,
                    "run_history": [],
                },
            )
        finally:
            freshness._CACHE = old_cache
            freshness._CACHE_TS = old_cache_ts

        self.assertEqual(snapshot["pipeline"]["scrape"]["last_scrape_at"], "2026-04-11T18:40:00+00:00")
        self.assertEqual(snapshot["pipeline"]["sync"]["last_graph_sync_at"], "2026-04-11T19:00:00+00:00")
        self.assertEqual(snapshot["pulse"]["queue"]["graph_posts"], 5)

    def test_shared_scheduler_control_fast_write_skips_readback_verification(self) -> None:
        bucket = _FakeRuntimeBucket()
        bucket.download_overrides["scraper/scheduler_control.json"] = RuntimeError("readback would fail")
        writer = _make_writer(bucket)

        saved = writer.save_shared_scraper_control_command({"request_id": "cmd-1", "action": "run_once"})

        self.assertTrue(saved)
        stored = json.loads(bucket.files["scraper/scheduler_control.json"].decode("utf-8"))
        self.assertEqual(stored["action"], "run_once")

    def test_shared_scheduler_control_fast_write_replaces_duplicate_object(self) -> None:
        bucket = _FakeRuntimeBucket()
        bucket.files["scraper/scheduler_control.json"] = json.dumps(
            {"request_id": "old", "action": "run_once"}
        ).encode("utf-8")
        writer = _make_writer(bucket)

        upload_calls = {"count": 0}
        original_upload = bucket.upload

        def flaky_upload(path: str, body: bytes, options: dict) -> None:
            if path == "scraper/scheduler_control.json" and upload_calls["count"] == 0:
                upload_calls["count"] += 1
                raise RuntimeError("Duplicate")
            upload_calls["count"] += 1
            original_upload(path, body, options)

        bucket.upload = flaky_upload  # type: ignore[assignment]

        saved = writer.save_shared_scraper_control_command({"request_id": "new", "action": "catchup_once"})

        self.assertTrue(saved)
        stored = json.loads(bucket.files["scraper/scheduler_control.json"].decode("utf-8"))
        self.assertEqual(stored["request_id"], "new")
        self.assertEqual(stored["action"], "catchup_once")


if __name__ == "__main__":
    unittest.main()
