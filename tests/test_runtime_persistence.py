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

    def upload(self, path: str, body: bytes, _options: dict) -> None:
        if self.fail_duplicate_upload and path in self.files:
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
    writer._freshness_snapshot_path = "pipeline/freshness_snapshot.json"
    writer._failure_table_warning_emitted = False
    writer._topic_review_warning_emitted = False
    writer._topic_promotion_warning_emitted = False
    return writer


class RuntimePersistenceTests(unittest.TestCase):
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

    def test_save_runtime_json_overwrites_duplicate_by_replacing_object(self) -> None:
        bucket = _FakeRuntimeBucket()
        bucket.fail_duplicate_upload = True
        bucket.files["admin/config.json"] = json.dumps({"widgets": {"w1": {"enabled": True}}}).encode("utf-8")
        writer = _make_writer(bucket)

        saved = writer.save_runtime_json("admin/config.json", {"widgets": {"w1": {"enabled": False}}})
        stored = writer.get_runtime_json("admin/config.json", default={})

        self.assertTrue(saved)
        self.assertEqual(stored["widgets"]["w1"]["enabled"], False)

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
            get_shared_freshness_snapshot=lambda default=None: {
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


if __name__ == "__main__":
    unittest.main()
