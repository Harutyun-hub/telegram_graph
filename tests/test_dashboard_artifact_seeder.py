from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from api import server


class _FakeCoordinator:
    def __init__(self, token: str | None = "seed-lock") -> None:
        self.token = token
        self.acquired: list[tuple[str, int]] = []
        self.released: list[tuple[str, str | None]] = []

    def acquire_lock(self, name: str, ttl_seconds: int) -> str | None:
        self.acquired.append((name, ttl_seconds))
        return self.token

    def release_lock(self, name: str, token: str | None) -> None:
        self.released.append((name, token))


class CanonicalDefaultArtifactSeederTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._startup_handlers = list(server.app.router.on_startup)
        cls._shutdown_handlers = list(server.app.router.on_shutdown)
        server.app.router.on_startup = []
        server.app.router.on_shutdown = []
        cls.client = TestClient(server.app)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()
        server.app.router.on_startup = cls._startup_handlers
        server.app.router.on_shutdown = cls._shutdown_handlers

    def _ctx(self) -> SimpleNamespace:
        end_date = date(2026, 4, 15)
        start_date = date.fromordinal(end_date.toordinal() - 14)
        return SimpleNamespace(
            from_date=start_date,
            to_date=end_date,
            days=15,
            is_operational=False,
            range_label=f"{start_date.isoformat()}..{end_date.isoformat()}",
            cache_key=f"{start_date.isoformat()}:{end_date.isoformat()}",
        )

    def _meta(self) -> dict:
        return {
            "cacheStatus": "refresh_success",
            "degradedTiers": [],
            "suppressedDegradedTiers": [],
            "tierTimes": {"pulse": 1.0},
            "snapshotBuiltAt": "2026-04-15T00:00:00+00:00",
            "isStale": False,
            "buildElapsedSeconds": 12.5,
            "buildMode": "parallel",
            "refreshFailureCount": 0,
        }

    def _strategic_degraded_meta(self) -> dict:
        meta = self._meta()
        meta["degradedTiers"] = ["strategic"]
        meta["tierTimes"] = {"pulse": 1.0, "strategic": None}
        return meta

    def _snapshot(self) -> dict:
        return {"communityHealth": {"score": 71}}

    def test_seed_canonical_default_artifact_skips_when_exact_artifact_is_already_fresh(self) -> None:
        ctx = self._ctx()
        loaded = {
            "status": "hit",
            "snapshot": self._snapshot(),
            "meta": self._meta(),
            "ctx": ctx,
            "cacheKey": ctx.cache_key,
            "snapshotBuiltAt": datetime(2026, 4, 15, tzinfo=timezone.utc),
            "trustedEndDate": "2026-04-15",
            "readMs": 4.0,
        }

        with patch.object(server.config, "DASH_DEFAULT_ARTIFACT_SEEDER_ENABLED", True), \
             patch.object(server, "_dashboard_freshness_snapshot", return_value={"generated_at": "2026-04-15T00:00:00+00:00"}), \
             patch.object(server, "_default_dashboard_context", return_value=ctx), \
             patch.object(server, "_load_persisted_dashboard_snapshot", return_value=loaded), \
             patch.object(server, "_is_persisted_snapshot_fresh", return_value=True), \
             patch.object(server, "seed_dashboard_snapshot") as seed_mock:
            result = server._seed_canonical_default_artifact_sync(force=False, reason="scheduled")

        self.assertEqual(result["status"], "already_fresh")
        self.assertFalse(result["started"])
        self.assertEqual(result["cacheKey"], ctx.cache_key)
        seed_mock.assert_not_called()

    def test_seed_canonical_default_artifact_builds_and_persists_under_runtime_lock(self) -> None:
        ctx = self._ctx()
        coordinator = _FakeCoordinator()

        with patch.object(server.config, "DASH_DEFAULT_ARTIFACT_SEEDER_ENABLED", True), \
             patch.object(server.config, "DASH_DEFAULT_ARTIFACT_SEED_TIMEOUT_SECONDS", 90.0), \
             patch.object(server.config, "DASH_DEFAULT_ARTIFACT_REFRESH_MINUTES", 30), \
             patch.object(server, "_dashboard_freshness_snapshot", return_value={"generated_at": "2026-04-15T00:00:00+00:00"}), \
             patch.object(server, "_default_dashboard_context", return_value=ctx), \
             patch.object(server, "_load_persisted_dashboard_snapshot", side_effect=[{"status": "miss", "readMs": 1.0}, {"status": "miss", "readMs": 1.1}]), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "get_runtime_coordinator", return_value=coordinator), \
             patch.object(
                 server,
                 "seed_dashboard_snapshot",
                 return_value={
                     "started": True,
                     "snapshot": self._snapshot(),
                     "meta": self._meta(),
                     "failureCount": 0,
                 },
             ) as seed_mock, \
             patch.object(
                 server,
                 "_persist_dashboard_snapshot_sync",
                 return_value={"exactSaved": True, "aliasSaved": True},
             ) as persist_mock:
            result = server._seed_canonical_default_artifact_sync(force=False, reason="scheduled")

        self.assertEqual(result["status"], "persisted")
        self.assertTrue(result["started"])
        self.assertEqual(result["source"], "build")
        self.assertEqual(result["cacheKey"], ctx.cache_key)
        seed_mock.assert_called_once_with(ctx, timeout_seconds=90.0, force=False)
        persist_mock.assert_called_once_with(
            ctx,
            self._snapshot(),
            self._meta(),
            trusted_end_date=ctx.to_date.isoformat(),
            write_default_alias=True,
        )
        self.assertEqual(coordinator.acquired[0][0], server._canonical_default_seed_lock_name(ctx.cache_key))
        self.assertEqual(coordinator.released[0], (server._canonical_default_seed_lock_name(ctx.cache_key), "seed-lock"))

    def test_seed_canonical_default_artifact_persists_when_only_strategic_is_degraded(self) -> None:
        ctx = self._ctx()
        coordinator = _FakeCoordinator()
        strategic_degraded = self._strategic_degraded_meta()

        with patch.object(server.config, "DASH_DEFAULT_ARTIFACT_SEEDER_ENABLED", True), \
             patch.object(server.config, "DASH_DEFAULT_ARTIFACT_SEED_TIMEOUT_SECONDS", 90.0), \
             patch.object(server.config, "DASH_DEFAULT_ARTIFACT_REFRESH_MINUTES", 30), \
             patch.object(server, "_dashboard_freshness_snapshot", return_value={"generated_at": "2026-04-15T00:00:00+00:00"}), \
             patch.object(server, "_default_dashboard_context", return_value=ctx), \
             patch.object(server, "_load_persisted_dashboard_snapshot", side_effect=[{"status": "miss", "readMs": 1.0}, {"status": "miss", "readMs": 1.1}]), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "get_runtime_coordinator", return_value=coordinator), \
             patch.object(
                 server,
                 "seed_dashboard_snapshot",
                 return_value={
                     "started": True,
                     "snapshot": self._snapshot(),
                     "meta": strategic_degraded,
                     "failureCount": 0,
                 },
             ), \
             patch.object(
                 server,
                 "_persist_dashboard_snapshot_sync",
                 return_value={"exactSaved": True, "aliasSaved": True},
             ) as persist_mock:
            result = server._seed_canonical_default_artifact_sync(force=False, reason="scheduled")

        self.assertEqual(result["status"], "persisted")
        persist_mock.assert_called_once_with(
            ctx,
            self._snapshot(),
            strategic_degraded,
            trusted_end_date=ctx.to_date.isoformat(),
            write_default_alias=True,
        )

    def test_seed_canonical_default_artifact_returns_lock_held_when_runtime_lock_is_unavailable(self) -> None:
        ctx = self._ctx()
        coordinator = _FakeCoordinator(token=None)

        with patch.object(server.config, "DASH_DEFAULT_ARTIFACT_SEEDER_ENABLED", True), \
             patch.object(server, "_dashboard_freshness_snapshot", return_value={"generated_at": "2026-04-15T00:00:00+00:00"}), \
             patch.object(server, "_default_dashboard_context", return_value=ctx), \
             patch.object(server, "_load_persisted_dashboard_snapshot", return_value={"status": "miss", "readMs": 1.0}), \
             patch.object(server, "get_runtime_coordinator", return_value=coordinator), \
             patch.object(server, "seed_dashboard_snapshot") as seed_mock:
            result = server._seed_canonical_default_artifact_sync(force=False, reason="scheduled")

        self.assertEqual(result["status"], "lock_held")
        self.assertFalse(result["started"])
        seed_mock.assert_not_called()

    def test_seed_default_dashboard_artifact_endpoint_returns_success_payload(self) -> None:
        result = {
            "status": "persisted",
            "started": True,
            "cacheKey": "2026-04-01:2026-04-15",
            "persistResult": {"exactSaved": True, "aliasSaved": True},
        }

        with patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(
                 server,
                 "_seed_canonical_default_artifact_once",
                 AsyncMock(return_value=result),
             ) as seed_mock:
            response = self.client.post(
                "/api/dashboard/default-artifact/seed",
                headers={"Authorization": "Bearer admin-secret"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "persisted")
        seed_mock.assert_awaited_once_with(force=True, reason="operator_run_once")


if __name__ == "__main__":
    unittest.main()
