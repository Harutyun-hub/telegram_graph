from __future__ import annotations

import asyncio
import os
import socket
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger

import config
from api.runtime_coordinator import get_runtime_coordinator
from social.analysis import SocialActivityAnalyzer
from social.graph import SocialGraphWriter
from social.postgres_store import SocialPostgresStore
from social.providers import SocialProviderError, get_provider_adapter
from social.store import SocialStore


def _iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


class SocialRuntimeService:
    def __init__(self, store: SocialStore) -> None:
        self._ensure_event_loop()
        self.store = store
        self.pg_store = SocialPostgresStore()
        self.scheduler = AsyncIOScheduler(timezone="UTC")
        self.job_id = "social_runtime_job"
        self.control_job_id = "social_runtime_control_job"
        self.interval_minutes = 360
        self.desired_active = False
        self.running_now = False
        self.last_run_started_at: Optional[datetime] = None
        self.last_run_finished_at: Optional[datetime] = None
        self.last_success_at: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self.last_result: Optional[dict[str, Any]] = None
        self._run_history = deque(maxlen=12)
        self._run_lock = asyncio.Lock()
        self._control_run_lock = asyncio.Lock()
        self._background_tasks: set[asyncio.Task] = set()
        self._analyzer: SocialActivityAnalyzer | None = None
        self._graph: SocialGraphWriter | None = None
        self._worker_id = f"{socket.gethostname()}:{os.getpid()}:social-runtime"
        self._last_control_request_id: str | None = None

    @staticmethod
    def _ensure_event_loop() -> None:
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

    async def startup(self) -> None:
        self._ensure_scheduler_started()
        settings = self.store.get_runtime_setting(
            "scheduler",
            {"is_active": False, "interval_minutes": 360},
        )
        self.interval_minutes = max(15, int(settings.get("interval_minutes", 360) or 360))
        self.desired_active = bool(settings.get("is_active", False)) and bool(config.SOCIAL_RUNTIME_ENABLED)
        self._upsert_control_job()
        if self.desired_active:
            self._upsert_interval_job()
        self._persist_snapshot(persisted=settings)
        logger.info(
            "Social runtime ready | active={} interval={}m postgres_worker={}",
            self.desired_active,
            self.interval_minutes,
            self.pg_store.enabled,
        )

    async def shutdown(self) -> None:
        try:
            self.scheduler.shutdown(wait=False)
        except Exception:
            pass
        for task in list(self._background_tasks):
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()
        if self._graph is not None:
            try:
                self._graph.close()
            except Exception:
                pass
            self._graph = None
        self._persist_snapshot()

    def _ensure_scheduler_started(self) -> None:
        if not self.scheduler.running:
            self.scheduler.start()

    def _upsert_interval_job(self) -> None:
        try:
            self.scheduler.remove_job(self.job_id)
        except Exception:
            pass
        self.scheduler.add_job(
            self._run_cycle,
            "interval",
            minutes=self.interval_minutes,
            id=self.job_id,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=180,
        )

    def _upsert_control_job(self) -> None:
        try:
            self.scheduler.remove_job(self.control_job_id)
        except Exception:
            pass
        self.scheduler.add_job(
            self._run_control_cycle,
            "interval",
            seconds=max(2, int(config.SOCIAL_CONTROL_POLL_SECONDS)),
            id=self.control_job_id,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=10,
        )

    def _next_run_iso(self) -> str | None:
        if not self.desired_active:
            return None
        job = self.scheduler.get_job(self.job_id)
        return _iso(job.next_run_time) if job else None

    def _get_analyzer(self) -> SocialActivityAnalyzer:
        if self._analyzer is None:
            self._analyzer = SocialActivityAnalyzer()
        return self._analyzer

    def _get_graph(self) -> SocialGraphWriter:
        if self._graph is None:
            self._graph = SocialGraphWriter()
        return self._graph

    def _scheduler_settings_payload(self) -> dict[str, Any]:
        return {
            "is_active": bool(self.desired_active),
            "interval_minutes": int(self.interval_minutes),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def _persist_snapshot(self, *, persisted: dict[str, Any] | None = None) -> None:
        try:
            payload = self.status(persisted=persisted)
            self.store.save_runtime_snapshot(payload)
        except Exception as exc:
            logger.warning("Failed to persist shared social runtime snapshot: {}", exc)

    async def start(self) -> dict[str, Any]:
        self._ensure_scheduler_started()
        self.desired_active = True and bool(config.SOCIAL_RUNTIME_ENABLED)
        if self.desired_active:
            self._upsert_interval_job()
        persisted = self.store.save_runtime_setting("scheduler", self._scheduler_settings_payload())
        status = self.status(persisted=persisted)
        self._persist_snapshot(persisted=persisted)
        return status

    async def stop(self) -> dict[str, Any]:
        self.desired_active = False
        try:
            self.scheduler.remove_job(self.job_id)
        except Exception:
            pass
        persisted = self.store.save_runtime_setting("scheduler", self._scheduler_settings_payload())
        status = self.status(persisted=persisted)
        self._persist_snapshot(persisted=persisted)
        return status

    async def set_interval(self, interval_minutes: int) -> dict[str, Any]:
        self.interval_minutes = max(15, int(interval_minutes or 360))
        if self.desired_active:
            self._ensure_scheduler_started()
            self._upsert_interval_job()
        persisted = self.store.save_runtime_setting("scheduler", self._scheduler_settings_payload())
        status = self.status(persisted=persisted)
        self._persist_snapshot(persisted=persisted)
        return status

    async def run_once(self) -> dict[str, Any]:
        if self.running_now:
            return self.status()
        task = asyncio.create_task(self._run_cycle())
        self._background_tasks.add(task)

        def _cleanup(done_task: asyncio.Task) -> None:
            self._background_tasks.discard(done_task)
            try:
                exc = done_task.exception()
            except asyncio.CancelledError:
                return
            if exc:
                logger.error("Social runtime crashed: {}", exc)

        task.add_done_callback(_cleanup)
        return self.status()

    async def retry_failure(self, *, stage: str, scope_key: str) -> dict[str, Any]:
        if self._run_lock.locked():
            raise RuntimeError("Social runtime is already running")
        async with self._run_lock:
            result = await asyncio.to_thread(self._retry_failure_sync, stage, scope_key)
        self._persist_snapshot()
        return result

    async def replay_activities(self, *, stage: str, activity_uids: list[str]) -> dict[str, Any]:
        if self._run_lock.locked():
            raise RuntimeError("Social runtime is already running")
        async with self._run_lock:
            result = await asyncio.to_thread(self._replay_activities_sync, stage, activity_uids)
        self._persist_snapshot()
        return result

    async def _run_control_cycle(self) -> None:
        if self._control_run_lock.locked():
            return
        async with self._control_run_lock:
            try:
                command = self.store.get_runtime_control_command(default={}) or {}
                request_id = str(command.get("request_id") or "").strip()
                if not request_id or request_id == self._last_control_request_id:
                    return
                if str(command.get("status") or "").strip().lower() != "pending":
                    return

                action = str(command.get("action") or "").strip().lower()
                in_progress = {
                    **command,
                    "status": "processing",
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "processed_by_role": "worker",
                }
                self.store.save_runtime_control_command(in_progress)
                try:
                    if action == "start":
                        status = await self.start()
                    elif action == "stop":
                        status = await self.stop()
                    elif action == "set_interval":
                        status = await self.set_interval(int(command.get("interval_minutes") or self.interval_minutes))
                    elif action == "run_once":
                        await self._run_cycle(use_runtime_lock=False)
                        status = self.status()
                    elif action == "retry_failure":
                        status = await self.retry_failure(
                            stage=str(command.get("stage") or ""),
                            scope_key=str(command.get("scope_key") or ""),
                        )
                    elif action == "replay_activities":
                        status = await self.replay_activities(
                            stage=str(command.get("stage") or "analysis"),
                            activity_uids=list(command.get("activity_uids") or []),
                        )
                    else:
                        raise ValueError(f"Unsupported social runtime control action: {action}")

                    self.store.save_runtime_control_command(
                        {
                            **in_progress,
                            "status": "completed",
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                            "runtime_status": status,
                        }
                    )
                except Exception as exc:
                    self.store.save_runtime_control_command(
                        {
                            **in_progress,
                            "status": "failed",
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                            "error": str(exc),
                        }
                    )
                    raise
                finally:
                    self._last_control_request_id = request_id
            except Exception as exc:
                logger.warning("Social runtime control cycle failed: {}", exc)

    async def _run_cycle(self, *, use_runtime_lock: bool = True) -> None:
        if self._run_lock.locked():
            logger.warning("Social runtime skipped because a previous run is still active")
            return
        async with self._run_lock:
            coordinator = get_runtime_coordinator()
            lock_token = None
            if use_runtime_lock:
                lock_token = coordinator.acquire_lock(
                    "worker:social-runtime",
                    ttl_seconds=max(300, self.interval_minutes * 60),
                )
                if not lock_token:
                    logger.warning("Social runtime skipped because the coordinator lock is already held")
                    return
            else:
                logger.info("Running social runtime cycle without runtime coordinator lock")

            self.running_now = True
            self.last_error = None
            self.last_run_started_at = datetime.now(timezone.utc)
            self.last_run_finished_at = None
            self._persist_snapshot()
            run = self.store.create_ingest_run(
                run_kind="runtime",
                provider_key=None,
                status="running",
                metrics={},
            )

            try:
                result = await asyncio.to_thread(self._run_cycle_sync)
                self.last_result = result
                self.last_success_at = datetime.now(timezone.utc)
                self.store.finish_ingest_run(run["id"], status="succeeded", metrics=result)
                self._record_history(result)
                logger.info("Social runtime completed | {}", result)
            except Exception as exc:
                self.last_error = str(exc)
                self.store.finish_ingest_run(run["id"], status="failed", error=str(exc), metrics={})
                logger.error("Social runtime failed: {}", exc)
            finally:
                if lock_token:
                    coordinator.release_lock("worker:social-runtime", lock_token)
                self.last_run_finished_at = datetime.now(timezone.utc)
                self.running_now = False
                self._persist_snapshot()

    def _provider_settings(self, provider_key: str) -> dict[str, Any]:
        return self.store.get_runtime_setting(
            f"provider:{provider_key}",
            {
                "max_pages": config.SOCIAL_FETCH_MAX_PAGES,
                "page_size": config.SOCIAL_FETCH_PAGE_SIZE,
            },
        )

    def _run_cycle_sync(self) -> dict[str, Any]:
        collect_result = self._run_collect_stage_sync()
        analysis_result = self._run_analysis_stage_sync()
        graph_result = self._run_graph_stage_sync()
        cleanup_result = self._run_cleanup_stage_sync()

        return {
            "sources_total": collect_result["sources_total"],
            "sources_processed": collect_result["sources_processed"],
            "accounts_total": collect_result["sources_total"],
            "accounts_processed": collect_result["sources_processed"],
            "activities_collected": collect_result["activities_collected"],
            "activities_analyzed": analysis_result["activities_analyzed"],
            "activities_graph_synced": graph_result["activities_graph_synced"],
            "collect_failures": collect_result["collect_failures"],
            "analysis_failures": analysis_result["analysis_failures"],
            "graph_failures": graph_result["graph_failures"],
            "platform_counts": collect_result["platform_counts"],
            "provider_counts": collect_result["provider_counts"],
            "cleanup": cleanup_result,
            "stages": {
                "collect": collect_result,
                "analysis": analysis_result,
                "graph": graph_result,
                "cleanup": cleanup_result,
            },
        }

    def _next_collect_after(self, source: dict[str, Any]) -> str:
        cadence_minutes = int(source.get("cadence_minutes") or self.interval_minutes or 360)
        return (datetime.now(timezone.utc) + timedelta(minutes=max(15, cadence_minutes))).isoformat()

    def _run_collect_stage_sync(
        self,
        *,
        sources: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        claimed_sources = sources
        if claimed_sources is None:
            if self.pg_store.enabled:
                claimed_sources = self.pg_store.claim_collect_sources(
                    worker_id=self._worker_id,
                    platforms=None,
                    limit=config.SOCIAL_STAGE_CLAIM_LIMIT,
                    lease_seconds=config.SOCIAL_STAGE_LEASE_SECONDS,
                )
            else:
                claimed_sources = self.store.list_due_sources(limit=config.SOCIAL_STAGE_CLAIM_LIMIT)

        result: dict[str, Any] = {
            "sources_total": len(claimed_sources),
            "sources_processed": 0,
            "activities_collected": 0,
            "collect_failures": 0,
            "platform_counts": {},
            "provider_counts": {},
        }

        for source in claimed_sources:
            provider_key = str(source.get("provider_key") or "scrapecreators")
            provider_settings = self._provider_settings(provider_key)
            adapter = get_provider_adapter(provider_key)
            run = self.store.create_ingest_run(
                run_kind="collect",
                entity_id=source.get("entity_id"),
                account_id=source.get("id"),
                provider_key=provider_key,
                platform=source.get("platform"),
                status="running",
            )
            try:
                normalized = adapter.collect_source(
                    source,
                    max_pages=max(1, int(provider_settings.get("max_pages", config.SOCIAL_FETCH_MAX_PAGES))),
                    page_size=max(1, int(provider_settings.get("page_size", config.SOCIAL_FETCH_PAGE_SIZE))),
                )
                saved = self.store.upsert_activities(normalized)
                self.store.mark_source_collect_success(
                    str(source["id"]),
                    next_collect_after=self._next_collect_after(source),
                )
                result["sources_processed"] += 1
                result["activities_collected"] += len(saved)
                result["platform_counts"][source["platform"]] = result["platform_counts"].get(source["platform"], 0) + len(saved)
                result["provider_counts"][provider_key] = result["provider_counts"].get(provider_key, 0) + len(saved)
                self.store.finish_ingest_run(
                    run["id"],
                    status="succeeded",
                    metrics={"activities": len(saved), "source_key": source.get("source_key")},
                )
            except Exception as exc:
                result["collect_failures"] += 1
                health_status = self._collect_health_status(exc)
                failure = self.store.record_failure(
                    stage="ingest",
                    scope_key=str(source.get("source_key") or source.get("id")),
                    error=str(exc),
                    entity_id=source.get("entity_id"),
                    account_id=source.get("id"),
                    provider_key=provider_key,
                    platform=source.get("platform"),
                    metadata={
                        "source_id": source.get("id"),
                        "source_key": source.get("source_key"),
                        "health_status": health_status,
                    },
                )
                if source.get("id"):
                    self.store.mark_source_collect_failure(
                        str(source["id"]),
                        health_status=health_status,
                        error=str(exc),
                    )
                self.store.finish_ingest_run(run["id"], status="failed", error=str(exc), metrics={})
                logger.warning(
                    "Social collect failure | source={} provider={} health={} dead_letter={}",
                    source.get("id"),
                    provider_key,
                    health_status,
                    failure.get("is_dead_letter"),
                )

        return result

    def _run_analysis_stage_sync(self, *, items: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        analyzer = self._get_analyzer()
        claimed_items = items
        if claimed_items is None:
            if self.pg_store.enabled:
                claimed_items = self.pg_store.claim_analysis_activities(
                    worker_id=self._worker_id,
                    limit=config.SOCIAL_STAGE_CLAIM_LIMIT,
                    lease_seconds=config.SOCIAL_STAGE_LEASE_SECONDS,
                    analysis_version=config.SOCIAL_ANALYSIS_PROMPT_VERSION,
                )
            else:
                claimed_items = self.store.list_pending_analysis(limit=config.SOCIAL_STAGE_CLAIM_LIMIT)

        result = {
            "activities_total": len(claimed_items),
            "activities_analyzed": 0,
            "analysis_failures": 0,
        }
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for item in claimed_items:
            grouped[(str(item["entity_id"]), str(item["platform"]))].append(item)

        batch_size = max(1, int(config.SOCIAL_ANALYSIS_BATCH_SIZE))
        for batch_items in grouped.values():
            for offset in range(0, len(batch_items), batch_size):
                batch = batch_items[offset:offset + batch_size]
                try:
                    results = analyzer.analyze_batch(batch)
                    for row in results:
                        self.store.save_analysis(**row)
                        result["activities_analyzed"] += 1
                except Exception as batch_exc:
                    logger.warning("Social batch analysis fallback triggered: {}", batch_exc)
                    for item in batch:
                        try:
                            row = analyzer.analyze_one(item)
                            self.store.save_analysis(**row)
                            result["activities_analyzed"] += 1
                        except Exception as single_exc:
                            result["analysis_failures"] += 1
                            failure = self.store.record_failure(
                                stage="analysis",
                                scope_key=item["activity_uid"],
                                error=str(single_exc),
                                activity_id=item["id"],
                                entity_id=item["entity_id"],
                                account_id=item.get("account_id"),
                                provider_key=item.get("provider_key"),
                                platform=item["platform"],
                            )
                            self.store.mark_activity_failure(
                                activity_id=item["id"],
                                activity_uid=item["activity_uid"],
                                stage="analysis",
                                error=str(single_exc),
                                dead_letter=bool(failure.get("is_dead_letter")),
                            )
        return result

    def _run_graph_stage_sync(self, *, items: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        graph = self._get_graph()
        claimed_items = items
        if claimed_items is None:
            if self.pg_store.enabled:
                claimed_items = self.pg_store.claim_graph_activities(
                    worker_id=self._worker_id,
                    limit=config.SOCIAL_STAGE_CLAIM_LIMIT,
                    lease_seconds=config.SOCIAL_STAGE_LEASE_SECONDS,
                    projection_version=config.SOCIAL_GRAPH_PROJECTION_VERSION,
                )
            else:
                claimed_items = self.store.list_pending_graph(limit=config.SOCIAL_STAGE_CLAIM_LIMIT)

        result = {
            "activities_total": len(claimed_items),
            "activities_graph_synced": 0,
            "graph_failures": 0,
        }
        for item in claimed_items:
            run = self.store.create_ingest_run(
                run_kind="graph",
                entity_id=item.get("entity_id"),
                account_id=item.get("account_id"),
                provider_key=item.get("provider_key"),
                platform=item.get("platform"),
                status="running",
            )
            try:
                graph.sync_activity(item)
                self.store.mark_graph_synced(
                    activity_id=item["id"],
                    activity_uid=item["activity_uid"],
                    projection_version=config.SOCIAL_GRAPH_PROJECTION_VERSION,
                )
                self.store.finish_ingest_run(
                    run["id"],
                    status="succeeded",
                    metrics={"activity_uid": item["activity_uid"]},
                )
                result["activities_graph_synced"] += 1
            except Exception as exc:
                result["graph_failures"] += 1
                failure = self.store.record_failure(
                    stage="graph",
                    scope_key=item["activity_uid"],
                    error=str(exc),
                    activity_id=item["id"],
                    entity_id=item["entity_id"],
                    account_id=item.get("account_id"),
                    provider_key=item.get("provider_key"),
                    platform=item["platform"],
                )
                self.store.mark_activity_failure(
                    activity_id=item["id"],
                    activity_uid=item["activity_uid"],
                    stage="graph",
                    error=str(exc),
                    dead_letter=bool(failure.get("is_dead_letter")),
                )
                self.store.finish_ingest_run(run["id"], status="failed", error=str(exc), metrics={})
        return result

    def _run_cleanup_stage_sync(self) -> dict[str, Any]:
        return self.pg_store.cleanup(
            lease_seconds=config.SOCIAL_STAGE_LEASE_SECONDS,
            payload_retention_days=config.SOCIAL_PAYLOAD_RETENTION_DAYS,
        )

    def _retry_failure_sync(self, stage: str, scope_key: str) -> dict[str, Any]:
        normalized_stage = str(stage or "").strip().lower()
        if normalized_stage not in {"ingest", "analysis", "graph"}:
            raise ValueError("Retry stage must be ingest, analysis, or graph")
        failure = self.store.get_failure(stage=normalized_stage, scope_key=scope_key)
        if not failure:
            raise ValueError("No active failure found for the requested scope")
        self.store.clear_failure(stage=normalized_stage, scope_key=scope_key)
        if normalized_stage == "ingest":
            source = self.store.get_source_by_scope_key(scope_key)
            if not source:
                raise ValueError("Social source not found for ingest retry")
            return {
                "stage": "ingest",
                "retry": self._run_collect_stage_sync(sources=[source]),
            }
        activity = self.store.prepare_activity_replay(
            [scope_key],
            stage=normalized_stage if normalized_stage == "graph" else "analysis",
        )
        if not activity:
            raise ValueError("Social activity not found for retry")
        if normalized_stage == "analysis":
            return {"stage": "analysis", "retry": self._run_analysis_stage_sync(items=activity)}
        return {"stage": "graph", "retry": self._run_graph_stage_sync(items=activity)}

    def _replay_activities_sync(self, stage: str, activity_uids: list[str]) -> dict[str, Any]:
        if not activity_uids:
            raise ValueError("At least one activity UID is required")
        normalized_stage = str(stage or "").strip().lower() or "analysis"
        activities = self.store.prepare_activity_replay(activity_uids, stage=normalized_stage)
        if not activities:
            raise ValueError("No matching social activities found")
        if normalized_stage == "analysis":
            return {"stage": "analysis", "replay": self._run_analysis_stage_sync(items=activities)}
        if normalized_stage == "graph":
            return {"stage": "graph", "replay": self._run_graph_stage_sync(items=activities)}
        raise ValueError("Replay stage must be analysis or graph")

    @staticmethod
    def _collect_health_status(exc: Exception) -> str:
        if isinstance(exc, SocialProviderError):
            return exc.health_status
        return "network_error"

    def status(self, persisted: dict[str, Any] | None = None) -> dict[str, Any]:
        return {
            "status": "active" if self.desired_active else "stopped",
            "is_active": self.desired_active,
            "interval_minutes": self.interval_minutes,
            "running_now": self.running_now,
            "last_run_started_at": _iso(self.last_run_started_at),
            "last_run_finished_at": _iso(self.last_run_finished_at),
            "last_success_at": _iso(self.last_success_at),
            "next_run_at": self._next_run_iso(),
            "last_error": self.last_error,
            "last_result": self.last_result,
            "run_history": list(self._run_history),
            "runtime_enabled": bool(config.SOCIAL_RUNTIME_ENABLED),
            "tiktok_enabled": bool(config.SOCIAL_TIKTOK_ENABLED),
            "postgres_worker_enabled": bool(self.pg_store.enabled),
            "worker_id": self._worker_id,
            "control_poll_seconds": int(config.SOCIAL_CONTROL_POLL_SECONDS),
            "persisted": persisted,
        }

    def _record_history(self, result: dict[str, Any]) -> None:
        self._run_history.append(
            {
                "finished_at": _iso(self.last_success_at),
                "sources_processed": int(result.get("sources_processed", 0) or 0),
                "accounts_processed": int(result.get("accounts_processed", 0) or 0),
                "activities_collected": int(result.get("activities_collected", 0) or 0),
                "activities_analyzed": int(result.get("activities_analyzed", 0) or 0),
                "activities_graph_synced": int(result.get("activities_graph_synced", 0) or 0),
                "collect_failures": int(result.get("collect_failures", 0) or 0),
                "analysis_failures": int(result.get("analysis_failures", 0) or 0),
                "graph_failures": int(result.get("graph_failures", 0) or 0),
            }
        )
