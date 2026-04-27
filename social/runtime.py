from __future__ import annotations

import asyncio
import os
import socket
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger

import config
from api.runtime_coordinator import get_runtime_coordinator
from social.analysis import SocialActivityAnalyzer
from social.graph import SocialGraphWriter
from social.postgres_store import SocialPostgresStore
from social.scrapecreators import ScrapeCreatorsClient, SocialCollectionError
from social.store import SocialStore


def _iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


_non_issue_cleanup_lock = threading.Lock()
_non_issue_cleanup_completed = False


def _is_social_worker_owner() -> bool:
    return str(os.getenv("APP_ROLE") or "").strip().lower() == "social-worker"


def _ensure_non_issue_topics_hidden(
    writer_factory: Callable[[], SocialGraphWriter],
) -> int:
    global _non_issue_cleanup_completed

    if not _is_social_worker_owner():
        return 0
    if _non_issue_cleanup_completed:
        return 0

    with _non_issue_cleanup_lock:
        if _non_issue_cleanup_completed:
            return 0
        try:
            updated = int(writer_factory().mark_non_issue_topics_proposed() or 0)
        except Exception as exc:
            logger.warning("Social non-issue topic cleanup skipped: {}", exc)
            return 0
        _non_issue_cleanup_completed = True
        return updated


class SocialRuntimeService:
    def __init__(self, store: SocialStore) -> None:
        self.store = store
        self.pg_store = SocialPostgresStore()
        self.scheduler = AsyncIOScheduler(timezone="UTC")
        self.job_id = "social_runtime_job"
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
        self._background_tasks: set[asyncio.Task] = set()
        self._connector: ScrapeCreatorsClient | None = None
        self._analyzer: SocialActivityAnalyzer | None = None
        self._graph: SocialGraphWriter | None = None
        self._worker_id = f"{socket.gethostname()}:{os.getpid()}:social-runtime"

    async def startup(self) -> None:
        self._ensure_scheduler_started()
        hidden_topics = 0
        if _is_social_worker_owner():
            loop = asyncio.get_running_loop()
            hidden_topics = await loop.run_in_executor(None, _ensure_non_issue_topics_hidden, self._get_graph)
        settings = self.store.get_runtime_setting(
            "scheduler",
            {"is_active": False, "interval_minutes": 360},
        )
        self.interval_minutes = max(1, int(settings.get("interval_minutes", 360) or 360))
        self.desired_active = bool(settings.get("is_active", False)) and bool(config.SOCIAL_RUNTIME_ENABLED)
        if self.desired_active:
            self._upsert_interval_job()
        logger.info(
            "Social runtime ready | active={} interval={}m postgres_worker={} hidden_non_issue_topics={}",
            self.desired_active,
            self.interval_minutes,
            self.pg_store.enabled,
            hidden_topics,
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

    def _remove_interval_job(self) -> None:
        try:
            self.scheduler.remove_job(self.job_id)
        except Exception:
            pass

    def _next_run_iso(self) -> str | None:
        if not self.desired_active:
            return None
        job = self.scheduler.get_job(self.job_id)
        return _iso(job.next_run_time) if job else None

    def _get_connector(self) -> ScrapeCreatorsClient:
        if self._connector is None:
            self._connector = ScrapeCreatorsClient()
        return self._connector

    def _get_analyzer(self) -> SocialActivityAnalyzer:
        if self._analyzer is None:
            self._analyzer = SocialActivityAnalyzer()
        return self._analyzer

    def _get_graph(self) -> SocialGraphWriter:
        if self._graph is None:
            self._graph = SocialGraphWriter()
        return self._graph

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

    async def start(self) -> dict[str, Any]:
        if not config.SOCIAL_RUNTIME_ENABLED:
            raise RuntimeError("Social runtime is disabled")
        self._ensure_scheduler_started()
        self.desired_active = True
        self.store.save_runtime_setting(
            "scheduler",
            {
                "is_active": True,
                "interval_minutes": self.interval_minutes,
            },
        )
        self._upsert_interval_job()
        return self.status()

    async def stop(self) -> dict[str, Any]:
        self._ensure_scheduler_started()
        self.desired_active = False
        self.store.save_runtime_setting(
            "scheduler",
            {
                "is_active": False,
                "interval_minutes": self.interval_minutes,
            },
        )
        self._remove_interval_job()
        return self.status()

    async def set_interval(self, interval_minutes: int) -> dict[str, Any]:
        self._ensure_scheduler_started()
        self.interval_minutes = max(1, int(interval_minutes or 1))
        self.store.save_runtime_setting(
            "scheduler",
            {
                "is_active": self.desired_active,
                "interval_minutes": self.interval_minutes,
            },
        )
        if self.desired_active:
            self._upsert_interval_job()
        return self.status()

    async def retry_failure(self, *, stage: str, scope_key: str) -> dict[str, Any]:
        if self._run_lock.locked():
            raise RuntimeError("Social runtime is already running")
        async with self._run_lock:
            return await asyncio.to_thread(self._retry_failure_sync, stage, scope_key)

    async def replay_activities(self, *, stage: str, activity_uids: list[str]) -> dict[str, Any]:
        if self._run_lock.locked():
            raise RuntimeError("Social runtime is already running")
        async with self._run_lock:
            return await asyncio.to_thread(self._replay_activities_sync, stage, activity_uids)

    async def _run_cycle(self) -> None:
        if self._run_lock.locked():
            logger.warning("Social runtime skipped because a previous run is still active")
            return
        async with self._run_lock:
            coordinator = get_runtime_coordinator()
            lock_token = coordinator.acquire_lock(
                "worker:social-runtime",
                ttl_seconds=max(300, self.interval_minutes * 60),
            )
            if not lock_token:
                logger.warning("Social runtime skipped because the coordinator lock is already held")
                return
            self.running_now = True
            self.last_error = None
            self.last_run_started_at = datetime.now(timezone.utc)
            self.last_run_finished_at = None
            run = self.store.create_ingest_run(run_kind="runtime", status="running", metrics={})

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
                coordinator.release_lock("worker:social-runtime", lock_token)
                self.last_run_finished_at = datetime.now(timezone.utc)
                self.running_now = False

    def _run_cycle_sync(self) -> dict[str, Any]:
        page_settings = self.store.get_runtime_setting(
            "scrapecreators",
            {
                "max_pages": config.SOCIAL_FETCH_MAX_PAGES,
                "page_size": config.SOCIAL_FETCH_PAGE_SIZE,
                "facebook_page_post_limit": config.SOCIAL_FACEBOOK_PAGE_POST_LIMIT,
                "facebook_page_comment_limit": config.SOCIAL_FACEBOOK_PAGE_COMMENT_LIMIT,
                "tiktok_enabled": bool(config.SOCIAL_TIKTOK_ENABLED),
            },
        )
        tiktok_enabled = bool(page_settings.get("tiktok_enabled", config.SOCIAL_TIKTOK_ENABLED))
        enabled_platforms = ["facebook", "instagram", "google"]
        if tiktok_enabled:
            enabled_platforms.append("tiktok")

        collect_result = self._run_collect_stage_sync(
            enabled_platforms=enabled_platforms,
            max_pages=max(1, int(page_settings.get("max_pages", config.SOCIAL_FETCH_MAX_PAGES))),
            page_size=max(1, int(page_settings.get("page_size", config.SOCIAL_FETCH_PAGE_SIZE))),
            facebook_page_post_limit=max(1, int(page_settings.get("facebook_page_post_limit", config.SOCIAL_FACEBOOK_PAGE_POST_LIMIT))),
            facebook_page_comment_limit=max(0, int(page_settings.get("facebook_page_comment_limit", config.SOCIAL_FACEBOOK_PAGE_COMMENT_LIMIT))),
            include_tiktok=tiktok_enabled,
        )
        analysis_result = self._run_analysis_stage_sync()
        graph_result = self._run_graph_stage_sync()
        cleanup_result = self._run_cleanup_stage_sync()

        return {
            "accounts_total": collect_result["accounts_total"],
            "accounts_processed": collect_result["accounts_processed"],
            "activities_collected": collect_result["activities_collected"],
            "activities_analyzed": analysis_result["activities_analyzed"],
            "activities_graph_synced": graph_result["activities_graph_synced"],
            "collect_failures": collect_result["collect_failures"],
            "analysis_failures": analysis_result["analysis_failures"],
            "graph_failures": graph_result["graph_failures"],
            "platform_counts": collect_result["platform_counts"],
            "cleanup": cleanup_result,
            "stages": {
                "collect": collect_result,
                "analysis": analysis_result,
                "graph": graph_result,
                "cleanup": cleanup_result,
            },
        }

    def _run_collect_stage_sync(
        self,
        *,
        enabled_platforms: list[str],
        max_pages: int,
        page_size: int,
        include_tiktok: bool,
        facebook_page_post_limit: int | None = None,
        facebook_page_comment_limit: int | None = None,
        accounts: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        connector = self._get_connector()
        claimed_accounts = accounts
        if claimed_accounts is None:
            if self.pg_store.enabled:
                claimed_accounts = self.pg_store.claim_collect_accounts(
                    worker_id=self._worker_id,
                    platforms=enabled_platforms,
                    limit=config.SOCIAL_STAGE_CLAIM_LIMIT,
                    lease_seconds=config.SOCIAL_STAGE_LEASE_SECONDS,
                )
            else:
                claimed_accounts = self.store.list_active_accounts(enabled_platforms)[: config.SOCIAL_STAGE_CLAIM_LIMIT]

        result: dict[str, Any] = {
            "accounts_total": len(claimed_accounts),
            "accounts_processed": 0,
            "activities_collected": 0,
            "collect_failures": 0,
            "platform_counts": {},
        }

        for account in claimed_accounts:
            run = self.store.create_ingest_run(
                run_kind="collect",
                entity_id=account.get("entity_id"),
                platform=account.get("platform"),
                status="running",
            )
            try:
                payloads = connector.collect_account(
                    account,
                    max_pages=max_pages,
                    page_size=page_size,
                    facebook_page_post_limit=facebook_page_post_limit,
                    facebook_page_comment_limit=facebook_page_comment_limit,
                    include_tiktok=include_tiktok,
                )
                normalized = connector.normalize_payloads(account, payloads)
                saved = self.store.upsert_activities(normalized)
                self.store.mark_account_collect_success(str(account["id"]))
                result["accounts_processed"] += 1
                result["activities_collected"] += len(saved)
                result["platform_counts"][account["platform"]] = result["platform_counts"].get(account["platform"], 0) + len(saved)
                self.store.finish_ingest_run(
                    run["id"],
                    status="succeeded",
                    metrics={"pages": len(payloads), "activities": len(saved)},
                )
            except Exception as exc:
                result["collect_failures"] += 1
                health_status = self._collect_health_status(exc)
                failure = self.store.record_failure(
                    stage="ingest",
                    scope_key=f"{account.get('entity_id')}:{account.get('platform')}:{account.get('source_kind')}",
                    error=str(exc),
                    entity_id=account.get("entity_id"),
                    platform=account.get("platform"),
                    metadata={
                        "account_id": account.get("id"),
                        "health_status": health_status,
                    },
                )
                if account.get("id"):
                    self.store.mark_account_collect_failure(
                        str(account["id"]),
                        health_status=health_status,
                        error=str(exc),
                    )
                self.store.finish_ingest_run(run["id"], status="failed", error=str(exc), metrics={})
                logger.warning(
                    "Social collect failure | account={} health={} dead_letter={}",
                    account.get("id"),
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

        child_items = [
            item for item in claimed_items
            if str(item.get("source_kind") or "").strip().lower() == "comment" or item.get("parent_activity_uid")
        ]
        if child_items:
            self.store.mark_analysis_not_needed([str(item["id"]) for item in child_items if item.get("id")])
        child_ids = {str(item.get("id")) for item in child_items if item.get("id")}
        claimed_items = [item for item in claimed_items if str(item.get("id")) not in child_ids]
        comments_by_parent = self.store.list_thread_comments(
            [str(item.get("activity_uid") or "") for item in claimed_items],
            limit_per_parent=config.SOCIAL_THREAD_COMMENT_LIMIT,
        )
        for item in claimed_items:
            item["thread_comments"] = comments_by_parent.get(str(item.get("activity_uid") or ""), [])

        result = {
            "activities_total": len(claimed_items),
            "activities_analyzed": 0,
            "analysis_failures": 0,
            "comments_marked_not_needed": len(child_items),
            "thread_comments_included": sum(len(item.get("thread_comments") or []) for item in claimed_items),
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
        cleanup = self.pg_store.cleanup(
            lease_seconds=config.SOCIAL_STAGE_LEASE_SECONDS,
            payload_retention_days=config.SOCIAL_PAYLOAD_RETENTION_DAYS,
        )
        return cleanup

    def _retry_failure_sync(self, stage: str, scope_key: str) -> dict[str, Any]:
        normalized_stage = str(stage or "").strip().lower()
        if normalized_stage not in {"ingest", "analysis", "graph"}:
            raise ValueError("Retry stage must be ingest, analysis, or graph")
        failure = self.store.get_failure(stage=normalized_stage, scope_key=scope_key)
        if not failure:
            raise ValueError("No active failure found for the requested scope")
        self.store.clear_failure(stage=normalized_stage, scope_key=scope_key)
        if normalized_stage == "ingest":
            account = self.store.get_account_by_scope_key(scope_key)
            if not account:
                raise ValueError("Social account not found for ingest retry")
            return {
                "stage": "ingest",
                "retry": self._run_collect_stage_sync(
                    enabled_platforms=[str(account["platform"])],
                    max_pages=config.SOCIAL_FETCH_MAX_PAGES,
                    page_size=config.SOCIAL_FETCH_PAGE_SIZE,
                    facebook_page_post_limit=config.SOCIAL_FACEBOOK_PAGE_POST_LIMIT,
                    facebook_page_comment_limit=config.SOCIAL_FACEBOOK_PAGE_COMMENT_LIMIT,
                    include_tiktok=bool(config.SOCIAL_TIKTOK_ENABLED),
                    accounts=[account],
                ),
            }
        activity = self.store.prepare_activity_replay([scope_key], stage=normalized_stage if normalized_stage == "graph" else "analysis")
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
        if isinstance(exc, SocialCollectionError):
            return exc.health_status
        return "network_error"

    def status(self) -> dict[str, Any]:
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
        }

    def _record_history(self, result: dict[str, Any]) -> None:
        self._run_history.append(
            {
                "finished_at": _iso(self.last_success_at),
                "accounts_processed": int(result.get("accounts_processed", 0) or 0),
                "activities_collected": int(result.get("activities_collected", 0) or 0),
                "activities_analyzed": int(result.get("activities_analyzed", 0) or 0),
                "activities_graph_synced": int(result.get("activities_graph_synced", 0) or 0),
                "collect_failures": int(result.get("collect_failures", 0) or 0),
                "analysis_failures": int(result.get("analysis_failures", 0) or 0),
                "graph_failures": int(result.get("graph_failures", 0) or 0),
            }
        )
