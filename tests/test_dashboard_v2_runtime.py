from __future__ import annotations

import unittest

from api.dashboard_v2_runtime import normalize_dashboard_v2_job_owner, should_run_dashboard_v2_jobs


class DashboardV2RuntimeTests(unittest.TestCase):
    def test_normalize_dashboard_v2_job_owner_defaults_to_worker(self) -> None:
        self.assertEqual(normalize_dashboard_v2_job_owner(""), "worker")
        self.assertEqual(normalize_dashboard_v2_job_owner("invalid"), "worker")
        self.assertEqual(normalize_dashboard_v2_job_owner("web"), "web")

    def test_worker_owner_allows_worker_and_all_roles(self) -> None:
        self.assertTrue(should_run_dashboard_v2_jobs(app_role="worker", job_owner="worker"))
        self.assertTrue(should_run_dashboard_v2_jobs(app_role="all", job_owner="worker"))
        self.assertFalse(should_run_dashboard_v2_jobs(app_role="web", job_owner="worker"))

    def test_web_owner_allows_only_web_role(self) -> None:
        self.assertTrue(should_run_dashboard_v2_jobs(app_role="web", job_owner="web"))
        self.assertFalse(should_run_dashboard_v2_jobs(app_role="worker", job_owner="web"))


if __name__ == "__main__":
    unittest.main()
