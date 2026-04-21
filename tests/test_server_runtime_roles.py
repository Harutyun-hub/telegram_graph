from __future__ import annotations

import unittest

from api import server


class ServerRuntimeRoleTests(unittest.TestCase):
    def test_normalize_app_role_defaults_to_all(self) -> None:
        self.assertEqual(server._normalize_app_role(None), "all")
        self.assertEqual(server._normalize_app_role(""), "all")
        self.assertEqual(server._normalize_app_role("mystery"), "all")
        self.assertEqual(server._normalize_app_role("social-worker"), "social-worker")

    def test_should_run_background_jobs_for_all_and_worker_only(self) -> None:
        self.assertTrue(server._should_run_background_jobs("all"))
        self.assertTrue(server._should_run_background_jobs("worker"))
        self.assertFalse(server._should_run_background_jobs("web"))
        self.assertFalse(server._should_run_background_jobs("social-worker"))

    def test_should_run_social_background_jobs_for_all_and_social_worker_only(self) -> None:
        self.assertTrue(server._should_run_social_background_jobs("all"))
        self.assertTrue(server._should_run_social_background_jobs("social-worker"))
        self.assertFalse(server._should_run_social_background_jobs("worker"))
        self.assertFalse(server._should_run_social_background_jobs("web"))


if __name__ == "__main__":
    unittest.main()
