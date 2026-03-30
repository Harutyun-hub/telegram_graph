from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from api import server, worker


class RuntimeHardeningTests(unittest.TestCase):
    def test_staging_forces_web_only_defaults(self) -> None:
        with patch.object(server.config, "IS_STAGING", True):
            role, warmers = server._apply_testing_release_invariants("all", True)

        self.assertEqual(role, "web")
        self.assertFalse(warmers)

    def test_worker_is_blocked_in_staging_testing_environment(self) -> None:
        with patch.object(worker.config, "IS_STAGING", True):
            with self.assertRaises(RuntimeError) as ctx:
                asyncio.run(worker.run_worker())

        self.assertIn("web-only", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
