from __future__ import annotations

import re
from pathlib import Path
import unittest


QUERY_ROOT = Path(__file__).resolve().parents[1] / "api" / "queries"
IMPORTED_SUBQUERY_PATTERN = re.compile(r"CALL \([A-Za-z_][A-Za-z0-9_]*\) \{")


class CypherSubqueryCompatTests(unittest.TestCase):
    def test_query_modules_do_not_use_imported_subquery_scope_syntax(self) -> None:
        offenders: list[str] = []
        for path in sorted(QUERY_ROOT.glob("*.py")):
            text = path.read_text()
            if IMPORTED_SUBQUERY_PATTERN.search(text):
                offenders.append(path.name)

        self.assertEqual(
            offenders,
            [],
            f"Imported CALL(var) subquery syntax is incompatible with the deployed Neo4j version: {offenders}",
        )


if __name__ == "__main__":
    unittest.main()
