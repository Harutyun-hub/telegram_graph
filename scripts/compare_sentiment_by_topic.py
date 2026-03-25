from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.dashboard_dates import build_dashboard_date_context
from api.queries import comparative


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare legacy vs graph-native topic sentiment results.")
    parser.add_argument("--from-date", required=True, dest="from_date", help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--to-date", required=True, dest="to_date", help="End date in YYYY-MM-DD format.")
    args = parser.parse_args()

    ctx = build_dashboard_date_context(args.from_date, args.to_date)
    result = comparative.compare_sentiment_by_topic(ctx)
    print(json.dumps(result, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
