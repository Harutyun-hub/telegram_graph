from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from urllib.request import urlopen

from supabase import create_client


RUNTIME_BUCKET = "runtime-config"
SCHEDULER_PATH = "scraper/scheduler_settings.json"


def _env(name: str) -> str:
    return str(os.getenv(name, "") or "").strip()


def _require_env(name: str) -> str:
    value = _env(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def main() -> int:
    try:
        url = _require_env("PROD_SUPABASE_URL")
        key = _require_env("PROD_SUPABASE_SERVICE_ROLE_KEY")
        client = create_client(url, key)

        raw = client.storage.from_(RUNTIME_BUCKET).download(SCHEDULER_PATH)
        current = json.loads(raw.decode("utf-8")) if raw else {}
        updated = {
            "is_active": True,
            "interval_minutes": int(current.get("interval_minutes", 15) or 15),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        storage = client.storage.from_(RUNTIME_BUCKET)
        payload = json.dumps(updated, ensure_ascii=True).encode("utf-8")
        if current:
            storage.update(
                SCHEDULER_PATH,
                payload,
                {"content-type": "application/json"},
            )
        else:
            storage.upload(
                SCHEDULER_PATH,
                payload,
                {"content-type": "application/json", "upsert": "true"},
            )
        signed = storage.create_signed_url(SCHEDULER_PATH, 60)
        signed_url = signed.get("signedURL") or signed.get("signed_url") or signed.get("signedUrl")
        verified_body = ""
        if signed_url:
            with urlopen(signed_url) as response:
                verified_body = response.read().decode("utf-8")
        print("Previous scheduler settings:")
        print(json.dumps(current, ensure_ascii=True))
        print("Updated scheduler settings:")
        print(json.dumps(updated, ensure_ascii=True))
        if verified_body:
            print("Verified scheduler settings:")
            print(verified_body)
        return 0
    except Exception as exc:
        print(f"reset_scraper_scheduler_state.py failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
