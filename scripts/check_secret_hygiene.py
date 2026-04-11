from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_SUFFIXES = {".session"}
FORBIDDEN_EXACT_NAMES = {".env"}
OPENAI_KEY_RE = re.compile(r"\bsk-[A-Za-z0-9]{20,}\b")


def tracked_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
    )
    return [
        REPO_ROOT / raw.decode("utf-8")
        for raw in result.stdout.split(b"\x00")
        if raw
    ]


def is_binary(path: Path) -> bool:
    try:
        chunk = path.read_bytes()[:1024]
    except OSError:
        return True
    return b"\x00" in chunk


def main() -> int:
    failures: list[str] = []

    for path in tracked_files():
        relative = path.relative_to(REPO_ROOT)
        if path.name in FORBIDDEN_EXACT_NAMES:
            failures.append(f"Tracked environment file is forbidden: {relative}")
            continue
        if path.suffix in FORBIDDEN_SUFFIXES:
            failures.append(f"Tracked Telegram/session artifact is forbidden: {relative}")
            continue
        if is_binary(path):
            continue

        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue

        if OPENAI_KEY_RE.search(content):
            failures.append(f"Potential OpenAI secret detected in {relative}")

    if failures:
        for failure in failures:
            print(failure)
        return 1

    print("Secret hygiene checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
