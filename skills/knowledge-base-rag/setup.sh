#!/bin/bash
set -e
SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pip install -r "$SKILL_DIR/requirements.txt" --quiet
echo "knowledge-base-rag skill dependencies installed."
