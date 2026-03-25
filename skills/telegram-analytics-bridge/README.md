# telegram-analytics-bridge

Production-ready OpenClaw skill for querying the Telegram analytics backend through existing API endpoints.

This skill is intended to stay simple:

- existing backend integration only
- no new backend endpoints
- no env var renames
- compact JSON output for agent consumption
- reliable enough for Railway cold starts and first-hit latency

## Runtime assumptions

- OpenClaw runs in a container that has `python3`
- The running container has `pydantic` installed
- The `telegram-analytics-bridge` files are present inside the container filesystem
- `ANALYTICS_API_BASE_URL` points to the direct backend root URL
- `ANALYTICS_API_KEY` is the dedicated OpenClaw/server-to-server analytics token
- The frontend proxy URL is not used here unless it is intentionally serving the backend API directly

## Folder tree

```text
skills/telegram-analytics-bridge/
├── SKILL.md
├── README.md
├── scripts/
│   ├── __init__.py
│   ├── actions.py
│   ├── bridge.py
│   ├── client.py
│   ├── formatters.py
│   ├── models.py
│   └── windows.py
└── tests/
    └── ...
```

## Required environment variables

- `ANALYTICS_API_BASE_URL`
  - Direct backend base URL
  - Example: `https://telegramgraph-production.up.railway.app`
  - Do not append `/api/dashboard`
- `ANALYTICS_API_KEY`
  - Dedicated backend token for OpenClaw
  - Do not use the frontend proxy token

## Dependency requirements

- Python `3.10+` recommended
- `pydantic` must be installed in the runtime used by OpenClaw

Example setup:

```bash
cd /Users/harutnahapetyan/Documents/Gemini/Telegram
python3 -m venv .venv-skill
source .venv-skill/bin/activate
pip install pydantic
export ANALYTICS_API_BASE_URL="https://telegramgraph-production.up.railway.app"
export ANALYTICS_API_KEY="replace_with_openclaw_analytics_token"
```

## Default reliability settings

The CLI defaults are tuned for production-style backend latency:

- `timeout=35.0`
- `max_retries=2`
- `backoff_base=0.5`

This keeps exponential backoff while tolerating Railway cold starts better.

## Local usage

Pretty JSON:

```bash
python3 skills/telegram-analytics-bridge/scripts/bridge.py get_top_topics --window 7d --limit 5 --json
```

Insight synthesis:

```bash
python3 skills/telegram-analytics-bridge/scripts/bridge.py ask_insights --window 7d --question "What is driving concern about residency permits?" --json
```

## Manual container testing

Enter the OpenClaw container:

```bash
docker exec -it openclaw-k5ni-openclaw-1 sh
```

If the container has `bash`, this is also fine:

```bash
docker exec -it openclaw-k5ni-openclaw-1 bash
```

Run a direct skill command inside the container:

```bash
python3 skills/telegram-analytics-bridge/scripts/bridge.py get_top_topics --window 7d --limit 5 --json
```

Run an insight command:

```bash
python3 skills/telegram-analytics-bridge/scripts/bridge.py ask_insights --window 7d --question "What is driving concern about residency permits?" --json
```

## Confirm backend connectivity

Health check:

```bash
curl -i "$ANALYTICS_API_BASE_URL/api/health"
```

Authenticated dashboard check:

```bash
curl -i \
  -H "Authorization: Bearer $ANALYTICS_API_KEY" \
  "$ANALYTICS_API_BASE_URL/api/dashboard?from=2026-03-10&to=2026-03-24"
```

If the health endpoint works but the dashboard call is slow, the backend may be cold-starting.

## Actions

### `get_top_topics(window, limit)`

Skill command:

```bash
python3 skills/telegram-analytics-bridge/scripts/bridge.py get_top_topics --window 7d --limit 5 --json
```

Backend curl:

```bash
curl -sS \
  -H "Authorization: Bearer ${ANALYTICS_API_KEY}" \
  "${ANALYTICS_API_BASE_URL}/api/dashboard?from=$(date -u -v-7d +%F 2>/dev/null || python3 - <<'PY'
from datetime import datetime, timedelta, timezone
print((datetime.now(timezone.utc) - timedelta(days=7)).date().isoformat())
PY
)&to=$(date -u +%F)"
```

### `get_declining_topics(window, limit)`

Skill command:

```bash
python3 skills/telegram-analytics-bridge/scripts/bridge.py get_declining_topics --window 30d --limit 5 --json
```

Backend curl:

```bash
curl -sS \
  -H "Authorization: Bearer ${ANALYTICS_API_KEY}" \
  "${ANALYTICS_API_BASE_URL}/api/dashboard?from=2026-02-18&to=2026-03-20"
```

### `get_problem_spikes(window)`

Skill command:

```bash
python3 skills/telegram-analytics-bridge/scripts/bridge.py get_problem_spikes --window 7d --json
```

Backend curl:

```bash
curl -sS \
  -H "Authorization: Bearer ${ANALYTICS_API_KEY}" \
  "${ANALYTICS_API_BASE_URL}/api/dashboard?from=2026-03-13&to=2026-03-20"
```

### `get_question_clusters(window, topic?)`

Skill command:

```bash
python3 skills/telegram-analytics-bridge/scripts/bridge.py get_question_clusters --window 7d --topic "Residency" --json
```

Backend curl:

```bash
curl -sS \
  -H "Authorization: Bearer ${ANALYTICS_API_KEY}" \
  "${ANALYTICS_API_BASE_URL}/api/dashboard?from=2026-03-13&to=2026-03-20"
```

### `get_sentiment_overview(window)`

Skill command:

```bash
python3 skills/telegram-analytics-bridge/scripts/bridge.py get_sentiment_overview --window 7d --json
```

Backend curl:

```bash
curl -sS \
  -H "Authorization: Bearer ${ANALYTICS_API_KEY}" \
  "${ANALYTICS_API_BASE_URL}/api/sentiment-distribution?timeframe=Last%207%20Days"
```

### `get_active_alerts()`

Skill command:

```bash
python3 skills/telegram-analytics-bridge/scripts/bridge.py get_active_alerts --json
```

Backend curl:

```bash
curl -sS \
  -H "Authorization: Bearer ${ANALYTICS_API_KEY}" \
  "${ANALYTICS_API_BASE_URL}/api/dashboard"
```

### `ask_insights(question, window?)`

Skill command:

```bash
python3 skills/telegram-analytics-bridge/scripts/bridge.py ask_insights --window 7d --question "What is driving concern about residency?" --json
```

Backend curl:

```bash
curl -sS \
  -X POST \
  -H "Authorization: Bearer ${ANALYTICS_API_KEY}" \
  -H "Content-Type: application/json" \
  "${ANALYTICS_API_BASE_URL}/api/insights/cards" \
  -d '{"filters":{"timeframe":"Last 7 Days"},"audience":"analyst"}'
```

## Recommended agent prompt block

Add this to the existing main OpenClaw agent prompt source in your compose env wiring:

```text
When a user asks about Telegram community analytics, trends, sentiment, recurring questions, issue spikes, alerts, or what is driving a discussion, use the telegram-analytics-bridge skill instead of answering from memory.

Prefer:
- ask_insights for "why", "what is driving", or synthesis questions
- get_top_topics for top trends
- get_declining_topics for fading topics
- get_problem_spikes for issue escalation
- get_question_clusters for repeated user questions
- get_sentiment_overview for mood/sentiment
- get_active_alerts for urgent current risks

Always request compact JSON and use the skill's telegram_text as the primary user-facing reply. Do not paste raw large JSON unless the user explicitly asks for it. If the skill returns low_confidence, respond cautiously and say the current evidence is limited rather than guessing.
```

## Optional CLI tuning

- `--timeout 35`
- `--max-retries 2`
- `--backoff-base 0.5`
- `--json`

## Troubleshooting

### Missing env vars

Symptoms:

- validation errors at startup
- auth failures
- requests target the wrong backend

Checks:

```bash
echo "$ANALYTICS_API_BASE_URL"
echo "$ANALYTICS_API_KEY"
```

Fix:

- ensure both vars are set in the OpenClaw runtime, not just in your local shell
- use the direct backend URL

### Missing `pydantic`

Symptoms:

- `ModuleNotFoundError: No module named 'pydantic'`

Fix:

```bash
pip install pydantic
```

If OpenClaw is containerized, install it in the image/runtime that actually runs the skill.

### Backend timeout or cold start

Symptoms:

- first request is slow
- skill returns a timeout message even though the backend later works

Fix:

- retry after the backend wakes up
- keep the default `35s` timeout and `2` retries unless you have strong evidence to change them
- confirm `/api/health` is fast and `/api/dashboard` is the slower endpoint

### Skill works in shell but not chat

Symptoms:

- direct `python3 ... bridge.py ... --json` works
- OpenClaw chat still answers from memory or ignores the skill

Check in this order:

1. the main agent prompt includes the skill-routing block
2. the OpenClaw service was restarted after prompt changes
3. the running container has the updated skill files
4. the runtime process has `ANALYTICS_API_BASE_URL` and `ANALYTICS_API_KEY`
5. the agent is configured to prefer skill evidence over freeform reasoning for analytics asks

## Test commands

```bash
python3 -m unittest discover -s skills/telegram-analytics-bridge/tests -p 'test_*.py'
```

```bash
python3 skills/telegram-analytics-bridge/scripts/bridge.py ask_insights --window 7d --question "What is driving concern about residency?" --json
```

Expected sample output shape:

```json
{
  "schema_version": "1.0",
  "generated_at": "2026-03-20T12:00:00Z",
  "ok": true,
  "action": "ask_insights",
  "window": "7d",
  "summary": "Residency questions are being driven by unresolved permit delays and documentation confusion.",
  "confidence": "medium",
  "bullets": [
    "Residency question clusters show repeated permit and paperwork confusion.",
    "Problem spikes point to delays and inconsistent guidance across channels."
  ],
  "items": [],
  "telegram_text": "Residency questions are being driven by unresolved permit delays and documentation confusion.\n- Residency question clusters show repeated permit and paperwork confusion.\n- Problem spikes point to delays and inconsistent guidance across channels.",
  "source_endpoints": [
    "/api/dashboard",
    "/api/insights/cards"
  ]
}
```

## Security note

If an OpenAI key or analytics API key was pasted into chat, terminal output, or screenshots during setup, treat it as compromised and rotate it:

- rotate the OpenAI API key
- rotate the analytics/OpenClaw API key

Do this before final production rollout.

Low-confidence sample:

```json
{
  "schema_version": "1.0",
  "generated_at": "2026-03-20T12:00:00Z",
  "ok": true,
  "action": "ask_insights",
  "window": "7d",
  "summary": "Evidence is too limited to answer that confidently right now.",
  "confidence": "low_confidence",
  "caveat": "The current backend evidence does not strongly match the question, so the skill did not infer an answer.",
  "bullets": [
    "Top current signals are still included below for context."
  ],
  "items": [],
  "telegram_text": "Evidence is too limited to answer that confidently right now.\n- Top current signals are still included below for context.",
  "source_endpoints": [
    "/api/dashboard",
    "/api/insights/cards"
  ]
}
```
