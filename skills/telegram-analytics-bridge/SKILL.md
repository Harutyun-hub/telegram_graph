---
name: telegram-analytics-bridge
description: Use this skill for Telegram community analytics questions, including trends, sentiment, recurring questions, issue spikes, active alerts, and "what is driving" explanations from the analytics backend.
---

# Telegram Analytics Bridge

Use this skill when OpenClaw receives Telegram-community analytics questions and should answer from backend evidence instead of memory.

## Trigger this skill for

- Telegram community analytics and intelligence requests
- Topic trends or declining topics
- Sentiment or community mood
- Problem spikes, urgent issues, or alerts
- Repeated user questions
- Explanatory asks like `what is driving ...`, `why are people talking about ...`, or `what is causing concern about ...`

## Core behavior

- Queries the analytics API with `Authorization: Bearer $ANALYTICS_API_KEY`
- Returns compact JSON designed for agent consumption
- Keeps output Telegram-friendly with `summary`, concise `bullets`, `items`, and `telegram_text`
- Avoids tables, long prose, and raw large dumps
- Keeps `ask_insights` deterministic: if evidence is weak, it returns `low_confidence` with a caveat instead of guessing

## Required environment

- `ANALYTICS_API_BASE_URL`
- `ANALYTICS_API_KEY`

## Invocation rules

- Always call the CLI with `--json`
- Use `telegram_text` as the primary user-facing reply
- Do not paste the full JSON unless the user explicitly asks for raw output
- If `confidence` is `low_confidence`, answer cautiously and mention that current evidence is limited
- For broad analytics questions, start with `ask_insights --window 7d`, then follow with a narrower action if needed

Run the CLI:

```bash
python3 skills/telegram-analytics-bridge/scripts/bridge.py <action> [flags] --json
```

## Action routing

- `ask_insights`
  - Use for causal or synthesis questions
  - Examples: `What is driving concern about residency permits?`, `Why are people talking about housing costs?`
- `get_top_topics`
  - Use for top discussion trends
- `get_declining_topics`
  - Use for fading topics or reduced discussion
- `get_problem_spikes`
  - Use for issue escalation or operational pain points
- `get_question_clusters`
  - Use for repeated user questions, optionally scoped to a topic
- `get_sentiment_overview`
  - Use for tone, mood, sentiment, or community temperature
- `get_active_alerts`
  - Use for urgent current risks or alerts

Supported commands:

- `get_top_topics --window 7d --limit 5`
- `get_declining_topics --window 30d --limit 5`
- `get_problem_spikes --window 7d`
- `get_question_clusters --window 7d [--topic "Residency"]`
- `get_sentiment_overview --window 7d`
- `get_active_alerts`
- `ask_insights --question "What is driving concern about residency?" [--window 7d]`

## Output rules

- Prefer `telegram_text` for direct Telegram replies
- Keep answers concise, professional, and evidence-oriented
- Preserve `summary`, `bullets`, `items`, and `source_endpoints`
- Do not render tables
- Treat `confidence: "low_confidence"` as a caveated answer, not a failure
- If the user asks for more detail, summarize from the structured fields instead of dumping large payloads

## Notes

- `window` supports `24h`, `7d`, `30d`, `90d`
- Production defaults tolerate backend cold starts better: `timeout=35s`, `max_retries=2`, exponential backoff enabled
- See [README.md](README.md) for setup, container testing, troubleshooting, and prompt guidance
