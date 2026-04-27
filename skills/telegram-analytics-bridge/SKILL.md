---
name: telegram-analytics-bridge
description: Use this skill for Telegram community analytics questions, including trends, sentiment, recurring questions, issue spikes, topic/channel investigations, graph context, comparisons, and "what is driving" explanations from the analytics backend.
---

# Telegram Analytics Bridge

Use this skill when OpenClaw receives Telegram-community analytics questions and should answer from backend evidence instead of memory.

## Trigger this skill for

- Telegram community analytics and intelligence requests
- Explicit source-management asks like `track @examplechannel`, `add this channel to tracking`, `track this Facebook page`, or `monitor this Instagram account`
- Topic trends or declining topics
- Sentiment or community mood
- Problem spikes, urgent issues, or alerts
- Repeated user questions
- Explanatory asks like `what is driving ...`, `why are people talking about ...`, or `what is causing concern about ...`
- Channel-specific questions like `What is going on in Docs Chat?`
- Graph or ecosystem questions like `Which channels are shaping the current discussion?`
- Comparison asks like `Compare residency permits and rental costs this week`

## Core behavior

- Queries the analytics API with `Authorization: Bearer $ANALYTICS_API_KEY`
- Can add explicit tracked sources through narrow backend write endpoints
- Returns compact JSON designed for agent consumption
- Keeps output Telegram-friendly with `summary`, concise `bullets`, `items`, and `telegram_text`
- Avoids tables, long prose, and raw large dumps
- Keeps `ask_insights` deterministic: if evidence is weak, it returns `low_confidence` with a caveat instead of guessing
- Uses the existing graph API surface first; it does not query Neo4j directly from the skill

## Required environment

- `ANALYTICS_API_BASE_URL`
- `ANALYTICS_API_KEY`

## Invocation rules

- Always call the CLI with `--json`
- Use `telegram_text` as the primary user-facing reply
- Do not paste the full JSON unless the user explicitly asks for raw output
- Use `add_source` only when the user explicitly asks to track a source
- If the source is ambiguous, ask for a full URL or `@handle` instead of guessing
- Do not claim a source was added unless the backend confirms `created`, `exists`, or `reactivated`
- If `confidence` is `low_confidence`, answer cautiously and mention that current evidence is limited
- Prefer the existing skill actions instead of answering analytics questions from memory
- `investigate_question` is the main bounded analyst workflow and may route to topic, channel, category, or graph context
- For direct exact drill-downs, use the narrower actions instead of overusing `investigate_question`

Run the CLI:

```bash
python3 skills/telegram-analytics-bridge/scripts/bridge.py <action> [flags] --json
```

## Action routing

- `ask_insights`
  - Use for causal or synthesis questions
  - Examples: `What is driving concern about residency permits?`, `Why are people talking about housing costs?`
- `add_source`
  - Use for explicit source-tracking requests
  - Examples: `Track https://t.me/examplechannel`, `Add @examplechannel to tracking`, `Track https://facebook.com/examplepage`
- `investigate_question`
  - Use for bounded analyst-style investigation of a user question
  - This action is intentionally low-fanout and may route to topic, channel, category, or graph context
- `get_top_topics`
  - Use for top discussion trends
- `search_entities`
  - Use when the question is ambiguous and you need to resolve the likely topic first
- `get_topic_detail`
  - Use for direct topic drill-down with growth, channels, and sample evidence
- `get_topic_evidence`
  - Use for compact evidence snippets, especially for explainability
- `get_freshness_status`
  - Use for operational trust checks and staleness caveats
- `investigate_topic`
  - Use for bounded topic investigation with detail, evidence, and freshness context
- `investigate_channel`
  - Use for channel-specific investigation with detail, recent posts, and freshness context
- `get_graph_snapshot`
  - Use for ecosystem-wide graph context, hidden patterns, and channel/topic landscape questions
- `get_node_context`
  - Use for graph-node drill-down on a topic, category, or channel
- `compare_topics`
  - Use for evidence-based topic comparison
- `compare_channels`
  - Use for evidence-based channel comparison
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

- `add_source --value "@examplechannel" [--source-type telegram] [--title "Example Channel"]`
- `add_source --value "https://facebook.com/examplepage" --source-type facebook_page`
- `add_source --value "https://instagram.com/exampleprofile" --source-type instagram_profile`
- `add_source --value "https://example.com" --source-type google_domain`
- `get_top_topics --window 7d --limit 5`
- `search_entities --query "residency permit delays" --limit 5`
- `get_topic_detail --topic "Residency permits" --window 7d`
- `get_topic_evidence --topic "Residency permits" --view questions --limit 5 --window 7d`
- `get_freshness_status`
- `investigate_topic --topic "Residency permits" --window 7d`
- `investigate_channel --channel "Docs Chat" --window 7d`
- `get_graph_snapshot --window 7d [--category "Documents"] [--signal-focus needs] [--max-nodes 12]`
- `get_node_context --entity "Residency permits" --type topic --window 7d`
- `compare_topics --topic-a "Residency permits" --topic-b "Rental costs" --window 7d`
- `compare_channels --channel-a "Docs Chat" --channel-b "Visa Support" --window 7d`
- `get_declining_topics --window 30d --limit 5`
- `get_problem_spikes --window 7d`
- `get_question_clusters --window 7d [--topic "Residency"]`
- `get_sentiment_overview --window 7d`
- `get_active_alerts`
- `ask_insights --question "What is driving concern about residency?" [--window 7d]`
- `investigate_question --question "What is driving concern about residency?" [--window 7d]`

## Output rules

- Prefer `telegram_text` for direct Telegram replies
- Keep answers concise, professional, and evidence-oriented
- Preserve `summary`, `bullets`, `items`, and `source_endpoints`
- Keep investigation answers bounded and compact; do not expand into freeform multi-step analysis beyond the defined actions
- For `add_source`, confirm only the exact backend result: created, already tracked, or reactivated
- For graph answers, summarize the graph rather than dumping raw nodes or links
- Do not render tables
- Treat `confidence: "low_confidence"` as a caveated answer, not a failure
- If the user asks for more detail, summarize from the structured fields instead of dumping large payloads

## Notes

- `window` supports `24h`, `7d`, `30d`, `90d`
- Production defaults tolerate backend cold starts better: `timeout=40s`, `max_retries=3`, exponential backoff enabled
- The skill is intentionally API-first for graph intelligence; direct Neo4j access is out of scope for this phase
- See [README.md](README.md) for setup, container testing, troubleshooting, and prompt guidance
