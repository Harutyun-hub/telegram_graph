---
name: knowledge-base-rag
description: Use this skill when the user wants to upload documents, add URLs, search their knowledge base, or ask questions grounded in their private documents with citations. This is the NotebookLM-style document intelligence skill.
---

# Knowledge Base RAG

Use this skill when OpenClaw needs to interact with the user's private knowledge base — uploading documents, adding URLs, and answering questions grounded in indexed content with citations.

## Trigger this skill for

- User uploads a file via Telegram/WhatsApp/Slack
- User asks to "add [URL] to my knowledge base" or "[collection name]"
- User asks a question about their documents: "what does my [collection] say about X"
- User says "search [collection] for X"
- User says "list my knowledge bases" or "show my collections"
- User asks "how many documents do I have in [collection]"
- User says "delete [document] from [collection]"

## Core behavior

- Calls the analytics backend `/api/kb/*` endpoints using `Authorization: Bearer $ANALYTICS_API_KEY`
- Returns compact JSON designed for agent consumption
- Always returns `telegram_text` as the primary user-facing reply
- Keeps answers concise and grounded — never fabricates from memory
- When confidence is `low_confidence`, answers cautiously and recommends uploading more relevant documents

## Required environment

- `ANALYTICS_API_BASE_URL` — base URL of the analytics backend
- `ANALYTICS_API_KEY` — OpenClaw server-to-server API key (`ANALYTICS_API_KEY_OPENCLAW`)

## Invocation rules

- Always call the CLI with `--json`
- Use `telegram_text` as the primary user-facing reply
- Do not paste raw JSON unless the user explicitly requests it
- For `ask_kb`: include citations in the reply as "[Source: filename, p.N]"
- For `low_confidence` answers, tell the user the evidence is limited and suggest uploading more documents

Run the CLI:

```bash
python3 skills/knowledge-base-rag/scripts/bridge.py <action> [flags] --json
```

## Action routing

- `ask_kb` — Answer a question grounded in a collection. Default collection: `default`
- `add_url` — Ingest a URL into a collection
- `list_collections` — Show all collections with document and chunk counts
- `search_kb` — Semantic + keyword search returning ranked snippets

Supported commands:

```bash
python3 scripts/bridge.py ask_kb --question "What are the key milestones?" --collection work --json
python3 scripts/bridge.py add_url --url "https://example.com/doc" --collection research --json
python3 scripts/bridge.py list_collections --json
python3 scripts/bridge.py search_kb --query "pricing model" --collection default --top-k 5 --json
```

## Output rules

- Use `telegram_text` for direct replies
- For `ask_kb`: render answer followed by source citations
- Keep answers under 900 characters for Telegram compatibility
- Treat `confidence: "low_confidence"` as a caveated answer, not a failure
- If the user asks "what's in my knowledge base", use `list_collections`

## Notes

- Default collection is `default` — if the user doesn't specify a collection, use `default`
- Documents can be uploaded via the web dashboard at `/agent` — OpenClaw handles URL ingestion only
- Production defaults: `timeout=35s`, `max_retries=2`, exponential backoff enabled
