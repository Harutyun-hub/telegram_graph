# OpenClaw Private Bridge

This service runs on the Hostinger machine near OpenClaw and exposes a minimal authenticated HTTP endpoint that executes the proven working OpenClaw CLI path for the web chat helper.

## Purpose
- private infrastructure only
- stateless execution only
- no transcript persistence
- only agent allowed in MVP: `web-api-assistant`

## Endpoint
- `POST /openclaw-agent/chat`

## Security
- bind the app to `127.0.0.1`
- expose through Nginx/TLS only
- require `Authorization: Bearer <OPENCLAW_BRIDGE_TOKEN>`
- do not log tokens or raw prompts

## Smoke test
```bash
curl -sS -X POST http://127.0.0.1:8544/openclaw-agent/chat \
  -H 'Authorization: Bearer <OPENCLAW_BRIDGE_TOKEN>' \
  -H 'Content-Type: application/json' \
  --data '{"requestId":"smoke","sessionId":"web_smoke_1234","messages":[{"role":"user","text":"Reply with exactly WEB_HELPER_OK"}]}'
```

Expected response text:
- `WEB_HELPER_OK`

