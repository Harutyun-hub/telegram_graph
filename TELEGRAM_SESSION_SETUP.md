# Telegram Session Setup for Railway Deployment

## Overview

This repository now uses one canonical production session flow:

- Railway uses exactly one `TELEGRAM_SESSION_STRING`
- You generate that string locally with QR login
- Local development keeps using a separate file-based session name
- The same production session must not be reused locally

This avoids Telegram invalidating the authorization key when the same session is used from different places.

## Required Environment Variables

In your local `.env`:

```bash
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_PHONE=+1234567890
TELEGRAM_SESSION_NAME=telegram_scraper_dev
```

Notes:
- `TELEGRAM_SESSION_NAME` is for local development only
- Railway production should use `TELEGRAM_SESSION_STRING`

## Canonical Production Flow

### Step 1: Generate a fresh Railway session locally

Run:

```bash
python scripts/export_telegram_session.py
```

What happens:
- A QR code appears in your terminal
- Open Telegram on your phone
- Go to `Settings -> Devices -> Link Desktop Device`
- Scan the QR code
- If Telegram asks for a password, enter your **Telegram two-step verification password**

Important:
- That password is not an SMS code
- That password is not your phone PIN
- That password is not the QR code text

### Step 2: Copy the generated session string

The script will:
- print `TELEGRAM_SESSION_STRING`
- save it to `.env.telegram_session`

That file is gitignored and should stay private.

### Step 3: Add it to Railway

In Railway:

1. Open your project
2. Open the backend service
3. Go to `Variables`
4. Set:

```text
TELEGRAM_SESSION_STRING=<paste the generated string>
```

5. Redeploy the service

### Step 4: Keep production and local usage separate

After creating a new Railway session:

- Railway should use the new `TELEGRAM_SESSION_STRING`
- local development should keep using `TELEGRAM_SESSION_NAME=telegram_scraper_dev`
- do not run local scraping with the same exported Railway session

## How the App Chooses a Session

### Railway / cloud

When `TELEGRAM_SESSION_STRING` is present:

```python
client = TelegramClient(StringSession(session_string), api_id, api_hash)
```

### Local development

When `TELEGRAM_SESSION_STRING` is not present:

```python
client = TelegramClient("telegram_scraper_dev", api_id, api_hash)
```

## Troubleshooting

### Error: "authorization key was used under two different IP addresses simultaneously"

Cause:
- the same Telegram session was used from more than one environment

Fix:
1. Generate a fresh Railway session with `python scripts/export_telegram_session.py`
2. Update Railway with the new `TELEGRAM_SESSION_STRING`
3. Stop using that same production session locally

### QR scan works, but Telegram asks for a password

That is your Telegram **two-step verification password**.

If you do not know it:
- open Telegram
- go to `Settings -> Privacy and Security -> Two-Step Verification`
- recover or reset it there before generating a new Railway session

### Error: "Invalid TELEGRAM_SESSION_STRING"

Fix:
1. Generate a fresh string locally
2. Replace the Railway variable
3. Redeploy Railway

### Session works locally but not on Railway

Check:
- the full string was copied
- no spaces or line breaks were added
- Railway has the latest value
- local `.env` still uses a separate `TELEGRAM_SESSION_NAME`

## Security Notes

- `TELEGRAM_SESSION_STRING` gives full access to that Telegram account
- Never commit `.env.telegram_session`
- Never share the string in chat, logs, or screenshots
- If compromised, revoke it in Telegram under `Settings -> Devices`

## Expected Railway Logs

Success:

```text
INFO: Using session from TELEGRAM_SESSION_STRING environment variable
SUCCESS: Session active — logged in as: YourName (@yourusername)
```

Failure:

```text
ERROR: TELEGRAM_SESSION_STRING is invalid or expired!
ERROR: Please run locally: python scripts/export_telegram_session.py
```
