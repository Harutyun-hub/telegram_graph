"""
session_manager.py — Handles Telethon authentication via QR code.

Supports two modes:
1. StringSession from environment variable (for Railway/cloud deployments)
2. File-based session (for local development only)

QR Login (recommended):
  1. Run the script
  2. A QR code appears in the terminal
  3. Open Telegram app → Settings → Devices → Link Desktop Device
  4. Scan the QR code
  5. Tap "Confirm" in the app
  6. Done — session saved permanently

Fallback (phone code):
  If QR login fails, falls back to phone code entry.

For Railway deployment:
  Use scripts/export_telegram_session.py locally to generate TELEGRAM_SESSION_STRING.
  Keep that production session exclusive to Railway; do not reuse it locally.
"""
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError
from loguru import logger
import asyncio
import qrcode
import config
import sys
import os


def _print_qr(url: str):
    """Print a scannable QR code to the terminal."""
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=1,
        border=1,
    )
    qr.add_data(url)
    qr.make(fit=True)

    print("\n" + "=" * 60)
    print("📱 QR CODE LOGIN")
    print("=" * 60)
    print()
    qr.print_ascii(invert=True)
    print()
    print("▶ Open Telegram on your phone")
    print("▶ Settings → Devices → Link Desktop Device")
    print("▶ Scan the QR code above")
    print("▶ Tap CONFIRM in the app")
    print("=" * 60 + "\n")


async def get_client() -> TelegramClient:
    """
    Returns an authenticated Telethon client.

    Priority:
    1. Use TELEGRAM_SESSION_STRING if available (Railway/cloud deployment)
    2. Fall back to file-based session (local development)
    3. Authenticate via QR code or phone code if no session exists
    """

    # Check for session string from environment (Railway deployment)
    session_string = os.getenv("TELEGRAM_SESSION_STRING")

    if session_string:
        logger.info("Using session from TELEGRAM_SESSION_STRING environment variable")
        client = TelegramClient(
            StringSession(session_string),
            config.TELEGRAM_API_ID,
            config.TELEGRAM_API_HASH,
        )
    else:
        # Use file-based session for local development
        logger.info(
            f"Using file-based local session: {config.TELEGRAM_SESSION_NAME}.session "
            "(keep this separate from Railway production session)"
        )
        client = TelegramClient(
            config.TELEGRAM_SESSION_NAME,
            config.TELEGRAM_API_ID,
            config.TELEGRAM_API_HASH,
        )

    await client.connect()

    # Already authorized — silent fast path
    if await client.is_user_authorized():
        me = await client.get_me()
        logger.success(f"Session active — logged in as: {me.first_name} (@{me.username})")
        return client

    # No session string in cloud deployment - can't authenticate interactively
    if session_string:
        logger.error("TELEGRAM_SESSION_STRING is invalid or expired!")
        logger.error("Please run locally: python scripts/export_telegram_session.py")
        logger.error("Then update TELEGRAM_SESSION_STRING in Railway environment variables")
        await client.disconnect()
        raise RuntimeError(
            "Invalid TELEGRAM_SESSION_STRING. Cannot authenticate interactively in cloud deployment. "
            "Run 'python scripts/export_telegram_session.py' locally and update the environment variable."
        )

    logger.info("No active session found. Starting QR code login...")

    # ── QR Login ─────────────────────────────────────────────
    try:
        qr_login = await client.qr_login()
        _print_qr(qr_login.url)

        # Keep refreshing until the user scans
        while True:
            try:
                await qr_login.wait(timeout=30)
                break  # Scan successful
            except asyncio.TimeoutError:
                # QR codes expire every 30s — regenerate
                logger.info("QR code expired — refreshing...")
                await qr_login.recreate()
                _print_qr(qr_login.url)
            except Exception as e:
                if "SESSION_PASSWORD_NEEDED" in str(e):
                    raise SessionPasswordNeededError(None)
                raise

    except SessionPasswordNeededError:
        print("\n🔒 Two-factor authentication is enabled.")
        password = input("Enter your Telegram password: ").strip()
        await client.sign_in(password=password)

    except Exception as e:
        logger.warning(f"QR login failed ({e}). Falling back to phone code login...")

        # ── Phone Code Fallback ───────────────────────────────
        await client.send_code_request(config.TELEGRAM_PHONE)

        print("\n" + "=" * 60)
        print("📱 PHONE CODE LOGIN (fallback)")
        print(f"   A code was sent to your Telegram app")
        print(f"   Check the 'Telegram' service chat in your app")
        print("=" * 60)

        code = input("Enter the 5-digit code: ").strip()
        try:
            await client.sign_in(config.TELEGRAM_PHONE, code)
        except SessionPasswordNeededError:
            password = input("Enter your Telegram password: ").strip()
            await client.sign_in(password=password)
        except Exception as ex:
            logger.error(f"Sign-in failed: {ex}")
            await client.disconnect()
            sys.exit(1)

    me = await client.get_me()
    logger.success(f"✅ Logged in as: {me.first_name} (@{me.username})")
    logger.info("Session saved — this won't be asked again.")
    return client
