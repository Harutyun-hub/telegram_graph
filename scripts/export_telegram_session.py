#!/usr/bin/env python3
"""
Canonical Railway Telegram session exporter.

Creates a fresh TELEGRAM_SESSION_STRING locally using QR login, prompts for
Telegram two-step verification only if Telegram requires it, and saves the
result to .env.telegram_session.
"""

import asyncio
import getpass
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession
from loguru import logger
import qrcode
import config

QR_TIMEOUT_SECONDS = 30
ENV_OUTPUT_PATH = Path(".env.telegram_session")


def print_qr(url: str) -> None:
    """Print a fresh QR code and operator guidance."""
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=1,
        border=1,
    )
    qr.add_data(url)
    qr.make(fit=True)

    print("\n" + "=" * 64)
    print("TELEGRAM QR LOGIN FOR RAILWAY")
    print("=" * 64)
    print()
    qr.print_ascii(invert=True)
    print()
    print("1. Open Telegram on your phone")
    print("2. Go to Settings -> Devices -> Link Desktop Device")
    print("3. Scan the QR code")
    print("4. If Telegram asks for a password, enter your Telegram two-step verification password here")
    print("=" * 64)


def prompt_yes_no(message: str) -> bool:
    response = input(f"{message} [y/N]: ").strip().lower()
    return response == "y"


def prompt_2fa_password() -> str:
    print("\nTelegram requested two-step verification.")
    print("Enter the Telegram two-step verification password for this account.")
    print("This is not your phone number, SMS code, phone lock PIN, or QR text.")
    return getpass.getpass("Telegram 2FA password: ")


async def complete_qr_login(client: TelegramClient) -> None:
    """Run QR login until scanned, refreshing the QR if it expires."""
    qr_login = await client.qr_login()
    print_qr(qr_login.url)

    while True:
        try:
            await qr_login.wait(timeout=QR_TIMEOUT_SECONDS)
            return
        except asyncio.TimeoutError:
            logger.info("QR code expired; generating a fresh one")
            await qr_login.recreate()
            print_qr(qr_login.url)
        except SessionPasswordNeededError:
            password = prompt_2fa_password()
            await client.sign_in(password=password)
            return
        except Exception as exc:
            message = str(exc)
            if (
                "SESSION_PASSWORD_NEEDED" in message
                or "Two-steps verification is enabled" in message
                or "password" in message.lower()
            ):
                password = prompt_2fa_password()
                await client.sign_in(password=password)
                return
            raise


async def export_session() -> str:
    """Create a fresh Railway-only string session."""
    print("\nThis command creates a NEW production session for Railway.")
    print("Do not keep using the same exported session locally after this.")

    if os.getenv("TELEGRAM_SESSION_STRING"):
        print("\nWARNING: TELEGRAM_SESSION_STRING is already set in this shell.")
        print("If you continue, you should replace the Railway value with the new one.")
        if not prompt_yes_no("Continue and create a new Railway session?"):
            raise SystemExit(1)

    client = TelegramClient(
        StringSession(),
        config.TELEGRAM_API_ID,
        config.TELEGRAM_API_HASH,
    )

    try:
        await client.connect()
        if not await client.is_user_authorized():
            print("\nStarting QR authentication...")
            await complete_qr_login(client)

        if not await client.is_user_authorized():
            raise RuntimeError("Telegram login did not complete successfully")

        me = await client.get_me()
        display_name = " ".join(part for part in [me.first_name, me.last_name] if part).strip() or "Unknown"
        username = f"@{me.username}" if getattr(me, "username", None) else "no username"
        print(f"\nAuthenticated as: {display_name} ({username})")

        return StringSession.save(client.session)
    finally:
        await client.disconnect()


def save_session_string(session_string: str) -> None:
    """Save session string to gitignored file and print Railway instructions."""
    generated_at = datetime.now(timezone.utc).isoformat()
    with open(ENV_OUTPUT_PATH, "w", encoding="utf-8") as handle:
        handle.write("# Telegram session string for Railway deployment\n")
        handle.write(f"# Generated at {generated_at}\n")
        handle.write("# Keep this file private. Do not commit it.\n")
        handle.write(f"TELEGRAM_SESSION_STRING={session_string}\n")

    print("\n" + "=" * 80)
    print("SESSION EXPORT SUCCESSFUL")
    print("=" * 80)
    print("\nAdd this Railway variable:")
    print("Name:  TELEGRAM_SESSION_STRING")
    print(f"Value: {session_string}")
    print(f"\nA copy was saved to: {ENV_OUTPUT_PATH.absolute()}")

    print("\nNext steps:")
    print("1. Open Railway -> your service -> Variables")
    print("2. Set TELEGRAM_SESSION_STRING to the new value")
    print("3. Redeploy Railway")
    print("4. Stop using this same production session locally")

    print("\nImportant:")
    print("• This string is for Railway only")
    print("• Keep local development on a separate file session name")
    print("• Never commit .env.telegram_session or any .session files")
    print("• If this string is exposed, revoke it in Telegram -> Settings -> Devices")
    print("=" * 80 + "\n")


async def main() -> None:
    print("\n" + "=" * 80)
    print("TELEGRAM SESSION EXPORT FOR RAILWAY")
    print("=" * 80)
    print("\nThis is the only supported production session flow in this repo.")
    print("It creates a fresh Railway-only TELEGRAM_SESSION_STRING via QR login.")

    if not config.TELEGRAM_API_ID or not config.TELEGRAM_API_HASH:
        print("\nERROR: TELEGRAM_API_ID and TELEGRAM_API_HASH must be set in .env")
        raise SystemExit(1)

    session_string = await export_session()
    save_session_string(session_string)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCancelled.")
        raise SystemExit(1)
    except Exception as exc:
        print(f"\nERROR: {exc}")
        raise SystemExit(1)
