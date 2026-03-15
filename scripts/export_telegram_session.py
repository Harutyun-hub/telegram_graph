#!/usr/bin/env python3
"""
Export Telegram session to environment variable string.

This script authenticates with Telegram and exports the session as a base64 string
that can be used as an environment variable in Railway or other cloud deployments.

Usage:
    python scripts/export_telegram_session.py

Output:
    - Displays the session string to copy
    - Saves to .env.telegram_session (gitignored)
"""

import asyncio
import sys
import os
import base64
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError
import qrcode
from loguru import logger
import config


def print_qr(url: str):
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
    print("📱 QR CODE LOGIN FOR SESSION EXPORT")
    print("=" * 60)
    print()
    qr.print_ascii(invert=True)
    print()
    print("▶ Open Telegram on your phone")
    print("▶ Settings → Devices → Link Desktop Device")
    print("▶ Scan the QR code above")
    print("▶ Tap CONFIRM in the app")
    print("=" * 60 + "\n")


async def export_session():
    """Authenticate and export Telegram session as string."""

    # Check if we already have a session string
    existing_session = os.getenv("TELEGRAM_SESSION_STRING")
    if existing_session:
        print("\n⚠️  WARNING: TELEGRAM_SESSION_STRING already exists in environment.")
        print("This script will create a NEW session and invalidate the old one.")
        response = input("Continue? (y/N): ").strip().lower()
        if response != 'y':
            print("Aborted.")
            return

    # Check for existing session file
    session_file = Path(f"{config.TELEGRAM_SESSION_NAME}.session")
    client = None

    if session_file.exists():
        print(f"\n✅ Found existing session file: {session_file}")
        print("Converting to string session...")

        # Load from existing file session
        file_client = TelegramClient(
            str(session_file.with_suffix('')),
            config.TELEGRAM_API_ID,
            config.TELEGRAM_API_HASH
        )

        await file_client.connect()

        if await file_client.is_user_authorized():
            # Export to string
            string_session = StringSession.save(file_client.session)
            await file_client.disconnect()

            # Verify the string session works
            client = TelegramClient(
                StringSession(string_session),
                config.TELEGRAM_API_ID,
                config.TELEGRAM_API_HASH
            )
            await client.connect()

            if await client.is_user_authorized():
                me = await client.get_me()
                print(f"✅ Successfully exported session for: {me.first_name} (@{me.username})")
                await client.disconnect()
                return string_session
            else:
                print("❌ String session verification failed. Creating new session...")
                await client.disconnect()
                client = None
        else:
            print("⚠️  Session file exists but is not authorized. Creating new session...")
            await file_client.disconnect()

    # Create new string session from scratch
    if not client:
        print("\n🔐 Creating new Telegram session...")
        client = TelegramClient(
            StringSession(),
            config.TELEGRAM_API_ID,
            config.TELEGRAM_API_HASH
        )

        await client.connect()

        if not await client.is_user_authorized():
            print("Starting authentication process...")

            # Try QR login first
            try:
                qr_login = await client.qr_login()
                print_qr(qr_login.url)

                # Keep refreshing until the user scans
                while True:
                    try:
                        await qr_login.wait(timeout=30)
                        break  # Scan successful
                    except asyncio.TimeoutError:
                        # QR codes expire every 30s — regenerate
                        logger.info("QR code expired — refreshing...")
                        await qr_login.recreate()
                        print_qr(qr_login.url)
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

                # Phone code fallback
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

        me = await client.get_me()
        print(f"\n✅ Authenticated as: {me.first_name} (@{me.username})")

        # Save the string session
        string_session = StringSession.save(client.session)
        await client.disconnect()
        return string_session


def save_session_string(session_string: str):
    """Save session string to file and display instructions."""

    # Save to local file (gitignored)
    env_file = Path(".env.telegram_session")
    with open(env_file, "w") as f:
        f.write(f"# Telegram Session String for Railway Deployment\n")
        f.write(f"# Generated: {asyncio.get_event_loop().time()}\n")
        f.write(f"# DO NOT COMMIT THIS FILE TO GIT\n\n")
        f.write(f"TELEGRAM_SESSION_STRING={session_string}\n")

    print("\n" + "=" * 80)
    print("🎉 SESSION EXPORT SUCCESSFUL!")
    print("=" * 80)

    print("\n📋 NEXT STEPS FOR RAILWAY DEPLOYMENT:")
    print("-" * 80)

    print("\n1️⃣  Copy this environment variable to Railway:")
    print("\n   Variable Name:  TELEGRAM_SESSION_STRING")
    print(f"   Variable Value: {session_string}")

    print("\n2️⃣  In Railway Dashboard:")
    print("   • Go to your project")
    print("   • Click on your service")
    print("   • Go to 'Variables' tab")
    print("   • Add New Variable:")
    print("     - Name: TELEGRAM_SESSION_STRING")
    print("     - Value: [paste the string above]")
    print("   • Click 'Add' and deploy")

    print("\n3️⃣  Session string also saved to:")
    print(f"   📄 {env_file.absolute()}")
    print("   (This file is gitignored and safe)")

    print("\n⚠️  IMPORTANT SECURITY NOTES:")
    print("-" * 80)
    print("• This session string grants full access to your Telegram account")
    print("• NEVER commit it to Git or share publicly")
    print("• Treat it like a password")
    print("• If compromised, revoke via Telegram Settings → Devices")
    print("• Each new export invalidates the previous session string")

    print("\n✅ Your app is now ready for Railway deployment with persistent Telegram auth!")
    print("=" * 80 + "\n")


async def main():
    """Main entry point."""
    print("\n" + "=" * 80)
    print("🚀 TELEGRAM SESSION EXPORT FOR RAILWAY")
    print("=" * 80)
    print("\nThis tool exports your Telegram session as an environment variable")
    print("for use in Railway.com or other cloud deployments.")
    print("-" * 80)

    # Validate config
    try:
        if not config.TELEGRAM_API_ID or not config.TELEGRAM_API_HASH:
            print("\n❌ ERROR: Missing Telegram API credentials in .env:")
            print("   - TELEGRAM_API_ID")
            print("   - TELEGRAM_API_HASH")
            print("\nGet them from: https://my.telegram.org/apps")
            sys.exit(1)

        if not config.TELEGRAM_PHONE:
            print("\n❌ ERROR: Missing TELEGRAM_PHONE in .env")
            print("   Format: +1234567890 (with country code)")
            sys.exit(1)

    except Exception as e:
        print(f"\n❌ Configuration error: {e}")
        sys.exit(1)

    try:
        session_string = await export_session()
        if session_string:
            save_session_string(session_string)
        else:
            print("\n❌ Failed to export session.")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n⚠️  Export cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Export failed: {e}")
        logger.exception("Session export error")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())