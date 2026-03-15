#!/usr/bin/env python3
"""
Direct session export - reads existing session file and converts to string.
"""

import sys
import sqlite3
import base64
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from telethon.sessions import StringSession


def export_existing_session():
    """Export existing session file to string format."""

    session_file = Path(f"{config.TELEGRAM_SESSION_NAME}.session")

    if not session_file.exists():
        print(f"❌ Session file not found: {session_file}")
        return None

    print(f"📂 Reading session file: {session_file}")

    try:
        # Connect to SQLite database
        conn = sqlite3.connect(session_file, timeout=1.0)
        cursor = conn.cursor()

        # Get auth key and data center info
        cursor.execute("SELECT auth_key, dc_id, server_address, port FROM sessions")
        result = cursor.fetchone()

        if not result:
            print("❌ No session data found in file")
            conn.close()
            return None

        auth_key, dc_id, server_address, port = result

        if not auth_key:
            print("❌ No auth key found in session")
            conn.close()
            return None

        print(f"✅ Found session: DC {dc_id}, Server {server_address}:{port}")

        # Create a StringSession and set the auth key
        string_session = StringSession()
        string_session.set_dc(dc_id, server_address, port)
        string_session.auth_key = auth_key

        # Save to string
        session_string = string_session.save()

        conn.close()

        print(f"✅ Successfully exported session")
        print(f"📏 Session string length: {len(session_string)} characters")

        return session_string

    except sqlite3.OperationalError as e:
        print(f"❌ Database error: {e}")
        print("\nTry closing any applications using the session file:")
        print("  - Stop any running Python scripts")
        print("  - Close any IDEs with the project open")
        print("  - Try: fuser telegram_scraper.session (to see what's using it)")
        return None
    except Exception as e:
        print(f"❌ Failed to read session: {e}")
        return None


def save_session_string(session_string: str):
    """Save session string and display instructions."""

    # Save to local file (gitignored)
    env_file = Path(".env.telegram_session")
    with open(env_file, "w") as f:
        f.write(f"# Telegram Session String for Railway Deployment\n")
        f.write(f"# DO NOT COMMIT THIS FILE TO GIT\n\n")
        f.write(f"TELEGRAM_SESSION_STRING={session_string}\n")

    print("\n" + "=" * 80)
    print("🎉 SESSION EXPORT SUCCESSFUL!")
    print("=" * 80)

    print("\n📋 RAILWAY DEPLOYMENT STEPS:")
    print("-" * 80)

    print("\n1️⃣  Copy this environment variable to Railway:\n")
    print("   Variable Name:  TELEGRAM_SESSION_STRING")
    print(f"   Variable Value: {session_string}")

    print("\n2️⃣  In Railway Dashboard:")
    print("   • Go to your project")
    print("   • Click on your service")
    print("   • Go to 'Variables' tab")
    print("   • Add New Variable")
    print("   • Paste the name and value above")
    print("   • Deploy")

    print(f"\n3️⃣  Session also saved to: {env_file.absolute()}")

    print("\n⚠️  SECURITY NOTES:")
    print("   • This grants full Telegram account access")
    print("   • Never commit to Git or share publicly")
    print("   • Revoke via: Telegram → Settings → Devices")

    print("\n✅ Ready for Railway deployment!")
    print("=" * 80)


def main():
    print("\n" + "=" * 80)
    print("🚀 DIRECT TELEGRAM SESSION EXPORT")
    print("=" * 80)
    print("\nExporting existing session file to environment variable...")
    print("-" * 80)

    session_string = export_existing_session()

    if session_string:
        save_session_string(session_string)
    else:
        print("\n❌ Export failed")
        print("\nAlternative: Create a new session")
        print("  1. Temporarily rename telegram_scraper.session")
        print("  2. Run: python scripts/export_telegram_session.py")
        print("  3. Authenticate with QR code")
        sys.exit(1)


if __name__ == "__main__":
    main()