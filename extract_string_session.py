#!/usr/bin/env python3
"""
Extract string session from existing session file without connecting to Telegram.
"""

import os
import sys
from pathlib import Path

try:
    from telethon.sessions import SQLiteSession, StringSession
except ImportError:
    print("❌ Error: telethon not installed. Run: pip install telethon")
    sys.exit(1)

print("="*70)
print("📋 EXTRACTING STRING SESSION FROM FILE")
print("="*70)

# Session file to convert
session_file = 'telegram_session_prod_new.session'

if not os.path.exists(session_file):
    print(f"❌ Error: {session_file} not found!")
    print("\nAvailable session files:")
    for f in Path('.').glob('*.session'):
        print(f"  - {f}")
    sys.exit(1)

print(f"📂 Reading: {session_file}")

try:
    # Load the SQLite session directly
    sqlite_session = SQLiteSession(session_file.replace('.session', ''))

    # Convert to string session format
    # This reads the auth_key directly from the SQLite database
    auth_key = sqlite_session.auth_key
    dc_id = sqlite_session.dc_id
    server_address = sqlite_session.server_address
    port = sqlite_session.port

    if not auth_key:
        print("❌ No auth_key found in session file!")
        print("The session might be empty or corrupted.")
        sys.exit(1)

    # Create a string session from the components
    string_session = StringSession()
    string_session.auth_key = auth_key
    string_session.dc_id = dc_id
    string_session.server_address = server_address
    string_session.port = port

    # Save the string session
    session_string = string_session.save()

    # Save to file
    output_file = 'telegram_session_prod_new_string.txt'
    with open(output_file, 'w') as f:
        f.write(session_string)

    print(f"\n✅ String session extracted successfully!")
    print(f"💾 Saved to: {output_file}")

    print("\n" + "="*70)
    print("📋 SESSION STRING FOR RAILWAY:")
    print("="*70)
    print(session_string)
    print("="*70)

    print("\n🚀 RAILWAY DEPLOYMENT STEPS:")
    print("1. Copy the session string above")
    print("2. Go to Railway → Your Project → Variables tab")
    print("3. Add new variable:")
    print("   • Name:  TELEGRAM_SESSION_STRING")
    print("   • Value: [paste the session string]")
    print("4. Redeploy your Railway service")

    print("\n⚠️  IMPORTANT:")
    print("• This session is for Railway ONLY")
    print("• Don't run local scraper with this session")
    print("• Keep the string secure")

    # Close the session
    sqlite_session.close()

except Exception as e:
    print(f"❌ Error extracting session: {e}")
    print("\nPossible issues:")
    print("1. Session file might be corrupted")
    print("2. Session might not be fully authenticated")
    print("3. Try generating a new session")
    sys.exit(1)