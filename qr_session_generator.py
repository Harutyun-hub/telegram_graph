#!/usr/bin/env python3
"""
Working QR code-based Telegram session generator.
This will generate a QR code that you can scan with your Telegram app.
"""

import asyncio
import os
import sys
from dotenv import load_dotenv

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    import qrcode
except ImportError:
    print("Installing required packages...")
    os.system("pip install telethon qrcode[pil]")
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    import qrcode

# Load environment variables
load_dotenv()

api_id = int(os.getenv('TELEGRAM_API_ID'))
api_hash = os.getenv('TELEGRAM_API_HASH')

if not api_id or not api_hash:
    print("❌ Error: Set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env file")
    sys.exit(1)

print("="*60)
print("📱 TELEGRAM SESSION GENERATOR - QR CODE METHOD")
print("="*60)
print(f"API ID: {api_id}")
print("="*60)

async def main():
    # Start with an empty string session for new authentication
    client = TelegramClient(StringSession(), api_id, api_hash)

    await client.connect()

    if not await client.is_user_authorized():
        print("\n⏳ Requesting QR Code...")

        # Request QR code for login
        qr_login = await client.qr_login()

        # Generate QR code
        qr = qrcode.QRCode(border=2)
        qr.add_data(qr_login.url)
        qr.make(fit=True)

        # Display in terminal
        print("\n📱 SCAN THIS QR CODE WITH TELEGRAM:")
        print("="*60)
        qr.print_ascii(invert=True)

        # Also save as image
        img = qr.make_image(fill_color="black", back_color="white")
        img.save("telegram_qr.png")
        print("\n💾 QR code also saved as: telegram_qr.png")
        print("   (Open this image if terminal QR doesn't work)")

        print("\n📲 TO SCAN:")
        print("1. Open Telegram on your phone")
        print("2. Go to Settings → Devices → Link Desktop Device")
        print("3. Point camera at QR code above")
        print("4. Confirm on your phone")
        print("\n⏳ Waiting for scan (expires in 2 minutes)...")

        # Wait for login
        try:
            await asyncio.wait_for(qr_login.wait(), timeout=120)
        except asyncio.TimeoutError:
            print("\n❌ QR code expired! Run the script again.")
            await client.disconnect()
            return

        print("\n✅ Successfully logged in!")

    # Get session string
    session_string = StringSession.save(client.session)

    # Get user info
    me = await client.get_me()
    print(f"\n👤 Logged in as: {me.first_name} {me.last_name or ''}")
    if me.username:
        print(f"📱 Username: @{me.username}")
    print(f"🆔 User ID: {me.id}")

    # Save session string to file
    with open("railway_session_string.txt", "w") as f:
        f.write(session_string)

    # Also save as regular session for backup
    file_client = TelegramClient("telegram_session_railway", api_id, api_hash)
    file_client.session.set_dc(client.session.dc_id,
                               client.session.server_address,
                               client.session.port)
    file_client.session.auth_key = client.session.auth_key
    file_client.session.save()
    await file_client.disconnect()

    print("\n" + "="*70)
    print("✅ SESSION STRING FOR RAILWAY:")
    print("="*70)
    print(session_string)
    print("="*70)

    print("\n📁 Files created:")
    print("  • railway_session_string.txt (use this for Railway)")
    print("  • telegram_session_railway.session (backup)")

    print("\n🚀 ADD TO RAILWAY:")
    print("1. Copy the session string above")
    print("2. Go to Railway → Variables")
    print("3. Add: TELEGRAM_SESSION_STRING = [paste string]")
    print("4. Redeploy")

    # Clean up QR image
    if os.path.exists("telegram_qr.png"):
        os.remove("telegram_qr.png")

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())