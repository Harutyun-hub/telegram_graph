#!/usr/bin/env python3
"""
Telegram session generator with QR code AND 2FA password support.
"""

import asyncio
import os
import sys
import getpass
from dotenv import load_dotenv

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.errors import SessionPasswordNeededError
    import qrcode
except ImportError:
    print("Installing required packages...")
    os.system("pip install telethon qrcode[pil]")
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.errors import SessionPasswordNeededError
    import qrcode

# Load environment variables
load_dotenv()

api_id = int(os.getenv('TELEGRAM_API_ID'))
api_hash = os.getenv('TELEGRAM_API_HASH')

if not api_id or not api_hash:
    print("❌ Error: Set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env file")
    sys.exit(1)

print("="*60)
print("📱 TELEGRAM SESSION GENERATOR WITH 2FA SUPPORT")
print("="*60)
print(f"API ID: {api_id}")
print("="*60)

async def main():
    # Start with an empty string session
    client = TelegramClient(StringSession(), api_id, api_hash)

    await client.connect()

    if not await client.is_user_authorized():
        print("\n⏳ Requesting QR Code...")

        try:
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

            print("\n📲 TO SCAN:")
            print("1. Open Telegram on your phone")
            print("2. Go to Settings → Devices → Link Desktop Device")
            print("3. Point camera at QR code")
            print("4. Confirm on your phone")
            print("\n⏳ Waiting for scan...")

            # Wait for QR scan
            await asyncio.wait_for(qr_login.wait(), timeout=120)

            print("\n✅ QR Code scanned successfully!")

        except SessionPasswordNeededError:
            print("\n🔐 Two-factor authentication is enabled on your account.")
            print("📝 You need to enter your 2FA password.")
            print("   (This is the password you set in Telegram → Settings → Privacy → Two-Step Verification)")

            # Prompt for 2FA password
            password = getpass.getpass("\n🔑 Enter your 2FA password: ")

            try:
                await client.sign_in(password=password)
                print("\n✅ 2FA authentication successful!")
            except Exception as e:
                print(f"\n❌ Wrong 2FA password: {e}")
                print("\n💡 Hints:")
                print("• This is NOT your phone lock password")
                print("• This is NOT a code sent by SMS")
                print("• This is the password you created in Telegram for Two-Step Verification")
                print("• If you forgot it, go to Telegram → Settings → Privacy → Two-Step Verification → Reset")
                await client.disconnect()
                return

        except asyncio.TimeoutError:
            print("\n❌ QR code expired! Run the script again.")
            await client.disconnect()
            return

        except Exception as e:
            # Check if it's a 2FA error after QR scan
            if "Two-steps verification is enabled" in str(e) or "SessionPasswordNeeded" in str(e.__class__.__name__):
                print("\n🔐 Your account has Two-Factor Authentication enabled.")
                print("📝 You need to enter your 2FA password after scanning the QR code.")

                password = getpass.getpass("\n🔑 Enter your 2FA password (the one you set in Telegram settings): ")

                try:
                    await client.sign_in(password=password)
                    print("\n✅ Successfully authenticated with 2FA!")
                except Exception as e2:
                    print(f"\n❌ Authentication failed: {e2}")
                    print("\n💡 Make sure you're entering the 2FA password you created in:")
                    print("   Telegram → Settings → Privacy → Two-Step Verification")
                    await client.disconnect()
                    return
            else:
                print(f"\n❌ Error: {e}")
                await client.disconnect()
                return

    # Check if we're logged in now
    if not await client.is_user_authorized():
        print("\n❌ Still not authorized. There might be an issue with authentication.")
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
    file_client = TelegramClient("telegram_session_railway_backup", api_id, api_hash)
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
    print("  • telegram_session_railway_backup.session (backup)")

    print("\n🚀 ADD TO RAILWAY:")
    print("1. Copy the session string above")
    print("2. Go to Railway → Variables")
    print("3. Add: TELEGRAM_SESSION_STRING = [paste string]")
    print("4. Redeploy")

    print("\n⚠️  IMPORTANT:")
    print("• Use this session ONLY on Railway")
    print("• Don't run local scraper while Railway is using it")

    # Clean up QR image
    if os.path.exists("telegram_qr.png"):
        os.remove("telegram_qr.png")

    await client.disconnect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n❌ Cancelled by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")