import os
import re
from pathlib import Path
from dotenv import load_dotenv
from dropbox import DropboxOAuth2FlowNoRedirect

load_dotenv()

app_key = os.getenv("DROPBOX_APP_KEY")
app_secret = os.getenv("DROPBOX_APP_SECRET")

if not app_key or not app_secret:
    raise SystemExit("ERROR: DROPBOX_APP_KEY or DROPBOX_APP_SECRET missing in .env")

flow = DropboxOAuth2FlowNoRedirect(
    app_key,
    app_secret,
    token_access_type="offline",
)

print("\n1. Open this URL in your browser:")
print("   " + flow.start())
print("\n2. Click 'Allow' (log in to Dropbox if prompted).")
print("3. Copy the authorization code shown and paste below.\n")

auth_code = input("Paste the authorization code here: ").strip()

result = flow.finish(auth_code)

print(f"\n[OK] Refresh token received ({len(result.refresh_token)} chars)")

env_path = Path(".env")
contents = env_path.read_text(encoding="utf-8")
new_contents = re.sub(
    r"^DROPBOX_REFRESH_TOKEN=.*$",
    f"DROPBOX_REFRESH_TOKEN={result.refresh_token}",
    contents,
    flags=re.MULTILINE,
)
env_path.write_text(new_contents, encoding="utf-8")

print("[OK] .env updated automatically")
print("[OK] Now run: python tests\\test_dropbox.py")
