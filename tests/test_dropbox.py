import os
from dotenv import load_dotenv
import dropbox

load_dotenv()

dbx = dropbox.Dropbox(
    app_key=os.getenv("DROPBOX_APP_KEY"),
    app_secret=os.getenv("DROPBOX_APP_SECRET"),
    oauth2_refresh_token=os.getenv("DROPBOX_REFRESH_TOKEN"),
)

folder = os.getenv("DROPBOX_FOLDER")
account = dbx.users_get_current_account()
print(f"[OK] Connected as: {account.name.display_name} ({account.email})")
print(f"[OK] Listing files in {folder}:")
result = dbx.files_list_folder(folder)
photos = [e for e in result.entries if hasattr(e, "size")]
print(f"     Found {len(photos)} files")
for entry in photos[:10]:
    print(f"     - {entry.name}  ({entry.size:,} bytes)")
if len(photos) > 10:
    print(f"     ... and {len(photos) - 10} more")
