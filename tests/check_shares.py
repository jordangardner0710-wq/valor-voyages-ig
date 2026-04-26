import os
from dotenv import load_dotenv
import dropbox

load_dotenv()

dbx = dropbox.Dropbox(
    app_key=os.getenv("DROPBOX_APP_KEY"),
    app_secret=os.getenv("DROPBOX_APP_SECRET"),
    oauth2_refresh_token=os.getenv("DROPBOX_REFRESH_TOKEN"),
)

print("=== SHARED FOLDERS ACCESSIBLE TO THIS ACCOUNT ===")
try:
    res = dbx.sharing_list_folders()
    if not res.entries:
        print("  (none)")
    for f in res.entries:
        print(f"  {f.path_lower}  (id={f.shared_folder_id}, owned_by_me={f.access_type})")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n=== MOUNTED SHARED FOLDERS (path-relative) ===")
try:
    res = dbx.sharing_list_received_files()
    if not res.entries:
        print("  (none)")
    for f in res.entries[:20]:
        print(f"  {f.name}")
except Exception as e:
    print(f"  ERROR: {e}")
