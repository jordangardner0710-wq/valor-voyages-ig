import os
from dotenv import load_dotenv
import dropbox
from dropbox.files import FolderMetadata, FileMetadata

load_dotenv()

dbx = dropbox.Dropbox(
    app_key=os.getenv("DROPBOX_APP_KEY"),
    app_secret=os.getenv("DROPBOX_APP_SECRET"),
    oauth2_refresh_token=os.getenv("DROPBOX_REFRESH_TOKEN"),
)

def walk(path, depth=0):
    if depth > 2:
        return
    try:
        result = dbx.files_list_folder(path)
    except Exception as e:
        print("  " * depth + f"[error listing {path}]: {e}")
        return
    for entry in result.entries:
        prefix = "  " * depth
        if isinstance(entry, FolderMetadata):
            print(f"{prefix}[DIR]  {entry.path_display}")
            walk(entry.path_lower, depth + 1)
        else:
            print(f"{prefix}[FILE] {entry.path_display}")

print("=== Walking your Dropbox (max depth 3) ===")
walk("")
