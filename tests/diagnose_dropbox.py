import os
from dotenv import load_dotenv
import dropbox

load_dotenv()

dbx = dropbox.Dropbox(
    app_key=os.getenv("DROPBOX_APP_KEY"),
    app_secret=os.getenv("DROPBOX_APP_SECRET"),
    oauth2_refresh_token=os.getenv("DROPBOX_REFRESH_TOKEN"),
)

print("=== ACCOUNT INFO ===")
account = dbx.users_get_current_account()
print(f"Display name: {account.name.display_name}")
print(f"Email: {account.email}")
print(f"Account ID: {account.account_id}")
print(f"Has team: {getattr(account, 'team', None) is not None}")
print(f"Team member ID: {getattr(account, 'team_member_id', None)}")

print("\n=== RECURSIVE ROOT LIST (first 30) ===")
try:
    result = dbx.files_list_folder("", recursive=True, limit=30)
    if not result.entries:
        print("  (no entries returned at all)")
    for entry in result.entries:
        kind = "DIR " if not hasattr(entry, "size") else "FILE"
        print(f"  [{kind}] {entry.path_display}")
    if result.has_more:
        print("  ... has more")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n=== TRYING PATH VARIATIONS ===")
for p in [
    "/Instagram Automation",
    "/Instagram Automation/raw",
    "/instagram automation/raw",
    "/Apps",
    "/Apps/Instagram Automation/raw",
]:
    try:
        r = dbx.files_list_folder(p)
        print(f"  OK    {p!r}  ({len(r.entries)} entries)")
    except Exception as e:
        err = str(e).split("ApiError")[-1][:100]
        print(f"  FAIL  {p!r}  -> {err}")

print("\n=== SEARCH FOR 'Instagram' ANYWHERE ===")
try:
    res = dbx.files_search_v2("Instagram")
    matches = res.matches[:10]
    if not matches:
        print("  (no matches)")
    for m in matches:
        print(f"  {m.metadata.get_metadata().path_display}")
except Exception as e:
    print(f"  ERROR: {e}")
