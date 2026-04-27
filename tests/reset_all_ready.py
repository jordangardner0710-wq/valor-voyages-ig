import os
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

load_dotenv()
creds = Credentials.from_service_account_file(
    os.getenv("GOOGLE_SA_JSON_PATH"),
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
)
gc = gspread.authorize(creds)
ws = gc.open_by_key(os.getenv("SHEET_ID")).worksheet(os.getenv("SHEET_TAB"))

records = ws.get_all_records()
flipped = 0
for i, row in enumerate(records, start=2):
    if (row.get("status") or "").strip() in ("ready", "metadata_ready"):
        ws.update_acell(f"F{i}", "pending_metadata")
        flipped += 1
print(f"[OK] Reset {flipped} row(s) -> pending_metadata")
