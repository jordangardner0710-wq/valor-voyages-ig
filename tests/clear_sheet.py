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

# Delete all data rows (keep header)
records = ws.get_all_records()
for _ in range(len(records)):
    ws.delete_rows(2)
print(f"[OK] Cleared {len(records)} rows from Sheet")
