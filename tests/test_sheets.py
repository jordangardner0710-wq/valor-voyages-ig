import os
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

load_dotenv()

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

creds = Credentials.from_service_account_file(
    os.getenv("GOOGLE_SA_JSON_PATH"),
    scopes=scopes,
)

gc = gspread.authorize(creds)

sheet_id = os.getenv("SHEET_ID")
tab = os.getenv("SHEET_TAB")

ss = gc.open_by_key(sheet_id)
print(f"[OK] Opened spreadsheet: {ss.title}")

ws = ss.worksheet(tab)
print(f"[OK] Worksheet: {ws.title}  ({ws.row_count} rows x {ws.col_count} cols)")

headers = ws.row_values(1)
print(f"\nHeaders ({len(headers)}):")
for i, h in enumerate(headers, 1):
    print(f"  {i:2d}. {h}")

all_rows = ws.get_all_values()
data_rows = max(0, len(all_rows) - 1)
print(f"\nData rows: {data_rows}")
if data_rows:
    print(f"\nFirst data row preview:")
    for h, v in zip(headers, all_rows[1]):
        v_short = v[:60] + ("..." if len(v) > 60 else "")
        print(f"  {h}: {v_short}")
