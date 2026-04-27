"""
Mark one or more rows as scheduled.
Usage:
  python tests\mark_scheduled.py 2
  python tests\mark_scheduled.py 2 3 4 5
"""

import sys
import os
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

load_dotenv()

if len(sys.argv) < 2:
    print("Usage: python tests\\mark_scheduled.py <row_number> [row_number ...]")
    sys.exit(1)

creds = Credentials.from_service_account_file(
    os.getenv("GOOGLE_SA_JSON_PATH"),
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
)
gc = gspread.authorize(creds)
ws = gc.open_by_key(os.getenv("SHEET_ID")).worksheet(os.getenv("SHEET_TAB"))

for arg in sys.argv[1:]:
    try:
        row_num = int(arg)
    except ValueError:
        print(f"[skip] not a number: {arg}")
        continue
    ws.update_acell(f"F{row_num}", "scheduled")
    print(f"[OK] row {row_num} -> scheduled")
