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
print(f"Total rows: {len(records)}\n")
for i, row in enumerate(records, start=2):
    print(f"Row {i}: {row.get('file_name')}")
    print(f"  status:        {row.get('status')!r}")
    print(f"  date_taken:    {row.get('date_taken')!r}")
    print(f"  gps_lat:       {row.get('gps_lat')!r}")
    print(f"  gps_lon:       {row.get('gps_lon')!r}")
    print(f"  location_text: {row.get('location_text')!r}")
    print(f"  file_path:     {row.get('file_path')!r}")
    print()
