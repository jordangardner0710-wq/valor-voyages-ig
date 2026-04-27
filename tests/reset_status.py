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
ws.update_acell("F2", "pending_metadata")
print("[OK] Row 2 status -> pending_metadata")
