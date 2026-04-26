import os
from dotenv import load_dotenv
load_dotenv()

vars_to_check = ["DROPBOX_APP_KEY", "DROPBOX_APP_SECRET", "DROPBOX_REFRESH_TOKEN", "DROPBOX_FOLDER"]
for v in vars_to_check:
    val = os.getenv(v)
    if val is None or val == "":
        print(f"  {v}: EMPTY OR MISSING")
    elif v == "DROPBOX_FOLDER":
        print(f"  {v}: {val!r}  (len={len(val)})")
    elif len(val) < 6:
        print(f"  {v}: TOO SHORT ({len(val)} chars) - value={val!r}")
    else:
        print(f"  {v}: len={len(val)}  starts={val[:4]!r}  ends={val[-4:]!r}")
