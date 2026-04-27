"""
Ingest: list files in Dropbox /Instagram Automation/raw, append new ones to the Sheet
as status=pending_metadata. Idempotent - matching by file_path (lowercase).
"""

import os
import io
from datetime import datetime, timezone
from dotenv import load_dotenv

import dropbox
from dropbox.files import FileMetadata
import gspread
from google.oauth2.service_account import Credentials
from PIL import Image
import piexif

load_dotenv()

DROPBOX_FOLDER = os.getenv("DROPBOX_FOLDER")
SHEET_ID = os.getenv("SHEET_ID")
SHEET_TAB = os.getenv("SHEET_TAB")

COLUMNS = [
    "file_name", "file_link", "media_type", "caption", "hashtags",
    "status", "scheduled_date", "notes", "created_at", "group_id",
    "date_taken", "gps_lat", "gps_lon", "location_text", "file_path", "post_id",
]

IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".heic")


def get_dropbox_client():
    return dropbox.Dropbox(
        app_key=os.getenv("DROPBOX_APP_KEY"),
        app_secret=os.getenv("DROPBOX_APP_SECRET"),
        oauth2_refresh_token=os.getenv("DROPBOX_REFRESH_TOKEN"),
    )


def get_sheets_worksheet():
    creds = Credentials.from_service_account_file(
        os.getenv("GOOGLE_SA_JSON_PATH"),
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID).worksheet(SHEET_TAB)


def list_dropbox_photos(dbx):
    photos = []
    res = dbx.files_list_folder(DROPBOX_FOLDER)
    while True:
        for entry in res.entries:
            if isinstance(entry, FileMetadata) and entry.name.lower().endswith(IMAGE_EXTS):
                photos.append(entry)
        if not res.has_more:
            break
        res = dbx.files_list_folder_continue(res.cursor)
    return photos


def get_existing_paths(ws):
    rows = ws.get_all_records()
    return {(row.get("file_path") or "").strip().lower() for row in rows}


def get_or_create_shared_link(dbx, path):
    try:
        return dbx.sharing_create_shared_link_with_settings(path).url
    except dropbox.exceptions.ApiError as e:
        if "shared_link_already_exists" in str(e):
            existing = dbx.sharing_list_shared_links(path=path, direct_only=True).links
            if existing:
                return existing[0].url
        return ""


def extract_exif(image_bytes):
    date_taken, gps_lat, gps_lon = "", "", ""
    try:
        img = Image.open(io.BytesIO(image_bytes))
        exif_blob = img.info.get("exif", b"")
        if not exif_blob:
            return date_taken, gps_lat, gps_lon
        exif = piexif.load(exif_blob)
        dt = exif.get("Exif", {}).get(piexif.ExifIFD.DateTimeOriginal)
        if dt:
            date_taken = dt.decode("utf-8", errors="ignore")
        gps = exif.get("GPS", {})
        if gps:
            def to_decimal(coord, ref):
                d = coord[0][0] / coord[0][1]
                m = coord[1][0] / coord[1][1]
                s = coord[2][0] / coord[2][1]
                v = d + m / 60 + s / 3600
                return -v if ref in ("S", "W") else v
            lat = gps.get(piexif.GPSIFD.GPSLatitude)
            lat_ref = (gps.get(piexif.GPSIFD.GPSLatitudeRef, b"N") or b"N").decode()
            if lat:
                gps_lat = f"{to_decimal(lat, lat_ref):.6f}"
            lon = gps.get(piexif.GPSIFD.GPSLongitude)
            lon_ref = (gps.get(piexif.GPSIFD.GPSLongitudeRef, b"E") or b"E").decode()
            if lon:
                gps_lon = f"{to_decimal(lon, lon_ref):.6f}"
    except Exception:
        pass
    return date_taken, gps_lat, gps_lon


def compute_group_id(date_taken):
    if not date_taken:
        return ""
    try:
        return date_taken.split()[0].replace(":", "-")
    except Exception:
        return ""


def main():
    print("=== Valor Voyages: ingest ===")
    dbx = get_dropbox_client()
    ws = get_sheets_worksheet()
    print(f"Watching: {DROPBOX_FOLDER}")
    print(f"Sheet:    {ws.spreadsheet.title} / {ws.title}")

    photos = list_dropbox_photos(dbx)
    print(f"\nDropbox: {len(photos)} image files")

    existing = get_existing_paths(ws)
    print(f"Sheet:   {len(existing)} rows tracked")

    new_photos = [p for p in photos if p.path_lower not in existing]
    print(f"\nNew to ingest: {len(new_photos)}")
    if not new_photos:
        return

    rows = []
    for i, photo in enumerate(new_photos, 1):
        print(f"  [{i}/{len(new_photos)}] {photo.name}")
        link = get_or_create_shared_link(dbx, photo.path_lower)
        if photo.size <= 25 * 1024 * 1024:
            try:
                _, resp = dbx.files_download(photo.path_lower)
                date_taken, gps_lat, gps_lon = extract_exif(resp.content)
            except Exception as e:
                print(f"    [warn] EXIF read failed: {e}")
                date_taken, gps_lat, gps_lon = "", "", ""
        else:
            date_taken, gps_lat, gps_lon = "", "", ""
        row = {
            "file_name": photo.name,
            "file_link": link,
            "media_type": "image",
            "caption": "",
            "hashtags": "",
            "status": "pending_metadata",
            "scheduled_date": "",
            "notes": "",
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "group_id": compute_group_id(date_taken),
            "date_taken": date_taken,
            "gps_lat": gps_lat,
            "gps_lon": gps_lon,
            "location_text": "",
            "file_path": photo.path_lower,
            "post_id": "",
        }
        rows.append(row)

    print(f"\nAppending {len(rows)} rows to Sheet...")
    values = [[r.get(c, "") for c in COLUMNS] for r in rows]
    ws.append_rows(values, value_input_option="USER_ENTERED")
    print("[OK] Done.")


if __name__ == "__main__":
    main()
