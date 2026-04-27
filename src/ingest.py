"""
Ingest: list new files in Dropbox /Instagram Automation/raw, group them by
upload time gaps (>60 min apart = different batch), append each batch as
its own group_id to the Sheet.
"""

import os
import io
import json
import time
import shutil
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

import dropbox
from dropbox.files import FileMetadata
import gspread
from google.oauth2.service_account import Credentials
from PIL import Image
import piexif
import requests

load_dotenv()

DROPBOX_FOLDER = os.getenv("DROPBOX_FOLDER")
SHEET_ID = os.getenv("SHEET_ID")
SHEET_TAB = os.getenv("SHEET_TAB")
BATCH_GAP_MINUTES = int(os.getenv("BATCH_GAP_MINUTES", "60"))

COLUMNS = [
    "file_name", "file_link", "media_type", "caption", "hashtags",
    "status", "scheduled_date", "notes", "created_at", "group_id",
    "date_taken", "gps_lat", "gps_lon", "location_text", "file_path", "post_id",
]

IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".heic")


def find_exiftool():
    if shutil.which("exiftool"):
        return "exiftool"
    for p in [r"C:\ExifTool\exiftool.exe", r"C:\Program Files\ExifTool\exiftool.exe"]:
        if os.path.isfile(p):
            return p
    return None


EXIFTOOL = find_exiftool()


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
    return {(r.get("file_path") or "").strip().lower() for r in rows}


def split_into_batches(photos, gap_minutes):
    """Sort by upload time; split into batches when gap > gap_minutes."""
    if not photos:
        return []
    sorted_photos = sorted(photos, key=lambda p: p.client_modified or p.server_modified)
    batches = [[sorted_photos[0]]]
    for photo in sorted_photos[1:]:
        prev = batches[-1][-1]
        prev_t = prev.client_modified or prev.server_modified
        cur_t = photo.client_modified or photo.server_modified
        gap = (cur_t - prev_t).total_seconds() / 60
        if gap > gap_minutes:
            batches.append([photo])
        else:
            batches[-1].append(photo)
    return batches


def get_or_create_shared_link(dbx, path):
    try:
        return dbx.sharing_create_shared_link_with_settings(path).url
    except dropbox.exceptions.ApiError as e:
        if "shared_link_already_exists" in str(e):
            existing = dbx.sharing_list_shared_links(path=path, direct_only=True).links
            if existing:
                return existing[0].url
        return ""


def extract_exif_exiftool(image_bytes):
    if not EXIFTOOL:
        return None
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as tmp:
            tmp.write(image_bytes)
            tmp_path = tmp.name
        result = subprocess.run(
            [EXIFTOOL, "-j", "-n",
             "-DateTimeOriginal", "-CreateDate",
             "-GPSLatitude", "-GPSLongitude",
             tmp_path],
            capture_output=True, timeout=30, check=False,
        )
        if result.returncode != 0 or not result.stdout:
            return None
        data = json.loads(result.stdout)
        if not data:
            return None
        meta = data[0]
        date_taken = str(meta.get("DateTimeOriginal") or meta.get("CreateDate") or "")
        gps_lat = meta.get("GPSLatitude")
        gps_lon = meta.get("GPSLongitude")
        gps_lat_str = f"{float(gps_lat):.6f}" if gps_lat not in (None, "", 0) else ""
        gps_lon_str = f"{float(gps_lon):.6f}" if gps_lon not in (None, "", 0) else ""
        return date_taken, gps_lat_str, gps_lon_str
    except Exception as e:
        print(f"    [warn] exiftool error: {e}")
        return None
    finally:
        if tmp_path:
            try:
                Path(tmp_path).unlink()
            except Exception:
                pass


def extract_exif_piexif(image_bytes):
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


def extract_exif(image_bytes):
    result = extract_exif_exiftool(image_bytes)
    if result is not None and (result[0] or result[1] or result[2]):
        return result
    return extract_exif_piexif(image_bytes)


def reverse_geocode(lat, lon):
    if not lat or not lon:
        return ""
    try:
        time.sleep(1.1)
        r = requests.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={"format": "json", "lat": lat, "lon": lon,
                    "zoom": 14, "addressdetails": 1},
            headers={"User-Agent": "valor-voyages-ig/1.0"},
            timeout=15,
        )
        if r.status_code != 200:
            return ""
        data = r.json()
        addr = data.get("address", {})
        parts = []
        for key in ("attraction", "tourism", "leisure", "natural", "park",
                    "neighbourhood", "suburb", "village", "town", "city"):
            if key in addr and addr[key] not in parts:
                parts.append(addr[key])
                break
        for key in ("state", "region", "state_district"):
            if key in addr and addr[key] not in parts:
                parts.append(addr[key])
                break
        if "country" in addr:
            parts.append(addr["country"])
        if parts:
            return ", ".join(parts)
        return (data.get("display_name") or "")[:120]
    except Exception as e:
        print(f"    [warn] reverse geocode failed: {e}")
        return ""


def process_batch(dbx, ws, batch_photos, batch_id):
    """Process a single batch: extract EXIF, borrow GPS, geocode, append rows."""
    rows = []
    for i, photo in enumerate(batch_photos, 1):
        print(f"    [{i}/{len(batch_photos)}] {photo.name}")
        link = get_or_create_shared_link(dbx, photo.path_lower)
        if photo.size <= 25 * 1024 * 1024:
            try:
                _, resp = dbx.files_download(photo.path_lower)
                date_taken, gps_lat, gps_lon = extract_exif(resp.content)
            except Exception as e:
                print(f"      [warn] EXIF: {e}")
                date_taken, gps_lat, gps_lon = "", "", ""
        else:
            date_taken, gps_lat, gps_lon = "", "", ""

        if gps_lat and gps_lon:
            print(f"      gps: {gps_lat}, {gps_lon}")

        rows.append({
            "file_name": photo.name,
            "file_link": link,
            "media_type": "image",
            "caption": "",
            "hashtags": "",
            "status": "pending_metadata",
            "scheduled_date": "",
            "notes": "",
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "group_id": batch_id,
            "date_taken": date_taken,
            "gps_lat": gps_lat,
            "gps_lon": gps_lon,
            "location_text": "",
            "file_path": photo.path_lower,
            "post_id": "",
        })

    # Borrow GPS within the batch
    donor_lat = donor_lon = donor_name = ""
    for r in rows:
        if r["gps_lat"] and r["gps_lon"]:
            donor_lat, donor_lon, donor_name = r["gps_lat"], r["gps_lon"], r["file_name"]
            break
    if donor_lat:
        for r in rows:
            if not r["gps_lat"]:
                r["gps_lat"] = donor_lat
                r["gps_lon"] = donor_lon
                r["notes"] = f"gps borrowed from {donor_name}"

    # Geocode once for the batch
    place = reverse_geocode(donor_lat, donor_lon) if donor_lat else ""
    if place:
        print(f"      place: {place}")
        for r in rows:
            r["location_text"] = place

    return rows


def main():
    print("=== Valor Voyages: ingest ===")
    print(f"exiftool: {EXIFTOOL or 'NOT FOUND - falling back to piexif'}")
    print(f"Batch gap: {BATCH_GAP_MINUTES} min")
    dbx = get_dropbox_client()
    ws = get_sheets_worksheet()

    photos = list_dropbox_photos(dbx)
    print(f"\nDropbox: {len(photos)} image files")
    existing_paths = get_existing_paths(ws)
    print(f"Sheet:   {len(existing_paths)} rows tracked")

    new_photos = [p for p in photos if p.path_lower not in existing_paths]
    print(f"New:     {len(new_photos)} photos")
    if not new_photos:
        return

    batches = split_into_batches(new_photos, BATCH_GAP_MINUTES)
    print(f"Batches: {len(batches)} (split by upload-time gaps > {BATCH_GAP_MINUTES} min)\n")

    all_rows = []
    for i, batch_photos in enumerate(batches, 1):
        batch_id = "batch-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        if len(batches) > 1:
            batch_id += f"-{i}"
        print(f"  Batch {i}/{len(batches)} (id={batch_id}, {len(batch_photos)} photos):")
        rows = process_batch(dbx, ws, batch_photos, batch_id)
        all_rows.extend(rows)

    print(f"\nAppending {len(all_rows)} rows to Sheet...")
    values = [[r.get(c, "") for c in COLUMNS] for r in all_rows]
    ws.append_rows(values, value_input_option="USER_ENTERED")
    print("[OK] Done.")


if __name__ == "__main__":
    main()
