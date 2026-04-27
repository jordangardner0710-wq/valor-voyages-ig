"""
Caption: pull rows where status=pending_metadata, group by group_id, generate
ONE caption per group via OpenAI vision, apply to all rows, then move all the
group's photos from /raw to /processed/<group_id>/ and write a caption.txt.

Archive step is non-fatal: if the move fails, captions are still saved to the
Sheet so we don't lose work.
"""

import os
import io
import json
import base64
from collections import defaultdict
from dotenv import load_dotenv

import dropbox
from dropbox.exceptions import ApiError
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI

load_dotenv()

SHEET_ID = os.getenv("SHEET_ID")
SHEET_TAB = os.getenv("SHEET_TAB")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
RAW_FOLDER = os.getenv("DROPBOX_FOLDER", "/Instagram Automation/raw")
PROCESSED_ROOT = RAW_FOLDER.rsplit("/", 1)[0] + "/processed"

SYSTEM_PROMPT = """You are writing one Instagram caption for Valor Voyages, a full-time RV family traveling the US.

Family context:
- Jordan (29, dad), Kayla (28, mom), Jemma (9, daughter)
- Full-time RVers in their Alliance fifth-wheel pulled by an F350
- Casual, conversational, real-life update voice

VOICE EXAMPLES (real prior posts):
"Seal Rock, Oregon. Wind finally died down enough to take out the drone!"
"SkyTrail through giant Redwood and Sequoias!"
"Cannon Beach, Oregon. Tide was way out this morning, walked all the way to the rocks."

LOCATION (CRITICAL):
- If location is provided, you MUST mention it in the caption.
- Preferred opener: "Place, State." then the observation.
- If location is missing, do NOT name a place.

PEOPLE (CRITICAL):
- Look at all images, count people, identify family.
- 1 woman = likely Kayla. 1 child = likely Jemma. Woman + child = Kayla and Jemma.
- ONLY name people clearly visible. NEVER infer from footprints or shadows.
- No people visible = describe scene, no family names.

RULES:
- 1-3 sentences. Short is good. Don't pad.
- Casual voice. Not poetic. Not Travel Channel.
- "We" first-person plural is the default.
- 0-2 exclamation points; sparingly.

BANNED phrases:
- "vibes", "soaking in/up", "taking in", "drinking in"
- "wanderlust", "happy place", "living my best life"
- "adventure awaits", "making memories", "memories forever"
- "moments like these", "love these walks/days"
- "what a beautiful day", "feeling blessed"
- "footprints in the sand", "stunning sunset"
- ANY abstract emotion summary instead of a concrete observation

HASHTAGS:
- 5-10, lowercase, no spaces
- ALWAYS include: #rvlife AND #fulltimerv
- Location-specific: 1-2 (verify spelling)
- When relevant: #alliance #f350
- Subject/activity tags

Respond ONLY with JSON: {"caption": "...", "hashtags": "#tag1 #tag2"}
"""


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


def fetch_pending_grouped(ws):
    records = ws.get_all_records()
    groups = defaultdict(list)
    for i, row in enumerate(records, start=2):
        if (row.get("status") or "").strip() == "pending_metadata":
            gid = (row.get("group_id") or "").strip() or f"_solo_{i}"
            groups[gid].append((i, row))
    return dict(groups)


def download_image(dbx, path):
    _, response = dbx.files_download(path)
    return response.content


def caption_group(client, image_bytes_list, context, total_count):
    image_blocks = []
    for img_bytes in image_bytes_list[:3]:
        b64 = base64.b64encode(img_bytes).decode()
        image_blocks.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{b64}", "detail": "high"},
        })

    parts = ["Photo metadata:"]
    if context.get("date_taken"):
        parts.append(f"- Taken: {context['date_taken']}")
    if context.get("location_text"):
        parts.append(f"- Location: {context['location_text']}  (YOU MUST mention this)")
    elif context.get("gps_lat") and context.get("gps_lon"):
        parts.append(f"- GPS: {context['gps_lat']}, {context['gps_lon']}")
    else:
        parts.append("- Location: UNKNOWN - do NOT name a specific place")
    parts.append("")
    if total_count > 1:
        parts.append(f"This is a CAROUSEL of {total_count} photos. Generate ONE caption.")
    parts.append("Look at the photos, identify family visible, write the caption + hashtags.")
    user_text = "\n".join(parts)

    content = [{"type": "text", "text": user_text}] + image_blocks
    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": content},
        ],
        response_format={"type": "json_object"},
        max_tokens=400,
        temperature=0.7,
    )
    return json.loads(response.choices[0].message.content), response.usage


def archive_group(dbx, group_id, rows, caption, hashtags):
    """Move all photos in the group to /processed/<group_id>/ and write caption.txt.
    Raises on failure; caller must catch."""
    dest_folder = f"{PROCESSED_ROOT}/{group_id}"
    try:
        dbx.files_create_folder_v2(dest_folder)
    except ApiError as e:
        if "conflict" not in str(e):
            raise

    new_paths = {}
    for row_idx, row in rows:
        old_path = row.get("file_path", "")
        if not old_path:
            continue
        file_name = old_path.rsplit("/", 1)[-1]
        new_path = f"{dest_folder}/{file_name}"
        try:
            dbx.files_move_v2(old_path, new_path, autorename=True)
            new_paths[row_idx] = new_path.lower()
            print(f"    [moved] {file_name} -> {dest_folder}/")
        except ApiError as e:
            print(f"    [warn] move failed for {file_name}: {e}")

    caption_text = f"{caption}\n\n{hashtags}\n"
    try:
        dbx.files_upload(
            caption_text.encode("utf-8"),
            f"{dest_folder}/caption.txt",
            mode=dropbox.files.WriteMode.overwrite,
        )
        print(f"    [wrote] {dest_folder}/caption.txt")
    except Exception as e:
        print(f"    [warn] caption.txt write: {e}")

    return new_paths


def update_group_in_sheet(ws, rows, caption, hashtags, new_paths):
    for row_idx, row in rows:
        ws.update(range_name=f"D{row_idx}:F{row_idx}",
                  values=[[caption, hashtags, "ready"]])
        if row_idx in new_paths:
            ws.update_acell(f"O{row_idx}", new_paths[row_idx])


def main():
    print("=== Valor Voyages: caption ===")
    dbx = get_dropbox_client()
    ws = get_sheets_worksheet()
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    print(f"Model: {OPENAI_MODEL}")

    groups = fetch_pending_grouped(ws)
    total_rows = sum(len(rows) for rows in groups.values())
    print(f"Pending: {total_rows} rows in {len(groups)} group(s)")
    if not groups:
        return

    total_in = total_out = 0
    for gid, rows in groups.items():
        first_row = rows[0][1]
        print(f"\nGroup {gid}: {len(rows)} photo(s)")

        images = []
        for row_idx, row in rows[:3]:
            try:
                images.append(download_image(dbx, row.get("file_path", "")))
            except Exception as e:
                print(f"    [warn] download {row.get('file_name')}: {e}")

        if not images:
            print(f"    [skip] no images")
            continue

        try:
            data, usage = caption_group(client, images, first_row, total_count=len(rows))
            total_in += usage.prompt_tokens
            total_out += usage.completion_tokens
        except Exception as e:
            print(f"    [error] caption: {e}")
            continue

        caption = (data.get("caption") or "").strip()
        hashtags = (data.get("hashtags") or "").strip()
        print(f"    caption: {caption}")
        print(f"    tags:    {hashtags}")

        # Archive is non-fatal: if it fails, we still save the caption to Sheet
        new_paths = {}
        try:
            new_paths = archive_group(dbx, gid, rows, caption, hashtags)
        except Exception as e:
            print(f"    [warn] archive failed (caption still saved): {e}")

        # Always update Sheet — don't lose captions even if archiving broke
        try:
            update_group_in_sheet(ws, rows, caption, hashtags, new_paths)
            print(f"    [OK] {len(rows)} row(s) -> ready")
        except Exception as e:
            print(f"    [error] sheet update failed: {e}")

    in_cost = total_in / 1_000_000 * 2.50
    out_cost = total_out / 1_000_000 * 10.00
    print(f"\n[OK] Tokens: {total_in:,} in + {total_out:,} out")
    print(f"[OK] Cost: ${in_cost + out_cost:.4f}")


if __name__ == "__main__":
    main()
