"""
Caption: pull rows where status=pending_metadata, group by group_id, generate ONE
caption per group via OpenAI vision (sees up to 3 representative images), apply
to all rows in the group. Transition status to "ready".
"""

import os
import json
import base64
from collections import defaultdict
from dotenv import load_dotenv

import dropbox
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI

load_dotenv()

DROPBOX_FOLDER = os.getenv("DROPBOX_FOLDER")
SHEET_ID = os.getenv("SHEET_ID")
SHEET_TAB = os.getenv("SHEET_TAB")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")

SYSTEM_PROMPT = """You are writing one Instagram caption for Valor Voyages, a full-time RV family traveling the US.

Family context:
- Jordan (29, dad) - usually the one taking photos
- Kayla (28, mom)
- Jemma (9, daughter)
- Full-time RVers in their Alliance fifth-wheel pulled by an F350
- Casual, conversational, real-life update voice

VOICE - captions sound like THIS (real prior posts):

Example 1:
"Seal Rock, Oregon. Wind finally died down enough to take out the drone!"

Example 2:
"SkyTrail through giant Redwood and Sequoias!"

Example 3:
"Cannon Beach, Oregon. Tide was way out this morning, walked all the way to the rocks."

Example 4:
"Okay! We got busy and forgot to post for a while! But quick update - we hooked up and left sunny Arizona to head to the rainy and humid Redwood Forest!"

Notice: short, casual, location often opens, exclamation points OK, conversational, NOT poetic.

LOCATION (CRITICAL):
- If location is provided, you MUST mention it in the caption.
- Preferred opener: "Place, State." then the observation.
- The provided location is authoritative.
- If location is missing, do NOT name a place.

PEOPLE IN THE PHOTOS (CRITICAL):
- You will see 1-3 representative photos from a carousel post (the post may contain more).
- Carefully look at the photos. Count how many people you see across the images.
- 1 person who looks like a woman = likely Kayla
- 1 person who looks like a child = likely Jemma
- woman + child together = Kayla and Jemma
- 2 adults + a child = all three
- ONLY name people clearly visible in the photos. NEVER infer from footprints, shadows, or implied presence.
- If no people visible in any of the photos: describe the scene/location, no family names.

GENERAL RULES:
- 1 to 3 sentences. Short is good. Do NOT pad.
- Casual update voice. Not poetry.
- "We" first-person plural is the default.
- 0-2 exclamation points; use sparingly.
- The caption must describe SOMETHING SPECIFIC, not generic emotion.

BANNED phrases (AI tells - never use):
- "vibes" of any kind
- "soaking in", "soaking up", "taking in", "drinking in"
- "wanderlust", "happy place", "living my best life"
- "adventure awaits", "making memories", "memories forever"
- "moments like these"
- "love these walks/days/moments"
- "what a beautiful day", "feeling blessed", "feeling grateful"
- "footprints in the sand", "wide open skies"
- ANY abstract emotion summary instead of a concrete observation

Hashtags:
- 5 to 10 hashtags total
- Lowercase, no spaces, no camelCase
- ALWAYS include: #rvlife AND #fulltimerv
- If location provided: 1-2 location-specific tags
- When relevant: #alliance #f350
- Subject/activity tags as relevant

Respond ONLY with JSON in this exact format:
{
  "caption": "The caption text here.",
  "hashtags": "#tag1 #tag2 #tag3"
}
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
    """Return dict: group_id -> list of (row_index, row_dict) for all pending rows."""
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
    """Generate one caption for a group of photos. Sends up to 3 representative images."""
    image_blocks = []
    for img_bytes in image_bytes_list[:3]:
        b64 = base64.b64encode(img_bytes).decode()
        image_blocks.append({
            "type": "image_url",
            "image_url": {
                "url": f"data:image/jpeg;base64,{b64}",
                "detail": "high",
            },
        })

    parts = ["Photo metadata:"]
    if context.get("date_taken"):
        parts.append(f"- Taken: {context['date_taken']}")
    if context.get("location_text"):
        parts.append(f"- Location: {context['location_text']}  (YOU MUST mention this in the caption)")
    elif context.get("gps_lat") and context.get("gps_lon"):
        parts.append(f"- GPS: {context['gps_lat']}, {context['gps_lon']}")
    else:
        parts.append("- Location: UNKNOWN - do NOT name a specific place")
    parts.append("")
    if total_count > 1:
        parts.append(f"This is a CAROUSEL post of {total_count} photos. You see up to 3 representative images. Generate ONE caption for the whole post.")
    parts.append("Look carefully at the photos, count people, identify family members visible, then write the caption + hashtags.")
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
    data = json.loads(response.choices[0].message.content)
    return data, response.usage


def update_group(ws, row_indices, caption, hashtags):
    """Apply same caption + hashtags + status=ready to all rows in a group."""
    for row_idx in row_indices:
        ws.update(
            range_name=f"D{row_idx}:F{row_idx}",
            values=[[caption, hashtags, "ready"]],
        )


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
        row_indices = [r[0] for r in rows]
        print(f"\nGroup {gid}: {len(rows)} photo(s)")

        # Download up to 3 representative photos
        images = []
        for row_idx, row in rows[:3]:
            try:
                images.append(download_image(dbx, row.get("file_path", "")))
            except Exception as e:
                print(f"    [warn] download {row.get('file_name')} failed: {e}")

        if not images:
            print(f"    [skip] no images downloaded")
            continue

        try:
            data, usage = caption_group(client, images, first_row, total_count=len(rows))
            total_in += usage.prompt_tokens
            total_out += usage.completion_tokens
        except Exception as e:
            print(f"    [error] caption failed: {e}")
            continue

        caption = (data.get("caption") or "").strip()
        hashtags = (data.get("hashtags") or "").strip()

        print(f"    caption: {caption}")
        print(f"    tags:    {hashtags}")

        update_group(ws, row_indices, caption, hashtags)
        print(f"    [OK] {len(rows)} row(s) -> ready")

    in_cost = total_in / 1_000_000 * 2.50
    out_cost = total_out / 1_000_000 * 10.00
    print(f"\n[OK] Tokens: {total_in:,} in + {total_out:,} out")
    print(f"[OK] Cost: ${in_cost + out_cost:.4f}")


if __name__ == "__main__":
    main()
