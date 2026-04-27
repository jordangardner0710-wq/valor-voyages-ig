"""
Caption: pull rows where status=pending_metadata, generate caption + hashtags
via OpenAI vision, write back, transition status to "ready".
"""

import os
import json
import base64
from dotenv import load_dotenv

import dropbox
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI

load_dotenv()

DROPBOX_FOLDER = os.getenv("DROPBOX_FOLDER")
SHEET_ID = os.getenv("SHEET_ID")
SHEET_TAB = os.getenv("SHEET_TAB")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

SYSTEM_PROMPT = """You are the caption writer for Valor Voyages, a travel brand on Instagram.

Write captions that:
- Capture the feeling of the moment, not just describe the photo
- Are 1-3 sentences, conversational
- Use sensory or temporal anchors (light, time of day, season, sound)
- Avoid cliches: no "wanderlust", "adventure awaits", "living my best life", "happy place"
- Sound human, not AI

Generate 8-15 relevant hashtags mixing travel, location, and content tags.

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


def fetch_pending(ws):
    records = ws.get_all_records()
    pending = []
    for i, row in enumerate(records, start=2):
        if (row.get("status") or "").strip() == "pending_metadata":
            pending.append((i, row))
    return pending


def download_image(dbx, path):
    _, response = dbx.files_download(path)
    return response.content


def caption_image(client, image_bytes, context):
    image_b64 = base64.b64encode(image_bytes).decode()

    parts = ["Generate a caption + hashtags for this photo."]
    if context.get("date_taken"):
        parts.append(f"Taken: {context['date_taken']}")
    if context.get("gps_lat") and context.get("gps_lon"):
        parts.append(f"GPS: {context['gps_lat']}, {context['gps_lon']}")
    if context.get("location_text"):
        parts.append(f"Location: {context['location_text']}")
    user_text = "\n".join(parts)

    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_text},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{image_b64}",
                            "detail": "low",
                        },
                    },
                ],
            },
        ],
        response_format={"type": "json_object"},
        max_tokens=400,
    )
    data = json.loads(response.choices[0].message.content)
    return data, response.usage


def update_row(ws, row_index, caption, hashtags):
    # Cols: D=caption, E=hashtags, F=status
    ws.update(
        range_name=f"D{row_index}:F{row_index}",
        values=[[caption, hashtags, "ready"]],
    )


def main():
    print("=== Valor Voyages: caption ===")
    dbx = get_dropbox_client()
    ws = get_sheets_worksheet()
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    pending = fetch_pending(ws)
    print(f"Pending rows: {len(pending)}")
    if not pending:
        return

    total_in = total_out = 0
    for i, (row_idx, row) in enumerate(pending, 1):
        path = row.get("file_path", "")
        print(f"  [{i}/{len(pending)}] row {row_idx}: {row.get('file_name')}")

        try:
            image_bytes = download_image(dbx, path)
        except Exception as e:
            print(f"    [error] download failed: {e}")
            continue

        try:
            data, usage = caption_image(client, image_bytes, row)
            total_in += usage.prompt_tokens
            total_out += usage.completion_tokens
        except Exception as e:
            print(f"    [error] caption failed: {e}")
            continue

        caption = (data.get("caption") or "").strip()
        hashtags = (data.get("hashtags") or "").strip()

        print(f"    caption: {caption[:80]}{'...' if len(caption) > 80 else ''}")
        print(f"    tags:    {hashtags[:80]}{'...' if len(hashtags) > 80 else ''}")

        update_row(ws, row_idx, caption, hashtags)
        print(f"    [OK] status -> ready")

    in_cost = total_in / 1_000_000 * 0.15
    out_cost = total_out / 1_000_000 * 0.60
    print(f"\n[OK] Tokens: {total_in:,} in + {total_out:,} out")
    print(f"[OK] Cost: ${in_cost + out_cost:.4f}")


if __name__ == "__main__":
    main()
