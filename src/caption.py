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
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")

SYSTEM_PROMPT = """You are writing one Instagram caption for a real family travel/lifestyle post for Valor Voyages.

Family context:
- Jordan (29), Kayla (28), Jemma (9)
- Young adults traveling the US with their daughter Jemma
- Only mention family members if they are actually visible in the image or clearly relevant

VOICE ANCHOR - captions should sound like these examples:

Example 1:
"The kind of sunset that slows everything down. December evenings have this quiet golden weight to them."

Example 2:
"Cold rain, warmer thermos. We didn't say much on this stretch - there wasn't much to say."

Example 3:
"Three hours into a four-hour drive and Jemma started naming the clouds."

Example 4:
"Found this overlook on accident. The kind of detour you don't regret."

Notice: specific sensory detail, conversational rhythm, no abstract emotion-words, no clichés, no exclamation points, no rhetorical questions.

Priority order:
1. Analyze what is actually visible in the image
2. Use location/GPS metadata to ground the place
3. Use date/time to anchor season or time of day
4. Use family context only if it genuinely fits

Hard rules:
- Caption the actual subject. No invented people, activities, or conversations.
- Mention Jemma only if she is visible or clearly involved.
- 2 to 4 sentences. Never just one short generic line.
- First sentence must be specific and grounded - a real observation, not a summary.
- Show details, don't summarize emotions.
- Modern, grounded, human. Write like a person texting a friend, not a travel blog.

BANNED phrases (these are AI tells - never use any of them):
- "vibes" of any kind ("coastal vibes", "good vibes", "summer vibes")
- "soaking in", "soaking up", "taking in", "drinking in"
- "wanderlust", "happy place", "living my best life"
- "adventure awaits", "making memories", "memories forever"
- "one city at a time", "the world is your oyster"
- "what a beautiful day", "feeling blessed", "feeling grateful"
- "moments like these"
- "where laughter dances with"
- Any single-sentence summary caption.

Location rules:
- Provided location_text/GPS is authoritative.
- Do NOT name a coast, beach, ocean, city, or landmark unless image or location data clearly supports it.
- If location is broad, stay broad. If location is missing, do NOT name a specific place.

Hashtags:
- 8 to 12 hashtags total
- Mix: location-specific, content-specific, family-travel niche
- Lowercase: #pnw not #PNW. #familytravel not #FamilyTravel.

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

    parts = ["Photo metadata:"]
    if context.get("date_taken"):
        parts.append(f"- Taken: {context['date_taken']}")
    if context.get("location_text"):
        parts.append(f"- Location: {context['location_text']}")
    elif context.get("gps_lat") and context.get("gps_lon"):
        parts.append(f"- GPS: {context['gps_lat']}, {context['gps_lon']}")
    else:
        parts.append("- Location: unknown (do NOT name a specific place in the caption or hashtags)")
    parts.append("")
    parts.append("Write the caption + hashtags following all the rules. Match the voice anchor examples.")
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
                            "detail": "high",
                        },
                    },
                ],
            },
        ],
        response_format={"type": "json_object"},
        max_tokens=500,
        temperature=0.7,
    )
    data = json.loads(response.choices[0].message.content)
    return data, response.usage


def update_row(ws, row_index, caption, hashtags):
    ws.update(
        range_name=f"D{row_index}:F{row_index}",
        values=[[caption, hashtags, "ready"]],
    )


def main():
    print("=== Valor Voyages: caption ===")
    dbx = get_dropbox_client()
    ws = get_sheets_worksheet()
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    print(f"Model: {OPENAI_MODEL}")

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

        print(f"    caption: {caption}")
        print(f"    tags:    {hashtags}")

        update_row(ws, row_idx, caption, hashtags)
        print(f"    [OK] status -> ready")

    # gpt-4o pricing: $2.50/1M input, $10/1M output
    in_cost = total_in / 1_000_000 * 2.50
    out_cost = total_out / 1_000_000 * 10.00
    print(f"\n[OK] Tokens: {total_in:,} in + {total_out:,} out")
    print(f"[OK] Cost: ${in_cost + out_cost:.4f}")


if __name__ == "__main__":
    main()
