"""
Digest: pull rows where status=ready, format as a readable markdown digest grouped
by group_id (carousels). Print to stdout. If DISCORD_WEBHOOK_URL is set, also post.
"""

import os
from collections import defaultdict
from datetime import datetime, timezone
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials
import requests

load_dotenv()

SHEET_ID = os.getenv("SHEET_ID")
SHEET_TAB = os.getenv("SHEET_TAB")
DISCORD_WEBHOOK_URL = (os.getenv("DISCORD_WEBHOOK_URL") or "").strip()


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


def fetch_ready(ws):
    records = ws.get_all_records()
    return [
        (i + 2, row)
        for i, row in enumerate(records)
        if (row.get("status") or "").strip() == "ready"
    ]


def group_rows(rows):
    groups = defaultdict(list)
    for idx, (row_idx, row) in enumerate(rows):
        gid = (row.get("group_id") or "").strip() or f"_solo_{idx}"
        groups[gid].append((row_idx, row))
    return groups


def format_digest(groups):
    lines = []
    lines.append("# Valor Voyages - Posts Ready to Schedule")
    lines.append(f"_Generated {datetime.now(timezone.utc).isoformat(timespec='seconds')}_")
    lines.append("")

    total_groups = len(groups)
    total_photos = sum(len(rows) for rows in groups.values())
    lines.append(
        f"**{total_groups} post{'s' if total_groups != 1 else ''} ready** "
        f"({total_photos} photo{'s' if total_photos != 1 else ''} total)"
    )
    lines.append("")

    for i, (gid, rows) in enumerate(sorted(groups.items()), 1):
        is_carousel = len(rows) > 1
        first = rows[0][1]
        kind = "Carousel" if is_carousel else "Single"
        lines.append(f"## Post {i} - {kind} ({len(rows)} photo{'s' if is_carousel else ''})")
        lines.append("")
        if first.get("date_taken"):
            lines.append(f"**Date taken:** {first.get('date_taken')}")
        if first.get("location_text"):
            lines.append(f"**Location:** {first.get('location_text')}")
        if first.get("group_id"):
            lines.append(f"**Group ID:** `{gid}`")
        lines.append("")
        lines.append("**Caption:**")
        lines.append(f"> {first.get('caption', '').replace(chr(10), chr(10) + '> ')}")
        lines.append("")
        lines.append(f"**Hashtags:** {first.get('hashtags', '')}")
        lines.append("")
        lines.append("**Photos:**")
        for row_idx, row in rows:
            lines.append(f"- [{row.get('file_name')}]({row.get('file_link')})  (row {row_idx})")
        lines.append("")
        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def post_to_discord(content, webhook_url):
    """Discord caps messages at 2000 chars. Split on newlines when possible."""
    while content:
        chunk = content[:1900]
        if len(content) > 1900:
            last_nl = chunk.rfind("\n")
            if last_nl > 1000:
                chunk = content[:last_nl]
        r = requests.post(webhook_url, json={"content": chunk}, timeout=15)
        r.raise_for_status()
        content = content[len(chunk):].lstrip("\n")


def main():
    print("=== Valor Voyages: digest ===")
    ws = get_sheets_worksheet()
    rows = fetch_ready(ws)
    print(f"Ready rows: {len(rows)}")

    if not rows:
        print("No posts ready. Exiting.")
        return

    groups = group_rows(rows)
    digest = format_digest(groups)

    print()
    print(digest)

    if DISCORD_WEBHOOK_URL:
        try:
            post_to_discord(digest, DISCORD_WEBHOOK_URL)
            print("\n[OK] Posted to Discord.")
        except Exception as e:
            print(f"\n[error] Discord post failed: {e}")
    else:
        print("\n[info] DISCORD_WEBHOOK_URL not set - skipping Discord delivery.")


if __name__ == "__main__":
    main()
