#!/usr/bin/env python3
"""Fetch TG channel preview pages and save as JSON for the static site."""
import json
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

CHANNELS = {
    "elon": "TechMelonElonMusk",
    "cz": "TechMelonCzHeyi",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15"
}

def parse_channel(channel):
    url = f"https://t.me/s/{channel}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch {channel}: {e}")
        return {"channel": channel, "updated": datetime.now(timezone.utc).isoformat(), "posts": [], "error": str(e)}

    soup = BeautifulSoup(r.text, "html.parser")
    posts = []

    for msg in soup.select("div.tgme_widget_message_wrap"):
        try:
            post_id = msg.get("data-post", "")
            text_el = msg.select_one("div.tgme_widget_message_text")
            text = text_el.get_text("\n", strip=True) if text_el else ""
            time_el = msg.select_one("time")
            time_str = time_el.get("datetime", "") if time_el else ""
            images = []
            for img in msg.select("a.tgme_widget_message_photo_wrap"):
                style = img.get("style", "")
                m = re.search(r"url\('([^']+)'\)", style)
                if m:
                    images.append(m.group(1))
            link_el = msg.select_one("a.tgme_widget_message_date")
            link = link_el.get("href", "") if link_el else ""

            action = "原创"
            if "转推了" in text or "retweeted" in text.lower():
                action = "转推"
            elif "回复了" in text or "replied" in text.lower():
                action = "回复"
            elif "关注了" in text or "followed" in text.lower():
                action = "关注"

            posts.append({
                "id": post_id,
                "text": text[:2000],
                "time": time_str,
                "action": action,
                "images": images[:4],
                "link": link,
            })
        except Exception as e:
            print(f"Parse error in {channel}: {e}")
            continue

    return {
        "channel": channel,
        "updated": datetime.now(timezone.utc).isoformat(),
        "posts": posts[:30],
    }

def main():
    all_data = {}
    for key, channel in CHANNELS.items():
        print(f"Fetching {channel}...")
        data = parse_channel(channel)
        all_data[key] = data
        with open(f"tweets-{key}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"  -> {len(data['posts'])} posts")

    with open("tweets.json", "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    print("Done.")

if __name__ == "__main__":
    main()
