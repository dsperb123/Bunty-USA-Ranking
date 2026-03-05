#!/usr/bin/env python3
"""
Daily ETF Ranking Alert
Sends MarketWatch + Seeking Alpha headlines, then lists all tickers
from Indices, Sel Sectors and Industries with composite score >= 60%.
No external dependencies required.
"""

import json
import os
import sys
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
SCORE_THRESHOLD  = 60.0
ALERT_GROUPS     = ["Indices", "Sel Sectors", "Industries"]
MAX_HEADLINES    = 4

RSS_FEEDS = [
    ("MarketWatch", "https://feeds.content.dowjones.io/public/rss/mw_marketpulse"),
    ("Seeking Alpha", "https://seekingalpha.com/market_currents.xml"),
]


def fetch_headlines(name, url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            xml_data = resp.read()
        root = ET.fromstring(xml_data)
        headlines = []
        for item in root.findall(".//item"):
            title = item.findtext("title")
            if title:
                headlines.append(title.strip())
            if len(headlines) >= MAX_HEADLINES:
                break
        return headlines
    except Exception as e:
        print(f"Could not fetch {name} headlines: {e}")
        return []


def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("ERROR: TELEGRAM_TOKEN or TELEGRAM_CHAT_ID not set.", file=sys.stderr)
        sys.exit(1)

    # Telegram messages max 4096 chars — split if needed
    chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    for chunk in chunks:
        data = urllib.parse.urlencode({
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       chunk,
            "parse_mode": "HTML",
        }).encode()
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if not result.get("ok"):
                print(f"Telegram error: {result}", file=sys.stderr)
                sys.exit(1)


def score_bar(score):
    """Visual progress bar for composite score."""
    filled = int(round(score / 10))
    empty  = 10 - filled
    return "█" * filled + "░" * empty


def main():
    try:
        with open("data/etf_ranking.json") as f:
            data = json.load(f)
    except FileNotFoundError:
        print("ERROR: data/etf_ranking.json not found.", file=sys.stderr)
        sys.exit(1)

    now_str = datetime.now(timezone.utc).strftime("%d %b %Y")
    all_groups = data.get("groups", {})

    lines = [f"📊 <b>US ETF Morning Brief — {now_str}</b>"]

    # ── Market Headlines ──────────────────────────────────
    for feed_name, feed_url in RSS_FEEDS:
        headlines = fetch_headlines(feed_name, feed_url)
        if headlines:
            lines.append(f"\n<b>── {feed_name} ──</b>")
            for h in headlines:
                lines.append(f"• {h}")

    # ── ETF Rankings (score >= 60%) ───────────────────────
    total = 0
    ranking_lines = []

    for group_name in ALERT_GROUPS:
        rows = all_groups.get(group_name, [])
        qualifying = [r for r in rows
                      if r.get("composite") is not None and r["composite"] >= SCORE_THRESHOLD]
        # already sorted by rank in JSON but re-sort to be safe
        qualifying.sort(key=lambda r: r["composite"], reverse=True)

        if qualifying:
            ranking_lines.append(f"\n<b>── {group_name} ──</b>")
            for r in qualifying:
                ticker = r.get("ticker", "?")
                name   = r.get("name", ticker).replace("&", "&amp;")
                score  = r["composite"]
                rank   = r.get("rank", "?")
                diff   = r.get("rank_diff", 0)
                chg    = r.get("chg")
                mg_bull = r.get("mg_bull", 0)

                # Rank trend arrow
                if diff < 0:
                    trend = "▲"
                elif diff > 0:
                    trend = "▼"
                else:
                    trend = "●"

                chg_str = f"  {'+' if chg and chg > 0 else ''}{chg:.1f}%" if chg is not None else ""
                mg_str  = "  MG" if mg_bull > 0 else ""
                bar     = score_bar(score)

                ranking_lines.append(
                    f"{trend} <b>{ticker}</b>  {score:.0f}%  {bar}{chg_str}{mg_str}\n"
                    f"   <i>#{rank} — {name}</i>"
                )
            total += len(qualifying)

    lines.append(f"\n<b>── ETF Rankings ≥{SCORE_THRESHOLD:.0f}% ──</b>")
    if total == 0:
        lines.append(f"No tickers scored above {SCORE_THRESHOLD:.0f}% today.")
    else:
        lines.extend(ranking_lines)
        lines.append(f"\n<i>{total} ticker(s) above {SCORE_THRESHOLD:.0f}%</i>")

    message = "\n".join(lines)
    print(message)
    send_telegram(message)
    print("✓ Alert sent.")


if __name__ == "__main__":
    main()
