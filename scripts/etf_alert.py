#!/usr/bin/env python3
"""
Daily ETF Ranking Alert
- MarketWatch + Seeking Alpha market headlines
- Indices & Sel Sectors: tickers with composite score >= 60%
- Industries: top 15 ranked tickers moving >+1%, with 3 news headlines each
No external dependencies required.
"""

import json
import os
import sys
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

TELEGRAM_TOKEN      = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID")
SCORE_THRESHOLD     = 60.0
INDUSTRY_TOP_N      = 20
INDUSTRY_MOVE_MIN   = 1.0
ALERT_GROUPS        = ["Indices", "Sel Sectors"]
MAX_HEADLINES       = 4

RSS_FEEDS = [
    ("MarketWatch", "https://feeds.content.dowjones.io/public/rss/mw_marketpulse"),
    ("Seeking Alpha", "https://seekingalpha.com/market_currents.xml"),
]


def fetch_rss_headlines(name, url):
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


def fetch_ticker_headlines(ticker, count=3):
    try:
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            xml_data = resp.read()
        root = ET.fromstring(xml_data)
        headlines = []
        for item in root.findall(".//item"):
            title = item.findtext("title")
            if title and title.strip():
                headlines.append(title.strip())
            if len(headlines) >= count:
                break
        return headlines
    except Exception as e:
        print(f"  No news for {ticker}: {e}")
    return []


def html_escape(text):
    return (text
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;"))


def post_message(text):
    """Send a single message to Telegram (max 4096 chars)."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("ERROR: secrets not set.", file=sys.stderr)
        sys.exit(1)
    url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       text[:4090],
        "parse_mode": "HTML",
    }).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        result = json.loads(resp.read())
        if not result.get("ok"):
            print(f"Telegram error: {result}", file=sys.stderr)
            sys.exit(1)


def score_bar(score):
    filled = int(round(score / 10))
    return "\u2588" * filled + "\u2591" * (10 - filled)


def main():
    try:
        with open("data/etf_ranking.json") as f:
            data = json.load(f)
    except FileNotFoundError:
        print("ERROR: data/etf_ranking.json not found.", file=sys.stderr)
        sys.exit(1)

    now_str = datetime.now(timezone.utc).strftime("%d %b %Y")
    all_groups = data.get("groups", {})

    # ── Message 1: Headlines + Indices/Sel Sectors ────────
    lines = [f"<b>[ US ETF Morning Brief -- {now_str} ]</b>"]

    for feed_name, feed_url in RSS_FEEDS:
        headlines = fetch_rss_headlines(feed_name, feed_url)
        if headlines:
            lines.append(f"\n<b>-- {feed_name} --</b>")
            for h in headlines:
                lines.append(f"- {html_escape(h)}")

    total_score = 0
    score_lines = []
    for group_name in ALERT_GROUPS:
        rows = all_groups.get(group_name, [])
        qualifying = [r for r in rows
                      if r.get("composite") is not None and r["composite"] >= SCORE_THRESHOLD]
        qualifying.sort(key=lambda r: r["composite"], reverse=True)
        if qualifying:
            score_lines.append(f"\n<b>-- {group_name} --</b>")
            for r in qualifying:
                ticker  = r.get("ticker", "?")
                name    = html_escape(r.get("name", ticker))
                score   = r["composite"]
                rank    = r.get("rank", "?")
                diff    = r.get("rank_diff", 0)
                chg     = r.get("chg")
                mg_bull = r.get("mg_bull", 0)
                trend   = "^" if diff < 0 else ("v" if diff > 0 else "-")
                chg_str = f"  {'+' if chg and chg > 0 else ''}{chg:.1f}%" if chg is not None else ""
                mg_str  = "  MG" if mg_bull > 0 else ""
                score_lines.append(
                    f"{trend} <b>{ticker}</b>  {score:.0f}%  {score_bar(score)}{chg_str}{mg_str}\n"
                    f"   <i>#{rank} -- {name}</i>"
                )
            total_score += len(qualifying)

    lines.append(f"\n<b>-- ETF Rankings &gt;={SCORE_THRESHOLD:.0f}% --</b>")
    if total_score == 0:
        lines.append("No Indices or Sel Sectors above 60% today.")
    else:
        lines.extend(score_lines)
        lines.append(f"\n<i>{total_score} ticker(s) above {SCORE_THRESHOLD:.0f}%</i>")

    post_message("\n".join(lines))
    print("Message 1 sent.")

    # ── Message 2: Industries movers header ───────────────
    industry_rows = all_groups.get("Industries", [])
    top15 = sorted(industry_rows, key=lambda r: r.get("rank", 999))[:INDUSTRY_TOP_N]
    movers = [r for r in top15
              if r.get("chg") is not None and r["chg"] >= INDUSTRY_MOVE_MIN]
    movers.sort(key=lambda r: r["chg"], reverse=True)

    header = (
        f"<b>-- Industries: Top {INDUSTRY_TOP_N} / Movers &gt;+{INDUSTRY_MOVE_MIN:.0f}% --</b>\n"
        f"<i>{len(movers)} mover(s) found -- details below</i>"
    ) if movers else (
        f"<b>-- Industries: Top {INDUSTRY_TOP_N} --</b>\n"
        f"<i>No tickers in top {INDUSTRY_TOP_N} moved more than +{INDUSTRY_MOVE_MIN:.0f}% today.</i>"
    )
    post_message(header)
    print("Message 2 sent.")

    # ── One message per mover with 3 headlines ─────────────
    for r in movers:
        ticker  = r.get("ticker", "?")
        name    = html_escape(r.get("name", ticker))
        score   = r.get("composite", 0)
        rank    = r.get("rank", "?")
        chg     = r["chg"]
        mg_bull = r.get("mg_bull", 0)
        mg_str  = "  MG" if mg_bull > 0 else ""

        print(f"  Fetching news for {ticker}...")
        headlines = fetch_ticker_headlines(ticker, count=3)

        msg_lines = [
            f"<b>{ticker}</b>  +{chg:.2f}%  |  Rank #{rank}  Score {score:.0f}%{mg_str}",
            f"<i>{name}</i>",
            "",
        ]
        if headlines:
            msg_lines.append("<b>News:</b>")
            for i, h in enumerate(headlines, 1):
                msg_lines.append(f"{i}. {html_escape(h)}")
        else:
            msg_lines.append("<i>No recent news found</i>")

        post_message("\n".join(msg_lines))
        print(f"  Sent {ticker}")

    print("All alerts sent.")


if __name__ == "__main__":
    main()
