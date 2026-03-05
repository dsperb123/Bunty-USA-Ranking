#!/usr/bin/env python3
"""
Daily ETF Ranking Alert
- MarketWatch + Seeking Alpha market headlines
- Indices & Sel Sectors: tickers with composite score >= 60%
- Industries: top 15 ranked tickers moving >+1% on the day,
  with a Yahoo Finance news headline per ticker to explain the move
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
INDUSTRY_TOP_N      = 15
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


def fetch_ticker_headline(ticker):
    try:
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            xml_data = resp.read()
        root = ET.fromstring(xml_data)
        for item in root.findall(".//item"):
            title = item.findtext("title")
            if title and title.strip():
                return title.strip()
    except Exception as e:
        print(f"  No news for {ticker}: {e}")
    return None


def html_escape(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("ERROR: TELEGRAM_TOKEN or TELEGRAM_CHAT_ID not set.", file=sys.stderr)
        sys.exit(1)
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

    lines = [f"<b>[ US ETF Morning Brief -- {now_str} ]</b>"]

    # -- Market Headlines
    for feed_name, feed_url in RSS_FEEDS:
        headlines = fetch_rss_headlines(feed_name, feed_url)
        if headlines:
            lines.append(f"\n<b>-- {feed_name} --</b>")
            for h in headlines:
                lines.append(f"- {html_escape(h)}")

    # -- Indices & Sel Sectors: score >= 60%
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
        lines.append("No Indices or Sel Sectors tickers scored above 60% today.")
    else:
        lines.extend(score_lines)
        lines.append(f"\n<i>{total_score} ticker(s) above {SCORE_THRESHOLD:.0f}%</i>")

    # -- Industries: top 15 ranked + moving >1% + news
    industry_rows = all_groups.get("Industries", [])
    top15 = sorted(industry_rows, key=lambda r: r.get("rank", 999))[:INDUSTRY_TOP_N]
    movers = [r for r in top15
              if r.get("chg") is not None and r["chg"] >= INDUSTRY_MOVE_MIN]
    movers.sort(key=lambda r: r["chg"], reverse=True)

    lines.append(f"\n<b>-- Industries: Top {INDUSTRY_TOP_N} / Movers &gt;+{INDUSTRY_MOVE_MIN:.0f}% --</b>")

    if not movers:
        lines.append(f"No top-{INDUSTRY_TOP_N} Industries tickers moved more than +{INDUSTRY_MOVE_MIN:.0f}% today.")
    else:
        lines.append("<i>Rank / Score / Move / News</i>\n")
        for r in movers:
            ticker  = r.get("ticker", "?")
            name    = html_escape(r.get("name", ticker))
            score   = r.get("composite", 0)
            rank    = r.get("rank", "?")
            chg     = r["chg"]
            mg_bull = r.get("mg_bull", 0)
            mg_str  = "  MG" if mg_bull > 0 else ""

            print(f"  Fetching news for {ticker}...")
            headline = fetch_ticker_headline(ticker)
            news_str = f"\n   &gt; <i>{html_escape(headline)}</i>" if headline else "\n   &gt; <i>No recent news found</i>"

            lines.append(
                f"** <b>{ticker}</b>  +{chg:.2f}%  |  #{rank}  {score:.0f}%{mg_str}\n"
                f"   {name}{news_str}\n"
            )
        lines.append(f"<i>{len(movers)} mover(s) in top {INDUSTRY_TOP_N}</i>")

    message = "\n".join(lines)
    print(message)
    send_telegram(message)
    print("Alert sent.")


if __name__ == "__main__":
    main()
