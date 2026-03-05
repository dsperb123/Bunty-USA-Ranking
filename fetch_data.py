#!/usr/bin/env python3
"""
US ETF Ranking — data fetcher
Uses same bull/bear event detection, WMA weighting and composite scoring
as the ASX Sector Strength script.
Outputs: data/etf_ranking.json
"""

import json
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import yfinance as yf

# ── Tickers ───────────────────────────────────────────────────────────────────
GROUPS = {
    "Indices": {
        "SPY":  "SPDR S&P 500 ETF",
        "QQQ":  "Invesco Nasdaq-100 ETF",
        "DIA":  "SPDR Dow Jones ETF",
        "IWM":  "iShares Russell 2000 ETF",
        "MDY":  "SPDR S&P MidCap 400 ETF",
        "RSP":  "Invesco S&P 500 EW ETF",
        "MGK":  "Vanguard Mega Cap Growth ETF",
        "QQQE": "Direxion Nasdaq-100 EW ETF",
        "TLT":  "iShares 20+ Year Treasury ETF",
        "IBIT": "iShares Bitcoin Trust",
        "ETHA": "iShares Ethereum Trust",
    },
    "Sel Sectors": {
        "XLK":  "SPDR Technology Select Sector ETF",
        "XLC":  "SPDR Communication Services ETF",
        "XLI":  "SPDR Industrials Select Sector ETF",
        "XLF":  "SPDR Financials Select Sector ETF",
        "XLY":  "SPDR Consumer Discretionary ETF",
        "XLV":  "SPDR Health Care Select Sector ETF",
        "XLE":  "SPDR Energy Select Sector ETF",
        "XLB":  "SPDR Materials Select Sector ETF",
        "XLP":  "SPDR Consumer Staples ETF",
        "XLRE": "SPDR Real Estate Select Sector ETF",
        "XLU":  "SPDR Utilities Select Sector ETF",
    },
    "Industries": {
        "SOXX": "iShares Semiconductor ETF",
        "SMH":  "VanEck Semiconductor ETF",
        "IGV":  "iShares Software ETF",
        "WCLD": "WisdomTree Cloud Computing ETF",
        "CIBR": "First Trust Cybersecurity ETF",
        "AIQ":  "Global X AI ETF",
        "ARKK": "ARK Innovation ETF",
        "ARKF": "ARK Fintech Innovation ETF",
        "ARKX": "ARK Space Exploration ETF",
        "ARKG": "ARK Genomic Revolution ETF",
        "BOTZ": "Global X Robotics & AI ETF",
        "ROBO": "Robo Global Robotics ETF",
        "BLOK": "Amplify Blockchain ETF",
        "FNGS": "MicroSectors FANG+ ETF",
        "FDN":  "First Trust Internet ETF",
        "SOCL": "Global X Social Media ETF",
        "FFTY": "Innovator IBD 50 ETF",
        "ITA":  "iShares Aerospace & Defense ETF",
        "XAR":  "SPDR Aerospace & Defense ETF",
        "PAVE": "Global X Infrastructure ETF",
        "XTN":  "SPDR S&P Transportation ETF",
        "JETS": "US Global Jets ETF",
        "BOAT": "SonicShares Global Shipping ETF",
        "KBE":  "SPDR S&P Bank ETF",
        "KRE":  "SPDR S&P Regional Banking ETF",
        "KIE":  "SPDR S&P Insurance ETF",
        "KCE":  "SPDR S&P Capital Markets ETF",
        "IPAY": "ETFMG Prime Mobile Payments ETF",
        "IBB":  "iShares Biotech ETF",
        "XBI":  "SPDR Biotech ETF",
        "IHI":  "iShares Medical Devices ETF",
        "XHE":  "SPDR S&P Health Care Equipment ETF",
        "PPH":  "VanEck Pharmaceutical ETF",
        "ICLN": "iShares Clean Energy ETF",
        "TAN":  "Invesco Solar ETF",
        "URA":  "Global X Uranium ETF",
        "NLR":  "VanEck Uranium & Nuclear ETF",
        "HYDR": "Global X Hydrogen ETF",
        "XLE":  "SPDR Energy Select Sector ETF",
        "XOP":  "SPDR Oil & Gas E&P ETF",
        "OIH":  "VanEck Oil Services ETF",
        "XES":  "SPDR Oil & Gas Equipment ETF",
        "FCG":  "First Trust Natural Gas ETF",
        "UNG":  "US Natural Gas Fund",
        "USO":  "US Oil Fund",
        "GLD":  "SPDR Gold Shares",
        "GDX":  "VanEck Gold Miners ETF",
        "SILJ": "ETFMG Junior Silver Miners ETF",
        "SLV":  "iShares Silver Trust",
        "WGMI": "Valkyrie Bitcoin Miners ETF",
        "LIT":  "Global X Lithium & Battery ETF",
        "REMX": "VanEck Rare Earth ETF",
        "COPX": "Global X Copper Miners ETF",
        "CPER": "US Copper Index Fund",
        "SLX":  "VanEck Steel ETF",
        "XME":  "SPDR S&P Metals & Mining ETF",
        "DBC":  "Invesco DB Commodity Fund",
        "DBA":  "Invesco DB Agriculture Fund",
        "VNQ":  "Vanguard Real Estate ETF",
        "SCHH": "Schwab US REIT ETF",
        "REZ":  "iShares Residential Real Estate ETF",
        "XHB":  "SPDR Homebuilders ETF",
        "XRT":  "SPDR S&P Retail ETF",
        "IBUY": "Amplify Online Retail ETF",
        "PEJ":  "Invesco Leisure & Entertainment ETF",
        "EATZ": "AdvisorShares Restaurant ETF",
        "PBJ":  "Invesco Food & Beverage ETF",
        "IYZ":  "iShares US Telecommunications ETF",
        "XTL":  "SPDR Telecom ETF",
        "DRIV": "Global X Autonomous & EV ETF",
        "KWEB": "KraneShares China Internet ETF",
        "GXC":  "SPDR S&P China ETF",
        "MSOS": "AdvisorShares US Cannabis ETF",
        "UTES": "Virtus Reaves Utilities ETF",
    },
}

# ── Config ────────────────────────────────────────────────────────────────────
D_LOOKBACK   = 30
W_LOOKBACK   = 13
M_LOOKBACK   = 12
CL_LEN       = 10
CL_MULT      = 1.7
DAILY_WT     = 0.50
WEEKLY_WT    = 0.30
MONTHLY_WT   = 0.20
WICK_THRESH  = 1.0
RANK_LBK     = 10
MG_WEEKLY_LBK = 13


# ── WMA ───────────────────────────────────────────────────────────────────────
def wma(series, period):
    weights = np.arange(1, period + 1, dtype=float)
    w_sum   = weights.sum()
    out     = np.full(len(series), np.nan)
    for i in range(period - 1, len(series)):
        out[i] = np.dot(series[i - period + 1: i + 1], weights) / w_sum
    return out


# ── Bull events ───────────────────────────────────────────────────────────────
def detect_bull_events(opens, highs, lows, closes):
    n = len(closes)
    events = np.zeros(n, dtype=float)
    ranges = highs - lows
    range_sma = pd.Series(ranges).rolling(CL_LEN).mean().shift(1).values
    y_level, y_win = np.nan, 0
    in_hi, in_win  = np.nan, 0
    cl_mid, cl_win, cl_ok = np.nan, 0, True
    tl_level, tl_win = np.nan, 0

    for i in range(5, n):
        bull = closes[i] > opens[i]
        bull4 = all(closes[i-k] > opens[i-k] for k in range(4))
        bull_ol = bull and opens[i] == lows[i]
        is_outside = highs[i] > highs[i-1] and lows[i] < lows[i-1]
        is_inside  = highs[i] < highs[i-1] and lows[i] > lows[i-1]

        bull_yellow = False
        if is_outside and lows[i] < lows[i-1]:
            y_level, y_win = highs[i], 3
        elif y_win > 0:
            bull_yellow = closes[i] > y_level
            y_win -= 1
            if bull_yellow or y_win == 0:
                y_level, y_win = np.nan, 0

        bull_inside = False
        if is_inside:
            in_hi, in_win = highs[i], 3
        elif in_win > 0:
            bull_inside = closes[i] > in_hi
            in_win -= 1
            if bull_inside or in_win == 0:
                in_hi, in_win = np.nan, 0

        bull_clim = False
        r_sma = range_sma[i] if not np.isnan(range_sma[i]) else 0
        if bull and ranges[i] >= r_sma * CL_MULT:
            cl_mid, cl_win, cl_ok = lows[i] + ranges[i] * 0.5, 2, True
        elif cl_win > 0:
            cl_ok  = cl_ok and (lows[i] >= cl_mid)
            cl_win -= 1
            if cl_win == 0:
                bull_clim = True

        bull_tl = False
        btl = lows[i-1] < lows[i-2] < lows[i-3] < lows[i-4]
        if btl:
            tl_level, tl_win = highs[i-1], 3
        if tl_win > 0 and not btl:
            bull_tl = highs[i] > tl_level
            tl_win -= 1
            if bull_tl or tl_win == 0:
                tl_level, tl_win = np.nan, 0

        tick_gap_dn = brk_gap_up = False
        if i >= 1:
            tick_gap_dn = opens[i] < closes[i-1] and bull and (opens[i] - lows[i]) < WICK_THRESH
            brk_gap_up  = opens[i] > closes[i-1] and closes[i] > highs[i-1]

        events[i] = float(any([bull4, bull_ol, bull_yellow, bull_inside,
                                bull_clim, bull_tl, tick_gap_dn, brk_gap_up]))
    return events


# ── Bear events ───────────────────────────────────────────────────────────────
def detect_bear_events(opens, highs, lows, closes):
    n = len(closes)
    events = np.zeros(n, dtype=float)
    ranges = highs - lows
    range_sma = pd.Series(ranges).rolling(CL_LEN).mean().shift(1).values
    y_level, y_win = np.nan, 0
    in_lo, in_win  = np.nan, 0
    cl_mid, cl_win, cl_ok = np.nan, 0, True
    th_level, th_win = np.nan, 0

    for i in range(5, n):
        bear = closes[i] < opens[i]
        bear4 = all(closes[i-k] < opens[i-k] for k in range(4))
        bear_oh = bear and opens[i] == highs[i]
        is_outside = highs[i] > highs[i-1] and lows[i] < lows[i-1]
        is_inside  = highs[i] < highs[i-1] and lows[i] > lows[i-1]

        bear_yellow = False
        if is_outside and highs[i] > highs[i-1]:
            y_level, y_win = lows[i], 3
        elif y_win > 0:
            bear_yellow = closes[i] < y_level
            y_win -= 1
            if bear_yellow or y_win == 0:
                y_level, y_win = np.nan, 0

        bear_inside = False
        if is_inside:
            in_lo, in_win = lows[i], 3
        elif in_win > 0:
            bear_inside = closes[i] < in_lo
            in_win -= 1
            if bear_inside or in_win == 0:
                in_lo, in_win = np.nan, 0

        bear_clim = False
        r_sma = range_sma[i] if not np.isnan(range_sma[i]) else 0
        if bear and ranges[i] >= r_sma * CL_MULT:
            cl_mid, cl_win, cl_ok = lows[i] + ranges[i] * 0.5, 2, True
        elif cl_win > 0:
            cl_ok  = cl_ok and (highs[i] <= cl_mid)
            cl_win -= 1
            if cl_win == 0:
                bear_clim = True

        bear_th = False
        bth = highs[i-1] > highs[i-2] > highs[i-3] > highs[i-4]
        if bth:
            th_level, th_win = lows[i-1], 3
        if th_win > 0 and not bth:
            bear_th = lows[i] < th_level
            th_win -= 1
            if bear_th or th_win == 0:
                th_level, th_win = np.nan, 0

        tick_gap_up = brk_gap_dn = False
        if i >= 1:
            tick_gap_up = opens[i] > closes[i-1] and bear and (highs[i] - opens[i]) < WICK_THRESH
            brk_gap_dn  = opens[i] < closes[i-1] and closes[i] < lows[i-1]

        events[i] = float(any([bear4, bear_oh, bear_yellow, bear_inside,
                                bear_clim, bear_th, tick_gap_up, brk_gap_dn]))
    return events


# ── Micro gaps (weekly) ───────────────────────────────────────────────────────
def detect_micro_gaps(highs, lows, closes, opens):
    n = len(closes)
    bull_ev = np.zeros(n, dtype=float)
    bear_ev = np.zeros(n, dtype=float)
    for i in range(2, n):
        mg_bull = (lows[i] > highs[i-2] and lows[i-1] >= lows[i-2] and lows[i] >= lows[i-1])
        mg_bear = (highs[i] < lows[i-2] and highs[i-1] <= highs[i-2] and highs[i] <= highs[i-1])
        bull_ev[i] = float(mg_bull)
        bear_ev[i] = float(mg_bear)
    return bull_ev, bear_ev


# ── Bull % via WMA ────────────────────────────────────────────────────────────
def calc_bull_pct(bull_ev, bear_ev, lookback):
    b = wma(bull_ev, lookback)
    s = wma(bear_ev, lookback)
    total = b + s
    with np.errstate(divide='ignore', invalid='ignore'):
        return np.where(total > 0, (b / total) * 100.0, 50.0)


# ── Resample OHLCV ────────────────────────────────────────────────────────────
def resample_ohlcv(df, freq):
    rule = {'W': 'W-FRI', 'M': 'ME'}[freq]
    return df.resample(rule).agg({
        'Open': 'first', 'High': 'max',
        'Low': 'min',   'Close': 'last',
        'Volume': 'sum'
    }).dropna(subset=['Close'])


def sf(v, n=2):
    try:
        f = float(v)
        return None if (np.isnan(f) or np.isinf(f)) else round(f, n)
    except Exception:
        return None


# ── Process one ticker ────────────────────────────────────────────────────────
def process_ticker(ticker, name):
    print(f"  {ticker} ({name})")
    t = yf.Ticker(ticker)
    daily = t.history(period="2y")
    if daily.empty or len(daily) < 35:
        print(f"    [WARN] insufficient data", file=sys.stderr)
        return None

    daily.index = pd.to_datetime(daily.index).tz_localize(None)
    daily = daily.sort_index()

    do, dh, dl, dc = daily['Open'].values, daily['High'].values, daily['Low'].values, daily['Close'].values

    d_bull = detect_bull_events(do, dh, dl, dc)
    d_bear = detect_bear_events(do, dh, dl, dc)
    d_pct  = calc_bull_pct(d_bull, d_bear, D_LOOKBACK)

    wkly   = resample_ohlcv(daily, 'W')
    wo, wh, wl, wc = wkly['Open'].values, wkly['High'].values, wkly['Low'].values, wkly['Close'].values
    w_bull = detect_bull_events(wo, wh, wl, wc)
    w_bear = detect_bear_events(wo, wh, wl, wc)
    w_pct  = calc_bull_pct(w_bull, w_bear, W_LOOKBACK)
    mg_bull_ev, mg_bear_ev = detect_micro_gaps(wh, wl, wc, wo)
    mg_bull = int(np.nansum(mg_bull_ev[-MG_WEEKLY_LBK:]))
    mg_bear = int(np.nansum(mg_bear_ev[-MG_WEEKLY_LBK:]))

    mnth   = resample_ohlcv(daily, 'M')
    mo, mh, ml, mc = mnth['Open'].values, mnth['High'].values, mnth['Low'].values, mnth['Close'].values
    m_bull = detect_bull_events(mo, mh, ml, mc)
    m_bear = detect_bear_events(mo, mh, ml, mc)
    m_pct  = calc_bull_pct(m_bull, m_bear, M_LOOKBACK)

    d_val = sf(d_pct[-1])
    w_val = sf(w_pct[-1])
    m_val = sf(m_pct[-1] if len(m_pct) > 0 else np.nan)
    comp  = (d_val or 50) * DAILY_WT + (w_val or 50) * WEEKLY_WT + (m_val or 50) * MONTHLY_WT

    if len(d_pct) > RANK_LBK:
        hist_comp = (sf(d_pct[-(RANK_LBK+1)]) or 50) * DAILY_WT + (w_val or 50) * WEEKLY_WT + (m_val or 50) * MONTHLY_WT
    else:
        hist_comp = comp

    price = sf(dc[-1])
    chg   = sf(((dc[-1] / dc[-2]) - 1) * 100) if len(dc) >= 2 and dc[-2] != 0 else None

    spark_raw = dc[-10:].tolist()
    lo, hi = min(spark_raw), max(spark_raw)
    rng = hi - lo if hi != lo else 1
    spark = [sf((v - lo) / rng * 100) for v in spark_raw]

    return {
        "ticker":    ticker,
        "name":      name,
        "price":     price,
        "chg":       chg,
        "daily":     d_val,
        "weekly":    w_val,
        "monthly":   m_val,
        "composite": sf(comp),
        "hist_composite": sf(hist_comp),
        "mg_bull":   mg_bull,
        "mg_bear":   mg_bear,
        "spark":     spark,
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    os.makedirs("data", exist_ok=True)
    output = {"updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "groups": {}}

    for group_name, tickers in GROUPS.items():
        print(f"\n── {group_name} ──")
        results = []
        for ticker, name in tickers.items():
            try:
                row = process_ticker(ticker, name)
                if row:
                    results.append(row)
            except Exception as e:
                print(f"  [ERROR] {ticker}: {e}", file=sys.stderr)

        results.sort(key=lambda x: x['composite'] or 0, reverse=True)
        for i, r in enumerate(results):
            r['rank'] = i + 1

        hist_sorted = sorted(results, key=lambda x: x['hist_composite'] or 0, reverse=True)
        hist_map = {r['ticker']: i + 1 for i, r in enumerate(hist_sorted)}
        for r in results:
            r['hist_rank'] = hist_map.get(r['ticker'], r['rank'])
            r['rank_diff'] = r['rank'] - r['hist_rank']

        output["groups"][group_name] = results
        print(f"  ✓ {len(results)} tickers")

    with open("data/etf_ranking.json", "w") as f:
        json.dump(output, f, indent=2)

    print("\n✓ data/etf_ranking.json written")


if __name__ == "__main__":
    main()
