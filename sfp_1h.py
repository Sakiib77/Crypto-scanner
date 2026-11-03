import os
import json
import time
import threading
from datetime import datetime, timezone
from collections import defaultdict

import requests
import websocket

BLOFIN_REST = "https://openapi.blofin.com"
BLOFIN_WS = "wss://openapi.blofin.com/ws/public"
DISCORD_WEBHOOK_URL = os.getenv("SFP_1H_DISCORD_WEBHOOK_URL")

TIMEFRAME = "1H"
CANDLE_CHANNEL = "candle1H"
HISTORY_CANDLES = 300
PIVOT_LEFT = 5
PIVOT_RIGHT = 5
MIN_SWEEP_PCT = 0.10
MAX_SWEEP_PCT = 1.00
MIN_WICK_RATIO = 0.50
VOLUME_PERIOD = 20
MIN_VOLUME_MULTIPLE = 1.30
WS_GROUP_SIZE = 25
WS_TIMEOUT = 25
RECONNECT_DELAY = 5

HISTORY = {}
LAST_COMPLETED_TS = {}
ALERTED = set()
LOCK = threading.RLock()
SESSION = requests.Session()


def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def fmt_time(ts):
    return datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def parse_row(row):
    if not isinstance(row, list) or len(row) < 6:
        return None
    try:
        return {
            "ts": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
            "confirm": str(row[-1]),
        }
    except (TypeError, ValueError, IndexError):
        return None


def parse_rows(rows):
    candles = [c for c in (parse_row(r) for r in rows) if c]
    candles.sort(key=lambda x: x["ts"])
    return candles


def get_symbols():
    url = f"{BLOFIN_REST}/api/v1/market/instruments"
    for attempt in range(5):
        try:
            r = SESSION.get(url, params={"instType": "SWAP"}, timeout=20)
            if r.status_code == 429:
                time.sleep(min(30, 5 * (attempt + 1)))
                continue
            r.raise_for_status()
            payload = r.json()
            symbols = sorted({
                x.get("instId") for x in payload.get("data", [])
                if x.get("instId", "").endswith("-USDT")
                and x.get("state") == "live"
                and x.get("instType") == "SWAP"
                and x.get("settleCurrency") == "USDT"
            })
            print(f"Live USDT perpetual symbols: {len(symbols)}")
            return symbols
        except Exception as e:
            print(f"Symbol request error: {e}")
            time.sleep(3)
    return []


def get_history(symbol):
    url = f"{BLOFIN_REST}/api/v1/market/candles"
    for attempt in range(5):
        try:
            r = SESSION.get(
                url,
                params={
                    "instId": symbol,
                    "bar": TIMEFRAME,
                    "limit": HISTORY_CANDLES,
                    "confirm": "1",
                },
                timeout=20,
            )
            if r.status_code == 429:
                wait = min(30, 5 * (2 ** attempt))
                print(f"Rate limited {symbol}; retry in {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            payload = r.json()
            if payload.get("code") not in (None, "0", 0):
                print(f"History API error {symbol}: {payload}")
                return []
            return [c for c in parse_rows(payload.get("data", [])) if c["confirm"] == "1"]
        except Exception as e:
            wait = min(30, 3 * (2 ** attempt))
            print(f"History error {symbol}: {e}; retry in {wait}s")
            time.sleep(wait)
    return []


def pivot_high(candles, i):
    if i < PIVOT_LEFT or i + PIVOT_RIGHT >= len(candles):
        return False
    p = candles[i]["high"]
    return (
        all(p > candles[j]["high"] for j in range(i-PIVOT_LEFT, i))
        and all(p >= candles[j]["high"] for j in range(i+1, i+PIVOT_RIGHT+1))
    )


def pivot_low(candles, i):
    if i < PIVOT_LEFT or i + PIVOT_RIGHT >= len(candles):
        return False
    p = candles[i]["low"]
    return (
        all(p < candles[j]["low"] for j in range(i-PIVOT_LEFT, i))
        and all(p <= candles[j]["low"] for j in range(i+1, i+PIVOT_RIGHT+1))
    )


def volume_average(candles, index):
    # Use the 20 completed candles immediately before the sweep candle.
    if index < VOLUME_PERIOD:
        return None
    values = [c["volume"] for c in candles[index-VOLUME_PERIOD:index]]
    return sum(values) / len(values) if values else None


def tradingview_url(symbol):
    # BloFin symbols are formatted BASE-USDT-SWAP; TradingView perpetual symbols use BASEUSDT.P.
    base = symbol.replace("-USDT", "")
    return f"https://www.tradingview.com/chart/?symbol=BINANCE%3A{base}USDT.P&interval=60"


def send_discord(symbol, side, pivot_price, sweep_price, sweep_pct,
                 close_price, wick_pct, volume, vol_avg, candle):
    if not DISCORD_WEBHOOK_URL:
        print("ERROR: SFP_1H_DISCORD_WEBHOOK_URL secret is missing.")
        return False

    title = "🟢 BULLISH 1H SFP" if side == "BULLISH" else "🔴 BEARISH 1H SFP"
    level_name = "Previous swing low" if side == "BULLISH" else "Previous swing high"
    wick_name = "Sweep wick low" if side == "BULLISH" else "Sweep wick high"
    volume_multiple = volume / vol_avg if vol_avg else 0
    message = (
        f"**{title} — {symbol}**\n"
        f"• Timeframe: 1H\n"
        f"• {level_name}: `{pivot_price:.10g}`\n"
        f"• {wick_name}: `{sweep_price:.10g}`\n"
        f"• Sweep depth: `{sweep_pct:.3f}%`\n"
        f"• Candle close: `{close_price:.10g}`\n"
        f"• Rejection wick: `{wick_pct:.1f}%`\n"
        f"• Volume: `{volume:.6g}`\n"
        f"• Volume multiple: `{volume_multiple:.2f}x` (20-candle average)\n"
        f"• Candle close time: `{fmt_time(candle['ts'])}`\n"
        f"[Open TradingView chart]({tradingview_url(symbol)})"
    )
    try:
        response = SESSION.post(
            DISCORD_WEBHOOK_URL,
            json={"content": message},
            timeout=15,
        )
        if response.status_code not in (200, 204):
            print(f"Discord error {response.status_code}: {response.text[:300]}")
            return False
        print(f"ALERT SENT: {side} {symbol} 1H SFP")
        return True
    except Exception as e:
        print(f"Discord request error: {e}")
        return False


def evaluate_new_candle(symbol, candle):
    with LOCK:
        candles = HISTORY.setdefault(symbol, [])
        if candles and candle["ts"] <= candles[-1]["ts"]:
            return
        candles.append(candle)
        if len(candles) > HISTORY_CANDLES:
            del candles[:-HISTORY_CANDLES]
        LAST_COMPLETED_TS[symbol] = candle["ts"]
        snapshot = list(candles)

    i = len(snapshot) - 1
    if i < max(PIVOT_LEFT + PIVOT_RIGHT + 1, VOLUME_PERIOD):
        return

    avg_vol = volume_average(snapshot, i)
    if not avg_vol or avg_vol <= 0:
        return
    volume = candle["volume"]
    if volume < avg_vol * MIN_VOLUME_MULTIPLE:
        return

    candle_range = candle["high"] - candle["low"]
    if candle_range <= 0:
        return

    upper_wick_ratio = (candle["high"] - max(candle["open"], candle["close"])) / candle_range
    lower_wick_ratio = (min(candle["open"], candle["close"]) - candle["low"]) / candle_range

    # Only use pivots that have all five right-side candles completed before this sweep candle.
    pivot_indices = range(PIVOT_LEFT, i - PIVOT_RIGHT)
    for p_idx in pivot_indices:
        p = snapshot[p_idx]
        if p["ts"] >= candle["ts"]:
            continue

        if pivot_high(snapshot, p_idx):
            level = p["high"]
            depth = (candle["high"] - level) / level * 100
            key = (symbol, "BEARISH", p["ts"], candle["ts"])
            if (
                MIN_SWEEP_PCT <= depth <= MAX_SWEEP_PCT
                and candle["close"] < level
                and upper_wick_ratio >= MIN_WICK_RATIO
                and key not in ALERTED
            ):
                if send_discord(
                    symbol, "BEARISH", level, candle["high"], depth,
                    candle["close"], upper_wick_ratio * 100, volume, avg_vol, candle
                ):
                    ALERTED.add(key)

        if pivot_low(snapshot, p_idx):
            level = p["low"]
            depth = (level - candle["low"]) / level * 100
            key = (symbol, "BULLISH", p["ts"], candle["ts"])
            if (
                MIN_SWEEP_PCT <= depth <= MAX_SWEEP_PCT
                and candle["close"] > level
                and lower_wick_ratio >= MIN_WICK_RATIO
                and key not in ALERTED
            ):
                if send_discord(
                    symbol, "BULLISH", level, candle["low"], depth,
                    candle["close"], lower_wick_ratio * 100, volume, avg_vol, candle
                ):
                    ALERTED.add(key)


def initialize_symbol(symbol):
    candles = get_history(symbol)
    if candles:
        with LOCK:
            HISTORY[symbol] = candles[-HISTORY_CANDLES:]
            LAST_COMPLETED_TS[symbol] = candles[-1]["ts"]
        print(f"Loaded {len(candles)} completed 1H candles for {symbol}")
    else:
        print(f"WARNING: No history loaded for {symbol}")


def ws_worker(group_no, symbols):
    args = [{"channel": CANDLE_CHANNEL, "instId": s} for s in symbols]
    while True:
        ws = None
        try:
            print(f"1H SFP WebSocket {group_no}: connecting ({len(symbols)} symbols)")
            ws = websocket.create_connection(BLOFIN_WS, timeout=WS_TIMEOUT, enable_multithread=True)
            ws.send(json.dumps({"op": "subscribe", "args": args}, separators=(",", ":")))
            print(f"1H SFP WebSocket {group_no}: connected")
            last_ping = time.time()
            while True:
                try:
                    message = ws.recv()
                    if message == "pong":
                        continue
                    if not message:
                        raise RuntimeError("empty WebSocket message")
                    payload = json.loads(message)
                    if payload.get("event") == "error":
                        print(f"WebSocket subscription error: {payload}")
                        continue
                    arg = payload.get("arg", {})
                    if arg.get("channel") != CANDLE_CHANNEL:
                        continue
                    data = payload.get("data", [])
                    if not data:
                        continue
                    c = parse_row(data[0])
                    if c and c["confirm"] == "1":
                        evaluate_new_candle(arg.get("instId", ""), c)
                    if time.time() - last_ping >= 15:
                        ws.send("ping")
                        last_ping = time.time()
                except websocket.WebSocketTimeoutException:
                    if time.time() - last_ping >= 15:
                        ws.send("ping")
                        last_ping = time.time()
        except Exception as e:
            print(f"1H SFP WebSocket {group_no} disconnected: {e}")
            time.sleep(RECONNECT_DELAY)
        finally:
            try:
                if ws:
                    ws.close()
            except Exception:
                pass


def main():
    print("=" * 65)
    print("BLOFIN 1H SFP / LIQUIDITY SWEEP SCANNER")
    print(f"Started: {now_utc()}")
    print(f"Rules: pivot 5L/5R | sweep {MIN_SWEEP_PCT:.2f}-{MAX_SWEEP_PCT:.2f}% | "
          f"wick >= {MIN_WICK_RATIO:.0%} | volume >= {MIN_VOLUME_MULTIPLE:.2f}x SMA20")
    print("Alerts only after a completed 1H candle closes.")
    print("=" * 65)

    if not DISCORD_WEBHOOK_URL:
        raise SystemExit("Missing GitHub secret: SFP_1H_DISCORD_WEBHOOK_URL")

    symbols = get_symbols()
    if not symbols:
        raise SystemExit("No live USDT perpetual symbols returned by BloFin.")

    for n, symbol in enumerate(symbols, 1):
        initialize_symbol(symbol)
        time.sleep(0.5)

    print(f"Historical initialization complete. Monitoring {len(symbols)} symbols.")
    for start in range(0, len(symbols), WS_GROUP_SIZE):
        group = symbols[start:start + WS_GROUP_SIZE]
        thread = threading.Thread(
            target=ws_worker,
            args=(start // WS_GROUP_SIZE + 1, group),
            daemon=True,
        )
        thread.start()
        time.sleep(1.0)

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
