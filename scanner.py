import requests

URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"

print("Connecting to Binance Futures...")

try:
    response = requests.get(URL, timeout=15)

    print("HTTP status:", response.status_code)

    if response.status_code == 200:
        data = response.json()

        symbols = []

        for symbol in data["symbols"]:
            if (
                symbol["quoteAsset"] == "USDT"
                and symbol["contractType"] == "PERPETUAL"
                and symbol["status"] == "TRADING"
            ):
                symbols.append(symbol["symbol"])

        print("SUCCESS!")
        print("USDT perpetual coins found:", len(symbols))
        print("First 20 coins:")
        print(symbols[:20])

    else:
        print("Binance did not allow the request.")
        print(response.text[:500])

except Exception as e:
    print("ERROR:", e)
