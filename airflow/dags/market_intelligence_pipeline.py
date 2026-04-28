from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import feedparser
from textblob import TextBlob
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
import hashlib
import requests

# ── Config ──────────────────────────────────────────────────────
OPENSEARCH_HOST = "aventra-opensearch"
OPENSEARCH_PORT = 9200
INDEX_PRICES    = "market-prices"
INDEX_NEWS      = "market-news"

WAHA_URL        = "http://waha-infra:3000"
WAHA_API_KEY    = "aventra-infra-123"
WAHA_SESSION    = "default"
ALERT_TO        = "628978279036@c.us"

# ── IDX Stocks ──────────────────────────────────────────────────
STOCKS = {
    # Perbankan
    "BBCA.JK": "Bank BCA",
    "BBRI.JK": "Bank BRI",
    "BMRI.JK": "Bank Mandiri",
    "BBNI.JK": "Bank BNI",
    "BRIS.JK": "Bank BRI Syariah",
    "MEGA.JK": "Bank Mega",
    "BTPS.JK": "Bank BTPN Syariah",
    # Teknologi & Telekomunikasi
    "TLKM.JK": "Telkom Indonesia",
    "EXCL.JK": "XL Axiata",
    "ISAT.JK": "Indosat Ooredoo",
    "GOTO.JK": "GoTo Group",
    "BUKA.JK": "Bukalapak",
    "EMTK.JK": "Elang Mahkota Teknologi",
    # Energi & Tambang
    "ADRO.JK": "Adaro Energy",
    "PTBA.JK": "Bukit Asam",
    "INCO.JK": "Vale Indonesia",
    "ANTM.JK": "Aneka Tambang",
    "TINS.JK": "Timah",
    "MEDC.JK": "Medco Energi",
    "PGAS.JK": "Perusahaan Gas Negara",
    # Consumer & Retail
    "ASII.JK": "Astra International",
    "UNVR.JK": "Unilever Indonesia",
    "ICBP.JK": "Indofood CBP",
    "INDF.JK": "Indofood",
    "MYOR.JK": "Mayora Indah",
    "HMSP.JK": "HM Sampoerna",
    # Properti & Infrastruktur
    "JSMR.JK": "Jasa Marga",
    "PTPP.JK": "PP Persero",
    "BSDE.JK": "BSD City",
    "SMRA.JK": "Summarecon",
}

# ── Crypto ──────────────────────────────────────────────────────
CRYPTO = {
    "BTC-USD":  "Bitcoin",
    "ETH-USD":  "Ethereum",
    "BNB-USD":  "BNB",
    "SOL-USD":  "Solana",
    "XRP-USD":  "XRP",
    "ADA-USD":  "Cardano",
    "AVAX-USD": "Avalanche",
    "DOT-USD":  "Polkadot",
    "LINK-USD": "Chainlink",
    "DOGE-USD": "Dogecoin",
    "SHIB-USD": "Shiba Inu",
    "TRX-USD":  "TRON",
    "LTC-USD":  "Litecoin",
    "BCH-USD":  "Bitcoin Cash",
    "AAVE-USD": "Aave",
    "CRV-USD":  "Curve",
}

# ── RSS Feeds ────────────────────────────────────────────────────
RSS_FEEDS = {
    "detik_finance":    "https://finance.detik.com/moneter/rss",
    "detik_bursa":      "https://finance.detik.com/bursa-valas/rss",
    "cnbc_market":      "https://www.cnbcindonesia.com/market/rss",
    "bloomberg_crypto": "https://feeds.bloomberg.com/crypto/news.rss",
    "coindesk":         "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "cointelegraph":    "https://cointelegraph.com/rss",
}

# ── Alert Thresholds ─────────────────────────────────────────────
PRICE_DROP_THRESHOLD    = -3.0
PRICE_PUMP_THRESHOLD    = 5.0
CRYPTO_DROP_THRESHOLD   = -7.0
CRYPTO_PUMP_THRESHOLD   = 10.0
SENTIMENT_VERY_POSITIVE = 0.5
SENTIMENT_VERY_NEGATIVE = -0.5

default_args = {
    "owner": "aventra",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}


# ── Helpers ──────────────────────────────────────────────────────
def get_opensearch_client():
    return OpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        use_ssl=False,
        verify_certs=False,
        timeout=30,
    )


def ensure_indices(client):
    price_mapping = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "ticker":        {"type": "keyword"},
                "name":          {"type": "keyword"},
                "type":          {"type": "keyword"},
                "price":         {"type": "float"},
                "open":          {"type": "float"},
                "high":          {"type": "float"},
                "low":           {"type": "float"},
                "prev_close":    {"type": "float"},
                "volume":        {"type": "long"},
                "avg_volume_3m": {"type": "long"},
                "change_pct":    {"type": "float"},
                "change_abs":    {"type": "float"},
                "year_high":     {"type": "float"},
                "year_low":      {"type": "float"},
                "market_cap":    {"type": "long"},
                "signal":        {"type": "keyword"},
                "fetched_at":    {"type": "date"},
            }
        }
    }

    news_mapping = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "news_id":         {"type": "keyword"},
                "title":           {
                    "type": "text",
                    "fields": {"raw": {"type": "keyword"}}
                },
                "summary":         {"type": "text"},
                "source":          {"type": "keyword"},
                "link":            {"type": "keyword"},
                "published_at":    {"type": "date"},
                "fetched_at":      {"type": "date"},
                "sentiment_score": {"type": "float"},
                "sentiment_label": {"type": "keyword"},
                "subjectivity":    {"type": "float"},
                "related_tickers": {"type": "keyword"},
                "category":        {"type": "keyword"},
            }
        }
    }

    for index_name, mapping in [
        (INDEX_PRICES, price_mapping),
        (INDEX_NEWS, news_mapping),
    ]:
        try:
            if not client.indices.exists(index_name):
                client.indices.create(index=index_name, body=mapping)
                print(f"✅ Created index: {index_name}")
            else:
                print(f"ℹ️ Index exists: {index_name}")
        except Exception as e:
            print(f"⚠️ Index {index_name} skipped: {e}")


def send_whatsapp_alert(message: str):
    try:
        response = requests.post(
            f"{WAHA_URL}/api/sendText",
            headers={
                "X-Api-Key": WAHA_API_KEY,
                "Content-Type": "application/json",
            },
            json={
                "session": WAHA_SESSION,
                "chatId": ALERT_TO,
                "text": message,
            },
            timeout=10,
        )
        if response.status_code in [200, 201]:
            print("✅ WhatsApp alert sent")
        else:
            print(f"⚠️ WhatsApp failed: {response.status_code}")
    except Exception as e:
        print(f"⚠️ WhatsApp error: {e}")


def get_signal(change_pct: float, asset_type: str) -> str:
    if asset_type == "crypto":
        if change_pct >= CRYPTO_PUMP_THRESHOLD:
            return "STRONG_BUY"
        elif change_pct >= 3.0:
            return "BUY"
        elif change_pct <= CRYPTO_DROP_THRESHOLD:
            return "STRONG_SELL"
        elif change_pct <= -3.0:
            return "SELL"
        return "HOLD"
    else:
        if change_pct >= PRICE_PUMP_THRESHOLD:
            return "STRONG_BUY"
        elif change_pct >= 2.0:
            return "BUY"
        elif change_pct <= PRICE_DROP_THRESHOLD:
            return "STRONG_SELL"
        elif change_pct <= -1.5:
            return "SELL"
        return "HOLD"


def detect_related_tickers(text: str) -> list:
    text_upper = text.upper()
    found = []
    ticker_keywords = {
        "BBCA":  ["BCA", "BANK CENTRAL ASIA", "BBCA"],
        "BBRI":  ["BRI", "BANK RAKYAT INDONESIA", "BBRI"],
        "BMRI":  ["MANDIRI", "BANK MANDIRI", "BMRI"],
        "BBNI":  ["BNI", "BANK NEGARA INDONESIA", "BBNI"],
        "TLKM":  ["TELKOM", "TELKOMSEL", "TLKM"],
        "GOTO":  ["GOTO", "GOJEK", "TOKOPEDIA"],
        "BUKA":  ["BUKALAPAK", "BUKA"],
        "ASII":  ["ASTRA", "ASII"],
        "UNVR":  ["UNILEVER", "UNVR"],
        "ADRO":  ["ADARO", "ADRO"],
        "ANTM":  ["ANTAM", "ANTM", "ANEKA TAMBANG"],
        "PGAS":  ["PGN", "PERUSAHAAN GAS", "PGAS"],
        "JSMR":  ["JASA MARGA", "JSMR", "TOL"],
        "BTC":   ["BITCOIN", "BTC"],
        "ETH":   ["ETHEREUM", "ETH"],
        "BNB":   ["BNB", "BINANCE"],
        "SOL":   ["SOLANA", "SOL"],
        "XRP":   ["XRP", "RIPPLE"],
        "DOGE":  ["DOGECOIN", "DOGE"],
    }
    for ticker, keywords in ticker_keywords.items():
        if any(kw in text_upper for kw in keywords):
            found.append(ticker)
    return found


def format_price(num: float) -> str:
    if num is None:
        return "N/A"
    if num >= 1000:
        return f"{num:,.2f}"
    elif num >= 1:
        return f"{num:.4f}"
    return f"{num:.8f}"


# ════════════════════════════════════════════════════════════════
# TASK 1 — Fetch Stock Prices
# ════════════════════════════════════════════════════════════════
def fetch_stock_prices(**context):
    results = []
    alerts  = []

    print(f"📈 Fetching {len(STOCKS)} IDX stocks...")

    for ticker, name in STOCKS.items():
        try:
            data       = yf.Ticker(ticker)
            info       = data.fast_info
            price      = info.last_price
            open_price = info.open
            high       = info.day_high
            low        = info.day_low
            prev_close = info.previous_close
            year_high  = info.year_high
            year_low   = info.year_low
            market_cap = info.market_cap
            volume     = info.last_volume
            avg_vol_3m = info.three_month_average_volume

            if not price or not open_price:
                print(f"⚠️ {ticker}: No price data")
                continue

            change_abs = price - open_price
            change_pct = (change_abs / open_price * 100)
            signal     = get_signal(change_pct, "stock")
            emoji      = "📈" if change_pct >= 0 else "📉"

            print(
                f"{emoji} {ticker} ({name}): "
                f"Rp{format_price(price)} | {change_pct:+.2f}% | {signal}"
            )

            if change_pct <= PRICE_DROP_THRESHOLD:
                alerts.append({
                    "type":    "PRICE_DROP",
                    "ticker":  ticker,
                    "name":    name,
                    "message": (
                        f"📉 *{name}* (`{ticker}`) TURUN SIGNIFIKAN!\n"
                        f"Harga:     Rp{format_price(price)}\n"
                        f"Perubahan: *{change_pct:+.2f}%*\n"
                        f"Open:      Rp{format_price(open_price)}\n"
                        f"High/Low:  Rp{format_price(high)} / Rp{format_price(low)}\n"
                        f"52w High:  Rp{format_price(year_high)}\n"
                        f"52w Low:   Rp{format_price(year_low)}\n"
                        f"Signal:    *{signal}* ⚠️"
                    )
                })

            if change_pct >= PRICE_PUMP_THRESHOLD:
                alerts.append({
                    "type":    "PRICE_PUMP",
                    "ticker":  ticker,
                    "name":    name,
                    "message": (
                        f"🚀 *{name}* (`{ticker}`) NAIK SIGNIFIKAN!\n"
                        f"Harga:     Rp{format_price(price)}\n"
                        f"Perubahan: *{change_pct:+.2f}%*\n"
                        f"Open:      Rp{format_price(open_price)}\n"
                        f"High/Low:  Rp{format_price(high)} / Rp{format_price(low)}\n"
                        f"52w High:  Rp{format_price(year_high)}\n"
                        f"52w Low:   Rp{format_price(year_low)}\n"
                        f"Signal:    *{signal}* 🎯"
                    )
                })

            results.append({
                "ticker":        ticker,
                "name":          name,
                "type":          "stock",
                "price":         round(price, 2),
                "open":          round(open_price, 2),
                "high":          round(high, 2) if high else None,
                "low":           round(low, 2) if low else None,
                "prev_close":    round(prev_close, 2) if prev_close else None,
                "volume":        int(volume) if volume else None,
                "avg_volume_3m": int(avg_vol_3m) if avg_vol_3m else None,
                "change_pct":    round(change_pct, 2),
                "change_abs":    round(change_abs, 2),
                "year_high":     round(year_high, 2) if year_high else None,
                "year_low":      round(year_low, 2) if year_low else None,
                "market_cap":    int(market_cap) if market_cap else None,
                "signal":        signal,
                "fetched_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            })

        except Exception as e:
            print(f"⚠️ Failed {ticker}: {e}")
            continue

    context["ti"].xcom_push(key="stock_results", value=results)
    context["ti"].xcom_push(key="stock_alerts",  value=alerts)
    print(f"\n✅ Stocks: {len(results)} fetched, {len(alerts)} alerts")


# ════════════════════════════════════════════════════════════════
# TASK 2 — Fetch Crypto Prices
# ════════════════════════════════════════════════════════════════
def fetch_crypto_prices(**context):
    results = []
    alerts  = []

    print(f"🪙 Fetching {len(CRYPTO)} cryptocurrencies...")

    for ticker, name in CRYPTO.items():
        try:
            data       = yf.Ticker(ticker)
            info       = data.fast_info
            price      = info.last_price
            open_price = info.open
            high       = info.day_high
            low        = info.day_low
            prev_close = info.previous_close
            year_high  = info.year_high
            year_low   = info.year_low
            market_cap = info.market_cap
            volume     = info.last_volume
            avg_vol_3m = info.three_month_average_volume

            if not price or not open_price:
                print(f"⚠️ {ticker}: No price data")
                continue

            change_abs = price - open_price
            change_pct = (change_abs / open_price * 100)
            signal     = get_signal(change_pct, "crypto")
            emoji      = "🟢" if change_pct >= 0 else "🔴"

            print(
                f"{emoji} {ticker} ({name}): "
                f"${format_price(price)} | {change_pct:+.2f}% | {signal}"
            )

            if change_pct <= CRYPTO_DROP_THRESHOLD:
                alerts.append({
                    "type":    "CRYPTO_DROP",
                    "ticker":  ticker,
                    "name":    name,
                    "message": (
                        f"🔴 *{name}* (`{ticker}`) CRASH!\n"
                        f"Harga:     ${format_price(price)}\n"
                        f"Perubahan: *{change_pct:+.2f}%*\n"
                        f"Open:      ${format_price(open_price)}\n"
                        f"High/Low:  ${format_price(high)} / ${format_price(low)}\n"
                        f"52w High:  ${format_price(year_high)}\n"
                        f"52w Low:   ${format_price(year_low)}\n"
                        f"Signal:    *{signal}* 🚨"
                    )
                })

            if change_pct >= CRYPTO_PUMP_THRESHOLD:
                alerts.append({
                    "type":    "CRYPTO_PUMP",
                    "ticker":  ticker,
                    "name":    name,
                    "message": (
                        f"🟢 *{name}* (`{ticker}`) PUMP!\n"
                        f"Harga:     ${format_price(price)}\n"
                        f"Perubahan: *{change_pct:+.2f}%*\n"
                        f"Open:      ${format_price(open_price)}\n"
                        f"High/Low:  ${format_price(high)} / ${format_price(low)}\n"
                        f"52w High:  ${format_price(year_high)}\n"
                        f"52w Low:   ${format_price(year_low)}\n"
                        f"Signal:    *{signal}* 🚀"
                    )
                })

            results.append({
                "ticker":        ticker,
                "name":          name,
                "type":          "crypto",
                "price":         round(price, 8),
                "open":          round(open_price, 8),
                "high":          round(high, 8) if high else None,
                "low":           round(low, 8) if low else None,
                "prev_close":    round(prev_close, 8) if prev_close else None,
                "volume":        int(volume) if volume else None,
                "avg_volume_3m": int(avg_vol_3m) if avg_vol_3m else None,
                "change_pct":    round(change_pct, 2),
                "change_abs":    round(change_abs, 8),
                "year_high":     round(year_high, 8) if year_high else None,
                "year_low":      round(year_low, 8) if year_low else None,
                "market_cap":    int(market_cap) if market_cap else None,
                "signal":        signal,
                "fetched_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            })

        except Exception as e:
            print(f"⚠️ Failed {ticker}: {e}")
            continue

    context["ti"].xcom_push(key="crypto_results", value=results)
    context["ti"].xcom_push(key="crypto_alerts",  value=alerts)
    print(f"\n✅ Crypto: {len(results)} fetched, {len(alerts)} alerts")


# ════════════════════════════════════════════════════════════════
# TASK 3 — Fetch & Analyze News Sentiment
# ════════════════════════════════════════════════════════════════
def fetch_and_analyze_news(**context):
    import time
    results  = []
    alerts   = []
    seen_ids = set()

    print(f"📰 Fetching from {len(RSS_FEEDS)} RSS feeds...")

    for source_name, feed_url in RSS_FEEDS.items():
        try:
            time.sleep(1)
            feed = feedparser.parse(feed_url)

            if not feed.entries:
                print(f"⚠️ {source_name}: No entries")
                continue

            print(f"✅ {source_name}: {len(feed.entries)} articles")

            for entry in feed.entries[:15]:
                title   = entry.get("title", "").strip()
                summary = entry.get(
                    "summary", entry.get("description", "")
                ).strip()
                link    = entry.get("link", "")

                if not title:
                    continue

                news_id = hashlib.md5(
                    f"{link}{title}".encode()
                ).hexdigest()

                if news_id in seen_ids:
                    continue
                seen_ids.add(news_id)

                published = entry.get("published_parsed")
                if published:
                    pub_dt = datetime(
                        *published[:6]
                    ).strftime("%Y-%m-%dT%H:%M:%S")
                else:
                    pub_dt = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

                text_to_analyze = f"{title} {summary}"
                blob            = TextBlob(text_to_analyze)
                sentiment_score = round(blob.sentiment.polarity, 4)
                subjectivity    = round(blob.sentiment.subjectivity, 4)

                if sentiment_score >= 0.2:
                    sentiment_label = "positive"
                elif sentiment_score <= -0.2:
                    sentiment_label = "negative"
                else:
                    sentiment_label = "neutral"

                title_lower = title.lower()
                if any(w in title_lower for w in
                       ["bitcoin", "crypto", "ethereum", "blockchain"]):
                    category = "crypto"
                elif any(w in title_lower for w in
                         ["saham", "ihsg", "bursa", "emiten", "idx"]):
                    category = "stock"
                elif any(w in title_lower for w in
                         ["rupiah", "kurs", "dolar", "forex"]):
                    category = "forex"
                elif any(w in title_lower for w in
                         ["inflasi", "bi rate", "suku bunga", "ekonomi"]):
                    category = "macro"
                else:
                    category = "general"

                related = detect_related_tickers(text_to_analyze)

                if related and sentiment_score >= SENTIMENT_VERY_POSITIVE:
                    alerts.append({
                        "type":    "SENTIMENT_POSITIVE",
                        "tickers": related,
                        "score":   sentiment_score,
                        "message": (
                            f"📰 *Sentimen POSITIF* — "
                            f"{', '.join(related)}\n"
                            f"Judul:     _{title}_\n"
                            f"Sumber:    {source_name}\n"
                            f"Sentiment: *{sentiment_score:+.2f}* 😊\n"
                            f"Link: {link}"
                        )
                    })
                elif related and sentiment_score <= SENTIMENT_VERY_NEGATIVE:
                    alerts.append({
                        "type":    "SENTIMENT_NEGATIVE",
                        "tickers": related,
                        "score":   sentiment_score,
                        "message": (
                            f"📰 *Sentimen NEGATIF* — "
                            f"{', '.join(related)}\n"
                            f"Judul:     _{title}_\n"
                            f"Sumber:    {source_name}\n"
                            f"Sentiment: *{sentiment_score:+.2f}* 😟\n"
                            f"Link: {link}"
                        )
                    })

                results.append({
                    "news_id":         news_id,
                    "title":           title,
                    "summary":         summary[:1000],
                    "source":          source_name,
                    "link":            link,
                    "published_at":    pub_dt,
                    "fetched_at":      datetime.utcnow().strftime(
                        "%Y-%m-%dT%H:%M:%S"),
                    "sentiment_score": sentiment_score,
                    "sentiment_label": sentiment_label,
                    "subjectivity":    subjectivity,
                    "related_tickers": related,
                    "category":        category,
                })

        except Exception as e:
            print(f"⚠️ Failed {source_name}: {e}")
            continue

    context["ti"].xcom_push(key="news_results", value=results)
    context["ti"].xcom_push(key="news_alerts",  value=alerts)
    print(f"\n✅ News: {len(results)} articles, {len(alerts)} alerts")


# ════════════════════════════════════════════════════════════════
# TASK 4 — Load to OpenSearch
# ════════════════════════════════════════════════════════════════
def load_to_opensearch(**context):
    stock_results  = context["ti"].xcom_pull(
        key="stock_results",  task_ids="fetch_stock_prices") or []
    crypto_results = context["ti"].xcom_pull(
        key="crypto_results", task_ids="fetch_crypto_prices") or []
    news_results   = context["ti"].xcom_pull(
        key="news_results",   task_ids="fetch_and_analyze_news") or []

    client     = get_opensearch_client()
    ensure_indices(client)
    all_prices = stock_results + crypto_results

    price_actions = [
        {"_index": INDEX_PRICES, "_source": p}
        for p in all_prices
    ]
    news_actions = [
        {"_index": INDEX_NEWS, "_id": n["news_id"], "_source": n}
        for n in news_results
    ]

    if price_actions:
        success, errors = bulk(client, price_actions, raise_on_error=False)
        print(f"✅ Prices indexed: {success}, errors: {len(errors)}")

    if news_actions:
        success, errors = bulk(client, news_actions, raise_on_error=False)
        print(f"✅ News indexed: {success}, errors: {len(errors)}")

    strong_buy  = sum(1 for p in all_prices if p["signal"] == "STRONG_BUY")
    strong_sell = sum(1 for p in all_prices if p["signal"] == "STRONG_SELL")
    pos_news    = sum(
        1 for n in news_results if n["sentiment_label"] == "positive"
    )
    neg_news    = sum(
        1 for n in news_results if n["sentiment_label"] == "negative"
    )

    context["ti"].xcom_push(key="summary", value={
        "stocks_indexed":  len(stock_results),
        "crypto_indexed":  len(crypto_results),
        "news_indexed":    len(news_results),
        "strong_buy":      strong_buy,
        "strong_sell":     strong_sell,
        "positive_news":   pos_news,
        "negative_news":   neg_news,
    })


# ════════════════════════════════════════════════════════════════
# TASK 5 — Send Detailed Alerts
# ════════════════════════════════════════════════════════════════
def send_alerts(**context):
    stock_alerts  = context["ti"].xcom_pull(
        key="stock_alerts",  task_ids="fetch_stock_prices") or []
    crypto_alerts = context["ti"].xcom_pull(
        key="crypto_alerts", task_ids="fetch_crypto_prices") or []
    news_alerts   = context["ti"].xcom_pull(
        key="news_alerts",   task_ids="fetch_and_analyze_news") or []
    summary       = context["ti"].xcom_pull(
        key="summary", task_ids="load_to_opensearch") or {}

    now      = datetime.utcnow()
    wib_hour = (now.hour + 7) % 24

    stock_drops  = [a for a in stock_alerts  if a["type"] == "PRICE_DROP"]
    stock_pumps  = [a for a in stock_alerts  if a["type"] == "PRICE_PUMP"]
    crypto_drops = [a for a in crypto_alerts if a["type"] == "CRYPTO_DROP"]
    crypto_pumps = [a for a in crypto_alerts if a["type"] == "CRYPTO_PUMP"]

    if stock_drops or stock_pumps:
        lines = [
            f"🏦 *IDX Stock Alert*",
            f"_{now.strftime('%Y-%m-%d %H:%M')} UTC | {wib_hour:02d}:00 WIB_",
            "",
        ]
        if stock_drops:
            lines.append("*📉 Saham Turun:*")
            for a in stock_drops[:3]:
                lines.append(a["message"])
                lines.append("")
        if stock_pumps:
            lines.append("*🚀 Saham Naik:*")
            for a in stock_pumps[:3]:
                lines.append(a["message"])
                lines.append("")
        send_whatsapp_alert("\n".join(lines))

    if crypto_drops or crypto_pumps:
        lines = [
            f"🪙 *Crypto Alert*",
            f"_{now.strftime('%Y-%m-%d %H:%M')} UTC | {wib_hour:02d}:00 WIB_",
            "",
        ]
        if crypto_drops:
            lines.append("*🔴 Crypto Crash:*")
            for a in crypto_drops[:3]:
                lines.append(a["message"])
                lines.append("")
        if crypto_pumps:
            lines.append("*🟢 Crypto Pump:*")
            for a in crypto_pumps[:3]:
                lines.append(a["message"])
                lines.append("")
        send_whatsapp_alert("\n".join(lines))

    sentiment_alerts = [
        a for a in news_alerts
        if a["type"] in ["SENTIMENT_POSITIVE", "SENTIMENT_NEGATIVE"]
    ]
    if sentiment_alerts:
        lines = [
            f"📰 *Market Sentiment Alert*",
            f"_{now.strftime('%Y-%m-%d %H:%M')} UTC_",
            "",
        ]
        for a in sentiment_alerts[:3]:
            lines.append(a["message"])
            lines.append("")
        send_whatsapp_alert("\n".join(lines))

    if now.minute < 30 and now.hour % 6 == 0:
        market_mood = (
            "🟢 BULLISH"
            if summary.get("positive_news", 0) > summary.get("negative_news", 0)
            else "🔴 BEARISH"
        )
        report = (
            f"📊 *Aventra Market Intelligence Report*\n"
            f"{now.strftime('%Y-%m-%d %H:%M')} UTC | {wib_hour:02d}:00 WIB_\n\n"
            f"*📈 Data Collected:*\n"
            f"Saham IDX: {summary.get('stocks_indexed', 0)} tickers\n"
            f"Crypto:    {summary.get('crypto_indexed', 0)} coins\n"
            f"Berita:    {summary.get('news_indexed', 0)} artikel\n\n"
            f"*🎯 Market Signals:*\n"
            f"Strong Buy:  {summary.get('strong_buy', 0)} aset\n"
            f"Strong Sell: {summary.get('strong_sell', 0)} aset\n\n"
            f"*📰 Sentiment Berita:*\n"
            f"Positif: {summary.get('positive_news', 0)} artikel\n"
            f"Negatif: {summary.get('negative_news', 0)} artikel\n"
            f"Market Mood: {market_mood}\n\n"
            f"_Powered by Aventra Market Intelligence_ 🚀"
        )
        send_whatsapp_alert(report)

    total_alerts = (
        len(stock_alerts) + len(crypto_alerts) + len(news_alerts)
    )
    print(f"✅ Total alerts processed: {total_alerts}")


# ════════════════════════════════════════════════════════════════
# DAG Definition
# ════════════════════════════════════════════════════════════════
with DAG(
    dag_id="market_intelligence_pipeline",
    default_args=default_args,
    description="Indonesia market intelligence — stocks, crypto, news sentiment",
    schedule_interval="0 */2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["market", "stocks", "crypto", "sentiment", "indonesia"],
) as dag:

    t_stocks = PythonOperator(
        task_id="fetch_stock_prices",
        python_callable=fetch_stock_prices,
    )

    t_crypto = PythonOperator(
        task_id="fetch_crypto_prices",
        python_callable=fetch_crypto_prices,
    )

    t_news = PythonOperator(
        task_id="fetch_and_analyze_news",
        python_callable=fetch_and_analyze_news,
    )

    t_load = PythonOperator(
        task_id="load_to_opensearch",
        python_callable=load_to_opensearch,
    )

    t_alerts = PythonOperator(
        task_id="send_alerts",
        python_callable=send_alerts,
    )

    [t_stocks, t_crypto, t_news] >> t_load >> t_alerts