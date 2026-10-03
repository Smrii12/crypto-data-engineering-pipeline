from flask import Flask, render_template, jsonify
import urllib.request
import json
import ssl
import os
import sqlite3
from datetime import datetime


app = Flask(__name__)

TEST_MODE = False

API_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd"
    "&order=market_cap_desc"
    "&per_page=100"
    "&page=1"
    "&sparkline=false"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

SSL_CONTEXT = ssl.create_default_context()

def fetch_crypto_data():

    request = urllib.request.Request(
        API_URL,
        headers=HEADERS
    )

    with urllib.request.urlopen(
        request,
        context=SSL_CONTEXT,
        timeout=20
    ) as response:

        data = response.read().decode("utf-8")

    return json.loads(data)

def fetch_usd_inr_rate():

    exchange_url = (
        "https://api.frankfurter.app/latest"
        "?from=USD&to=INR"
    )

    request = urllib.request.Request(
        exchange_url,
        headers=HEADERS
    )

    with urllib.request.urlopen(
        request,
        context=SSL_CONTEXT,
        timeout=20
    ) as response:

        exchange_data = json.loads(
            response.read().decode("utf-8")
        )

    return float(exchange_data["rates"]["INR"])

def validate_crypto_record(record):

    token_id = record.get("id")
    current_price = record.get("current_price")
    market_cap = record.get("market_cap")
    total_volume = record.get("total_volume")

    if not token_id:
        raise ValueError("Missing token ID")

    if current_price is None:
        raise ValueError("Missing current price")

    if market_cap is None:
        raise ValueError("Missing market cap")

    if total_volume is None:
        raise ValueError("Missing 24h volume")

    return {
        "token_id": str(token_id),
        "usd_price": float(current_price),
        "usd_market_cap": float(market_cap),
        "usd_24h_volume": float(total_volume)
    }

def process_data(raw_api_list):

    production_sink = []
    dead_letter_queue = []

    if TEST_MODE and len(raw_api_list) > 1:

        raw_api_list[1]["market_cap"] = (
            "CORRUPTED_TEXT_STRING"
        )

    for record in raw_api_list:

        try:

            clean_record = validate_crypto_record(record)

            production_sink.append(clean_record)

        except Exception as error:

            quarantine = {
                "original_record": record,
                "error": str(error),
                "timestamp": datetime.now().isoformat()
            }

            dead_letter_queue.append(quarantine)

    current_date = datetime.now().strftime("%Y-%m-%d")

    production_folder = os.path.join(
        "data_lake",
        "production",
        f"date={current_date}"
    )

    dlq_folder = os.path.join(
        "data_lake",
        "dlq",
        f"date={current_date}"
    )

    os.makedirs(
        production_folder,
        exist_ok=True
    )

    os.makedirs(
        dlq_folder,
        exist_ok=True
    )

    batch_timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    production_file = os.path.join(
        production_folder,
        f"batch_{batch_timestamp}.json"
    )

    with open(
        production_file,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            production_sink,
            file,
            indent=4
        )

    if dead_letter_queue:

        dlq_file = os.path.join(
            dlq_folder,
            f"poisoned_{batch_timestamp}.json"
        )

        with open(
            dlq_file,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                dead_letter_queue,
                file,
                indent=4
            )

    return production_sink, dead_letter_queue

def load_into_sqlite(records):

    connection = sqlite3.connect(":memory:")

    cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE market_metrics (
            token_id TEXT,
            usd_price REAL,
            usd_market_cap REAL,
            usd_24h_volume REAL
        )
    """)

    for record in records:

        cursor.execute("""
            INSERT INTO market_metrics
            VALUES (?, ?, ?, ?)
        """, (
            record["token_id"],
            record["usd_price"],
            record["usd_market_cap"],
            record["usd_24h_volume"]
        ))

    connection.commit()

    return connection

def run_analytics(records):

    connection = load_into_sqlite(records)

    cursor = connection.cursor()

    cursor.execute("""
        SELECT
            token_id,
            usd_market_cap
        FROM market_metrics
        ORDER BY usd_market_cap DESC
        LIMIT 10
    """)

    market_cap = [
        {
            "token_id": row[0],
            "market_cap": row[1]
        }
        for row in cursor.fetchall()
    ]

    cursor.execute("""
        SELECT
            token_id,
            usd_24h_volume
        FROM market_metrics
        ORDER BY usd_24h_volume DESC
        LIMIT 10
    """)

    volume = [
        {
            "token_id": row[0],
            "volume": row[1]
        }
        for row in cursor.fetchall()
    ]


    cursor.execute("""
        SELECT
            token_id,
            usd_price
        FROM market_metrics
        WHERE usd_price > 100
        ORDER BY usd_price DESC
    """)

    price_filter = [
        {
            "token_id": row[0],
            "price": row[1]
        }
        for row in cursor.fetchall()
    ]

    cursor.execute("""
        SELECT
            COUNT(*),
            AVG(usd_price),
            MAX(usd_price),
            MIN(usd_price)
        FROM market_metrics
    """)

    row = cursor.fetchone()

    statistics = {
        "total_assets": row[0],
        "average_price": row[1],
        "highest_price": row[2],
        "lowest_price": row[3]
    }

    cursor.execute("""
        SELECT
            CASE
                WHEN usd_price < 1
                    THEN 'Under $1'

                WHEN usd_price < 100
                    THEN '$1-$100'

                ELSE 'Above $100'

            END AS price_category,

            COUNT(*) AS asset_count

        FROM market_metrics

        GROUP BY price_category

        ORDER BY
            CASE price_category

                WHEN 'Under $1'
                    THEN 1

                WHEN '$1-$100'
                    THEN 2

                WHEN 'Above $100'
                    THEN 3

            END
    """)

    distribution = [
        {
            "category": row[0],
            "count": row[1]
        }
        for row in cursor.fetchall()
    ]

    connection.close()

    return {
        "market_cap": market_cap,
        "volume": volume,
        "price_filter": price_filter,
        "statistics": statistics,
        "distribution": distribution
    }

@app.route("/")
def home():

    return render_template(
        "index.html"
    )

@app.route("/fetch-data")
def fetch_data():

    try:

        raw_api_list = fetch_crypto_data()

        usd_inr_rate = fetch_usd_inr_rate()

        production_sink, dead_letter_queue = process_data(
            raw_api_list
        )

        analytics = run_analytics(
            production_sink
        )

        return jsonify({

            "success": True,

            "records_fetched":
                len(raw_api_list),

            "records_clean":
                len(production_sink),

            "records_in_dlq":
                len(dead_letter_queue),

            "usd_inr_rate":
                usd_inr_rate,

            "analytics":
                analytics
        })

    except Exception as error:

        return jsonify({

            "success": False,

            "error":
                str(error)

        }), 500

if __name__ == "__main__":

    app.run(
        debug=True
    )