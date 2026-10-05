# Databricks notebook source
import requests
import time
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F

# -----------------------------
# CONFIG
# -----------------------------
PRODUCTS = [
    "BTC-USD",
    "ETH-USD",
    "SOL-USD",
    "XRP-USD",
    "DOGE-USD",
    "ADA-USD",
    "AVAX-USD",
    "LINK-USD",
    "LTC-USD",
    "BCH-USD",
    "AAVE-USD",
    "DOT-USD",
    "UNI-USD",
    "ATOM-USD",
    "NEAR-USD"
]

START_DATE = datetime(2020, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime.now(timezone.utc)

BASE_URL = "https://api.coinbase.com/api/v3/brokerage/market/products"

# -----------------------------
# FUNCTION TO GET ONE WINDOW
# -----------------------------
def get_coinbase_candles(product_id, start_dt, end_dt):

    url = f"{BASE_URL}/{product_id}/candles"

    params = {
        "start": str(int(start_dt.timestamp())),
        "end": str(int(end_dt.timestamp())),
        "granularity": "ONE_DAY",
        "limit": 350
    }

    response = requests.get(
        url,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    candles = response.json()["candles"]

    rows = []

    for c in candles:
        rows.append({
            "product_id": product_id,
            "timestamp": int(c["start"]),
            "open": float(c["open"]),
            "high": float(c["high"]),
            "low": float(c["low"]),
            "close": float(c["close"]),
            "volume": float(c["volume"])
        })

    return rows

# -----------------------------
# PULL ALL DATA
# -----------------------------
all_rows = []
failed_products = []

for product in PRODUCTS:

    current_start = START_DATE
    product_failed = False

    while current_start < END_DATE:

        current_end = min(
            current_start + timedelta(days=300),
            END_DATE
        )

        print(
            f"Pulling {product}: "
            f"{current_start.date()} to {current_end.date()}"
        )

        try:
            rows = None
            for attempt in range(3):
                try:
                    rows = get_coinbase_candles(
                        product,
                        current_start,
                        current_end
                    )
                    break
                except Exception as e:
                    if attempt == 2:
                        raise
                    wait = 2 ** attempt
                    print(f"  Retry {attempt + 1}/3 after {wait}s: {e}")
                    time.sleep(wait)

            all_rows.extend(rows)

        except Exception as e:
            print(f"ERROR pulling {product}: {e}")
            product_failed = True
            break

        current_start = current_end

    if product_failed:
        failed_products.append(product)

if failed_products:
    raise RuntimeError(
        f"Failed to pull {len(failed_products)}/{len(PRODUCTS)} products: {failed_products}. "
        f"Aborting to prevent data loss from overwrite."
    )

# -----------------------------
# CREATE SPARK DATAFRAME
# -----------------------------
crypto_df = spark.createDataFrame(all_rows)

crypto_df = (
    crypto_df
    .withColumn(
        "trade_timestamp",
        F.from_unixtime("timestamp").cast("timestamp")
    )
    .withColumn(
        "trade_date",
        F.to_date("trade_timestamp")
    )
    .withColumn(
        "_ingested_at",
        F.current_timestamp()
    )
    .withColumn(
        "_source",
        F.lit("Coinbase")
    )
)

# Remove duplicate candles just in case date windows overlap
crypto_df = crypto_df.dropDuplicates(
    ["product_id", "timestamp"]
)

# -----------------------------
# CREATE BRONZE SCHEMA
# -----------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")

# -----------------------------
# MERGE INTO DELTA TABLE (preserves existing data on partial failures)
# -----------------------------
from delta.tables import DeltaTable

TARGET_TABLE = "workspace.bronze.coinbase_crypto_candles_raw"

if spark.catalog.tableExists(TARGET_TABLE):
    target = DeltaTable.forName(spark, TARGET_TABLE)
    (
        target.alias("target")
        .merge(
            crypto_df.alias("source"),
            "target.product_id = source.product_id AND target.timestamp = source.timestamp"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Merged into {TARGET_TABLE}")
else:
    (
        crypto_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE)
    )
    print(f"Created {TARGET_TABLE}")

# -----------------------------
# RESULTS
# -----------------------------
print(f"Total rows loaded: {crypto_df.count():,}")

print("Rows by product:")
crypto_df.groupBy("product_id") \
    .count() \
    .orderBy("product_id") \
    .show()

display(
    crypto_df
    .select(
        "product_id",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "_ingested_at"
    )
    .orderBy("product_id", "trade_date")
)