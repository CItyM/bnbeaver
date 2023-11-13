from decimal import Decimal
import argparse
from datetime import datetime, timedelta
import json
from typing import Set
import requests
import hashlib
import hmac
import time
import os
from dotenv import load_dotenv
import sqlite3
from collections import defaultdict

load_dotenv()

BASE_URL = "https://api.binance.com"
AUTO_INVEST_HISTORY_URL = f"{BASE_URL}/sapi/v1/lending/auto-invest/history/list"
CONVERT_TRADE_FLOW_URL = f"{BASE_URL}/sapi/v1/convert/tradeFlow"
AVG_PRICE_URL = f"{BASE_URL}/api/v3/avgPrice"

API_KEY = str(os.getenv("API_KEY"))
API_SECRET = str(os.getenv("API_SECRET"))
HEADERS = {"X-MBX-APIKEY": API_KEY}


def generate_hash(values) -> bytes:
    """
    Generate a SHA-256 hash of the provided values.

    This function takes a variable number of arguments, concatenates
    them using a pipe (|) as a delimiter after converting each to a string,
    and returns the SHA-256 hash of the resulting concatenated string.

    Parameters:
    values: List of values to be hashed. Non-string values will be converted
            to strings.

    Returns:
    bytes: The SHA-256 hash of the concatenated values.

    Example:
    >>> generate_hash("John", 123, 45.6)
    b'\x1fO\xa4\x8b\xe7\xd1\xf3\xa0\xe0\xd6...\xe2\x8c\x98\xd8\x8c\x0e\x92\x8f'
    """

    values = list(values)
    values = [str(value) for value in values]
    concatenated_data = "|".join(values)
    return hashlib.sha256(concatenated_data.encode()).digest()


def init_db():
    conn = sqlite3.connect("tx.db")

    cur = conn.cursor()
    cur.execute(
        """
        create table if not exists transactions
        (binance_id text, timestamp int, s_asset text, s_amount text, b_asset text, b_amount text, price text, fee text, type string)
        """
    )
    cur.close()
    return conn


def inset_transactions(connection, transactions):
    cur = connection.cursor()
    cur.executemany(
        """
        insert into transactions (binance_id, timestamp, s_asset, s_amount, b_asset, b_amount, price, fee, type)
        values (:binance_id, :timestamp, :s_asset, :s_amount, :b_asset, :b_amount, :price, :fee, :type);
        """,
        transactions,
    )
    connection.commit()
    cur.close()


def get_all_transactions(connection):
    cursor = connection.cursor()
    cursor.execute("select * from transactions")
    rows = cursor.fetchall()
    return list(map(dict, rows))


def get_row_hashed(connection) -> Set[bytes]:
    """
    Retrieve and hash each row from the `transactions` table in the provided SQLite connection.

    This function fetches all rows from the `transactions` table and then computes a hash for
    each row using the `generate_hash` function. The resulting hashes are stored in a set,
    which ensures uniqueness, and then returned.

    Parameters:
    connection (sqlite3.Connection): The SQLite database connection object.

    Returns:
    Set[bytes]: A set of SHA-256 hashes, one for each row in the `transactions` table.

    Example:
    >>> conn = sqlite3.connect('example.db')
    >>> hashed_rows = get_row_hashed(conn)
    >>> print(hashed_rows)
    {b'\x1fO\xa4\x8b\xe7\xd1\xf3\xa0...', b'\x2aH\xb2...'}

    Note:
    The `generate_hash` function should be defined and should accept a database row (tuple)
    as its argument to generate the hash.
    """

    rows = get_all_transactions(connection)
    return set(map(lambda row: generate_hash(dict(row).values()), rows))


def create_tx_info(
    binance_id: str,
    timestamp: int,
    s_asset: str,
    s_amount: str,
    b_asset: str,
    b_amount: str,
    price: str,
    fee: str | None,
    tx_type: str,
):
    return {
        "binance_id": binance_id,
        "timestamp": timestamp,
        "s_asset": s_asset,
        "s_amount": s_amount,
        "b_asset": b_asset,
        "b_amount": b_amount,
        "price": price,
        "fee": fee,
        "type": tx_type,
    }


def add_signature(params):
    params_string = "&".join(f"{key}={value}" for key, value in params.items())

    signature = hmac.new(
        API_SECRET.encode(),
        params_string.encode(),
        hashlib.sha256,
    ).hexdigest()

    params["signature"] = signature


def get_timestamp():
    return int(time.time() * 1000)


def get_start_time_timestamp(end_time: int, days_interval: int):
    end_time_datetime = datetime.fromtimestamp(end_time / 1000)
    interval_time = timedelta(days=days_interval)
    return int((end_time_datetime - interval_time).timestamp() * 1000)


def get_auto_invest_tx(
    connection,
    hashed_txs: Set[bytes],
    period: int,
    days_interval: int,
    end_time: int | None = None,
):
    if period == 0:
        return

    timestamp = get_timestamp()

    if not end_time:
        end_time = timestamp

    start_time = get_start_time_timestamp(end_time, days_interval)

    params = {
        "size": 100,
        "timestamp": timestamp,
        "startTime": start_time,
        "endTime": end_time,
    }

    if end_time:
        params["endTime"] = end_time

    add_signature(params)

    response = requests.get(AUTO_INVEST_HISTORY_URL, headers=HEADERS, params=params)

    data = response.json()

    if response.status_code != 200:
        print(response.status_code, data["msg"])
        if data["code"] == -1021:
            get_auto_invest_tx(
                connection, hashed_txs, period, days_interval, start_time
            )
        return

    transactions = []
    for tx in data["list"]:
        if tx["transactionStatus"] == "SUCCESS":
            tx_info = create_tx_info(
                tx["id"],
                tx["transactionDateTime"],
                tx["sourceAsset"],
                tx["sourceAssetAmount"],
                tx["targetAsset"],
                tx["targetAssetAmount"],
                tx["executionPrice"],
                tx["transactionFee"],
                "BUY",
            )

            hashed_tx = generate_hash(tx_info.values())
            if hashed_tx in hashed_txs:
                continue

            hashed_txs.add(hashed_tx)
            transactions.append(tx_info)

    inset_transactions(connection, transactions)

    period -= days_interval
    days_interval = days_interval if period > days_interval else period

    get_auto_invest_tx(connection, hashed_txs, period, days_interval, start_time)


def get_convert_tx(
    connection,
    hashed_txs: Set[bytes],
    period: int,
    days_interval: int,
    end_time: int | None = None,
):
    if period == 0:
        return

    timestamp = get_timestamp()

    if not end_time:
        end_time = timestamp

    start_time = get_start_time_timestamp(end_time, days_interval)

    params = {
        "limit": 1000,
        "timestamp": timestamp,
        "startTime": start_time,
        "endTime": end_time,
    }

    add_signature(params)

    response = requests.get(CONVERT_TRADE_FLOW_URL, headers=HEADERS, params=params)

    data = response.json()

    if response.status_code != 200:
        print(response.status_code, data["msg"])
        if data["code"] == -1021:
            get_convert_tx(connection, hashed_txs, period, days_interval, start_time)
        return

    transactions = []

    for tx in data["list"]:
        tx_info = create_tx_info(
            tx["orderId"],
            tx["createTime"],
            tx["fromAsset"],
            tx["fromAmount"],
            tx["toAsset"],
            tx["toAmount"],
            tx["ratio"],
            None,
            "SELL",
        )

        hashed_tx = generate_hash(tx_info.values())
        if hashed_tx in hashed_txs:
            continue

        hashed_txs.add(hashed_tx)
        transactions.append(tx_info)

    inset_transactions(connection, transactions)

    period -= days_interval
    days_interval = days_interval if period > days_interval else period

    get_convert_tx(connection, hashed_txs, period, days_interval, start_time)


def nested_dict():
    return defaultdict(list)


def get_all_unique_assets(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        select distinct t.b_asset as asset 
        from transactions t 
        union
        select distinct t2.s_asset as asset
        from transactions t2
        """
    )
    rows = cursor.fetchall()
    return list(map(lambda row: dict(row)["asset"], rows))


def get_avg_price(asset: str) -> Decimal:
    params = {"symbol": f"{asset}USDT"}

    response = requests.get(AVG_PRICE_URL, headers=HEADERS, params=params)
    price = response.json()["price"]
    return Decimal(price)


def calculate_average_prices(connection):
    unique_assets = get_all_unique_assets(connection)
    data = get_all_transactions(connection)

    results = dict()

    for unique_asset in unique_assets:
        if "USD" in unique_asset or unique_asset == "EUR":
            continue

        transactions = list(
            filter(
                lambda item: item["b_asset"] == unique_asset
                or item["s_asset"] == unique_asset,
                data,
            )
        )

        asset_amount = Decimal(0)
        usd_spent = Decimal(0)

        for tx in transactions:
            if tx["type"] == "BUY" and "USD" in tx["s_asset"]:
                asset_amount += Decimal(tx["b_amount"])
                usd_spent += Decimal(tx["s_amount"]) + Decimal(tx["fee"])
            if tx["type"] == "SELL" and "USD" in tx["b_asset"]:
                asset_amount -= Decimal(tx["s_amount"])

        if asset_amount == Decimal(0):
            continue

        avg_price = usd_spent / asset_amount
        price = get_avg_price(unique_asset)
        potential_profit_loss = asset_amount * price - usd_spent

        results[unique_asset] = {
            "asset_amount": str(asset_amount),
            "usd_spent": str(usd_spent),
            "avg_price": str(avg_price),
            "price": str(price),
            "potential_profit_loss": str(potential_profit_loss),
        }

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-p",
        "--period",
        type=int,
        help="The period in days to collect transactions (min:1;)",
    )

    parser.add_argument(
        "-i",
        "--interval",
        type=int,
        help="The interval in days of the first and last transaction (min: 1; max: 30;)",
    )

    args = parser.parse_args()

    connection = init_db()
    connection.row_factory = sqlite3.Row

    hashed_row = get_row_hashed(connection)

    period = args.period if args.period else 365
    days_interval = args.interval if args.interval else 30
    days_interval = period if period < days_interval else days_interval

    print("Getting Auto-Ivest transactions...")
    get_auto_invest_tx(connection, hashed_row, period, days_interval)
    print("Getting Convert transactions...")
    get_convert_tx(connection, hashed_row, period, days_interval)

    calculate_average_prices(connection)

    connection.close()
