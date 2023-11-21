import sqlite3
from sqlite3 import Connection
from typing import Iterable, List, Optional

from model import Transaction

db_connection: Optional[Connection] = None


def _get_db_connection() -> Connection:
    """
    Returns a singleton SQLite connection.
    If the connection does not exist, it is created.
    """
    global db_connection

    if not db_connection:
        db_connection = sqlite3.connect("tx.db")
        db_connection.row_factory = sqlite3.Row

    return db_connection


def init_db():
    connection = _get_db_connection()
    cursur = connection.cursor()
    try:
        cursur.execute(
            """
            create table if not exists transactions
            (
                binance_id text,
                timestamp int,
                s_asset text,
                s_amount text,
                b_asset text,
                b_amount text,
                price text,
                tx_type text,
                fee text,
                primary key (binance_id, timestamp)
            );
            """
        )
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        cursur.close()


def inset_new_transactions(transactions: Iterable[Transaction]):
    connection = _get_db_connection()
    cursur = connection.cursor()
    try:
        cursur.executemany(
            """
            insert or ignore into transactions
            (
                binance_id,
                timestamp,
                s_asset,
                s_amount,
                b_asset,
                b_amount,
                price,
                tx_type,
                fee
            )
            values
            (
                :binance_id,
                :timestamp,
                :s_asset,
                :s_amount,
                :b_asset,
                :b_amount,
                :price,
                :tx_type,
                :fee
            );
            """,
            (transaction.to_db_row() for transaction in transactions),
        )
        connection.commit()
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        cursur.close()


def get_all_transactions() -> List[Transaction]:
    connection = _get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute("select * from transactions")
        rows = cursor.fetchall()
        return [Transaction.from_db_row(row) for row in rows]
    except Exception as e:
        print(f"An error occurred: {e}")
        return []
    finally:
        cursor.close()


def get_all_unique_assets() -> List[str]:
    connection = _get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            select distinct t.b_asset as asset
            from transactions as t
            union
            select distinct t2.s_asset as asset
            from transactions as t2
            """
        )
        rows = cursor.fetchall()
        return [row[0] for row in rows]
    except Exception as e:
        print(f"An error occurred: {e}")
        return []
    finally:
        cursor.close()
