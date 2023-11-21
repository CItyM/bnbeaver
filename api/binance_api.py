from collections.abc import Coroutine
from aiohttp import ClientSession
import time
from typing import Any, Set
from util import (
    add_signature,
    determine_timestamp_now,
    determine_timestamp_start_time,
)
from constant import (
    AUTO_INVEST_HISTORY_URL,
    AVG_PRICE_URL,
    CONVERT_TRADE_FLOW_URL,
    MY_TRADES_URL,
)
import requests


class BinanceApi:
    def __init__(
        self, api_key: str, api_secret: str, session: ClientSession
    ) -> None:
        self.headers = {"X-MBX-APIKEY": api_key}
        self.api_secret = api_secret
        self.session = session

    async def get_auto_invest_tx(
        self, start_time: int, end_time: int
    ) -> Coroutine[Any, Any, Any]:
        timestamp = determine_timestamp_now()

        params = {
            "size": 100,
            "timestamp": timestamp,
            "startTime": start_time,
            "endTime": end_time,
        }

        add_signature(params, self.api_secret)

        async with self.session.get(
            AUTO_INVEST_HISTORY_URL, headers=self.headers, params=params
        ) as response:
            if response.status != 200:
                data = await response.json()

                print(response.status, data["msg"])

                if data["code"] == -1021:
                    return self.get_auto_invest_tx(start_time, end_time)

                return Coroutine()

            return await response.json()

    async def get_avg_price(self, symbol: str) -> Coroutine[Any, Any, Any]:
        params = {"symbol": symbol}

        async with self.session.get(
            AVG_PRICE_URL, headers=self.headers, params=params
        ) as response:
            if response.status != 200:
                data = await response.json()
                print(response.status, data["msg"])
                return Coroutine()

            return await response.json()

    async def get_convert_tx(
        self, start_time: int, end_time: int
    ) -> Coroutine[Any, Any, Any]:
        timestamp = determine_timestamp_now()

        params = {
            "limit": 1000,
            "timestamp": timestamp,
            "startTime": start_time,
            "endTime": end_time,
        }

        add_signature(params, self.api_secret)

        async with self.session.get(
            CONVERT_TRADE_FLOW_URL, headers=self.headers, params=params
        ) as response:
            if response.status != 200:
                data = await response.json()

                print(response.status, data["msg"])

                if data["code"] == -1021:
                    await self.get_convert_tx(start_time, end_time)

                return Coroutine()

            return await response.json()

    def get_spot_tx(
        self,
        connection,
        hashed_txs: Set[bytes],
        symbol: str,
        period: int,
        days_interval: int,
        end_time: int | None = None,
    ):
        if period == 0:
            return

        timestamp = determine_timestamp_now()

        if not end_time:
            end_time = timestamp

        start_time = determine_timestamp_start_time(end_time, days_interval)

        params = {
            "limit": 1000,
            "symbol": symbol,
            "timestamp": timestamp,
            "startTime": start_time,
            "endTime": end_time,
        }

        add_signature(params, self.api_secret)

        response = requests.get(
            MY_TRADES_URL, headers=self.headers, params=params
        )

        data = response.json()

        print(period)
        if response.status_code != 200:
            print(response.status_code, data["msg"])
            self.get_spot_tx(
                connection,
                hashed_txs,
                symbol,
                period,
                days_interval,
                start_time,
            )

        time.sleep(0.5)

        # if data:
        # print(json.dumps(data))
        # transactions = []
        # for tx in data:
        #     print(json.dumps(tx))
        # tx_info = create_tx_info(
        #     tx["orderId"],
        #     tx["createTime"],
        #     tx["fromAsset"],
        #     tx["fromAmount"],
        #     tx["toAsset"],
        #     tx["toAmount"],
        #     tx["ratio"],
        #     None,
        #     "SELL",
        # )

        #     hashed_tx = hash_values(tx_info.values())
        #     if hashed_tx in hashed_txs:
        #         continue

        #     hashed_txs.add(hashed_tx)
        #     transactions.append(tx_info)

        # inset_transactions(connection, transactions)

        period -= days_interval
        days_interval = days_interval if period > days_interval else period

        self.get_spot_tx(
            connection, hashed_txs, symbol, period, days_interval, start_time
        )
