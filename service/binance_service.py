from decimal import Decimal
from typing import List

from api import BinanceApi, RateLimiter
from constant import (
    API_RATE_LIMIT,
    SAPI_IP_RATE_LIMIT,
    SAPI_UID_RATE_LIMIT,
    AUTO_INVEST_HISTORY_WEIGHT_IP,
    AVG_PRICE_WEIGHT_IP,
    CONVERT_TRADE_FLOW_WEIGHT_UID,
    MY_TRADES_WEIGHT_IP,
)
from db import database
from model import Transaction
from util import determine_start_end_times


class BinanceService:
    def __init__(
        self, binance_api: BinanceApi, period: int, days_interval: int
    ) -> None:
        self.binance_api = binance_api
        self.spot_times = determine_start_end_times(period, 1)
        self.auto_invest_times = (
            self.convert_times
        ) = determine_start_end_times(period, days_interval)
        self.api_rate_limiter = RateLimiter(API_RATE_LIMIT, 60)
        self.sapi_ip_rate_limiter = RateLimiter(SAPI_IP_RATE_LIMIT, 60)
        self.sapi_uid_rate_limiter = RateLimiter(SAPI_UID_RATE_LIMIT, 60)

    async def download_transactions(self) -> None:
        transactions = []

        transactions.extend(await self._get_auto_invest_transactions())
        transactions.extend(await self._get_convert_transactions())

        database.inset_new_transactions(transactions)

    async def get_asset_usdt_average_price(self, asset: str) -> Decimal:
        await self.api_rate_limiter.add_task(
            self.binance_api.get_avg_price(f"{asset}USDT"),
            AVG_PRICE_WEIGHT_IP,
        )

        await self.api_rate_limiter.execute_tasks()

        result = self.api_rate_limiter.get_results()[0]

        price = result["price"]

        return Decimal(price)

    async def _get_auto_invest_transactions(self) -> List[Transaction]:
        transactions = []

        for start_time, end_time in self.auto_invest_times:
            await self.sapi_ip_rate_limiter.add_task(
                self.binance_api.get_auto_invest_tx(start_time, end_time),
                AUTO_INVEST_HISTORY_WEIGHT_IP,
            )

        await self.sapi_ip_rate_limiter.execute_tasks()

        for result in self.sapi_ip_rate_limiter.get_results():
            transactions.extend(result["list"])

        return [
            Transaction.from_auto_invest_tx(transaction)
            for transaction in transactions
            if transaction["transactionStatus"] == "SUCCESS"
        ]

    async def _get_convert_transactions(self) -> List[Transaction]:
        transactions = []

        for start_time, end_time in self.convert_times:
            await self.sapi_uid_rate_limiter.add_task(
                self.binance_api.get_convert_tx(start_time, end_time),
                CONVERT_TRADE_FLOW_WEIGHT_UID,
            )

        await self.sapi_uid_rate_limiter.execute_tasks()

        for result in self.sapi_uid_rate_limiter.get_results():
            transactions.extend(result["list"])

        return [
            Transaction.from_convert_tx(transaction)
            for transaction in transactions
        ]
