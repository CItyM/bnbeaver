from decimal import Decimal
import json

from db import database
from service import BinanceService


class CalculationService:
    def __init__(self, binance_service: BinanceService) -> None:
        self.binance_service = binance_service

    async def calculate_average_prices(self):
        unique_assets = database.get_all_unique_assets()
        transactions = database.get_all_transactions()

        results = dict()

        for unique_asset in unique_assets:
            if "USD" in unique_asset or unique_asset == "EUR":
                continue

            ua_tx = [
                tx
                for tx in transactions
                if tx.b_asset == unique_asset or tx.s_asset == unique_asset
            ]

            asset_amount = Decimal(0)
            usd_spent = Decimal(0)

            for tx in ua_tx:
                if tx.tx_type == "BUY" and "USD" in tx.s_asset:
                    asset_amount += tx.b_amount
                    usd_spent += tx.s_amount + tx.fee
                if tx.tx_type == "SELL" and "USD" in tx.b_asset:
                    asset_amount -= tx.s_amount
                    usd_spent -= tx.b_amount - tx.fee

            if asset_amount <= Decimal(0):
                continue

            avg_price = usd_spent / asset_amount

            current_price = (
                await self.binance_service.get_asset_usdt_average_price(
                    unique_asset
                )
            )
            potential_profit_loss = (
                asset_amount * current_price - asset_amount * avg_price
            )

            results[unique_asset] = {
                "asset_amount": str(asset_amount),
                "usd_spent": str(usd_spent),
                "avg_price": str(avg_price),
                "current_price": str(current_price),
                "potential_profit_loss": str(potential_profit_loss),
            }

        print(json.dumps(results, indent=2))
