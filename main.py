import aiohttp
import asyncio
import os

from api import BinanceApi
from db import database
from service import BinanceService, CalculationService
from cli import args_parser
from dotenv import load_dotenv

load_dotenv()

API_KEY = str(os.getenv("API_KEY"))
API_SECRET = str(os.getenv("API_SECRET"))


# def get_all_tx(period: int, days_interval: int):
#     connection = init_db()

#     rows = get_all_transactions(connection)
#     hashed_rows = get_rows_hashed(rows)

#     get_spot_tx(connection, hashed_rows, "SOLUSDT", period, days_interval)
#     # print("Getting Auto-Ivest transactions...")
#     # get_auto_invest_tx(connection, hashed_rows, period, days_interval)

#     # print("Getting Convert transactions...")
#     # get_convert_tx(connection, hashed_rows, period, days_interval)

#     connection.close()


async def main():
    database.init_db()

    args = args_parser()
    if not args:
        return
    (period, days_interval) = args
    print(period, days_interval)

    async with aiohttp.ClientSession() as session:
        binance_api = BinanceApi(API_KEY, API_SECRET, session)
        binance_service = BinanceService(binance_api, period, days_interval)
        calculation_service = CalculationService(binance_service)

        await binance_service.download_transactions()

        await calculation_service.calculate_average_prices()


asyncio.run(main())
