from dataclasses import dataclass
from decimal import Decimal
from typing import Tuple


@dataclass
class Transaction:
    binance_id: str
    timestamp: str
    s_asset: str
    s_amount: Decimal
    b_asset: str
    b_amount: Decimal
    price: Decimal
    tx_type: str
    fee: Decimal = Decimal("0")

    @classmethod
    def from_auto_invest_tx(cls, tx: dict):
        return cls(
            binance_id=tx["id"],
            timestamp=tx["transactionDateTime"],
            s_asset=tx["sourceAsset"],
            s_amount=Decimal(tx["sourceAssetAmount"]),
            b_asset=tx["targetAsset"],
            b_amount=Decimal(tx["targetAssetAmount"]),
            price=Decimal(tx["executionPrice"]),
            tx_type="BUY",
            fee=Decimal(tx["transactionFee"]),
        )

    @classmethod
    def from_convert_tx(cls, tx: dict):
        return cls(
            binance_id=tx["orderId"],
            timestamp=tx["createTime"],
            s_asset=tx["fromAsset"],
            s_amount=Decimal(tx["fromAmount"]),
            b_asset=tx["toAsset"],
            b_amount=Decimal(tx["toAmount"]),
            price=Decimal(tx["ratio"]),
            tx_type="SELL",
        )

    @classmethod
    def from_db_row(cls, row: dict):
        return cls(
            binance_id=row["binance_id"],
            timestamp=row["timestamp"],
            s_asset=row["s_asset"],
            s_amount=Decimal(row["s_amount"]),
            b_asset=row["b_asset"],
            b_amount=Decimal(row["b_amount"]),
            price=Decimal(row["price"]),
            tx_type=row["tx_type"],
            fee=Decimal(row["fee"]),
        )

    # TODO: fix tx info
    @classmethod
    def from_spot_tx(cls, tx: dict):
        return cls(
            binance_id=tx["id"],
            timestamp=tx["transactionDateTime"],
            s_asset=tx["sourceAsset"],
            s_amount=tx["sourceAssetAmount"],
            b_asset=tx["targetAsset"],
            b_amount=tx["targetAssetAmount"],
            price=tx["executionPrice"],
            tx_type="BUY",
            fee=tx["transactionFee"],
        )

    def to_db_row(self) -> Tuple:
        return (
            self.binance_id,
            self.timestamp,
            self.s_asset,
            str(self.s_amount),
            self.b_asset,
            str(self.b_amount),
            str(self.price),
            self.tx_type,
            str(self.fee),
        )
