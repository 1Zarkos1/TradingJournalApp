from datetime import datetime, timedelta
from typing import List, Optional
from sqlalchemy import String, ForeignKey, PrimaryKeyConstraint, Engine, select
from sqlalchemy.orm import Session
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from tinkoff.invest import Client, schemas 

class Base(DeclarativeBase):
    pass

class Asset(Base):
    __tablename__ = "asset"

    ticker: Mapped[str] = mapped_column(primary_key=True)
    figi: Mapped[str]
    name: Mapped[str] = mapped_column(nullable=True)
    uid: Mapped[str]
    position_uid: Mapped[str]
    currency: Mapped[str]
    country: Mapped[str]
    sector: Mapped[str]
    short_available: Mapped[bool]
    operations: Mapped[List["Operation"]] = relationship(back_populates="asset")

    @classmethod
    def assets_populated(cls, engine: Engine) -> bool:
        with Session(engine) as session:
            return bool(session.scalar(select(cls)))
    
    @classmethod
    def populate_assets(cls, client: Client, engine: Engine) -> None:
        stocks_available = client.instruments.shares().instruments
        assets = []
        for stock in stocks_available:
            asset = cls(
                ticker=stock.ticker,
                figi=stock.figi,
                name=stock.name,
                uid=stock.uid,
                position_uid=stock.position_uid,
                currency=stock.currency,
                country=stock.country_of_risk,
                sector=stock.sector,
                short_available=stock.sell_available_flag
            )
            assets.append(asset)
        with Session(engine) as session:
            session.add_all(assets)
            session.commit()

    def __str__(self) -> str:
        return f"{self.ticker} - {self.figi} - {self.sector}"


class Operation(Base):
    __tablename__ = "operation"

    id: Mapped[str] = mapped_column(primary_key=True)
    ticker: Mapped[str] = mapped_column(ForeignKey("asset.ticker"))
    asset: Mapped["Asset"] = relationship(back_populates="operations")
    position_id: Mapped[int] = mapped_column(ForeignKey("position.id"), nullable=True)
    position: Mapped["Position"] = relationship(back_populates="operations")
    side: Mapped[str]
    time: Mapped[datetime]
    quantity: Mapped[int]
    price: Mapped[float]
    fee: Mapped[float] = 0

    @property
    def payment(self) -> float:
        return self.quantity * self.share_price
    
    def __str__(self) -> str:
        return f"{self.ticker} - {self.side} - {self.time} - {self.quantity} - {self.price}"

class Position(Base):
    __tablename__ = "position"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    ticker: Mapped[str]
    side: Mapped[str]
    open_price: Mapped[float] = mapped_column(default=0)
    closing_price: Mapped[float] = mapped_column(default=0)
    closed: Mapped[bool] = mapped_column(default=0)
    currency: Mapped[str]
    fee: Mapped[float] = mapped_column(default=0)
    operations: Mapped[List["Operation"]] = relationship(back_populates="position")
    result: Mapped[float] = mapped_column(default=0)
    note: Mapped[str] = mapped_column(nullable=True)

    def update(self, operation: Operation, payment: float) -> None:
        self.result += payment
        same_side_position_quantity = sum(
            [op.quantity for op in self.operations 
            if op.side == operation.side]
        )
        new_operation_price_fraction = operation.price * (operation.quantity / same_side_position_quantity)
        existing_quantity_to_total_ratio = (same_side_position_quantity - operation.quantity) / same_side_position_quantity
        if self.side == operation.side:
            self.open_price = (
                self.open_price 
                * existing_quantity_to_total_ratio 
                + new_operation_price_fraction
            )
        else:
            self.closing_price = (
                self.closing_price 
                * existing_quantity_to_total_ratio 
                + new_operation_price_fraction
            )

            opposite_side_position_quantity = sum(
                [op.quantity for op in self.operations 
                if op.side != operation.side]
            )
            if same_side_position_quantity == opposite_side_position_quantity:
                self.closed = True

    @property
    def resulting_percentage(self):
        return round(self.result / self.open_price, 2)
    
    def __str__(self) -> str:
        return f"{self.ticker} - {self.side} - {self.open_price} - {self.closing_price} - {self.closed} - {self.result}"
