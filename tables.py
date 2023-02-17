from datetime import datetime, timedelta
from typing import List, Optional
from sqlalchemy import String, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

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


    def update_position(self, operation) -> None:
        if self.side == operation.side:
            # update open price based on operations
            ...
        else:
            # update closing price based on operations
            # check if position is closed
            ...
        self.fee += operation.fee
        self.result += operation.payment

    @property
    def resulting_percentage(self):
        return round(self.result / self.open_price, 2)
