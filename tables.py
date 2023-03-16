import os
import csv
from pathlib import Path
from datetime import datetime
from typing import List

from sqlalchemy import create_engine, ForeignKey, Engine, select, inspect, event, and_
from sqlalchemy.orm import Session, DeclarativeBase, Mapped, mapped_column, relationship, column_property
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql import case
from tinkoff.invest import Client, schemas

from utils import extract_money_amount

class Base(DeclarativeBase):
    pass

def initialize_db(engine: Engine, name: str, base_mapper: DeclarativeBase = Base, reset: bool=False) -> None:
    if reset:
        base_mapper.metadata.drop_all(engine)
    if not os.path.exists(f'{name}') or not inspect(engine).has_table("operation"):
        base_mapper.metadata.create_all(engine)

def get_engine(db_name: str):
    return create_engine(f"sqlite:///{db_name}")


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
                short_available=stock.short_enabled_flag
            )
            assets.append(asset)
        with Session(engine) as session:
            session.add_all(assets)
            session.commit()

    @classmethod
    def get_figi_to_ticker_mapping(cls, engine: Engine) -> dict:
        with Session(engine) as session:
            assets = session.scalars(select(cls)).all()
        return {
            asset.figi: asset.ticker
            for asset in assets
        }
    
    @classmethod
    def analyze_screener(cls, engine: Engine):
        today = datetime.now().strftime("%Y-%m-%d")
        username = os.getlogin()
        directory = Path(f"C:\\Users\\{username}\\Downloads")
        files = os.listdir(directory)
        try:
            filename = [
                filename for filename in files 
                if filename.endswith(f"{today}.csv")
            ][0]
            print(filename)
        except IndexError:
            raise Exception("No screener files in specified directory")
        
        with open(directory/filename, 'r') as f:
            screener_tickers = csv.DictReader(f)
            tickers = {row["Ticker"]:{"long":"", "short":""} for row in screener_tickers}
            with Session(engine) as session:
                db_response = session.scalars(select(Asset).where(Asset.ticker.in_(tickers.keys()))).all()
                for db_entry in db_response:
                    tickers[db_entry.ticker] = {
                        "long": True,
                        "short": db_entry.short_available or ""
                    }
        write_directory = f"C:\\Users\\{username}\\Desktop\\screener_results.csv"
        with open(write_directory, 'w', newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["ticker", "long", "short"], delimiter=",")
            writer.writerows(
                [
                    {
                        "ticker": ticker, 
                        "long": tickers[ticker]["long"],
                        "short": tickers[ticker]["short"],
                    }
                    for ticker in tickers
                ]
            )

    def __repr__(self) -> str:
        return f"Asset<ticker={self.ticker}, figi={self.figi}, name={self.name}, sector={self.sector}>"

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
    
    @classmethod
    def add_operation(cls, operation: schemas.Operation, session: Session) -> None:
        position = Position.get_related_position(operation, session)
        operation_entry = cls(
            id = operation.id,
            ticker = operation.ticker,
            position = position,
            side = operation.operation_type,
            time = operation.date,
            quantity = operation.quantity,
            price = extract_money_amount(operation.price),
            fee = 0
        )
        session.add(operation_entry)
        position.update(operation_entry, extract_money_amount(operation.payment))

    def add_fee(self, api_operation):
        fee = extract_money_amount(api_operation.payment)
        self.fee = fee
        self.position.fee += fee
    
    def __repr__(self) -> str:
        return f"Operation<ticker={self.ticker}, side={self.side}, time={self.time}, quantity={self.quantity}, price={self.price}>"

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
    operations: Mapped[List["Operation"]] = relationship(back_populates="position", lazy="subquery")
    result: Mapped[float] = mapped_column(default=0)
    note: Mapped[str] = mapped_column(nullable=True)

    @hybrid_property
    def open_date(self):
        return min(self.operations, key=lambda opr: opr.time).time

    @open_date.expression
    def open_date(cls):
        return (
            select(Operation.time)
            .where(Operation.position_id == cls.id)
            .order_by(Operation.time).limit(1)
            .scalar_subquery()
        )
    
    @hybrid_property
    def close_date(self):
        return max(self.operations, key=lambda opr: opr.time).time if self.closed else ""

    @close_date.expression
    def close_date(cls):
        return (
            select(Operation.time)
            .where(Operation.position_id == cls.id)
            .order_by(Operation.time.desc()).limit(1)
            .scalar_subquery()
        )

    @property
    def size(self) -> int:
        return self.get_operations_quantity(self.side)
    
    def get_operations_quantity(self, side: str) -> int:
        return sum(
                [operation.quantity for operation in self.operations 
                if operation.side == side]
            )
    
    @classmethod
    def get_related_position(cls, operation: schemas.Operation, session: Session) -> None:
        # check if there is any open position for particular ticker
        position = session.scalar(
            select(cls).where(and_(cls.closed == False, 
                                   cls.ticker == operation.ticker))
        )
        if not position:
            position = cls(
                ticker = operation.ticker,
                side = operation.operation_type,
                currency = operation.currency,
                open_price = 0,
                result = 0
            )
        return position

    @classmethod
    def get_positions(cls, engine, filter_field="", filter_value="", sorting_field=""):
        with Session(engine) as session:
            query = select(Position)
            if filter_field and filter_value:
                query = query.where(getattr(cls, filter_field) > filter_value)
            if sorting_field:
                query = query.order_by(getattr(cls, sorting_field))
            return session.scalars(query).all()

    def update(self, operation: Operation, payment: float) -> None:
        self.result += round(payment, 2)
        same_side_position_quantity = self.get_operations_quantity(operation.side)
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

            opposite_side_position_quantity = self.get_operations_quantity(
                "Sell" if operation.side == "Buy" else "Buy"
            )

            if same_side_position_quantity == opposite_side_position_quantity:
                self.closed = True

    @property
    def resulting_percentage(self):
        return round(
            self.result 
            / self.get_operations_quantity(self.side) 
            / self.open_price * 100, 2
        ) if self.closed else 0
    
    def __repr__(self) -> str:
        return f"Position<id={self.id}, ticker={self.ticker}, open_date={self.open_date}, closed={self.closed}, result={self.result}>"

# event.listen(Position.operations, "append", Position.update)

class AdditionalPayment(Base):
    __tablename__ = "additional_payment"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(ForeignKey("asset.ticker"), nullable=True)
    description: Mapped[str]
    # currency: Mapped[str]
    payment: Mapped[float]

    def __repr__(self):
        return f"Payment<description={self.description}, ticker={self.ticker}, value={self.payment}>"