import os
import csv
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List

from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, 
    ForeignKey, 
    Engine, 
    select, 
    inspect, 
    and_, 
    JSON,
    Interval
)
from sqlalchemy.orm import Session, DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql import functions
from tinkoff.invest import Client, schemas

from utils import extract_money_amount


load_dotenv(".env")

DB_SUFFIX = os.environ.get("DB_SUFFIX")
class Base(DeclarativeBase):
    pass

def initialize_db(engine: Engine, name: str, base_mapper: DeclarativeBase = Base, 
                  reset: bool=False) -> None:
    if reset:
        base_mapper.metadata.drop_all(engine)
    if not os.path.exists(f'{name}') or not inspect(engine).has_table("operation"):
        base_mapper.metadata.create_all(engine)

def get_engine(account_name: str):
    return create_engine(f"sqlite:///{account_name.lower()}_{DB_SUFFIX}")

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
    def assets_populated(cls, session: Session) -> bool:
        return bool(session.scalar(select(cls)))
    
    @classmethod
    def populate_assets(cls, client: Client, session: Session, assets_to_add: List = []) -> None:
        stocks_available = assets_to_add or client.instruments.shares().instruments
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
                sector=getattr(stock, "sector", "futures"),
                short_available=stock.short_enabled_flag
            )
            assets.append(asset)
        session.add_all(assets)
        session.commit()

    @classmethod
    def get_figi_to_ticker_mapping(cls, session: Session) -> dict:
        assets = session.scalars(select(cls)).all()
        return {
            asset.figi: asset.ticker
            for asset in assets
        }
    
    @classmethod
    def analyze_screener(cls, engine: Engine) -> None:
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
        return (
            f"Asset<ticker={self.ticker}, figi={self.figi}, name={self.name}, sector={self.sector}>"
        )

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
            time = operation.date.replace(tzinfo=timezone.utc),
            quantity = operation.quantity,
            price = extract_money_amount(operation.price),
            fee = 0
        )
        session.add(operation_entry)
        position.update(operation_entry, extract_money_amount(operation.payment))

    def add_fee(self, api_operation: schemas.Operation, session: Session) -> None:
        fee = extract_money_amount(api_operation.payment)
        self.fee = fee
        self.position.fee += fee
        session.add(self)
    
    def to_dict(self) -> dict:
        return {
            "side": self.side,
            "date": self.time.strftime("%d/%m/%Y"),
            "time": self.time.strftime("%H:%M:%S"),
            "quantity": self.quantity,
            "price": self.price,
            "fee": self.fee
        }

    def __repr__(self) -> str:
        return (
            f"Operation<ticker={self.ticker}, side={self.side}, time={self.time}, "
            f"quantity={self.quantity}, price={self.price}>"
        )

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
    operations: Mapped[List["Operation"]] = relationship(back_populates="position", lazy="selectin", cascade="all, delete-orphan")
    result: Mapped[float] = mapped_column(default=0)
    chart: Mapped["ChartData"] = relationship(back_populates="position", cascade="all, delete-orphan")
    walkaway: Mapped["WalkAwayData"] = relationship(back_populates="position", cascade="all, delete-orphan")
    note: Mapped[str] = mapped_column(nullable=True)

    @hybrid_property
    def open_date(self) -> datetime:
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
    def close_date(self) -> datetime:
        return max(self.operations, key=lambda opr: opr.time).time if self.closed else ""

    @close_date.expression
    def close_date(cls):
        return (
            select(Operation.time)
            .where(Operation.position_id == cls.id)
            .order_by(Operation.time.desc()).limit(1)
            .scalar_subquery()
        )
        
    @hybrid_property
    def size(self) -> int:
        return self.get_operations_quantity(self.side)
    
    @size.expression
    def size(cls):
        return (
            select(functions.sum(Operation.quantity))
            .where((Operation.position_id == cls.id) & (Operation.side == cls.side))
        ).scalar_subquery()
    
    def get_operations_quantity(self, side: str) -> int:
        return sum(
                [operation.quantity for operation in self.operations 
                if operation.side == side]
            )
    
    @classmethod
    def get_related_position(cls, operation: schemas.Operation, session: Session) -> "Position":
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
    def get_positions(cls, engine: Engine, filters: dict ={}, sorting_field: str ="close_date", 
                      sorting_order: int = 1) -> List["Position"]:
        with Session(engine) as session:
            query = select(Position)
            sorting_field = getattr(cls, sorting_field, None)
            for filter_field, filter_value in filters.items():
                match filter_field:
                    case "ticker":
                        query = query.where(getattr(cls, filter_field).ilike(filter_value))
                    case "from_date":
                        query = query.where(getattr(cls, "open_date") > filter_value)
                        print(query)
                    case "to_date":
                        query = query.where(getattr(cls, "open_date") < filter_value)
                    case "side":
                        if filter_value != "all":
                            value = "Buy" if filter_value == "long" else "Sell"
                            query = query.where(getattr(cls, "side") == value)
                    case "status":
                        if filter_value != "all":
                            value = Position.result > 0 if filter_value == "win" else Position.result < 0
                            query = query.where(Position.closed.is_(True) & value)
            try:
                if sorting_field:
                    sorting_field = sorting_field.desc if sorting_order else sorting_field.asc
                    query = query.order_by(sorting_field())
            except Exception as e:
                print(e)
            return session.scalars(query).all()

    def update(self, operation: Operation, payment: float) -> None:
        self.result += round(payment, 2)
        same_side_position_quantity = self.get_operations_quantity(operation.side)
        new_operation_price_fraction = operation.price * (operation.quantity / same_side_position_quantity)
        existing_quantity_to_total_ratio = (same_side_position_quantity - operation.quantity) / same_side_position_quantity
        if self.side == operation.side:
            self.open_price = round(
                self.open_price 
                * existing_quantity_to_total_ratio 
                + new_operation_price_fraction,
                2
            )
        else:
            self.closing_price = round(
                self.closing_price 
                * existing_quantity_to_total_ratio 
                + new_operation_price_fraction,
                2
            )

            opposite_side_position_quantity = self.get_operations_quantity(
                "Sell" if operation.side == "Buy" else "Buy"
            )

            if same_side_position_quantity == opposite_side_position_quantity:
                self.closed = True

    @property
    def resulting_percentage(self) -> float:
        return round(
            self.result 
            / self.get_operations_quantity(self.side) 
            / self.open_price * 100, 2
        ) if self.closed else 0
    
    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "ticker": self.ticker,
            "side": self.side,
            "open_price": self.open_price,
            "closing_price": self.closing_price,
            "open_date": self.open_date,
            "close_date": self.close_date,
            "size": self.size,
            "currency": self.currency,
            "fee": self.fee,
            "closed": self.closed,
            "result": self.result
        }

    def __repr__(self) -> str:
        return (
            f"Position<id={self.id}, ticker={self.ticker}, open_date={self.open_date}, "
            f"closed={self.closed}, result={self.result}>"
        )

# event.listen(Position.operations, "append", Position.update)

class AdditionalPayment(Base):
    __tablename__ = "additional_payment"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(ForeignKey("asset.ticker"), nullable=True)
    description: Mapped[str]
    currency: Mapped[str]
    payment: Mapped[float]

    def __repr__(self) -> str:
        return f"Payment<description={self.description}, ticker={self.ticker}, value={self.payment}>"
    

class ChartData(Base):
    __tablename__ = "chart_data"

    id: Mapped[int] = mapped_column(ForeignKey("position.id"), primary_key=True, nullable=False)
    position: Mapped["Position"] = relationship(back_populates="chart")
    ticker: Mapped[str] = mapped_column(ForeignKey("asset.ticker"), nullable=False)
    candle_interval: Mapped[timedelta]
    candles = mapped_column(JSON)

    def __repr__(self) -> str:
        return f"ChartData<ticker={self.ticker}, interval={self.candle_interval}>"


class WalkAwayData(Base):
    __tablename__ = "walk_away_data"

    id: Mapped[int] = mapped_column(ForeignKey("position.id"), primary_key=True, nullable=False)
    position: Mapped["Position"] = relationship(back_populates="walkaway")
    ticker: Mapped[str] = mapped_column(ForeignKey("asset.ticker"), nullable=False)
    history_data = mapped_column(JSON)

    def __repr__(self) -> str:
        return f"WalkAwayData<id={self.id}, ticker={self.ticker}, interval={self.candle_interval}>"