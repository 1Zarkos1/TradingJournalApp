#%%
import os
from dataclasses import dataclass
from typing import List
from pprint import pprint
from datetime import datetime, timedelta

from dotenv import load_dotenv
from tinkoff.invest import Client, schemas

load_dotenv(".env")

API_TOKEN = os.getenv("T_TOKEN")

executed_operation = schemas.OperationState.OPERATION_STATE_EXECUTED

@dataclass
class Position():
    id_: str
    ticker: str
    side: str
    time: datetime
    shares: int
    share_price: float
    payment: float
    fee: float = 0

@dataclass
class Trade():
    main_side_positions: List[Position]
    closing_side_positions: List[Position]
    ticker: str
    side: str
    closed: bool = False
    fee: float = 0
    
    @property
    def result(self):
        main_side = sum([position.payment for position in self.main_side_positions])
        closing_side = sum([position.payment for position in self.closing_side_positions])
        return round(main_side + closing_side, 2)

def get_instruments_tickers(client):
    stocks_available = client.instruments.shares().instruments
    return {stock.figi:stock.ticker for stock in stocks_available}

def extract_money_amount(moneyObj):
    return round(moneyObj.units + moneyObj.nano*0.000000001, 2)

#%%

with Client(API_TOKEN) as client:
    tickers = get_instruments_tickers(client)
    accounts_available = client.users.get_accounts().accounts
    print(accounts_available)
    account_id = accounts_available[0].id
    operations_response = client.operations.get_operations(
        account_id=account_id, 
        from_=datetime.utcnow()-timedelta(days=30)
    )

#%%
trades_executed: List[Trade] = []

operations = sorted(
    operations_response.operations,
    key=lambda obj: obj.date
)
position: Position = None
for operation in operations:
    if operation.operation_type == schemas.OperationType.OPERATION_TYPE_SERVICE_FEE:
        print(operation)
        continue
    if operation.state != executed_operation:
        continue
    if (parent_id := operation.parent_operation_id) and position:
        if position.id_ == parent_id:
            position.fee = extract_money_amount(operation.payment)
            trade.fee += position.fee
            continue
        else:
            print(operation)
            continue
            # raise Exception("Parent operation is not found")
    id_ = operation.id
    ticker = tickers.get(operation.figi, None)
    if not ticker:
        print(operation)
        continue
    side = "Long" if operation.operation_type == schemas.OperationType.OPERATION_TYPE_BUY else "Short"
    shares = operation.quantity
    share_price = extract_money_amount(operation.price)
    payment = extract_money_amount(operation.payment)
    position = Position(
        id_, ticker, side, operation.date, shares, share_price, payment
    )
    for trade in trades_executed:
        if not trade.closed and trade.ticker == position.ticker:
            if trade.side == position.side:
                trade.main_side_positions.append(position)
            else:
                trade.closing_side_positions.append(position)
            # check if trade is closed now
            if sum(
                [position.shares for position in trade.main_side_positions]
            ) - sum (
                [position.shares for position in trade.closing_side_positions]
            ) == 0:
                trade.closed = True
            break
    else:
        # create new trade, set side
        trade = Trade([position], [], position.ticker, position.side)
        # add to a list
        trades_executed.append(trade)
# %%
pprint(trades_executed)

# %%
pprint(operations)
# %%
total_fee = 0
total_profit = 0
trades: List = []

for trade in trades_executed:
    if trade.closed:
        tr = {
            "ticker": trade.ticker,
            "side": trade.side,
            "time_opened": trade.main_side_positions[0].time,
            "open_price": trade.main_side_positions[0].share_price,
            "time_closed": trade.closing_side_positions[0].time,
            "cosing_price": trade.closing_side_positions[0].share_price,
            "result": trade.result,
            "percent": round(trade.result / trade.main_side_positions[0].share_price * 100, 2)
        }
        trades.append(tr)
        total_profit += trade.result
        total_fee += trade.fee

# %%
print(total_profit)
print(total_fee)
print(total_profit + total_fee)
# %%
print(extract_money_amount(operations[0].payment))
# %%
from datetime import datetime
first = datetime(2023, 1, 18)
second = datetime.now()
# %%
difference = second - first
# %%
print(len(trades_executed))
# %%
pprint(trades)
# %%
import pandas as pd

df = pd.DataFrame(trades)
# %%
df.loc[df["percent"] <= 0, "percent"].plot.hist(bins=5)
# %%
with Client(API_TOKEN) as client:
    # tickers = get_instruments_tickers(client)
    client.market_data.get_candles()
    prices = client.market_data.get_close_prices()
# %%
prices.close_prices
# %%
ticker_prices: dict = {}
for price in prices.close_prices:
    ticker = tickers.get(price.figi, "unknown")
    ticker_prices[ticker] = round(price.price.units + price.price.nano * 0.000000001, 2)
# %%
ticker_prices.get("HWM")
# %%
from datetime import timezone
from tinkoff.invest import CandleInterval
with Client(API_TOKEN) as client:
    # tickers = get_instruments_tickers(client)
    candles = client.market_data.get_candles(
        figi="BBG00YTS96G2",
        from_=datetime(2023, 2, 9, 20, 55, 0, tzinfo=timezone.utc),
        to=datetime(2023, 2, 9, 21, 0, 0, tzinfo=timezone.utc),
        interval=CandleInterval.CANDLE_INTERVAL_5_MIN
    )
# %%
candles.candles
# %%
load_dotenv(".env")
# %%
print(os.environ.get("LAST_SYNC"))
# %%
from dotenv import set_key
# %%
set_key(".env", "LAST_SYNC", "")
# %%
with Client(API_TOKEN) as client:
    stocks_available = client.instruments.shares().instruments

# %%
pprint(stocks_available[1])
# %%
from tables import Base, Asset, Operation, Position
from sqlalchemy.engine import create_engine

engine = create_engine("sqlite:///test.db", echo=True)
# Base.metadata.create_all(engine)
# %%
with engine.connect() as connection:
    ass = Asset(
        ticker = "ASG",
        figi = "ASG",
        uid = "ASG",
        position_uid = "ASG",
        currency = "ASG",
        country = "ASG",
        sector = "ASG",
        short_available = False
    )
    oper = Operation(
        id = "some",
        asset = ass,
        position_id = 0,
        side = "Short",
        time = datetime.now(),
        quantity = 2,
        price = 123.5,
        fee = 0.5
    )
# %%
from sqlalchemy.orm import Session

with Session(engine) as session:
    session.add_all([ass, oper])
    session.commit()
# %%
from sqlalchemy import select

k = session.scalars(select(Operation)).one()
# %%
k.price
# %%
ass.operations
# %%
with Session(engine) as session:
    asset = session.scalars(select(Asset).join(Operation).where(Operation.id=="some"))
    print(asset.all())
# %%
if not os.path.exists(f'{DB_NAME}') or not engine.dialect.has_table("operation"):
    Base.metadata.create_all()