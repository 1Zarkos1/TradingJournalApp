#%%
import os
from datetime import timezone, datetime

from dotenv import load_dotenv
from sqlalchemy import create_engine, select, inspect, and_
from sqlalchemy.orm import Session
from tinkoff.invest import Client
from tinkoff.invest.schemas import OperationState, OperationType

from tables import Base, Asset, Operation, Position

load_dotenv(".env")

API_TOKEN = os.getenv("T_TOKEN")
DB_NAME = os.getenv("DB_NAME")
EXECUTED_OPERATION = OperationState.OPERATION_STATE_EXECUTED
ACCOUNT_NAME = "Trading"

OPERATION_TYPES = {
    OperationType.OPERATION_TYPE_BUY: "Buy",
    OperationType.OPERATION_TYPE_SELL: "Sell",
    OperationType.OPERATION_TYPE_BROKER_FEE: "Fee"
}

engine = create_engine(f"sqlite:///{DB_NAME}")

# initialize DB
Base.metadata.drop_all(engine)
if not os.path.exists(f'{DB_NAME}') or not inspect(engine).has_table("operation"):
    Base.metadata.create_all(engine)
#%%
def extract_money_amount(moneyObj):
    return round(moneyObj.units + moneyObj.nano*0.000000001, 2)

def assets_populated(engine):
    with Session(engine) as session:
        return session.scalar(select(Asset))
    
def populate_assets(client, engine):
    stocks_available = client.instruments.shares().instruments
    assets = []
    for stock in stocks_available:
        asset = Asset(
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

def get_available_accounts(client):
    accounts_response = client.users.get_accounts().accounts
    available_accounts: dict = {
        account.name: {
            "id": account.id,
            "open_date": account.opened_date
        }
        for account in accounts_response
    }
    return available_accounts

def get_account_operations(client, account, from_date=None, to_date=None):
    operations_response = client.operations.get_operations(
        account_id=account["id"], 
        from_=from_date or account["open_date"],
        to=to_date or datetime.now(timezone.utc)
    )
    return sorted(
        operations_response.operations,
        key=lambda obj: obj.date
    )

def update_position(operation, position):
    position.result += operation.price * operation.quantity
    same_side_position_quantity = sum(
        [op.quantity for op in position.operations 
         if op.side == operation.side]
    )
    new_operation_price_fraction = operation.price * (operation.quantity / same_side_position_quantity)
    existing_quantity_to_total_ratio = same_side_position_quantity - operation.quantity / same_side_position_quantity
    if position.side == operation.side:
        position.open_price = (
            position.open_price 
            * existing_quantity_to_total_ratio 
            + new_operation_price_fraction
        )
    else:
        position.closing_price = (
            position.closing_price 
            * existing_quantity_to_total_ratio 
            + new_operation_price_fraction
        )

        opposite_side_position_quantity = sum(
            [op.quantity for op in position.operations 
             if op.side != operation.side]
        )
        if same_side_position_quantity == opposite_side_position_quantity:
            position.closed = True
#%%
with Client(API_TOKEN) as client:
    if not assets_populated(engine):
        populate_assets(client, engine)
    with Session(engine) as session:
        assets = session.scalars(select(Asset)).all()
    tickers = {
        asset.figi: asset.ticker
        for asset in assets
    }

    available_accounts = get_available_accounts(client)
    try:
        selected_account = available_accounts[ACCOUNT_NAME]
    except KeyError:
        raise Exception("There is no account available with that name")
    
    operations_response = get_account_operations(
        client, 
        selected_account
    )
#%%
with Session(engine) as session:
    for operation in operations_response:
        # process only executed operations
        if operation.state == EXECUTED_OPERATION:
            # some of operations are not processed like dividends or currency trading
            # in that case they are just logged
            operation_type = OPERATION_TYPES.get(operation.operation_type)
            ticker = tickers.get(operation.figi)
            price = extract_money_amount(operation.price)
            if not operation_type or not ticker:
                print(f'------{operation}-------')
                continue

            # check if there is any open position for particular ticker
            position = session.scalar(
                select(Position).where(and_(Position.closed == False, Position.ticker == ticker))
            )

            if operation_type == "Fee":
                parent_operation = session.scalar(
                    select(Operation)
                    .where(Operation.id == operation.parent_operation_id)
                )
                if parent_operation:
                    fee = extract_money_amount(operation.payment)
                    parent_operation.fee = fee
                    parent_operation.position.fee += fee
            else:
                if not position:
                    position = Position(
                        ticker = ticker,
                        side = operation_type,
                        currency = operation.currency,
                        open_price = 0,
                        result = 0
                    )
                operation_entry = Operation(
                    id = operation.id,
                    ticker = ticker,
                    position = position,
                    side = operation_type,
                    time = operation.date,
                    quantity = operation.quantity,
                    price = price,
                    fee = 0
                )
                session.add(operation_entry)
                update_position(operation_entry, position)
    session.commit()
# %%
with Session(engine) as session:
    results = session.scalar(select(Asset).where(Asset.ticker == "ORCL"))
    print(results)
# %%
with Session(engine) as session:
    results = session.scalars(select(Position).where(Position.closed == False)).all()
    for result in results:
        print(result.id, result.ticker, result.side, result.closed)
        for operation in result.operations:
            print("------", operation.ticker, operation.time, operation.side)
# %%
with Client(API_TOKEN) as client:
    accounts = get_available_accounts(client)
    deals = get_account_operations(
        client, 
        accounts["Trading"]
    )
# %%
from pprint import pprint
pprint([deal for deal in deals])
# %%
from datetime import timedelta

timedelta()