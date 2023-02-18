import os
from typing import List
from datetime import timezone, datetime, timedelta

from dotenv import load_dotenv
from sqlalchemy import create_engine, select, inspect, and_, Engine
from sqlalchemy.orm import Session
from tinkoff.invest import Client
from tinkoff.invest.schemas import OperationState, OperationType, MoneyValue, Operation as Sdk_Operation

from tables import Base, Asset, Operation, Position
from utils import extract_money_amount

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

def get_available_accounts(client: Client) -> dict:
    accounts_response = client.users.get_accounts().accounts
    available_accounts: dict = {
        account.name: {
            "id": account.id,
            "open_date": account.opened_date
        }
        for account in accounts_response
    }
    return available_accounts

def get_account_operations(
        client: Client, 
        account: dict, 
        from_date: None | datetime = None, 
        to_date: None | datetime = None, 
        download_days_interval: int = None
    ) -> List[Sdk_Operation]:
    operations = []
    from_date = from_date or account["open_date"]
    to_date = to_date or datetime.now(timezone.utc)
    if download_days_interval:
        batch_end_date = from_date + timedelta(days=download_days_interval)
        batch_end_date = to_date if batch_end_date > to_date else batch_end_date
    else:
        batch_end_date = to_date
    while batch_end_date <= to_date:
        operations_response = client.operations.get_operations(
            account_id=account["id"], 
            from_=from_date,
            to=batch_end_date
        )
        operations.extend(operations_response.operations)
        try:
            if batch_end_date == to_date:
                break
            from_date = batch_end_date
            batch_end_date = batch_end_date + timedelta(days=download_days_interval)
            if batch_end_date > to_date:
                batch_end_date = to_date
                print(batch_end_date)
        except Exception:
            break
    return sorted(
        operations,
        key=lambda obj: obj.date
    )

with Client(API_TOKEN) as client:
    if not Asset.assets_populated(engine):
        Asset.populate_assets(client, engine)
    with Session(engine) as session:
        assets = session.scalars(select(Asset)).all()
    tickers = {
        asset.figi: asset.ticker
        for asset in assets
    }

    available_accounts = get_available_accounts(client)
    try:
        selected_account: dict = available_accounts[ACCOUNT_NAME]
    except KeyError:
        raise Exception("There is no account available with that name")
    
    operations_response = get_account_operations(
        client, 
        selected_account,
        from_date=datetime(2023, 2, 15, tzinfo=timezone.utc),
        download_days_interval=30
    )

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
                payment = extract_money_amount(operation.payment)
                position.update(operation_entry, payment)
    session.commit()