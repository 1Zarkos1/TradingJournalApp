import os
from typing import List
from datetime import timezone, datetime, timedelta

from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.orm import Session
from tinkoff.invest import Client
from tinkoff.invest.schemas import OperationState, OperationType, Operation as Sdk_Operation

from tables import Base, Asset, Operation, AdditionalPayment, initialize_db, get_engine
from utils import extract_money_amount

load_dotenv(".env")

API_TOKEN = os.getenv("T_TOKEN")
ACCOUNT_NAME = os.getenv("DEFAULT_ACCOUNT_NAME")
DB_NAME = f"{ACCOUNT_NAME.lower()}_{os.getenv('DB_NAME')}"
EXECUTED_OPERATION = OperationState.OPERATION_STATE_EXECUTED
PAGE_SIZE = int(os.getenv("DEFAULT_PAGE_SIZE"))

OPERATION_TYPES = {
    OperationType.OPERATION_TYPE_BUY: "Buy",
    OperationType.OPERATION_TYPE_SELL: "Sell",
    OperationType.OPERATION_TYPE_BROKER_FEE: "Fee"
}

def get_available_accounts() -> dict:
    tokens = [os.environ.get(key) for key in dict(os.environ) if key.endswith("_TOKEN")]
    accounts = {}
    for token in tokens:
        with Client(token) as client:
            accounts_response = client.users.get_accounts().accounts
            print(accounts_response)
            available_accounts: dict = {
                account.name: {
                    "id": account.id,
                    "open_date": account.opened_date,
                    "token": token
                }
                for account in accounts_response
            }
            accounts = accounts | available_accounts
    return accounts

def get_account(available_accounts, account_name: str = ACCOUNT_NAME) -> dict:
    try:
        return available_accounts[account_name]
    except KeyError:
        raise Exception("There is no account available with that name")

def get_account_operations(
        client: Client, 
        account: dict, 
        from_date: None | datetime = None, 
        to_date: None | datetime = None, 
        batch_interval: int = None # days
    ) -> List[Sdk_Operation]:
    operations = []
    from_date = from_date or account["open_date"]
    to_date = to_date or datetime.now(timezone.utc)
    if batch_interval:
        batch_end_date = from_date + timedelta(days=batch_interval)
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
            batch_end_date = batch_end_date + timedelta(days=batch_interval)
            if batch_end_date > to_date:
                batch_end_date = to_date
                print(batch_end_date)
        except Exception:
            break
    return sorted(
        operations,
        key=lambda obj: obj.date
    )

def record_operations(operations_response: List[Sdk_Operation]) -> None:
    with Session(engine) as session:
        for operation in operations_response:
            # process only executed operations
            if operation.state == EXECUTED_OPERATION:

                operation.operation_type = OPERATION_TYPES.get(operation.operation_type)
                operation.ticker = tickers.get(operation.figi)
                
                if not operation.operation_type:
                    payment = AdditionalPayment(
                        ticker=operation.ticker,
                        description=operation.type,
                        currency=operation.currency,
                        payment=extract_money_amount(operation.payment)
                    )
                    session.add(payment)
                elif operation.operation_type == "Fee":
                    parent_operation = session.scalar(
                        select(Operation)
                        .where(Operation.id == operation.parent_operation_id)
                    )
                    try:
                        parent_operation.add_fee(operation, session)
                    except Exception:
                        # raise Exception("Parent operation is not found")
                        ...
                else:
                    Operation.add_operation(operation, session)
        session.commit()

def synchronize_operations(client: Client, account_name: str, last_operation_date: datetime) -> None:
    with Client(API_TOKEN) as client:
        accounts = get_available_accounts(client)
        selected_account = get_account(accounts, account_name)
        operations_response = get_account_operations(client, selected_account, last_operation_date)
        record_operations(operations_response)


if __name__ == "__main__":

    engine = get_engine(DB_NAME)

    initialize_db(engine, DB_NAME)

    with Client(API_TOKEN) as client:
        if not Asset.assets_populated(engine):
            Asset.populate_assets(client, engine)
        tickers = Asset.get_figi_to_ticker_mapping(engine)

        available_accounts = get_available_accounts(client)
        selected_account: dict = get_account(available_accounts, ACCOUNT_NAME)

        operations_response = get_account_operations(
            client, 
            selected_account,
            from_date=datetime(2023, 2, 17, 17, tzinfo=timezone.utc),
            batch_interval=30
        )

    record_operations(operations_response)