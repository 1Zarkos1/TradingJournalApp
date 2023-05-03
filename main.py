import os
from typing import List, Dict
from datetime import timezone, datetime, timedelta

from dotenv import load_dotenv
from sqlalchemy import select, Engine, inspect
from sqlalchemy.orm import Session
from tinkoff.invest import Client
from tinkoff.invest.schemas import OperationState, OperationType, Operation as Sdk_Operation, CandleInterval
from tinkoff.invest.exceptions import RequestError
from grpc import StatusCode

from tables import Asset, Operation, AdditionalPayment, Position, ChartData, WalkAwayData
from utils import extract_money_amount, get_account_info_from_env, set_account_info_to_env

load_dotenv(".env")

API_TOKEN = os.getenv("T_TOKEN")
ACCOUNT_NAME = os.getenv("DEFAULT_ACCOUNT_NAME")
DB_NAME = f"{ACCOUNT_NAME.lower()}_{os.getenv('DB_SUFFIX')}"
EXECUTED_OPERATION = OperationState.OPERATION_STATE_EXECUTED
PAGE_SIZE = int(os.getenv("DEFAULT_PAGE_SIZE"))

OPERATION_TYPES = {
    OperationType.OPERATION_TYPE_BUY: "Buy",
    OperationType.OPERATION_TYPE_SELL: "Sell",
    OperationType.OPERATION_TYPE_BROKER_FEE: "Fee"
}

def get_available_accounts(get_online=False) -> dict:
    tokens = {
        key: os.environ.get(key) 
        for key in dict(os.environ) 
        if key.endswith("_TOKEN")
    }
    accounts = {}
    for token_key, token in tokens.items():
        acc_name = token_key.split("_")[0]
        available_accounts = get_account_info_from_env(acc_name, token)
        if not available_accounts:
            print("poling from net")
            with Client(token) as client:
                try:
                    accounts_response = client.users.get_accounts().accounts
                except RequestError as e:
                    if e.code == StatusCode.UNAVAILABLE:
                        print("Unavailable")
                for account in accounts_response:
                    set_account_info_to_env(account)
                    available_accounts = get_account_info_from_env(acc_name, token)
        accounts = accounts | available_accounts
    return accounts

def get_account(available_accounts: dict, account_name: str = ACCOUNT_NAME) -> dict:
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
    from_date = from_date.replace(tzinfo=timezone.utc)
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

def record_operations(operations_response: List[Sdk_Operation], engine: Engine, client: Client) -> None:
    with Session(engine) as session:
        if not Asset.assets_populated(session):
            Asset.populate_assets(client, session)
        tickers = Asset.get_figi_to_ticker_mapping(session)
        last_trade = session.scalar(select(Operation).order_by(Operation.time.desc()))
        last_trade_id = getattr(last_trade, "id", 0)
        operations_count = 0
        for operation in operations_response:
            # process only executed operations
            if operation.state == EXECUTED_OPERATION:
                if operation.id == last_trade_id:
                    continue

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
                    if not operation.ticker:
                        asset = client.instruments.get_instrument_by(
                            id_type=1, 
                            id=operation.figi
                        ).instrument
                        Asset.populate_assets(client, session, [asset])
                        tickers[asset.figi] = asset.ticker
                        operation.ticker = asset.ticker
                    Operation.add_operation(operation, session)
                    operations_count += 1
        session.commit()
        return operations_count

def synchronize_operations(client: Client, engine: Engine, account_name: str, token: str, last_operation_date: datetime = None) -> None:
    with Client(token) as client:
        accounts = get_available_accounts()
        selected_account = get_account(accounts, account_name)
        operations_response = get_account_operations(client, selected_account, last_operation_date)
        return record_operations(operations_response, engine, client)

def get_waa_data_from_db(engine: Engine, position: Position) -> dict:
    with Session(engine) as session:
        data: WalkAwayData = session.scalar(select(WalkAwayData).where(WalkAwayData.position == position))
    if data:
        data = data.history_data
    return data


def get_walk_away_analysis_data(engine: Engine, token: str, position: Position) -> dict:
    price_history = get_waa_data_from_db(engine, position)
    if not price_history:
        close_date = position.close_date.replace(tzinfo=timezone.utc)
        with Session(engine) as session:
            figi = session.scalar(select(Asset.figi).where(Asset.ticker == position.ticker))
        candle_parameters = {
            "5min": {
                "candle_range": 2, 
                "from": timedelta(seconds=0),
                "to": timedelta(seconds=300)
            },
            "1hour": {
                "candle_range": 2, 
                "from": (+timedelta(seconds=3600)-timedelta(seconds=300)),
                "to": timedelta(seconds=3600)
            },
            "day": {
                "candle_range": 5, 
                "from": -timedelta(days=1),
                "to": timedelta(0)
            },
            "2day": {
                "candle_range": 5, 
                "from": timedelta(1),
                "to": timedelta(2)
            },
            "week": {
                "candle_range": 5, 
                "from": timedelta(6),
                "to": timedelta(7)
            }
        }
        price_history = {}
        with Client(token) as client:
            for interval, values in candle_parameters.items():
                candles = client.market_data.get_candles(
                    figi = figi,
                    from_ = close_date + values.get("from"),
                    to = close_date + values.get("to"),
                    interval = values.get("candle_range")
                ).candles
                if candles:
                    closing_money_value = extract_money_amount(candles[-1].close)
                    price_history[interval] = closing_money_value
                else:
                    price_history[interval] = "0"
            with Session(engine, expire_on_commit=False) as session:
                walk_away_obj = WalkAwayData(
                    position=position,
                    ticker=position.ticker,
                    history_data=price_history
                )
                session.add(walk_away_obj)
                session.commit()
    return price_history

def get_chart_data_from_api(engine: Engine, token: str, position: Position) -> Dict[datetime, Dict[str, float]]:
    trade_time_padding = timedelta(seconds=3600)
    from_ = position.open_date.replace(tzinfo=timezone.utc) - trade_time_padding
    to = position.close_date.replace(tzinfo=timezone.utc) + trade_time_padding
    with Session(engine) as session:
        figi = session.scalar(select(Asset.figi).where(Asset.ticker == position.ticker))
    interval = CandleInterval.CANDLE_INTERVAL_5_MIN
    candles = []
    with Client(token) as client:
        if to - from_ > timedelta(days=1):
            batch_to = from_ + timedelta(days=1)
            while batch_to < to:
                candles.extend(
                    client.market_data.get_candles(figi=figi, from_=from_, to=batch_to, interval=interval).candles
                )
                from_, batch_to = batch_to, batch_to + timedelta(days=1)
        candles.extend(
            client.market_data.get_candles(figi=figi, from_=from_, to=to, interval=interval).candles
        )
    candle_values = {
            candle.time.timestamp(): {
                "open": extract_money_amount(candle.open),
                "close": extract_money_amount(candle.close),
                "high": extract_money_amount(candle.high),
                "low": extract_money_amount(candle.low)
            }
            for candle in candles
    }
    return candle_values

def get_chart_data(engine: Engine, token: str, position: Position) -> List[tuple]:
    with Session(engine) as session:
        data = session.scalar(select(ChartData).where(ChartData.position == position))
    if data:
        candles = {float(timestamp): values for timestamp, values in data.candles.items()}
    else:
        candles = get_chart_data_from_api(engine, token, position)
        chart_data = ChartData(
            position=position,
            ticker=position.ticker,
            candle_interval=timedelta(seconds=300),
            candles=candles
        )
        with Session(engine, expire_on_commit=False) as session:
            session.add(chart_data)
            session.commit()
    return candles
        