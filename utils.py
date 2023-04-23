from datetime import timedelta

import pandas as pd
import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import Session
from tinkoff.invest.schemas import MoneyValue


def extract_money_amount(moneyObj: MoneyValue) -> float:
    return round(moneyObj.units + moneyObj.nano*0.000000001, 2)

def convert_timedelta_to_str(time: timedelta) -> str:
    days = f"{time.days}d "
    hours = time.seconds // 3600
    minutes = time.seconds % 3600 // 60
    seconds = time.seconds % 60
    time_str = f"{hours}:{minutes}:{seconds}"
    return days + time_str if time.days else time_str

def get_positions_stats(data):
    data = [pos.to_dict() for pos in data]
    df = pd.DataFrame(data=data)
    df = df.set_index("id")
    df["result_percent"] = ((df["result"] / (df["open_price"] * df["size"])) * 100).round(2)
    df["time_in_trade"] = df["close_date"] - df["open_date"]
    df["status"] = df["result"].apply(lambda x: "win" if x > 0 else "loss")
    df["result"] = np.where(df.currency == "usd", df.result*82, df.result)
    df = df.loc[df["closed"]]

    group_by_side = (
        df[["side", "result", "ticker", "fee", "result_percent", "status", "time_in_trade"]]
        .groupby(["side", "status"], as_index=True)
        .aggregate(
            number_of_trades = ("ticker", "count"), 
            total_result = ("result", "sum"), 
            average_result = ("result", "mean"), 
            total_fee = ("fee", "sum"), 
            result_percent = ("result_percent", "mean"),
            average_time_in_trade = ("time_in_trade", "mean")
        )
    )

    group_by_side.loc[("all", "all"),:] = (
        df["ticker"].count(),
        df["result"].sum(),
        df["result"].mean(),
        df["fee"].sum(),
        df["result_percent"].mean(),
        df["time_in_trade"].mean()
    )

    group_by_side = group_by_side.round(2)
    group_by_side["number_of_trades"] = group_by_side["number_of_trades"].astype(int)
    group_by_side = group_by_side.to_dict("index")
    for section in group_by_side:
        time = group_by_side[section]["average_time_in_trade"]
        group_by_side[section]["average_time_in_trade"] = convert_timedelta_to_str(time)
    return group_by_side