import calendar
import os
from datetime import timedelta, datetime, date, time, timezone
from dataclasses import dataclass
from typing import Callable, List

import numpy as np
import pandas as pd
import pyqtgraph as pg
from dotenv import load_dotenv, set_key
from tinkoff.invest.schemas import MoneyValue, Account
from pyqtgraph import QtCore, QtGui
from PyQt6.QtWidgets import QWidget, QPushButton, QLabel, QCheckBox
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QCursor, QPixmap

@dataclass
class Field:
    attribute: str
    header_value: str
    value: Callable = None
    modifier: Callable = None
    class_: str = ''
    widget: QWidget = QLabel

def iconModifier(widget: QLabel):
    if text := widget.text():
        icon_path = "static/edit.png"
        widget.setToolTip(text)
    else:
        icon_path = "static/add.png"
    image = QPixmap(icon_path)
    image = image.scaled(15, 15)
    widget.setPixmap(image) 

tradelist_fields: List[Field] = [
    Field(
        attribute="chb",
        value=lambda pos: "",
        class_="chb",
        header_value="checkbox",
        widget=QCheckBox
    ),
    Field(
        attribute="status",
        value=lambda pos: "OPEN" if not pos.closed else ("WIN" if pos.result > 0 else "LOSS"),
        modifier=lambda widget: widget.setProperty("class", f"status-label {widget.text().lower()}"),
        class_="status-label",
        header_value="status"
    ),
    Field(
        attribute="open_date",
        value=lambda pos: pos.open_date.strftime("%b %d, %Y").upper(),
        header_value="date"
    ),
    Field(
        widget=QPushButton,
        attribute="ticker",
        modifier=lambda widget: widget.setCursor(QCursor(Qt.CursorShape.PointingHandCursor)),
        class_="ticker-label",
        header_value="symbol"
    ),
    Field(
        attribute="open_price",
        header_value="entry"
    ),
    Field(
        attribute="closing_price",
        header_value="exit"
    ),
    Field(
        attribute="size",
        header_value="size"
    ),
    Field(
        attribute="side",
        value= lambda position: "long" if position.side == "Buy" else "short",
        class_="side",
        header_value="side"
    ),
    Field(
        attribute="result",
        value=lambda pos: str(round(pos.result, 2)) if pos.closed else "0",
        header_value="return $"
    ),
    Field(
        attribute="resulting_percentage",
        header_value="return %"
    ),
    Field(
        widget=QLabel,
        value=lambda pos: pos.note or "",
        modifier=iconModifier,
        class_="note-icon",
        attribute="note",
        header_value="note"
    )
]

trading_hours = {
    "rub": (
        time(7, 0, 0, tzinfo=timezone.utc),
        time(15, 40, 0, tzinfo=timezone.utc)
    ),
    "usd": (
        time(11, 30, 0, tzinfo=timezone.utc),
        time(22, 45, 0, tzinfo=timezone.utc)
    )
}

def extract_money_amount(moneyObj: MoneyValue | float) -> float:
    if isinstance(moneyObj, float):
        return moneyObj
    else:
        return round(moneyObj.units + moneyObj.nano*0.000000001, 2)

def assign_class(position: "Position", widget: QWidget) -> QWidget:
    class_ = "red"
    side = position.side.lower()
    close = position.closing_price
    try:
        history_price = float(widget.text())
        if (
            (history_price > close and side == "buy") 
            or (history_price < close and side == "sell")
        ):
            class_ = "green"
        widget.setProperty("class", widget.property("class")+ " " + class_)
    except Exception as e:
        print(e)
    return widget

def time_in_trading_hours(currency: str, trade_time: time) -> bool:
    trade_time = trade_time.replace(tzinfo=timezone.utc)
    exchange_working_hours = trading_hours[currency]
    return trade_time >= exchange_working_hours[0] and trade_time <= exchange_working_hours[1]

def date_in_weekday(trade_date):
    if trade_date and trade_date.weekday() not in (5, 6):
        return False

def get_applicable_datetime(position: "Position", intended_interval: timedelta,
                            time_direction: str) -> datetime:
    initial_time = position.close_date.replace(tzinfo=timezone.utc)
    intended_datetime: datetime = initial_time + intended_interval
    if (
        (
            time_in_trading_hours(position.currency, intended_datetime.time()) 
            and intended_datetime.weekday() not in (5, 6)
        )
        or intended_interval < timedelta(0)
        or (
            intended_datetime.weekday() == 6 
            and time_direction == "from"
            and intended_interval >= timedelta(1)
        )
    ):
        return intended_datetime
    else:
        if intended_datetime.weekday() in (5, 6):
            intended_datetime += timedelta(2)
        if not time_in_trading_hours(position.currency, intended_datetime.time()):
            time_between_sessions = (
                (datetime.combine(date.min, trading_hours[position.currency][0]) + timedelta(1))
                - datetime.combine(date.min, trading_hours[position.currency][1])
            )
            intended_datetime += time_between_sessions
        return intended_datetime
            

def convert_timedelta_to_str(time: timedelta) -> str:
    days = f"{time.days}d "
    hours = time.seconds // 3600
    minutes = time.seconds % 3600 // 60
    seconds = time.seconds % 60
    time_str = f"{hours}:{minutes}:{seconds}"
    return days + time_str if time.days else time_str

def modify_positions_stats(
        data: List["Position"], closed_only: bool = True, 
        exclude_outliers: bool = False) -> pd.DataFrame:
    data = [pos.to_dict() for pos in data]
    df = pd.DataFrame(data=data)
    df = df.set_index("id")
    df["result_percent"] = ((df["result"] / (df["open_price"] * df["size"])) * 100).round(2)
    df["time_in_trade"] = df["close_date"] - df["open_date"]
    df["status"] = df["result"].apply(lambda x: "win" if x > 0 else "loss")
    df["result"] = np.where(df.currency == "usd", df.result*82, df.result)
    if closed_only:
        df = df.loc[df["closed"]]
    if exclude_outliers:
        q_low = df["result"].quantile(0.01)
        q_hi  = df["result"].quantile(0.99)

        # df = df[(df["result"] > q_low)]
        df = df[(df["result"] < q_hi) & (df["result"] > q_low)]
    return df

def get_month_mapping(year: int, month: int) -> List[date]:
    if month:
        month_first_weekday, last_day_of_the_month = calendar.monthrange(year, month)
        month_last_weekday = date(year, month, last_day_of_the_month).weekday()
        first_day_of_the_first_week = date(year, month, 1) - timedelta(month_first_weekday)
        last_day_of_the_last_week = date(year, month, last_day_of_the_month) + timedelta(6 - month_last_weekday)
        first_week_number = first_day_of_the_first_week.isocalendar().week

        current_date = first_day_of_the_first_week
        calendar_map = []
        while current_date <= last_day_of_the_last_week:
            calendar_map.append(current_date)
            current_date += timedelta(1)
    else:
        calendar_map = [
            date(year, month_num, calendar.monthrange(year, month_num)[1]) 
            for month_num in range(1, 13)
        ]

    return calendar_map

def group_df_by_frequency(df, freq):
    df = (
        df[["open_date", "result", "ticker"]]
        .groupby(pd.Grouper(key="open_date", freq=freq), as_index=True)
        .aggregate(
            number_of_trades = ("ticker", "count"), 
            total_result = ("result", "sum")
        )
    )
    df.index = df.index.date
    df.number_of_trades = df.number_of_trades.astype(int)
    df.total_result = df.total_result.round(2)
    df = df.loc[df["number_of_trades"] != 0]

    return df

def transform_group_df_to_dict(df: pd.DataFrame, calendar_days: List[date], total: bool = False):
    calendar_mapping = {}
    for day in calendar_days:
        try:
            info = df.loc[day]
            calendar_mapping[day] = {
                "trades": info.number_of_trades,
                "result": info.total_result
            }
        except KeyError:
            # if total:
            #     continue
            calendar_mapping[day] = {}

    return calendar_mapping


def get_calendar_performance(
        data: List["Position"], year: int = date.today().year, 
        month: int = date.today().month) -> dict:
    unit_frequency = "M" if month == 0 else "D"
    summary_frequency = "Q" if month == 0 else "W"
    calendar_days = get_month_mapping(year, month)
    df = modify_positions_stats(data)
    if month:
        df = df.loc[(df["open_date"].dt.month == month) & (df["open_date"].dt.year == year)]
    else:
        df = df.loc[df["open_date"].dt.year == year]
    unit_df = group_df_by_frequency(df, unit_frequency)
    calendar_mapping = transform_group_df_to_dict(unit_df, calendar_days)

    summary_df = group_df_by_frequency(df, summary_frequency)
    summary_calendar_mapping = transform_group_df_to_dict(summary_df, calendar_days, True)

    return calendar_mapping, summary_calendar_mapping

def get_positions_stats(data: List["Position"]) -> dict:
    df = modify_positions_stats(data)

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

def get_account_info_from_env(name: str, token: str) -> dict | None:
    var_prefix = f"{name}_"
    load_dotenv(".env")
    acc_name = os.environ.get(f"{var_prefix}NAME")
    id_ = os.environ.get(f"{var_prefix}ID")
    open_date = os.environ.get(f"{var_prefix}OPEN_DATE")
    token = token
    if all([acc_name, id_, open_date, token]):
        return {
            acc_name: {
                "id": id_,
                "open_date": datetime.fromtimestamp(float(open_date)),
                "token": token
            }
        }
    else:
        return None

def set_account_info_to_env(account_resp: Account) -> None:
    var_prefix = f"{account_resp.name.upper()}_"
    set_key(".env", f"{var_prefix}ID", account_resp.id)
    set_key(".env", f"{var_prefix}OPEN_DATE", str(account_resp.opened_date.timestamp()))
    set_key(".env", f"{var_prefix}NAME", account_resp.name)

def find_accounts_db_in_system(db_suffix: str) -> List[str]:
    accounts_available = []
    for filename in os.listdir("."):
        if filename.endswith(db_suffix):
            accounts_available.append(filename.split("_")[0].lower())
    return accounts_available

class CandlestickItem(pg.GraphicsObject):
    ## Create a subclass of GraphicsObject.
    ## The only required methods are paint() and boundingRect() 
    ## (see QGraphicsItem documentation)
    def __init__(self, data):
        pg.GraphicsObject.__init__(self)
        self.data = data  ## data must have fields: time, open, close, min, max
        self.generatePicture()
    
    def generatePicture(self):
        ## pre-computing a QPicture object allows paint() to run much more quickly, 
        ## rather than re-drawing the shapes every time.
        self.picture = QtGui.QPicture()
        p = QtGui.QPainter(self.picture)
        p.setPen(pg.mkPen('w'))
        times = list(self.data.keys())
        candleHalfWidth = (times[1] - times[0]) / 3.
        for timestamp, prices in self.data.items():
            p.drawLine(
                QtCore.QPointF(timestamp, prices["low"]), 
                QtCore.QPointF(timestamp, prices["high"])
            )
            p.setBrush(pg.mkBrush('r' if prices["open"] > prices["close"] else 'g'))
            p.drawRect(
                QtCore.QRectF(
                    timestamp - candleHalfWidth, prices["open"], 
                    candleHalfWidth*2, prices["close"]-prices["open"]
                )
            )
        p.end()
    
    def paint(self, p, *args):
        p.drawPicture(0, 0, self.picture)
    
    def boundingRect(self):
        ## boundingRect _must_ indicate the entire area that will be drawn on
        ## or else we will get artifacts and possibly crashing.
        ## (in this case, QPicture does all the work of computing the bouning rect for us)
        return QtCore.QRectF(self.picture.boundingRect())