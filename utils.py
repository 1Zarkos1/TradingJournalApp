from datetime import timedelta
from dataclasses import dataclass
from typing import Callable, List

import numpy as np
import pandas as pd
import pyqtgraph as pg
from tinkoff.invest.schemas import MoneyValue
from pyqtgraph import QtCore, QtGui
from PyQt6.QtWidgets import (
    QWidget,  
    QPushButton, 
    QLabel, 
    QCheckBox
)
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
        value=lambda pos: "WIN" if pos.result > 0 else "LOSS",
        modifier=lambda widget: widget.setProperty("class", f"status-label {widget.text() == 'LOSS' and 'lost'}"),
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

def extract_money_amount(moneyObj: MoneyValue) -> float:
    return round(moneyObj.units + moneyObj.nano*0.000000001, 2)

def assign_class(position, widget):
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
        w = (self.data[1][0] - self.data[0][0]) / 3.
        for (t, open, close, min, max) in self.data:
            p.drawLine(QtCore.QPointF(t, min), QtCore.QPointF(t, max))
            if open > close:
                p.setBrush(pg.mkBrush('r'))
            else:
                p.setBrush(pg.mkBrush('g'))
            p.drawRect(QtCore.QRectF(t-w, open, w*2, close-open))
        p.end()
    
    def paint(self, p, *args):
        p.drawPicture(0, 0, self.picture)
    
    def boundingRect(self):
        ## boundingRect _must_ indicate the entire area that will be drawn on
        ## or else we will get artifacts and possibly crashing.
        ## (in this case, QPicture does all the work of computing the bouning rect for us)
        return QtCore.QRectF(self.picture.boundingRect())