from dataclasses import dataclass
import sys
import math
from functools import partial
from datetime import datetime
from typing import Callable, List

from PyQt6.QtWidgets import (
    QApplication, 
    QWidget, 
    QMainWindow, 
    QPushButton, 
    QLabel, 
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
    QLineEdit,
    QCheckBox,
    QSizePolicy
)
from PyQt6.QtCore import QSize, QtMsgType, Qt, QEvent, QObject
from PyQt6.QtGui import QFont, QCursor, QMouseEvent
from sqlalchemy import select
from sqlalchemy.orm import Session

from main import get_available_accounts, API_TOKEN, ACCOUNT_NAME, PAGE_SIZE, Client, synchronize_operations
from tables import Asset, Position, Operation, AdditionalPayment, get_engine

@dataclass
class Field:
    attribute: str
    header_value: str
    value: Callable = None
    modifier: Callable = None
    class_: str = ''
    widget: QWidget = QLabel

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
        value=lambda pos: pos.open_date.strftime("%b %d, %Y"),
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
        attribute="note",
        header_value="note"
    )
]

engine = get_engine('trading_invest.db')

class JournalApp(QMainWindow):
    def __init__(self) -> None:
        super().__init__()

        self.currentPage = 0
        self.records = Position.get_positions(engine)
        self.setWindowTitle("TradingJournal")
        self.setFont(QFont(["Poppins", "sans-serif"]))
        with open("style.css", "r") as f:
            self.setStyleSheet(f.read())
        self.initTradeListUI()

    def initAccountSelectionUI(self):
        accounts = get_available_accounts()
        central = QWidget(self)
        layout = QVBoxLayout()
        central.setLayout(layout)
        central.setProperty("class", "central")
        self.setCentralWidget(central)
        layout.addWidget(QLabel("Select trading account:"))
        [layout.addWidget(QPushButton(account_name)) for account_name in accounts]

    def drawTradeListHeader(self, layout: QGridLayout) -> None:
        button = QPushButton("Sync trades")
        button.clicked.connect(self.updateTrades)
        layout.addWidget(button, 0, len(tradelist_fields)//2-1, 1, 3)
        for col_num, field in enumerate(tradelist_fields):
            header_column = QLabel(field.header_value.upper())
            # header_column.setAlignment(Qt.AlignmentFlag.AlignHCenter)
            header_column.setProperty("class", "header-label")
            header_column.installEventFilter(self)
            layout.addWidget(header_column, 1, col_num, alignment=Qt.AlignmentFlag.AlignHCenter)

    def eventFilter(self, a0: 'QObject', a1: 'QEvent') -> bool:
        if a1.type() == QMouseEvent.Type.MouseButtonPress and a1.button() == Qt.MouseButton.LeftButton:
            self.sortResults(a0)
        return super().eventFilter(a0, a1)

    def sortResults(self, label_obj):
        sort_field = [obj.attribute for obj in tradelist_fields if obj.header_value == label_obj.text().lower()][0]
        self.records = Position.get_positions(engine, sorting_field=sort_field)
        print(self.records)
        self.initTradeListUI()

    def initTradeListUI(self):
        central = QWidget(self)
        layout = QGridLayout()
        layout.setSpacing(0)
        central.setLayout(layout)

        self.drawTradeListHeader(layout)

        currentPageRecords = self.records[self.currentPage*PAGE_SIZE:self.currentPage*PAGE_SIZE+PAGE_SIZE]
        
        for row_n, position in enumerate(currentPageRecords, start=2):
            for col_n, field in enumerate(tradelist_fields):
                value = field.value(position) if getattr(field, "value") else str(getattr(position, field.attribute))
                css_class = f"tradelist-field {field.class_} {'even' if not row_n % 2 else ''}"
                widget = field.widget(value)
                widget.setProperty("class", css_class)
                field.modifier(widget) if getattr(field, "modifier") else None
                isinstance(widget, QLabel) and widget.setAlignment(Qt.AlignmentFlag.AlignHCenter)
                layout.addWidget(widget, row_n, col_n)

        self.drawPageSelection(layout)

        self.setCentralWidget(central)

    def drawPageSelection(self, layout):
        number_of_pages = math.ceil(len(self.records)/PAGE_SIZE)
        page_selection_widget = QWidget()
        page_selection_layout = QHBoxLayout()
        page_selection_widget.setLayout(page_selection_layout)
        for page in range(1, number_of_pages+1):
            button = QPushButton(str(page), page_selection_widget)
            if page-1 == self.currentPage:
                button.setProperty("class", "current-page")
            button.clicked.connect(partial(self.changePage, page))
            page_selection_layout.addWidget(button)
        layout.addWidget(page_selection_widget, PAGE_SIZE+2, 0, 1, len(tradelist_fields), alignment=Qt.AlignmentFlag.AlignRight)

    def changePage(self, page):
        self.currentPage = page - 1
        self.initTradeListUI()

    def apply_filter(self, button: QPushButton):
        print(button.text())
        # print(button)

    def process_filter(self):
        filter_text = self.filter_field.text()
        f_field = filter_text.split(":")[0]
        filter_value = filter_text.split(":")[1].strip()
        if f_field in ["open_date", "close_date"]:
            filter_value = datetime.strptime(filter_value, "%m/%d/%Y")

    def updateTrades(self):
        with Session(engine) as session:
            last_trade = session.scalar(select(Operation).order_by(Operation.time.desc()))
        with Client(API_TOKEN) as client:
            synchronize_operations(client, ACCOUNT_NAME, last_trade.time)
        self.records = Position.get_positions(engine)


app = QApplication(sys.argv)

window = JournalApp()
window.show()

app.exec()