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
    QSizePolicy,
    QMdiSubWindow,
    QPlainTextEdit
)
from PyQt6.QtCore import QSize, QtMsgType, Qt, QEvent, QObject
from PyQt6.QtGui import QFont, QCursor, QMouseEvent, QIcon, QImage, QPixmap
from sqlalchemy import select
from sqlalchemy.sql.expression import update
from sqlalchemy.orm import Session

from main import get_available_accounts, API_TOKEN, ACCOUNT_NAME, PAGE_SIZE, Client, synchronize_operations
from tables import Asset, Position, Operation, AdditionalPayment, get_engine, initialize_db

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
        widget=QLabel,
        value=lambda pos: pos.note or "",
        modifier=iconModifier,
        class_="note-icon",
        attribute="note",
        header_value="note"
    )
]

class NoteSubWindow(QWidget):

    def __init__(self, parent: 'QWidget', obj: "QObject") -> None:
        super().__init__()
        self._parent = parent
        self.setWindowTitle("AddNote")
        self._editedNote = obj
        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()
        self.setLayout(layout)
        textEdit = QPlainTextEdit(self._editedNote.toolTip())
        okBtn = QPushButton("Save")
        cancelBtn = QPushButton("Cancel")
        okBtn.clicked.connect(partial(self._parent.saveNote, textEdit, self._editedNote.id))
        cancelBtn.clicked.connect(self.close)
        layout.addWidget(textEdit)
        layout.addWidget(okBtn)
        layout.addWidget(cancelBtn)
    

class JournalApp(QMainWindow):
    def __init__(self) -> None:
        super().__init__()

        self.currentPage = 0
        self.setFont(QFont(["Poppins", "sans-serif"]))
        with open("style.css", "r") as f:
            self.setStyleSheet(f.read())
        self.initAccountSelectionUI()

    def initAccountSelectionUI(self, account_name: str = ACCOUNT_NAME):
        accounts = get_available_accounts()
        if properties := accounts.get(account_name):
            self.setUpAppForSelectedAccount(account_name, properties)
        else:
            central = QWidget(self)
            layout = QVBoxLayout()
            central.setLayout(layout)
            central.setProperty("class", "central")
            self.setCentralWidget(central)
            layout.addWidget(QLabel("Select trading account:"))
            for account_name, account_properties in accounts.items():
                selection_btn = QPushButton(account_name)
                selection_btn.clicked.connect(partial(self.setUpAppForSelectedAccount, account_name, account_properties))
                layout.addWidget(selection_btn)

    def setUpAppForSelectedAccount(self, account_name, account_properties):
        self.setWindowTitle(f"TradingJournal - {account_name}")
        self.account = account_name
        self._token = account_properties.get("token")
        self._engine = get_engine(account_name)
        initialize_db(self._engine, self._engine.url.database)
        self._records = Position.get_positions(self._engine)
        self.initTradeListUI()

    def drawTopMenuButtons(self, layout) -> None:
        widget = QWidget()
        buttonsLayout = QHBoxLayout()
        widget.setLayout(buttonsLayout)
        accountChange = QPushButton("Change account")
        accountChange.clicked.connect(self.initAccountSelectionUI)
        syncTrades = QPushButton("Sync trades")
        syncTrades.clicked.connect(self.updateTrades)
        buttonsLayout.addWidget(accountChange)
        buttonsLayout.addWidget(syncTrades)
        layout.addWidget(widget, 0, 0, 1, len(tradelist_fields))

    def drawTradeListHeader(self, layout: QGridLayout) -> None:
        self.drawTopMenuButtons(layout)
        for col_num, field in enumerate(tradelist_fields):
            header_column = QLabel(field.header_value.upper())
            header_column.setAlignment(Qt.AlignmentFlag.AlignHCenter)
            header_column.setProperty("class", "header-label")
            header_column.installEventFilter(self)
            layout.addWidget(header_column, 1, col_num)

    def eventFilter(self, a0: 'QObject', a1: 'QEvent') -> bool:
        if a1.type() == QMouseEvent.Type.MouseButtonPress and a1.button() == Qt.MouseButton.LeftButton:
            if "note" in a0.property("class"):
                self.drawNoteSubWindow(a0)
            else:
                self.sortResults(a0)
        return super().eventFilter(a0, a1)
    
    def drawNoteSubWindow(self, obj):
        self.subwindow = NoteSubWindow(parent=self, obj=obj)
        self.subwindow.show()
    
    def changeNote(self, widget):
        print("note")

    def saveNote(self, note, posId):
        with Session(self._engine) as session:
            exp = update(Position).where(Position.id == posId).values(note=note.toPlainText())
            session.execute(exp)
            session.commit()

    def sortResults(self, label_obj):
        sort_field = [obj.attribute for obj in tradelist_fields if obj.header_value == label_obj.text().lower()][0]
        self._records = Position.get_positions(self._engine, sorting_field=sort_field)
        self.initTradeListUI()

    def initTradeListUI(self):
        central = QWidget(self)
        layout = QGridLayout()
        layout.setSpacing(0)
        central.setLayout(layout)

        self.drawTradeListHeader(layout)

        currentPageRecords = self._records[self.currentPage*PAGE_SIZE:self.currentPage*PAGE_SIZE+PAGE_SIZE]
        
        for row_n, position in enumerate(currentPageRecords, start=2):
            for col_n, field in enumerate(tradelist_fields):
                value = field.value(position) if getattr(field, "value") else str(getattr(position, field.attribute))
                css_class = f"tradelist-field {field.class_} {'even' if not row_n % 2 else ''}"
                widget = field.widget(value)
                widget.setProperty("class", css_class)
                field.modifier(widget) if getattr(field, "modifier") else None
                isinstance(widget, QLabel) and widget.setAlignment(Qt.AlignmentFlag.AlignHCenter)
                layout.addWidget(widget, row_n, col_n)

                if field.attribute == "note":
                    widget.id = position.id
                    widget.installEventFilter(self)

        self.drawPageSelection(layout)

        self.setCentralWidget(central)

    def drawPageSelection(self, layout):
        number_of_pages = math.ceil(len(self._records)/PAGE_SIZE)
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
        with Session(self._engine) as session:
            last_trade = session.scalar(select(Operation).order_by(Operation.time.desc()))
        with Client(self._token) as client:
            synchronize_operations(client, self._engine, self.account, self._token, last_trade and last_trade.time)
        self._records = Position.get_positions(self._engine)
        self.initTradeListUI()


app = QApplication(sys.argv)

window = JournalApp()
window.show()

app.exec()