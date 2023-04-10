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
    QPlainTextEdit,
    QCompleter,
    QComboBox,
    QDateTimeEdit
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
        self.selectedPositions = []
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
        self.wordList = set([pos.ticker for pos in self._records])
        self.initTradeListUI()
 
    def initTradeListUI(self):
        central = QWidget(self)
        self.tradeListLayout = QGridLayout()
        self.tradeListLayout.setSpacing(0)
        central.setLayout(self.tradeListLayout)

        self.drawTradeListHeader(self.tradeListLayout)

        self.drawTradeListBody(self.tradeListLayout)

        self.drawPageSelection(self.tradeListLayout)

        self.drawTotalStats(self.tradeListLayout)

        self.drawFilterField()

        self.setCentralWidget(central)

    ### UI Draw Methods ###

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
        header_column = QCheckBox()
        header_column.stateChanged.connect(self.toggleSelectedPositions)
        layout.addWidget(header_column, 1, 0)
        for col_num, field in enumerate(tradelist_fields[1:], start=1):
            header_column = QLabel(field.header_value.upper())
            header_column.setAlignment(Qt.AlignmentFlag.AlignHCenter)
            header_column.setProperty("class", "header-label")
            header_column.installEventFilter(self)
            layout.addWidget(header_column, 1, col_num)

    def drawTradeListBody(self, layout):
        currentPageRecords = self._records[self.currentPage*PAGE_SIZE:self.currentPage*PAGE_SIZE+PAGE_SIZE]
        
        for row_n, position in enumerate(currentPageRecords, start=2):
            for col_n, field in enumerate(tradelist_fields):
                value = field.value(position) if getattr(field, "value") else str(getattr(position, field.attribute))
                css_class = f"tradelist-field {field.class_} {'even' if not row_n % 2 else 'odd'}"
                widget = field.widget(value)
                widget.setProperty("class", css_class)
                field.modifier(widget) if getattr(field, "modifier") else None
                isinstance(widget, QLabel) and widget.setAlignment(Qt.AlignmentFlag.AlignHCenter)
                layout.addWidget(widget, row_n, col_n)

                if field.attribute == "note":
                    widget.id = position.id
                    widget.installEventFilter(self)

                if field.attribute == "ticker":
                    widget.clicked.connect(partial(self.drawOperationListUI, position))

                if field.attribute == "chb":
                    if position in self.selectedPositions:
                        widget.setChecked(True)
                    widget.stateChanged.connect(partial(self.selectPositions, position))

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

    def drawTotalStats(self, layout, delete=False):
        if delete:
            self.tradeListLayout.removeWidget(self.total_widget)
        positions = self.selectedPositions or self._records
        self.total_widget = QWidget()
        self.total_widget.setProperty("class", "total")
        total_layout = QHBoxLayout()
        total_trades = len(positions)
        succesful_trades = sum([1 for trade in positions if trade.closed and trade.result > 0])
        self.total_widget.setLayout(total_layout)
        total_layout.addWidget(QLabel(f"total: {total_trades} trades (w: {succesful_trades} / l: {total_trades-succesful_trades})"))
        total_layout.addWidget(QLabel(f"successful trades: {round(succesful_trades/total_trades*100, 2)} %"))
        total_layout.addWidget(QLabel(f"R {round(sum([trade.result for trade in positions if trade.closed]), 2)} (return rub)"))
        layout.addWidget(self.total_widget, PAGE_SIZE+3, 0, 1, len(tradelist_fields), alignment=Qt.AlignmentFlag.AlignJustify)
    
    def drawOperationListUI(self, position):
        operations = position.operations
        widget = QWidget()
        layout = QGridLayout()
        self.setCentralWidget(widget)
        self.centralWidget().setLayout(layout)
        btn = QPushButton("return")
        btn.clicked.connect(self.initTradeListUI)
        layout.addWidget(btn, 0, 0)
        for n, operation in enumerate(operations, start=1):
            layout.addWidget(QLabel(str(operation)), n, 0)

    def drawFilterField(self):

        filter_widget = QWidget()
        filter_widget.setProperty("class", "filter-container")
        filter_layout = QHBoxLayout()
        filter_widget.setLayout(filter_layout)

        filter_line = QLineEdit()
        filter_line.setPlaceholderText("Symbol")
        completer = QCompleter(self.wordList)
        completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        filter_line.setCompleter(completer)
        filter_line.returnPressed.connect(lambda filter_line=filter_line: self.filterPositions("ticker", filter_line.text()))
        completer.activated.connect(lambda filter_line: self.filterPositions("ticker", filter_line))
        filter_layout.addWidget(filter_line)

        side = QComboBox()
        side.addItems(["all", "short", "long"])
        side.currentTextChanged.connect(lambda filter_value: self.filterPositions("side", filter_value))
        filter_layout.addWidget(side)

        status = QComboBox()
        status.addItems(["all", "win", "loss"])
        status.currentTextChanged.connect(lambda filter_value: self.filterPositions("status", filter_value))
        filter_layout.addWidget(status)

        from_date = QDateTimeEdit()
        from_date.dateTimeChanged.connect(lambda qdate: self.filterPositions("from_date", qdate.toPyDateTime()))
        from_date.setCalendarPopup(True)
        filter_layout.addWidget(from_date)

        to_date = QDateTimeEdit()
        to_date.dateTimeChanged.connect(lambda qdate: self.filterPositions("to_date", qdate.toPyDateTime()))
        to_date.setCalendarPopup(True)
        to_date.setDateTime(datetime.now())
        filter_layout.addWidget(to_date)

        clear_button = QPushButton("clear filters")
        clear_button.clicked.connect(self.resetFilters)
        filter_layout.addWidget(clear_button)

        self.tradeListLayout.addWidget(filter_widget, PAGE_SIZE+4, 0, 1, len(tradelist_fields), alignment=Qt.AlignmentFlag.AlignHCenter)

    def drawNoteSubWindow(self, obj):
        self.subwindow = NoteSubWindow(parent=self, obj=obj)
        self.subwindow.show()

    ### Slots ###

    def toggleSelectedPositions(self, state):
        try:
            for i in range(2, 2+PAGE_SIZE):
                chb = self.tradeListLayout.itemAtPosition(i, 0).widget()
                chb.setChecked(state)
        except AttributeError as e:
            ...

    def selectPositions(self, position, state):
        if state:
            self.selectedPositions.append(position)
        else:
            self.selectedPositions.remove(position)
        self.drawTotalStats(self.tradeListLayout, delete=True)

    def eventFilter(self, a0: 'QObject', a1: 'QEvent') -> bool:
        if a1.type() == QMouseEvent.Type.MouseButtonPress and a1.button() == Qt.MouseButton.LeftButton:
            if "note" in a0.property("class"):
                self.drawNoteSubWindow(a0)
            else:
                self.sortResults(a0)
        return super().eventFilter(a0, a1)

    def changeNote(self, widget):
        print("note")

    def saveNote(self, note, posId):
        with Session(self._engine) as session:
            exp = update(Position).where(Position.id == posId).values(note=note.toPlainText())
            session.execute(exp)
            session.commit()
        self.drawTradeListBody(self.tradeListLayout)

    def sortResults(self, label_obj):
        sort_field = [obj.attribute for obj in tradelist_fields if obj.header_value == label_obj.text().lower()][0]
        sort_order = getattr(label_obj, "sort_order", None)
        label_obj.sort_order = 0 if sort_order is None or sort_order == 1 else 1
        self._records = Position.get_positions(self._engine, sorting_field=sort_field, sorting_order=label_obj.sort_order)
        self.drawTradeListBody(self.tradeListLayout)

    def changePage(self, page):
        self.currentPage = page - 1
        self.initTradeListUI()

    def filterPositions(self, filter_field, filter_value):
        self._records = Position.get_positions(self._engine, filter_field, filter_value)
        self.drawTradeListBody(self.tradeListLayout)
        self.drawPageSelection(self.tradeListLayout)
        self.drawTotalStats(self.tradeListLayout)

    def updateTrades(self):
        with Session(self._engine) as session:
            last_trade = session.scalar(select(Operation).order_by(Operation.time.desc()))
        with Client(self._token) as client:
            synchronize_operations(client, self._engine, self.account, self._token, last_trade and last_trade.time)
        self._records = Position.get_positions(self._engine)
        self.initTradeListUI()

    def resetFilters(self):
        self._records = Position.get_positions(self._engine)
        self.initTradeListUI()

class Test(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.initUI()

    def initUI(self):
        central = QLineEdit()
        central.returnPressed.connect(lambda central=central: print(central.text()))
        wordList = ["alpha", "omega", "omicron", "zeta"]
        completer = QCompleter(wordList)
        completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        central.setCompleter(completer)
        completer.activated.connect(lambda *args: print(args))
        self.setCentralWidget(central)   


app = QApplication(sys.argv)

window = JournalApp()
window.show()

# window = Test()
# window.show()

app.exec()