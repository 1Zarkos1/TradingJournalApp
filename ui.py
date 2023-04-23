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
import pyqtgraph as pg

from main import get_available_accounts, API_TOKEN, ACCOUNT_NAME, PAGE_SIZE, Client, synchronize_operations
from tables import Asset, Position, Operation, AdditionalPayment, get_engine, initialize_db
from utils import get_positions_stats, get_positions_stats

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
        self._accountOpenDate = account_properties.get("open_date")
        self._engine = get_engine(account_name)
        initialize_db(self._engine, self._engine.url.database)
        self._records = Position.get_positions(self._engine)
        self.selectedPositions = []
        self.activeFilters = {}
        self.tickersTraded = set([pos.ticker for pos in self._records])
        self.initTradeListUI()
 
    ### UI Draw Methods ###

    def initTradeListUI(self):
        central = QWidget(self)
        self.tradeListLayout = QVBoxLayout()
        central.setLayout(self.tradeListLayout)

        self.drawTopMenuButtons()
        self.drawFilterField()
        self.drawTradeListTable()
        self.drawPageSelection()
        self.drawTotalStats()

        self.setCentralWidget(central)

    def drawTopMenuButtons(self) -> None:
        self.topMenuButtonsWidget = QWidget()
        buttonsLayout = QHBoxLayout()
        self.topMenuButtonsWidget.setLayout(buttonsLayout)
        accountChange = QPushButton("Change account")
        accountChange.clicked.connect(self.initAccountSelectionUI)
        syncTrades = QPushButton("Sync trades")
        syncTrades.clicked.connect(self.updateTrades)
        buttonsLayout.addWidget(accountChange)
        buttonsLayout.addWidget(syncTrades)
        self.tradeListLayout.addWidget(self.topMenuButtonsWidget)

    def drawTradeListTable(self, update=False):
        if update:
            currentTableWidget = self.tradeListTableWidget
        self.tradeListTableWidget = QWidget()
        layout = QGridLayout()
        layout.setSpacing(0)
        self.tradeListTableWidget.setLayout(layout)

        self.drawTradeListTableHeader(layout)
        self.drawTradeListTableBody(layout)

        if update:
            self.tradeListLayout.replaceWidget(currentTableWidget, self.tradeListTableWidget)
            self.tradeListLayout.removeWidget(currentTableWidget)
        else:
            self.tradeListLayout.addWidget(self.tradeListTableWidget)

    def drawTradeListTableHeader(self, layout) -> None:
        header_column = QCheckBox()
        currentPageRecords = self._records[self.currentPage*PAGE_SIZE:self.currentPage*PAGE_SIZE+PAGE_SIZE]
        if currentPageRecords and len(set(currentPageRecords).intersection(self.selectedPositions)) == len(currentPageRecords):
            header_column.setChecked(True)
        header_column.stateChanged.connect(self.toggleSelectedPositions)
        layout.addWidget(header_column, 0, 0)
        for col_num, field in enumerate(tradelist_fields[1:], start=1):
            header_column = QLabel(field.header_value.upper())
            header_column.setAlignment(Qt.AlignmentFlag.AlignHCenter)
            header_column.setProperty("class", "header-label")
            header_column.installEventFilter(self)
            layout.addWidget(header_column, 0, col_num)

    def drawTradeListTableBody(self, layout):
        currentPageRecords = self._records[self.currentPage*PAGE_SIZE:self.currentPage*PAGE_SIZE+PAGE_SIZE]
        
        for row_n, position in enumerate(currentPageRecords, start=1):
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

    def drawPageSelection(self, update=False):
        if update:
            currentPageSelection = self.pageSelectionWidget
        number_of_pages = math.ceil(len(self._records)/PAGE_SIZE)
        self.pageSelectionWidget = QWidget()
        layout = QHBoxLayout()
        self.pageSelectionWidget.setLayout(layout)
        for page in range(1, number_of_pages+1):
            button = QPushButton(str(page), self.pageSelectionWidget)
            if page-1 == self.currentPage:
                button.setProperty("class", "current-page")
            button.clicked.connect(partial(self.changePage, page))
            layout.addWidget(button)
        if update:
            self.tradeListLayout.replaceWidget(currentPageSelection, self.pageSelectionWidget)
            self.tradeListLayout.removeWidget(currentPageSelection)
        else:
            self.tradeListLayout.addWidget(self.pageSelectionWidget, alignment=Qt.AlignmentFlag.AlignRight)

    def drawTotalStats(self, update=False):
        if update:
            currentStats = self.totalStatsWidget
        positions = self.selectedPositions or self._records
        self.totalStatsWidget = QWidget()
        self.totalStatsWidget.setProperty("class", "total")
        self.totalStatsWidget.installEventFilter(self)
        layout = QHBoxLayout()
        self.totalStatsWidget.setLayout(layout)
        total_trades = len(positions)
        succesful_trades = sum([1 for trade in positions if trade.closed and trade.result > 0])
        success_percent = round(succesful_trades/total_trades*100, 2) if total_trades else 0
        layout.addWidget(QLabel(f"total: {total_trades} trades (w: {succesful_trades} / l: {total_trades-succesful_trades})"))
        layout.addWidget(QLabel(f"successful trades: {success_percent} %"))
        layout.addWidget(QLabel(f"R {round(sum([trade.result for trade in positions if trade.closed]), 2)} (return rub)"))
        if update:
            self.tradeListLayout.replaceWidget(currentStats, self.totalStatsWidget)
            self.tradeListLayout.removeWidget(currentStats)
        else:
            self.tradeListLayout.addWidget(self.totalStatsWidget, alignment=Qt.AlignmentFlag.AlignJustify)

    def drawFilterField(self, update=False):
        if update:
            currentFilter = self.filterWidget

        self.filterWidget = QWidget()
        self.filterWidget.setProperty("class", "filter-container")
        layout = QHBoxLayout()
        self.filterWidget.setLayout(layout)

        filter_line = QLineEdit()
        filter_line.setPlaceholderText("Symbol")
        completer = QCompleter(self.tickersTraded)
        completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        filter_line.setCompleter(completer)
        filter_line.returnPressed.connect(lambda filter_line=filter_line: self.filterPositions("ticker", filter_line.text()))
        completer.activated.connect(lambda filter_line: self.filterPositions("ticker", filter_line))
        layout.addWidget(filter_line)

        side = QComboBox()
        side.addItems(["all", "short", "long"])
        side.currentTextChanged.connect(lambda filter_value: self.filterPositions("side", filter_value))
        layout.addWidget(side)

        status = QComboBox()
        status.addItems(["all", "win", "loss"])
        status.currentTextChanged.connect(lambda filter_value: self.filterPositions("status", filter_value))
        layout.addWidget(status)

        from_date = QDateTimeEdit()
        from_date.setDateTime(self._accountOpenDate)
        from_date.dateTimeChanged.connect(lambda qdate: self.filterPositions("from_date", qdate.toPyDateTime()))
        from_date.setCalendarPopup(True)
        layout.addWidget(from_date)

        to_date = QDateTimeEdit()
        to_date.setDateTime(datetime.now())
        to_date.dateTimeChanged.connect(lambda qdate: self.filterPositions("to_date", qdate.toPyDateTime()))
        to_date.setCalendarPopup(True)
        layout.addWidget(to_date)

        clear_button = QPushButton("clear filters")
        clear_button.clicked.connect(self.resetFilters)
        layout.addWidget(clear_button)

        if update:
            self.tradeListLayout.replaceWidget(currentFilter, self.filterWidget)
        else:
            self.tradeListLayout.addWidget(self.filterWidget, alignment=Qt.AlignmentFlag.AlignHCenter)
 
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
    
    def updateUIForRecords(self):
        self.drawTradeListTable(update=True)
        self.drawPageSelection(update=True)
        self.drawTotalStats(update=True)
    
    def drawTotalStatsPage(self):
        self.statsPageWidget = QWidget()
        self.statsPageLayout = QVBoxLayout()
        self.statsPageWidget.setLayout(self.statsPageLayout)
        self.statsPageLayout.setSpacing(8)
        positions = self.selectedPositions or self._records
        stats = get_positions_stats(positions)

        btn = QPushButton("Return")
        btn.clicked.connect(self.initTradeListUI)
        self.statsPageLayout.addWidget(btn)

        for section, data in stats.items():
            section_widget = QWidget()
            section_widget.setProperty("class", "section")
            self.statsPageLayout.addWidget(section_widget)
            section_layout = QGridLayout()
            section_layout.setSpacing(0)
            section_widget.setLayout(section_layout)
            section_header = QLabel(f"Side: {section[0]} - Result: {section[1]}")
            section_header.setProperty("class", "stats-section-header")
            section_header.setAlignment(Qt.AlignmentFlag.AlignHCenter)
            section_layout.addWidget(section_header, 0, 0, 1, 2)
            for n, items in enumerate(data.items(), start=2):
                label, value = items
                section_data = QLabel(f"{label.replace('_', ' ').capitalize()}: {str(value)}")
                section_data.setProperty("class", "stats-section-data")
                section_layout.addWidget(section_data, n//2, n%2)

        self.setCentralWidget(self.statsPageWidget)
    
    def drawNoteSubWindow(self, obj):
        self.subwindow = NoteSubWindow(parent=self, obj=obj)
        self.subwindow.show()

    ### Slots ###

    def toggleSelectedPositions(self, state):
        try:
            tableLayout = self.tradeListTableWidget.layout()
            for i in range(1, 1+PAGE_SIZE):
                chb = tableLayout.itemAtPosition(i, 0).widget()
                chb.setChecked(state)
        except AttributeError as e:
            ...

    def selectPositions(self, position, state):
        if state:
            self.selectedPositions.append(position)
        else:
            self.selectedPositions.remove(position)
        self.drawTotalStats(update=True)

    def eventFilter(self, a0: 'QObject', a1: 'QEvent') -> bool:
        if a1.type() == QMouseEvent.Type.MouseButtonPress and a1.button() == Qt.MouseButton.LeftButton:
            if "note" in a0.property("class"):
                self.drawNoteSubWindow(a0)
            elif "total" in a0.property("class"):
                self.drawTotalStatsPage()
            else:
                w = self.tradeListTableWidget # keep reference to the old table before redrawing otherwise super() raises C++ error
                self.sortResults(a0)
        return super().eventFilter(a0, a1)

    def saveNote(self, note, posId):
        with Session(self._engine) as session:
            exp = update(Position).where(Position.id == posId).values(note=note.toPlainText())
            session.execute(exp)
            session.commit()
        self.drawTradeListTable(update=True)

    def sortResults(self, label_obj):
        sort_field = [obj.attribute for obj in tradelist_fields if obj.header_value == label_obj.text().lower()][0]
        sort_order = getattr(label_obj, "sort_order", None)
        label_obj.sort_order = 0 if sort_order is None or sort_order == 1 else 1
        self._records = Position.get_positions(self._engine, filters=self.activeFilters, sorting_field=sort_field, sorting_order=label_obj.sort_order)
        self.updateUIForRecords()

    def changePage(self, page):
        self.currentPage = page - 1
        self.updateUIForRecords()

    def filterPositions(self, filter_field, filter_value):
        self.activeFilters[filter_field] = filter_value
        self._records = Position.get_positions(self._engine, filters=self.activeFilters)
        self.updateUIForRecords()

    def updateTrades(self):
        with Session(self._engine) as session:
            last_trade = session.scalar(select(Operation).order_by(Operation.time.desc()))
        with Client(self._token) as client:
            synchronize_operations(client, self._engine, self.account, self._token, last_trade and last_trade.time)
        self._records = Position.get_positions(self._engine)
        self.updateUIForRecords()

    def resetFilters(self):
        self.activeFilters = {}
        self._records = Position.get_positions(self._engine)
        self.initTradeListUI()

class Test(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.initUI()

    def initUI(self):
        central = QLineEdit()
        central.returnPressed.connect(lambda central=central: print(central.text()))
        tickersTraded = ["alpha", "omega", "omicron", "zeta"]
        completer = QCompleter(tickersTraded)
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