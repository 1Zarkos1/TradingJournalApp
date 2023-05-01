import sys
import math
import ctypes
import time
import calendar
from functools import partial
from datetime import datetime, date, timedelta
from typing import List, Callable

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
    QPlainTextEdit,
    QCompleter,
    QComboBox,
    QDateTimeEdit,
    QScrollArea
)
from PyQt6.QtCore import Qt, QEvent, QObject
from PyQt6.QtGui import QFont, QMouseEvent, QIcon
from sqlalchemy import select
from sqlalchemy.orm import Session
import pyqtgraph as pg
import numpy as np

from main import (
    get_available_accounts, 
    ACCOUNT_NAME, 
    PAGE_SIZE, 
    Client, 
    synchronize_operations, 
    get_walk_away_analysis_data, 
    get_graph_data
)
from tables import Position, Operation, get_engine, initialize_db
from utils import (
    get_positions_stats, 
    assign_class, 
    tradelist_fields, 
    CandlestickItem, 
    modify_positions_stats,
    get_calendar_performance
)


# make app icon show in taskbar on Windows
myappid = "tInvest"
ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)


class NoteSubWindow(QWidget):

    def __init__(self, parent: 'QWidget', obj: "QObject") -> None:
        super().__init__()
        self._parent = parent
        self.setWindowTitle("AddNote")
        self.position = obj.position
        self.setFont(QFont(["Roboto", "Poppins", "sans-serif"]))
        with open("style.css", "r") as f:
            self.setStyleSheet(f.read())
        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()
        self.setLayout(layout)
        self.setProperty("class", "note-subwindow buttons-section")
        textEdit = QPlainTextEdit(self.position.note)
        textEdit.setProperty("class", "note-edit")
        okBtn = QPushButton("Save")
        cancelBtn = QPushButton("Cancel")
        okBtn.clicked.connect(partial(self._parent.saveNote, textEdit, self.position, self))
        cancelBtn.clicked.connect(self.close)
        layout.addWidget(textEdit)
        layout.addWidget(okBtn)
        layout.addWidget(cancelBtn)
    

class JournalApp(QMainWindow):
    def __init__(self) -> None:
        super().__init__()

        self.currentPage = 0
        self.setFont(QFont(["Roboto", "Poppins", "sans-serif"]))
        self.setWindowIcon(QIcon("static/bar.png"))
        with open("style.css", "r") as f:
            self.setStyleSheet(f.read())
        self.initAccountSelectionUI()

    def setUpAppForSelectedAccount(self, account_name: str, account_properties: dict) -> None:
        self.setWindowTitle(f"TradingJournal - {account_name}")
        self.account = account_name
        self._token = account_properties.get("token")
        self._accountOpenDate = account_properties.get("open_date")
        self._engine = get_engine(account_name)
        initialize_db(self._engine, self._engine.url.database)
        self._records = Position.get_positions(self._engine)
        self.selectedPositions = []
        self.activeFilters = {}
        self.selectedPositions = []
        self.sortingField = ("open_date", 0)
        self.tickersTraded = set([pos.ticker for pos in self._records])
        self.initTradeListUI()
 
    ### UI Draw Methods ###

    def initAccountSelectionUI(self, account_name: str = ACCOUNT_NAME) -> None:
        accounts = get_available_accounts()
        if properties := accounts.get(account_name):
            self.setUpAppForSelectedAccount(account_name, properties)
        else:
            central = QWidget(self)
            layout = QVBoxLayout()
            central.setLayout(layout)
            layout.setAlignment(Qt.AlignmentFlag.AlignTop)
            central.setProperty("class", "note-subwindow buttons-section")
            self.setCentralWidget(central)
            label = QLabel("Select trading account:")
            label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            label.setProperty("class", "header-label")
            layout.addWidget(label)
            for account_name, account_properties in accounts.items():
                selection_btn = QPushButton(account_name)
                selection_btn.clicked.connect(partial(self.setUpAppForSelectedAccount, account_name, account_properties))
                layout.addWidget(selection_btn)

    def initTradeListUI(self) -> None:
        central = QWidget(self)
        self.tradeListLayout = QVBoxLayout()
        self.tradeListLayout.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.tradeListLayout.setSpacing(0)
        central.setProperty("class", "centra-tradelist")
        central.setLayout(self.tradeListLayout)

        self.drawTopMenuButtons(self.tradeListLayout)
        self.drawFilterField()
        self.drawTradeListTable()
        self.drawPageSelection()
        self.drawTotalStats()

        self.setCentralWidget(central)

    def drawTopMenuButtons(self, layout: QVBoxLayout, returnBtn: bool = False, 
                           calendarBtn: bool = True) -> None:
        self.topMenuButtonsWidget = QWidget()
        buttonsLayout = QHBoxLayout()
        buttonsLayout.setAlignment(Qt.AlignmentFlag.AlignHCenter)
        self.topMenuButtonsWidget.setProperty("class", "buttons-section")
        self.topMenuButtonsWidget.setLayout(buttonsLayout)
        accountChange = QPushButton("Change account")
        accountChange.clicked.connect(self.initAccountSelectionUI)
        syncTrades = QPushButton("Sync trades")
        syncTrades.clicked.connect(self.updateTrades)
        buttonsLayout.addWidget(accountChange)
        buttonsLayout.addWidget(syncTrades)
        # switchToChart = QPushButton("Chart")
        # switchToChart.clicked.connect(self.drawGraphPage)
        # buttonsLayout.addWidget(switchToChart)
        if calendarBtn:
            calendarSwitch = QPushButton("Calendar view")
            calendarSwitch.clicked.connect(self.drawCalendarUI)
            buttonsLayout.addWidget(calendarSwitch)
        if returnBtn:
            returnBtn = QPushButton("return")
            returnBtn.clicked.connect(self.initTradeListUI)
            buttonsLayout.addWidget(returnBtn)
        layout.addWidget(self.topMenuButtonsWidget)

    def drawCalendarUI(self, *args, year = date.today().year, month = date.today().month) -> None:
        print(year, month)
        widget = QWidget()
        layout = QVBoxLayout()
        layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        widget.setLayout(layout)
        widget.setProperty("class", "calendar-ui")
        self.setCentralWidget(widget)
        layout.setAlignment(Qt.AlignmentFlag.AlignTop)

        self.drawTopMenuButtons(layout, returnBtn=True, calendarBtn=False)
        perf = get_calendar_performance(self.selectedPositions or self._records, year, month)
        self.drawCalendarTable(layout, perf, year, month)

    def drawCalendarTable(self, mLayout: QVBoxLayout, performance, year, month):
        widget = QWidget()
        layout = QGridLayout()
        widget.setLayout(layout)
        header_labels = list(calendar.day_name)
        header_labels.append("weekly")
        dateSelection = self.constructCalendarSelectionDate(year, month)
        layout.addWidget(dateSelection, 0, 0, 1, 8)
        for col_n, day_name in enumerate(header_labels):
            header_column = QLabel(day_name.upper())
            header_column.setAlignment(Qt.AlignmentFlag.AlignHCenter)
            header_column.setProperty("class", "header-label")
            layout.addWidget(header_column, 1, col_n)
        row_n = 2
        col_n = 0
        for n, (day, values) in enumerate(performance.items()):
            if col_n == 6:
                row_n += 1
            col_n = n - ((row_n - 2) * 7)
            if col_n == 6:
                cell = self.constructCalendarCell("", values)
                layout.addWidget(cell, row_n, 7)
                values = {}
            cell = self.constructCalendarCell(day, values)
            layout.addWidget(cell, row_n, col_n)

        mLayout.addWidget(widget)

    def constructCalendarSelectionDate(self, year, month):
        monthName = list(calendar.month_name)[month]
        header = QLabel(f"{monthName} {year}")
        backBtn = QPushButton(QIcon("static/left-arrow.png"), "")
        backBtn.clicked.connect(lambda x: self.changeCalendarDate(year, month, -1))
        forwardBtn = QPushButton(QIcon("static/right-arrow.png"), "")
        forwardBtn.clicked.connect(lambda x: self.changeCalendarDate(year, month, 1))
        widget = QWidget()
        widget.setProperty("class", "date-selection")
        layout = QHBoxLayout()
        widget.setLayout(layout)
        layout.addWidget(backBtn, alignment=Qt.AlignmentFlag.AlignLeft)
        layout.addWidget(header, alignment=Qt.AlignmentFlag.AlignHCenter)
        layout.addWidget(forwardBtn, alignment=Qt.AlignmentFlag.AlignRight)

        return widget

    def constructCalendarCell(self, day: date, values):
        widget = QWidget()
        widget.setProperty("class", "calendar-cell")
        layout = QVBoxLayout()
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        widget.setLayout(layout)
        layout.addWidget(QLabel(f"{day.day if day else ''}"), alignment=Qt.AlignmentFlag.AlignHCenter)
        if values:
            result = QLabel(f"$ {values['result']}")
            result.setProperty("class", "green" if values["result"] > 0 else "red")
            layout.addWidget(result, alignment=Qt.AlignmentFlag.AlignHCenter)
            layout.addWidget(QLabel(f"{int(values['trades'])} Trades"), alignment=Qt.AlignmentFlag.AlignHCenter)

        return widget

    def drawTradeListTable(self, update: bool = False) -> None:
        if update:
            currentTableWidget = self.tradeListTableWidget
        widget = QWidget()
        widget.setProperty("class", "tl-try")
        layout = QGridLayout()
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)
        widget.setLayout(layout)

        self.tradeListTableWidget = QScrollArea()
        self.tradeListTableWidget.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        # scroll.setContentsMargins(0, 0, 0, 0)
        self.tradeListTableWidget.setWidget(widget)
        self.tradeListTableWidget.setWidgetResizable(True)

        self.drawTradeListTableHeader(layout)
        self.drawTradeListTableBody(layout)

        if update:
            self.tradeListLayout.replaceWidget(currentTableWidget, self.tradeListTableWidget)
            self.tradeListLayout.removeWidget(currentTableWidget)
        else:
            self.tradeListLayout.addWidget(self.tradeListTableWidget)
            # self.tradeListLayout.addWidget(scroll)

    def drawTradeListTableHeader(self, layout: QGridLayout) -> None:
        currentPageRecords = self._records[self.currentPage*PAGE_SIZE:self.currentPage*PAGE_SIZE+PAGE_SIZE]
        header_column = QCheckBox()
        if currentPageRecords and len(set(currentPageRecords).intersection(self.selectedPositions)) == len(currentPageRecords):
            header_column.setChecked(True)
        header_column.stateChanged.connect(self.toggleSelectedPositions)
        header_column.setProperty("class", "cbox-list header-label")
        layout.addWidget(header_column, 0, 0)
        for col_num, field in enumerate(tradelist_fields[1:], start=1):
            header_column = QLabel(field.header_value.upper())
            header_column.setAlignment(Qt.AlignmentFlag.AlignHCenter)
            header_column.setProperty("class", "header-label")
            header_column.installEventFilter(self)
            layout.addWidget(header_column, 0, col_num)

    def drawTradeListTableBody(self, layout: QGridLayout) -> None:
        currentPageRecords = self._records[self.currentPage*PAGE_SIZE:self.currentPage*PAGE_SIZE+PAGE_SIZE]
        
        for row_n, position in enumerate(currentPageRecords, start=1):
            for col_n, field in enumerate(tradelist_fields):
                value = field.value(position) if getattr(field, "value") else str(getattr(position, field.attribute))
                css_class = f"tradelist-field {field.class_} {'even' if not row_n % 2 else 'odd'}"
                widget = field.widget(value)
                widget.setProperty("class", css_class)
                field.modifier(widget) if getattr(field, "modifier") else None
                isinstance(widget, QLabel) and widget.setAlignment(Qt.AlignmentFlag.AlignHCenter|Qt.AlignmentFlag.AlignVCenter)
                layout.addWidget(widget, row_n, col_n)

                if field.attribute == "note":
                    widget.position = position
                    widget.installEventFilter(self)

                if field.attribute == "ticker":
                    widget.clicked.connect(partial(self.drawIndividualPositionUI, position))

                if field.attribute == "chb":
                    if position in self.selectedPositions:
                        widget.setChecked(True)
                    widget.stateChanged.connect(partial(self.selectPositions, position))

    def drawPageSelection(self, update: bool = False) -> None:
        if update:
            currentPageSelection = self.pageSelectionWidget
        number_of_pages = math.ceil(len(self._records)/PAGE_SIZE)
        self.pageSelectionWidget = QWidget()
        self.pageSelectionWidget.setProperty("class", "buttons-section page-btns")
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

    def drawTotalStats(self, update: bool = False) -> None:
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

    def drawFilterField(self, update: bool = False) -> None:
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
 
    def drawIndividualPositionUI(self, position: Position) -> None:
        operations = position.operations
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        widget.setProperty("class", "position-ui")
        self.setCentralWidget(widget)
        layout.setAlignment(Qt.AlignmentFlag.AlignTop)

        self.drawTopMenuButtons(layout, returnBtn=True)

        # draw chart
        self.drawPositionChart(layout, position)
        # draw trade summary info
        self.drawPositionSummary(layout, position)
        # draw executions summary
        self.drawOperationsSummary(layout, operations)
        # draw walk away analysis
        self.drawWalkAwaySection(layout, position, self._engine, self._token)
        # draw notes section
        self.drawNoteSection(layout, position)
        
    def drawPositionChart(self, layout: QVBoxLayout, position: Position) -> None:
        data = get_graph_data(self._engine, self._token, position)
        item = CandlestickItem(data)
        w = pg.PlotWidget()
        w.addItem(item)
        t1= pg.TargetItem()
        from datetime import timezone
        d = position.close_date.replace(tzinfo=timezone.utc).timestamp()
        t2= pg.TargetItem(
            pos=(d, position.closing_price),
            pen="#F4511E",
            label="{1:0.2f}"
        )
        w.addItem(t1)
        w.addItem(t2)
        layout.addWidget(w)

    def drawWalkAwaySection(self, layout: QVBoxLayout, position: Position, engine: "Engine", token: str) -> None:
        response = get_walk_away_analysis_data(engine, token, position)
        data = [{field: values["price"] for field, values in response.items()}]
        table = self.drawTableWidget(data, partial(assign_class, position))
        layout.addWidget(table)

    def drawPositionSummary(self, layout: QVBoxLayout, position: Position) -> None:
        tradeSummarySection = QWidget()
        tsLayout = QGridLayout()
        tsLayout.setSpacing(0)
        tradeSummarySection.setLayout(tsLayout)
        for col_num, field in enumerate(tradelist_fields[1:-1], start=0):
            header_column = QLabel(field.header_value.upper())
            header_column.setAlignment(Qt.AlignmentFlag.AlignHCenter)
            header_column.setProperty("class", "header-label")
            header_column.installEventFilter(self)
            tsLayout.addWidget(header_column, 0, col_num)
        for col_n, field in enumerate(tradelist_fields[1:-1]):
            value = field.value(position) if getattr(field, "value") else str(getattr(position, field.attribute))
            css_class = f"tradelist-field {field.class_}"
            dataValue = field.widget(value)
            dataValue.setProperty("class", css_class)
            field.modifier(dataValue) if getattr(field, "modifier") else None
            isinstance(dataValue, QLabel) and dataValue.setAlignment(Qt.AlignmentFlag.AlignHCenter|Qt.AlignmentFlag.AlignVCenter)
            tsLayout.addWidget(dataValue, 1, col_n)
        layout.addWidget(tradeSummarySection)

    def drawOperationsSummary(self, layout: QVBoxLayout, operations: List[Operation]) -> None:
        data = [operation.to_dict() for operation in operations]
        table = self.drawTableWidget(data)
        layout.addWidget(table)
    
    def drawNoteSection(self, layout: QVBoxLayout, position: Position, add_update: bool = False) -> None:
        noteSection = QWidget()
        noteSection.setProperty("class", "buttons-section note-section")
        nLayout = QVBoxLayout()
        noteSection.setLayout(nLayout)
        noteHeader = QLabel("notes".upper())
        noteHeader.setAlignment(Qt.AlignmentFlag.AlignHCenter)
        noteHeader.setProperty("class", "header-label")
        noteValue = QPlainTextEdit(position.note) if add_update else QLabel(position.note)
        noteValue.setProperty("class", "note-edit")
        nLayout.addWidget(noteHeader)
        nLayout.addWidget(noteValue)
        btnText = "Save note" if add_update else ("Add note" if not position.note else "Update note")
        addButton = QPushButton(btnText)
        addButton.clicked.connect(partial(self.processNote, position, noteValue, noteSection))
        delButton = QPushButton("Delete note")
        delButton.clicked.connect(partial(self.processNote, position, QPlainTextEdit(), noteSection))
        nLayout.addWidget(addButton)
        nLayout.addWidget(delButton)
        layout.addWidget(noteSection)
    
    def drawTotalStatsPage(self) -> None:
        self.statsPageWidget = QWidget()
        self.statsPageLayout = QVBoxLayout()
        self.statsPageLayout.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.statsPageWidget.setLayout(self.statsPageLayout)
        self.statsPageLayout.setSpacing(8)
        positions = self.selectedPositions or self._records
        stats = get_positions_stats(positions)

        self.drawTopMenuButtons(self.statsPageLayout, returnBtn=True)

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
    
    def drawTableWidget(self, values: List[dict], widget_modifier: Callable = lambda w: w) -> QWidget:
        table = QWidget()
        layout = QGridLayout()
        layout.setSpacing(0)
        table.setLayout(layout)
        for col_num, field in enumerate(values[0]):
            header_column = QLabel(field.upper())
            header_column.setAlignment(Qt.AlignmentFlag.AlignHCenter)
            header_column.setProperty("class", "header-label")
            layout.addWidget(header_column, 0, col_num)
        for row_n, data in enumerate(values, start=1):
            for col_n, value in enumerate(data.values()):
                widget = QLabel(str(value))
                css_class = f"tradelist-field"
                widget.setProperty("class", css_class)
                widget = widget_modifier(widget)
                widget.setAlignment(Qt.AlignmentFlag.AlignHCenter)
                layout.addWidget(widget, row_n, col_n)
        
        return table
    
    def drawGraphPage(self) -> None:
        df = modify_positions_stats(self._records)
        widget = QWidget()
        layout = QVBoxLayout()
        widget.setLayout(layout)
        self.setCentralWidget(widget)

        pg.setConfigOptions(antialias=True)
        w = pg.GraphicsLayoutWidget(show=True)
        pItem = pg.PlotWidget()
        w.addItem(pItem)
        axis = pg.DateAxisItem("bottom")
        pItem.setAxisItems({"bottom": axis})
        # d = df.groupby([pd.Grouper(key='close_date', freq='W')])["close_date",'result'].sum()
        # d = d.reset_index()
        pItem.plot(df["close_date"].astype('int64') // 10**9, df["result"].cumsum())
        layout.addWidget(w)


    def _update_canvas(self):
        t = np.linspace(0, 10, 101)
        # Shift the sinusoid as a function of time.
        self._line.set_data(t, np.sin(t + time.time()))
        self._line.figure.canvas.draw()


    def drawNoteSubWindow(self, obj: QLabel) -> None:
        self.subwindow = NoteSubWindow(parent=self, obj=obj)
        self.subwindow.show()

    ### Slots ###

    def toggleSelectedPositions(self, state: int) -> None:
        try:
            tableLayout = self.tradeListTableWidget.layout()
            for i in range(1, 1+PAGE_SIZE):
                chb = tableLayout.itemAtPosition(i, 0).widget()
                chb.setChecked(state)
        except AttributeError as e:
            ...
 
    def updateUIForRecords(self) -> None:
        self.drawTradeListTable(update=True)
        self.drawPageSelection(update=True)
        self.drawTotalStats(update=True)
    
    def selectPositions(self, position: Position, state: int) -> None:
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

    def saveNote(self, note: QPlainTextEdit, position: Position, subwindow: QWidget) -> None:
        position.note = note.toPlainText()
        subwindow.close()
        with Session(self._engine) as session:
            session.add(position)
            session.commit()
            session.refresh(position)
        self.drawTradeListTable(update=True)

    def sortResults(self, label_obj: QLabel) -> None:
        column_name = label_obj.text().lower()
        sort_field = [obj.attribute for obj in tradelist_fields if obj.header_value == column_name][0]
        sort_order = int(not self.sortingField[1]) if column_name == self.sortingField[0] else 0
        self.sortingField = (column_name, sort_order)
        self._records = Position.get_positions(self._engine, filters=self.activeFilters, sorting_field=sort_field, sorting_order=sort_order)
        self.updateUIForRecords()

    def changePage(self, page: int) -> None:
        self.currentPage = page - 1
        self.updateUIForRecords()

    def filterPositions(self, filter_field: str, filter_value: str) -> None:
        self.activeFilters[filter_field] = filter_value
        self._records = Position.get_positions(self._engine, filters=self.activeFilters)
        self.updateUIForRecords()

    def updateTrades(self) -> None:
        with Session(self._engine) as session:
            last_trade = session.scalar(select(Operation).order_by(Operation.time.desc()))
        with Client(self._token) as client:
            synchronize_operations(client, self._engine, self.account, self._token, last_trade and last_trade.time)
        self._records = Position.get_positions(self._engine)
        self.updateUIForRecords()

    def resetFilters(self) -> None:
        self.activeFilters = {}
        self._records = Position.get_positions(self._engine)
        self.initTradeListUI()
    
    def processNote(self, position: Position, noteWidget: QPlainTextEdit, noteSection: QWidget) -> None:
        layout = self.centralWidget().layout()
        layout.removeWidget(noteSection)
        noteSection.setParent(None)
        if isinstance(noteWidget, QLabel):
            self.drawNoteSection(layout, position, add_update=True)
        else:
            position.note = noteWidget.toPlainText()
            with Session(self._engine) as session:
                session.add(position)
                session.commit()
                session.refresh(position)
            self.drawNoteSection(layout, position, add_update=False)

    def changeCalendarDate(self, year, month, value):
        newDate = date(year, month, 15) + timedelta(weeks=4*value)
        self.drawCalendarUI(year=newDate.year, month=newDate.month)


app = QApplication(sys.argv)

window = JournalApp()
window.show()

sys.exit(app.exec())