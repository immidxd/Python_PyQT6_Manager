#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import logging
import asyncio
import subprocess
import re
import traceback  # Додаємо імпорт traceback для логування помилок

# Ініціалізуємо логер
logger = logging.getLogger(__name__)

from PyQt6 import QtCore
from PyQt6.QtWidgets import (
 QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QCheckBox, QLabel,
 QSpinBox, QPushButton, QTableWidget, QSizePolicy, QAbstractScrollArea,
 QMessageBox, QHeaderView, QTableWidgetItem, QAbstractItemView,
 QListWidget, QListWidgetItem, QGraphicsOpacityEffect, QGroupBox,
 QGraphicsDropShadowEffect, QComboBox, QScrollArea, QCalendarWidget,
 QDialog, QToolButton, QMenu, QApplication, QProgressBar
)
from PyQt6.QtGui import QFont, QPixmap, QColor, QCursor, QIcon, QMouseEvent, QAction
from PyQt6.QtCore import (
 Qt, QTimer, QEvent, QEasingCurve, QPoint, pyqtSignal, QPropertyAnimation, QDate,
 QThread, QTime, QDateTime, QSize, QRect, QMargins
)

import qtawesome as qta

from db import session
from db import Session as session_factory  # Додаємо імпорт session_factory з db
from models import (
 Product, Type, Subtype, Brand, Gender, Color, Country, Status, Condition, Import,
 Order, OrderStatus, PaymentStatus, DeliveryMethod, Client, PaymentMethod, DeliveryStatus,
 Address, OrderDetails as OrderDetail
)
from widgets import (
 RangeSlider, CollapsibleWidget, CollapsibleSection, FilterSection, FocusableSearchLineEdit
)
from services.theme_service import (
 apply_theme, update_text_colors
)
from services.filter_service import (
 remember_query, get_suggestions, get_order_statuses, get_payment_statuses_db,
 get_delivery_methods_db, build_orders_query_params, update_filter_counts
)
from workers import OrderParsingWorker

from sqlalchemy.orm import joinedload
from sqlalchemy import or_, desc, func, Float, cast, String, distinct
from datetime import datetime, timedelta

# У секції імпортів додаємо:
from .scripts import parsing_api
import threading
import time
import types

# Перенесені функції для фільтрації на рівень модуля
def fix_unpaid_filter(q, session):
    """
    Фільтр «Тільки неоплачені» (payment_status_id=1 => «оплачено» => виключаємо).
    """
    q = q.filter(Order.payment_status_id != 1)
    return q

def fix_paid_filter(q, session):
    """Функція для застосування фільтру 'Тільки оплачені'"""
    # Фільтруємо замовлення, статус оплати яких 'оплачено' (id=1)
    q = q.join(PaymentStatus, Order.payment_status_id == PaymentStatus.id)
    q = q.filter(Order.payment_status_id == 1)
    return q

class OrdersTab(QWidget):
 """
 Вкладка "Замовлення":
   - «Тільки неоплачені» ліворуч
   - Слайдери (місяці, рік) праворуч, з QScrollArea
   - При великому вікні не "розпливаються",
     а при малому — можна прокрутити.
   - Без зайвих контурів groupbox та scrollarea
   - Кнопки мають фіксовану максимальну ширину, щоб не розтягувались.
 """
 toggle_animation_finished = pyqtSignal()

 def __init__(self, parent=None):
     super(OrdersTab, self).__init__(parent)
     self.parent = parent
     self.parent_window = parent  # Додаємо parent_window для стандартизації з іншими методами
     self.session = session  # Використовуємо імпортовану змінну session замість функції create_session

     self.is_dark_theme = False
     self.data_loaded = False

     self.page_size = 50
     self.current_page = 1
     self.all_orders = []
     self.total_pages = 1
     
     # Змінна для відстеження підсвіченого рядка
     self.highlighted_row = None

     self.logo_label = None
     self.orders_theme_button = None
     
     # Змінна для збереження вибраної дати фільтрації
     self.selected_filter_date = None

     # Чекбокс "Тільки неоплачені"
     self.unpaid_checkbox = QCheckBox("Тільки неоплачені")
     self.unpaid_checkbox.setFont(QFont("Arial", 13))
     self.unpaid_checkbox.setChecked(False)
     self.unpaid_checkbox.stateChanged.connect(self.on_checkbox_state_changed)

     # Чекбокс "Тільки оплачені"
     self.paid_checkbox = QCheckBox("Тільки оплачені")
     self.paid_checkbox.setFont(QFont("Arial", 13))
     self.paid_checkbox.setChecked(False)
     self.paid_checkbox.stateChanged.connect(self.on_checkbox_state_changed)

     self.search_timer = QTimer()
     self.search_timer.setSingleShot(True)
     self.search_timer.timeout.connect(lambda: asyncio.ensure_future(self.apply_orders_filters()))

     self.completer_timer = QTimer()
     self.completer_timer.setSingleShot(True)
     self.completer_timer.setInterval(500)
     self.completer_timer.timeout.connect(self.update_completer)
     self.orders_popup_fade_animation = None
     self.current_suggestion_index = -1

     # Додаємо змінну для відслідковування поточного завдання пошуку
     self.current_filter_task = None

     self.setup_ui()
     self.setMouseTracking(True)

     # Встановлюємо асинхронний парсинг в кінці ініціалізації
     self._install_async_parsing()

 def _install_async_parsing(self):
     """Встановлює асинхронний парсинг, зберігаючи оригінальні функції"""
     try:
         # Перевіряємо наявність методів перед їх заміною
         if hasattr(self, "refresh_orders_table"):
             self._original_refresh_orders_table = self.refresh_orders_table
         
         # Створюємо методи, якщо вони не існують
         if not hasattr(self, "refresh_orders_table"):
             def refresh_orders_table_placeholder(self):
                 logging.warning("Викликаємо заглушку refresh_orders_table")
                 pass
             self._original_refresh_orders_table = refresh_orders_table_placeholder
         
         # Замінюємо функції
         # Зберігаємо оригінальну функцію парсингу, якщо вона існує
         if hasattr(self, "parse_google_sheets"):
             self._original_parse_google_sheets = self.parse_google_sheets
         
         self.parse_google_sheets = types.MethodType(parse_google_sheets, self)
         self.refresh_orders_table = types.MethodType(refresh_orders_table, self)
         self.start_ui_update_thread = types.MethodType(start_ui_update_thread, self)
         
         logging.info("Встановлено асинхронний парсинг Google Sheets")
     except Exception as e:
         logging.error(f"Помилка при встановленні асинхронного парсингу: {e}")
         logging.error(traceback.format_exc())

 def mousePressEvent(self, event):
     if self.orders_search_bar and self.orders_search_bar.hasFocus():
         self.orders_search_bar.clearFocus()
     super().mousePressEvent(event)

 def setup_ui(self):
     main_layout = QVBoxLayout(self)
     main_layout.setContentsMargins(10, 10, 10, 10)
     main_layout.setSpacing(0)

     top_widget = QWidget()
     top_layout = QHBoxLayout(top_widget)
     top_layout.setContentsMargins(0, 0, 0, 10)
     top_layout.setSpacing(5)

     self.logo_label = QLabel()
     logo_pixmap = QPixmap("style/images/icons/logo.png")
     logo_pixmap = logo_pixmap.scaled(
         50, 50,
         Qt.AspectRatioMode.KeepAspectRatio,
         Qt.TransformationMode.SmoothTransformation
     )
     self.logo_label.setPixmap(logo_pixmap)
     self.logo_label.setFixedSize(60, 60)

     search_layout = QHBoxLayout()
     search_layout.setSpacing(5)
     self.orders_search_bar = FocusableSearchLineEdit()
     self.orders_search_bar.setPlaceholderText("Пошук замовлення...")
     self.orders_search_bar.setClearButtonEnabled(True)
     self.orders_search_bar.setFont(QFont("Arial", 13))
     search_icon = qta.icon('fa5s.search', color='#888888')
     self.orders_search_bar.addAction(search_icon, QLineEdit.ActionPosition.LeadingPosition)

     # Додаємо обробник натискання клавіші Enter для запуску пошуку
     self.orders_search_bar.returnPressed.connect(self.on_search_enter_pressed)

     # Підказки
     self.orders_completer_list = QListWidget()
     self.orders_completer_list.setFocusPolicy(Qt.FocusPolicy.NoFocus)
     self.orders_completer_list.setMouseTracking(True)
     self.orders_completer_list.setWindowFlags(
         QtCore.Qt.WindowType.FramelessWindowHint
         | QtCore.Qt.WindowType.Tool
         | QtCore.Qt.WindowType.NoDropShadowWindowHint
         | QtCore.Qt.WindowType.WindowStaysOnTopHint
     )
     self.orders_completer_list.setAttribute(QtCore.Qt.WidgetAttribute.WA_TranslucentBackground, True)
     self.orders_completer_list.setMaximumHeight(250)
     self.orders_popup_opacity_effect = QGraphicsOpacityEffect(self.orders_completer_list)
     self.orders_completer_list.setGraphicsEffect(self.orders_popup_opacity_effect)
     self.orders_popup_fade_animation = QPropertyAnimation(self.orders_popup_opacity_effect, b"opacity")
     self.orders_popup_fade_animation.setDuration(250)
     self.orders_popup_fade_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)

     self.orders_search_bar.textChanged.connect(self.debounced_completer_update)
     self.orders_completer_list.itemClicked.connect(self.insert_orders_completion)
     self.orders_search_bar.installEventFilter(self)
     self.orders_completer_list.installEventFilter(self)

     search_layout.addWidget(self.orders_search_bar)

     # Кнопка теми
     self.orders_theme_button = QPushButton()
     self.orders_theme_button.setObjectName("themeToggleButtonOrders")
     self.orders_theme_button.setFixedSize(35, 35)
     self.orders_theme_button.setFont(QFont("Arial", 13))
     self.orders_theme_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
     self.orders_theme_button.clicked.connect(self.on_theme_button_clicked)
     search_layout.addWidget(self.orders_theme_button, 0, Qt.AlignmentFlag.AlignRight)

     top_layout.addWidget(self.logo_label, alignment=Qt.AlignmentFlag.AlignVCenter)
     top_layout.addLayout(search_layout)
     main_layout.addWidget(top_widget, 0)

     center_widget = QWidget()
     center_layout = QVBoxLayout(center_widget)
     center_layout.setContentsMargins(0, 0, 0, 0)
     center_layout.setSpacing(8)
     main_layout.addWidget(center_widget, 1)

     # CollapsibleWidget "Фільтри Пошуку"
     self.filters_panel = CollapsibleWidget("Фільтри Пошуку")
     self.filters_panel.toggle_animation.setDuration(500)
     self.filters_panel.toggle_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)
     self.filters_panel.content_area.setMaximumHeight(0)
     self.filters_panel.content_area.setVisible(False)
     self.filters_panel.toggle_button.setChecked(False)
     self.filters_panel.toggle_animation_finished.connect(self.on_filters_panel_toggled)

     self.create_filters_panel()
     center_layout.addWidget(self.filters_panel, 0)

     # Таблиця
     self.orders_column_names = [
         "ID",
         "Товари",
         "Номера-Клони",
         "Клієнт",
         "Ціна",
         "Дод. Операція",
         "Знижка",
         "Сума",
         "Статус",
         "Оплата",
         "Метод Опл.",
         "Уточнення",
         "Коментар",
         "Дата Сплати",
         "Доставка",
         "Трек-номер",
         "Отримувач",
         "Статус Дост.",
         "Дата",
         "Пріоритет"
     ]
     self.orders_mandatory_indices = [1, 3, 4, 7, 8, 9, 14, 18]
     self.orders_optional_indices = [0, 2, 5, 6, 10, 11, 12, 13, 15, 16, 17, 19]

     self.orders_table = QTableWidget()
     self.orders_table.setColumnCount(len(self.orders_column_names))
     self.orders_table.setHorizontalHeaderLabels(self.orders_column_names)
     self.orders_table.verticalHeader().setVisible(False)
     self.orders_table.setAlternatingRowColors(True)
     self.orders_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
     self.orders_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
     self.orders_table.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)
     self.orders_table.setFont(QFont("Arial", 13))
     self.orders_table.setShowGrid(True)
     self.orders_table.setGridStyle(Qt.PenStyle.SolidLine)
     self.orders_table.horizontalHeader().setSectionsMovable(True)
     self.orders_table.verticalHeader().setSectionsMovable(False)
     self.orders_table.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
     self.orders_table.setHorizontalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
     self.orders_table.horizontalHeader().setFixedHeight(35)
     
     # Змінюємо політику розміру таблиці, щоб вона розтягувалась
     self.orders_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
     self.orders_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

     for idx in self.orders_optional_indices:
         self.orders_table.setColumnHidden(idx, True)

     self.orders_opacity_effect = QGraphicsOpacityEffect()
     self.orders_table.setGraphicsEffect(self.orders_opacity_effect)

     center_layout.addWidget(self.orders_table, 10)

     # Пагінація
     self.orders_pagination_layout = QHBoxLayout()
     self.orders_pagination_layout.setContentsMargins(0, 10, 0, 0)  # Додаємо верхній відступ
     self.orders_pagination_layout.setSpacing(8)
     self.orders_pagination_layout.setAlignment(Qt.AlignmentFlag.AlignHCenter)
     self.orders_page_buttons_layout = QHBoxLayout()
     self.orders_page_buttons_layout.setSpacing(5)
     self.orders_page_buttons_layout.setContentsMargins(0, 0, 0, 0)
     self.orders_pagination_layout.addLayout(self.orders_page_buttons_layout)
     center_layout.addLayout(self.orders_pagination_layout, 0)

     # CollapsibleSection "Відображувані"
     self.orders_displayed_section = CollapsibleSection("Відображувані")
     self.orders_displayed_section.toggle_button.setFont(QFont("Arial", 14, QFont.Weight.Bold))
     self.orders_displayed_section.toggle_animation.setDuration(500)
     self.orders_displayed_section.toggle_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)

     displayed_layout = QVBoxLayout()
     displayed_layout.setSpacing(8)
     self.orders_displayed_checkboxes = []
     for index, column_name in enumerate(self.orders_column_names):
         if index not in self.orders_mandatory_indices:
             checkbox = QCheckBox(column_name)
             checkbox.setFont(QFont("Arial", 12))
             checkbox.setChecked(not self.orders_table.isColumnHidden(index))
             checkbox.stateChanged.connect(lambda state, idx=index: self.toggle_orders_column(idx, state))
             self.orders_displayed_checkboxes.append(checkbox)
             displayed_layout.addWidget(checkbox)

     self.orders_displayed_section.setContentLayout(displayed_layout)
     self.orders_displayed_section.toggle_button.setChecked(False)
     self.orders_displayed_section.on_toggle()
     center_layout.addWidget(self.orders_displayed_section, 0)

     # Нижній
     bottom_widget = QWidget()
     bottom_layout = QHBoxLayout(bottom_widget)
     bottom_layout.setContentsMargins(0, 10, 0, 0)
     bottom_layout.setSpacing(10)

     bottom_layout.addWidget(self.unpaid_checkbox, alignment=Qt.AlignmentFlag.AlignLeft)
     bottom_layout.addWidget(self.paid_checkbox, alignment=Qt.AlignmentFlag.AlignLeft)
     bottom_layout.addStretch(1)

     buttons_layout = QHBoxLayout()
     buttons_layout.setSpacing(8)

     common_button_style = """
         QPushButton {
             border: 1px solid #cccccc;
             background-color: #f0f0f0;
             color: #000000;
             border-radius: 5px;
             padding: 5px 10px;
             max-width: 150px;
         }
         QPushButton:hover {
             background-color: #e0e0e0;
         }
     """
     self.orders_filter_button = QPushButton("Застосувати")
     self.orders_filter_button.setObjectName("applyFilterButtonOrders")
     self.orders_filter_button.setFixedHeight(35)
     self.orders_filter_button.setFont(QFont("Arial", 13))
     self.orders_filter_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
     self.orders_filter_button.setStyleSheet(common_button_style)
     buttons_layout.addWidget(self.orders_filter_button)

     self.orders_refresh_button = QPushButton()
     self.orders_refresh_button.setObjectName("refreshButtonOrders")
     self.orders_refresh_button.setFixedHeight(35)
     self.orders_refresh_button.setFixedWidth(35)
     self.orders_refresh_button.setFont(QFont("Arial", 13))
     self.orders_refresh_button.setIcon(qta.icon('fa5s.sync', color='#000000'))
     self.orders_refresh_button.setIconSize(QtCore.QSize(18, 18))
     self.orders_refresh_button.setStyleSheet("""
         QPushButton {
             background-color: #f8f8f8;
             border: 1px solid #dcdcdc;
             border-radius: 5px;
             padding: 5px;
         }
         QPushButton:hover {
             background-color: #e8e8e8;
         }
         QPushButton:pressed {
             background-color: #d0d0d0;
         }
         """)
     self.orders_refresh_button.clicked.connect(lambda: asyncio.ensure_future(self.run_orders_parsing_script()))
     buttons_layout.addWidget(self.orders_refresh_button)
     
     # Кнопка для парсингу замовлень
     # self.parse_orders_button = QPushButton("Парсинг замовлень")
     # self.parse_orders_button.setObjectName("parseOrdersButton")
     # self.parse_orders_button.setFixedHeight(35)
     # self.parse_orders_button.setFont(QFont("Arial", 13))
     # self.parse_orders_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
     # self.parse_orders_button.setStyleSheet(common_button_style)
     # self.parse_orders_button.clicked.connect(lambda: print("Parsing"))
     # buttons_layout.addWidget(self.parse_orders_button)
     
     # Прогрес-бар для відображення прогресу парсингу
     # self.parse_progress = QProgressBar()
     # self.parse_progress.setObjectName("parseProgressBar")
     # self.parse_progress.setMaximum(100)
     # self.parse_progress.setValue(0)
     # self.parse_progress.setVisible(False)
     # buttons_layout.addWidget(self.parse_progress)
     
     bottom_layout.addLayout(buttons_layout)
     center_layout.addWidget(bottom_widget, 0)

     self.orders_filter_button.clicked.connect(lambda: asyncio.ensure_future(self.apply_orders_filters()))
     self.orders_table.cellDoubleClicked.connect(self.show_orders_cell_info)
     self.orders_table.horizontalHeader().sectionClicked.connect(self.select_orders_column)
     self.orders_table.itemSelectionChanged.connect(self.on_orders_selection_changed)

     self.set_orders_scroll_style()

     # Прив'язуємо сигнали
     self.month_slider.valueChanged.connect(self.on_slider_value_changed)
     self.year_slider.valueChanged.connect(self.on_slider_value_changed)
     self.month_min.valueChanged.connect(self.on_spinbox_value_changed)
     self.month_max.valueChanged.connect(self.on_spinbox_value_changed)
     self.year_min.valueChanged.connect(self.on_spinbox_value_changed)
     self.year_max.valueChanged.connect(self.on_spinbox_value_changed)
     
     # Оновлюємо вигляд кнопки календаря
     self._update_calendar_button_state()

 def set_orders_scroll_style(self):
     """
     Минималистичный автоскрывающийся скроллбар:
     - Полупрозрачный (появляется только при наведении)
     - Без рейла/фона
     - Накладывается на край таблицы
     - Плавное появление/исчезновение
     """
     # Стиль для основной таблицы заказов
     table_scroll_style = """
     QTableWidget {
         border: 1px solid #cccccc;
     }
   
     QScrollBar:vertical {
         border: none;
         background: transparent;
         width: 3px;
         margin: 0px;
     }
   
     QScrollBar:vertical:hover {
         width: 8px;
     }
   
     QScrollBar::handle:vertical {
         background: rgba(120, 120, 120, 0.3);
         border-radius: 4px;
         min-height: 30px;
     }
   
     QScrollBar::handle:vertical:hover {
         background: rgba(80, 80, 80, 0.7);
     }
   
     QScrollBar::add-line:vertical,
     QScrollBar::sub-line:vertical {
         height: 0px;
         background: none;
         border: none;
     }
   
     QScrollBar::add-page:vertical,
     QScrollBar::sub-page:vertical {
         background: transparent;
         border: none;
     }
   
     /* Горизонтальный скроллбар */
     QScrollBar:horizontal {
         border: none;
         background: transparent;
         height: 3px;
         margin: 0px;
     }
   
     QScrollBar:horizontal:hover {
         height: 8px;
     }
   
     QScrollBar::handle:horizontal {
         background: rgba(120, 120, 120, 0.3);
         border-radius: 4px;
         min-width: 30px;
     }
   
     QScrollBar::handle:horizontal:hover {
         background: rgba(80, 80, 80, 0.7);
     }
   
     QScrollBar::add-line:horizontal,
     QScrollBar::sub-line:horizontal {
         width: 0px;
         background: none;
         border: none;
     }
   
     QScrollBar::add-page:horizontal,
     QScrollBar::sub-page:horizontal {
         background: transparent;
         border: none;
     }
     """
     self.orders_table.setStyleSheet(table_scroll_style)
   
     # Аналогичный стиль для всех остальных виджетов с прокруткой
     global_scroll_style = """
     QScrollBar:vertical {
         border: none;
         background: transparent;
         width: 3px;
         margin: 0px;
     }
   
     QScrollBar:vertical:hover {
         width: 8px;
     }
   
     QScrollBar::handle:vertical {
         background: rgba(120, 120, 120, 0.3);
         border-radius: 4px;
         min-height: 30px;
     }
   
     QScrollBar::handle:vertical:hover {
         background: rgba(80, 80, 80, 0.7);
     }
   
     QScrollBar::add-line:vertical,
     QScrollBar::sub-line:vertical {
         height: 0px;
         background: none;
         border: none;
     }
   
     QScrollBar::add-page:vertical,
     QScrollBar::sub-page:vertical {
         background: transparent;
         border: none;
     }
   
     /* Горизонтальный скроллбар */
     QScrollBar:horizontal {
         border: none;
         background: transparent;
         height: 3px;
         margin: 0px;
     }
   
     QScrollBar:horizontal:hover {
         height: 8px;
     }
   
     QScrollBar::handle:horizontal {
         background: rgba(120, 120, 120, 0.3);
         border-radius: 4px;
         min-width: 30px;
     }
   
     QScrollBar::handle:horizontal:hover {
         background: rgba(80, 80, 80, 0.7);
     }
   
     QScrollBar::add-line:horizontal,
     QScrollBar::sub-line:horizontal {
         width: 0px;
         background: none;
         border: none;
     }
   
     QScrollBar::add-page:horizontal,
     QScrollBar::sub-page:horizontal {
         background: transparent;
         border: none;
     }
     """
   
     # Применяем стиль для основных контейнеров с прокруткой
     if hasattr(self, 'scroll_area'):
         self.scroll_area.setStyleSheet(global_scroll_style)
       
     if hasattr(self, 'orders_completer_list'):
         self.orders_completer_list.setStyleSheet(global_scroll_style)
       
     # Применяем стиль для фильтров секций
     for section_name in ['answer_status_section', 'payment_status_section', 'delivery_section']:
         if hasattr(self, section_name):
             section = getattr(self, section_name)
             if hasattr(section, 'scroll_area'):
                 section.scroll_area.setStyleSheet(global_scroll_style)
               
     # Применяем стиль глобально для всего виджета, чтобы затронуть все скроллбары
     current_style = self.styleSheet() or ""
     if "QScrollBar" not in current_style:
         self.setStyleSheet(current_style + global_scroll_style)

 def on_theme_button_clicked(self):
     if self.parent_window:
         self.parent_window.toggle_theme_global()

 def apply_external_theme(self, is_dark):
     self.is_dark_theme = is_dark
     update_text_colors(self.parent_window, self.is_dark_theme)
     self.update_orders_theme_icon()

     if self.is_dark_theme:
         logo_pixmap = QPixmap("style/images/icons/logo_dark.png")
     else:
         logo_pixmap = QPixmap("style/images/icons/logo.png")

     logo_pixmap = logo_pixmap.scaled(
         50, 50,
         Qt.AspectRatioMode.KeepAspectRatio,
         Qt.TransformationMode.SmoothTransformation
     )
     if self.logo_label:
         self.logo_label.setPixmap(logo_pixmap)

     # Оновлюємо стиль календаря при зміні теми
     self._update_calendar_button_state()
     
     # Оновлюємо кольори тексту для QLabel в комірках таблиці
     self.update_product_labels_color()

     self.update_orders_page_buttons()
     
 def update_product_labels_color(self):
     """Оновлює колір тексту для QLabel з товарами при зміні теми"""
     # Отримуємо виділені рядки
     selected_rows = set(index.row() for index in self.orders_table.selectedIndexes())
     
     # Проходимо по всіх комірках і оновлюємо стиль QLabel
     for row in range(self.orders_table.rowCount()):
         label = self.orders_table.cellWidget(row, 1)
         if label and isinstance(label, QLabel) and label.property("role") == "product_cell":
             # Визначаємо колір тексту - білий для виділених, залежний від теми для невиділених
             is_selected = row in selected_rows
             text_color = "white" if is_selected else ("white" if self.is_dark_theme else "black")
             
             label.setStyleSheet(f"""
                 QLabel {{
                     padding: 0px 15px;
                     margin: 0px;
                     min-height: 38px;
                     background-color: transparent;
                     color: {text_color};
                 }}
             """)
             
             # Скидаємо всі відступи та використовуємо лише властивість вирівнювання Qt
             margins = QMargins(15, 0, 15, 0)
             label.setContentsMargins(margins)
             label.setFixedHeight(38)

 def update_orders_theme_icon(self):
     from services.theme_service import update_theme_icon_for_button
     if self.orders_theme_button:
         update_theme_icon_for_button(self.orders_theme_button, self.is_dark_theme)

 def debounced_completer_update(self, text: str):
     """
     Відкладене оновлення підказок при введенні тексту.
     """
     self.current_suggestion_index = -1
     
     # Оновлюємо підказки, але не запускаємо пошук за кожним символом
     if text.strip():
         self.completer_timer.start()
     else:
         self.fade_out_orders_popup()
         
         # Запускаємо пошук тільки при порожньому полі - очищення фільтрів
         if hasattr(self, 'data_loaded') and self.data_loaded:
             asyncio.ensure_future(self.apply_orders_filters())

 def update_completer(self):
     """
     Автодоповнення для Замовлень (пошук за клієнтом, трек-номером).
     Тільки показує підказки, але не запускає фактичний пошук.
     """
     text = self.orders_search_bar.text().strip()
     if not text:
         self.fade_out_orders_popup()
         return

     from sqlalchemy import or_
     results = []
     try:
         possible_orders = (
             session.query(Order)
             .join(Client, Order.client_id == Client.id)
             .options(joinedload(Order.client))
             .filter(
                 or_(
                     Client.first_name.ilike(f"%{text}%"),
                     Client.last_name.ilike(f"%{text}%"),
                     Order.tracking_number.ilike(f"%{text}%")
                 )
             )
             .limit(50)
             .all()
         )
         for od in possible_orders:
             cli = od.client
             if cli:
                 f_full = f"{cli.first_name or ''} {cli.last_name or ''}".strip()
             else:
                 f_full = "?"
             t_num = od.tracking_number or ""
             results.append(f"{od.id} / {f_full} / {t_num}")
     except Exception as e:
         logging.error(f"Помилка при отриманні підказок: {e}")
         pass

     self.orders_completer_list.clear()
     if not results:
         no_item = QListWidgetItem("Немає підказок…")
         no_item.setFlags(Qt.ItemFlag.NoItemFlags)
         self.orders_completer_list.addItem(no_item)
     else:
         for r in results:
             item = QListWidgetItem(r)
             self.orders_completer_list.addItem(item)

     self.position_orders_completer_list()
     if not self.orders_completer_list.isVisible():
         self.fade_in_orders_popup()
     else:
         self.orders_completer_list.update()

 def position_orders_completer_list(self):
     self.orders_completer_list.setFixedWidth(self.orders_search_bar.width())
     list_pos = self.orders_search_bar.mapToGlobal(QtCore.QPoint(0, self.orders_search_bar.height()))
     self.orders_completer_list.move(list_pos)

 def fade_in_orders_popup(self):
     self.orders_popup_fade_animation.stop()
     self.orders_popup_opacity_effect.setOpacity(0.0)
     self.orders_completer_list.show()
     self.orders_popup_fade_animation.setStartValue(0.0)
     self.orders_popup_fade_animation.setEndValue(1.0)
     self.orders_popup_fade_animation.start()

 def fade_out_orders_popup(self):
     def on_finished():
         self.orders_completer_list.hide()

     if not self.orders_completer_list.isVisible():
         return

     self.orders_popup_fade_animation.stop()
     start_opacity = self.orders_popup_opacity_effect.opacity()
     try:
         self.orders_popup_fade_animation.finished.disconnect()
     except TypeError:
         pass
     self.orders_popup_fade_animation.finished.connect(on_finished)
     self.orders_popup_fade_animation.setStartValue(start_opacity)
     self.orders_popup_fade_animation.setEndValue(0.0)
     self.orders_popup_fade_animation.start()

 def insert_orders_completion(self, item):
     """
     Коли користувач вибирає підказку (клацаючи мишею або натискаючи Enter),
     ми встановлюємо текст пошуку та викликаємо фільтрацію.
     """
     if not item:
         return
     txt = item.text()
     if txt == "Немає підказок…":
         return
     
     # Вставка тексту та запуск пошуку
     self.orders_search_bar.setText(txt)
     self.fade_out_orders_popup()
     # Запускаємо пошук, але переконуємося, що старі завдання скасовані
     asyncio.ensure_future(self.apply_orders_filters())

 def eventFilter(self, obj, event):
     if obj == self.orders_search_bar:
         if event.type() == QEvent.Type.FocusOut:
             if self.orders_completer_list.isVisible():
                 mouse_pos = QCursor.pos()
                 if not self.orders_completer_list.geometry().contains(mouse_pos):
                     self.fade_out_orders_popup()
         elif event.type() == QEvent.Type.KeyPress:
             if event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
                 if 0 <= self.current_suggestion_index < self.orders_completer_list.count():
                     item = self.orders_completer_list.item(self.current_suggestion_index)
                     self.insert_orders_completion(item)
                 else:
                     # Запускаємо пошук, але переконуємося, що старі завдання скасовані
                     asyncio.ensure_future(self.apply_orders_filters())
                     self.fade_out_orders_popup()
                 return True
             elif event.key() == Qt.Key.Key_Escape:
                 self.fade_out_orders_popup()
                 return True
             elif event.key() == Qt.Key.Key_Down:
                 if self.orders_completer_list.count() > 0:
                     new_index = self.current_suggestion_index + 1
                     if new_index < self.orders_completer_list.count():
                         self.current_suggestion_index = new_index
                         self.orders_completer_list.setCurrentRow(self.current_suggestion_index)
                 return True
             elif event.key() == Qt.Key.Key_Up:
                 if self.orders_completer_list.count() > 0:
                     new_index = self.current_suggestion_index - 1
                     if new_index >= 0:
                         self.current_suggestion_index = new_index
                         self.orders_completer_list.setCurrentRow(self.current_suggestion_index)
                 return True
     elif obj == self.orders_completer_list:
         if event.type() == QEvent.Type.MouseButtonPress:
             item = self.orders_completer_list.itemAt(event.pos())
             if item:
                 self.insert_orders_completion(item)
         elif event.type() == QEvent.Type.KeyPress:
             if event.key() == Qt.Key.Key_Escape:
                 self.fade_out_orders_popup()
                 self.orders_search_bar.setFocus()
                 return True
             elif event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
                 item = self.orders_completer_list.currentItem()
                 self.insert_orders_completion(item)
                 return True
             elif event.key() == Qt.Key.Key_Down:
                 new_index = self.current_suggestion_index + 1
                 if new_index < self.orders_completer_list.count():
                     self.current_suggestion_index = new_index
                     self.orders_completer_list.setCurrentRow(self.current_suggestion_index)
                 return True
             elif event.key() == Qt.Key.Key_Up:
                 new_index = self.current_suggestion_index - 1
                 if new_index >= 0:
                     self.current_suggestion_index = new_index
                     self.orders_completer_list.setCurrentRow(self.current_suggestion_index)
                 return True
     return False

 def create_filters_panel(self):
     """
     Створюємо QScrollArea з двома групами: ліва (CheckBox-фільтри),
     права (Слайдери, ComboBox, etc). Без бордюрів.
     """
     filters_layout = QHBoxLayout()
     filters_layout.setContentsMargins(0, 0, 0, 0)
     filters_layout.setSpacing(20)

     scroll_area = QScrollArea()
     scroll_area.setWidgetResizable(True)
     scroll_area.setStyleSheet("QScrollArea { border: none; }")
     scroll_area.setMinimumHeight(210)
     scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
     scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)

     container_widget = QWidget()
     container_widget.setStyleSheet("QGroupBox { border: none; }")
     container_layout = QHBoxLayout(container_widget)
     container_layout.setContentsMargins(10, 5, 10, 0)
     container_layout.setSpacing(20)

     # Ліва група (статус відповіді, оплати, доставка)
     left_group = QGroupBox()
     left_group.setTitle("")
     left_group.setStyleSheet("border: none;")
     left_layout = QVBoxLayout(left_group)
     left_layout.setContentsMargins(0, 0, 0, 0)
     left_layout.setSpacing(15)

     # Права група (місяці, рік, і т.д.)
     right_group = QGroupBox()
     right_group.setTitle("")
     right_group.setStyleSheet("border: none;")
     right_layout = QVBoxLayout(right_group)
     right_layout.setContentsMargins(0, 0, 0, 0)
     right_layout.setSpacing(10)

     container_layout.addWidget(left_group, stretch=1)
     container_layout.addWidget(right_group, stretch=1)

     scroll_area.setWidget(container_widget)
     filters_layout.addWidget(scroll_area)

     self.filters_panel.setContentLayout(QVBoxLayout())
     self.filters_panel.content_area.layout().addLayout(filters_layout)

     self.populate_orders_filters(left_layout, right_layout)

 def populate_orders_filters(self, left_layout, right_layout):
     # Статус відповіді
     # Отримуємо унікальні статуси відповіді з бази даних
     order_statuses = session.query(OrderStatus).order_by(OrderStatus.status_name).all()
     answer_statuses = [status.status_name for status in order_statuses]
     
     # Створюємо секцію з чекбоксами
     self.answer_status_section = FilterSection(
         "Статус відповіді", items=answer_statuses, columns=4, maxHeight=600
     )
     self.answer_status_section.toggle_animation.setDuration(500)
     self.answer_status_section.toggle_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)
     self.answer_status_section.toggle_button.setChecked(False)
     self.answer_status_section.on_toggle()
     self.answer_status_checkboxes = self.answer_status_section.all_checkboxes
     self.answer_status_section.checkbox_state_changed.connect(self.on_checkbox_state_changed)
     left_layout.addWidget(self.answer_status_section)

     # Статус оплати
     payment_statuses = session.query(PaymentStatus).order_by(PaymentStatus.status_name).all()
     payment_status_list = [status.status_name for status in payment_statuses]
     
     self.payment_status_section = FilterSection(
         "Статус оплати", items=payment_status_list, columns=4, maxHeight=600
     )
     self.payment_status_section.toggle_animation.setDuration(500)
     self.payment_status_section.toggle_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)
     self.payment_status_section.toggle_button.setChecked(False)
     self.payment_status_section.on_toggle()
     self.payment_status_checkboxes = self.payment_status_section.all_checkboxes
     self.payment_status_section.checkbox_state_changed.connect(self.on_checkbox_state_changed)
     left_layout.addWidget(self.payment_status_section)

     # Доставка
     delivery_methods = session.query(DeliveryMethod).order_by(DeliveryMethod.method_name).all()
     delivery_method_list = [method.method_name for method in delivery_methods]
     
     self.delivery_section = FilterSection(
         "Доставка", items=delivery_method_list, columns=4, maxHeight=600
     )
     self.delivery_section.toggle_animation.setDuration(500)
     self.delivery_section.toggle_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)
     self.delivery_section.toggle_button.setChecked(False)
     self.delivery_section.on_toggle()
     self.delivery_checkboxes = self.delivery_section.all_checkboxes
     self.delivery_section.checkbox_state_changed.connect(self.on_checkbox_state_changed)
     left_layout.addWidget(self.delivery_section)


     # Додаємо слайдери місяців та років у праву колонку
     label_font = QFont("Arial", 11)

     # Слайдер місяців з етикеткою "Місяці"
     month_label = QLabel("Місяці")
     month_label.setFont(label_font)
     self.month_min = QSpinBox()
     self.month_min.setFont(QFont("Arial", 13))
     self.month_min.setPrefix("Від ")
     self.month_min.setMinimum(1)
     self.month_min.setMaximum(12)
     self.month_min.setValue(1)
     self.month_min.setFixedWidth(80)

     self.month_max = QSpinBox()
     self.month_max.setFont(QFont("Arial", 13))
     self.month_max.setPrefix("До ")
     self.month_max.setMinimum(1)
     self.month_max.setMaximum(12)
     self.month_max.setValue(12)
     self.month_max.setFixedWidth(80)

     self.month_slider = RangeSlider()
     self.month_slider.setObjectName("monthSlider")
     self.month_slider.left_margin = 0
     self.month_slider.right_margin = 9
     self.month_slider.setRange(1, 12)
     self.month_slider.setLow(1)
     self.month_slider.setHigh(12)
     self.month_slider.setMinimumWidth(800)

     month_input_layout = QHBoxLayout()
     month_input_layout.setSpacing(5)
     month_input_layout.addWidget(month_label)
     month_input_layout.addWidget(self.month_min)
     month_input_layout.addWidget(self.month_max)

     month_slider_layout = QHBoxLayout()
     month_slider_layout.setContentsMargins(0, 0, 0, 0)
     month_slider_layout.setSpacing(0)
     month_slider_layout.addStretch(1)
     month_slider_layout.addWidget(self.month_slider, 0, Qt.AlignmentFlag.AlignRight)

     right_layout.addLayout(month_input_layout)
     right_layout.addLayout(month_slider_layout)

     # Слайдер років з етикеткою "Роки"
     year_label = QLabel("Роки")
     year_label.setFont(label_font)
     self.year_min = QSpinBox()
     self.year_min.setFont(QFont("Arial", 13))
     self.year_min.setPrefix("Від ")
     self.year_min.setMinimum(2020)
     self.year_min.setMaximum(2030)
     self.year_min.setValue(2020)
     self.year_min.setFixedWidth(80)

     self.year_max = QSpinBox()
     self.year_max.setFont(QFont("Arial", 13))
     self.year_max.setPrefix("До ")
     self.year_max.setMinimum(2020)
     self.year_max.setMaximum(2030)
     self.year_max.setValue(2030)
     self.year_max.setFixedWidth(80)

     self.year_slider = RangeSlider()
     self.year_slider.setObjectName("yearSlider")
     self.year_slider.left_margin = 0
     self.year_slider.right_margin = 9
     self.year_slider.setRange(2020, 2030)
     self.year_slider.setLow(2020)
     self.year_slider.setHigh(2030)
     self.year_slider.setMinimumWidth(800)

     year_input_layout = QHBoxLayout()
     year_input_layout.setSpacing(5)
     year_input_layout.addWidget(year_label)
     year_input_layout.addWidget(self.year_min)
     year_input_layout.addWidget(self.year_max)

     year_slider_layout = QHBoxLayout()
     year_slider_layout.setContentsMargins(0, 0, 0, 0)
     year_slider_layout.setSpacing(0)
     year_slider_layout.addStretch(1)
     year_slider_layout.addWidget(self.year_slider, 0, Qt.AlignmentFlag.AlignRight)

     right_layout.addLayout(year_input_layout)
     right_layout.addLayout(year_slider_layout)

     # combobox style - використовуємо такий же як у вкладці Товари
     combobox_style = """
     QComboBox {
         border: 1px solid #cccccc;
         background-color: #ffffff;
         color: #000000;
         font-size: 13pt;
         border-radius: 5px;
         padding: 0px 40px 0px 10px;
         min-height: 35px;
         min-width: 120px;
     }
     QComboBox:focus {
         border: 2px solid #7851A9;
     }
     QComboBox::drop-down {
         border: none;
         background: transparent;
         width: 40px;
         subcontrol-position: top right;
         subcontrol-origin: margin;
     }
     QComboBox::down-arrow {
         image: url(style/images/icons/down_arrow_flat.png);
         width: 12px;
         height: 12px;
     }
     QComboBox QAbstractItemView {
         background-color: #ffffff;
         color: #000000;
         selection-background-color: #d5d5d5;
         border: 1px solid #cccccc;
     }
     QComboBox QAbstractItemView::item {
         text-align: center;
     }
     """

     # Створюємо ComboBox-и в нижній частині правої колонки
     self.orders_sort_combobox = QComboBox()
     self.orders_sort_combobox.setFont(QFont("Arial", 13))
     self.orders_sort_combobox.setFixedHeight(35)
     self.orders_sort_combobox.setStyleSheet(combobox_style)
     self.orders_sort_combobox.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
     self.orders_sort_combobox.addItem("Сортування")
     self.orders_sort_combobox.model().item(0).setEnabled(False)
     self.orders_sort_combobox.addItem("Від дорожчого")
     self.orders_sort_combobox.addItem("Від найдешевшого")
     self.orders_sort_combobox.addItem("Від найбільшого (по кількості)")
     self.orders_sort_combobox.addItem("Від найдавнішого")
     self.orders_sort_combobox.addItem("Від найновішого")
     self.orders_sort_combobox.setCurrentIndex(0)

     self.calendar_button = QToolButton()
     self.calendar_button.setText("Дата")
     self.calendar_button.setFont(QFont("Arial", 13))
     self.calendar_button.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
     self.calendar_button.setFixedHeight(35)
     self.calendar_button.setStyleSheet("""
         QToolButton {
             border: 1px solid #cccccc;
             background-color: #f0f0f0;
             color: #000000;
             border-radius: 5px;
             padding: 5px 10px;
             min-width: 100px;
             text-align: center;
         }
         QToolButton:hover {
             background-color: #e0e0e0;
         }
     """)
     
     # Додаємо обробник кліку на кнопку календаря
     self.calendar_button.clicked.connect(self.show_date_picker)

     self.priority_combobox = QComboBox()
     self.priority_combobox.setFont(QFont("Arial", 13))
     self.priority_combobox.setFixedHeight(35)
     self.priority_combobox.setStyleSheet(combobox_style)
     self.priority_combobox.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
     self.priority_combobox.addItem("Пріоритет")
     self.priority_combobox.model().item(0).setEnabled(False)
     self.priority_combobox.addItem("Будь-який")
     self.priority_combobox.addItem("0")
     self.priority_combobox.addItem("1")
     self.priority_combobox.addItem("2")
     self.priority_combobox.addItem("3")
     self.priority_combobox.setCurrentIndex(0)

     self.reset_button_orders = QPushButton("Скинути Фільтри")
     self.reset_button_orders.setObjectName("resetFiltersButtonOrders")
     self.reset_button_orders.setFont(QFont("Arial", 13))
     self.reset_button_orders.setFixedHeight(35)
     self.reset_button_orders.setStyleSheet("""
         QPushButton {
             border: 1px solid #cccccc;
             background-color: #f0f0f0;
             color: #000000;
             border-radius: 5px;
             padding: 5px 10px;
             max-width: 150px;
         }
         QPushButton:hover {
             background-color: #e0e0e0;
         }
     """)
     self.reset_button_orders.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
     self.reset_button_orders.clicked.connect(lambda: asyncio.ensure_future(self.reset_orders_filters()))

     # ComboBox-и та кнопка Reset в нижній частині
     combo_bottom_layout = QHBoxLayout()
     combo_bottom_layout.setSpacing(10)
     combo_bottom_layout.addWidget(self.orders_sort_combobox)
     combo_bottom_layout.addWidget(self.calendar_button)
     combo_bottom_layout.addWidget(self.priority_combobox)
     combo_bottom_layout.addStretch(1)
     
     btn_layout = QHBoxLayout()
     btn_layout.setSpacing(10)
     btn_layout.addLayout(combo_bottom_layout)
     btn_layout.addWidget(self.reset_button_orders)
     
     right_layout.addLayout(btn_layout)
     right_layout.addStretch(1)

     # Прив'язуємо сигнали
     self.month_slider.valueChanged.connect(self.on_slider_value_changed)
     self.year_slider.valueChanged.connect(self.on_slider_value_changed)
     self.month_min.valueChanged.connect(self.on_spinbox_value_changed)
     self.month_max.valueChanged.connect(self.on_spinbox_value_changed)
     self.year_min.valueChanged.connect(self.on_spinbox_value_changed)
     self.year_max.valueChanged.connect(self.on_spinbox_value_changed)

 def on_filters_panel_toggled(self):
     self.toggle_animation_finished.emit()

 def _update_calendar_button_state(self):
     """Оновлює стан кнопки календаря відповідно до вибраної дати"""
     if hasattr(self, 'selected_filter_date') and self.selected_filter_date:
         # Перетворюємо QDate у рядок за допомогою методу toString()
         date_str = self.selected_filter_date.toString("dd.MM.yyyy")
         self.calendar_button.setText(f"Дата: {date_str}")
         
         # Застосовуємо стиль активного фільтра з урахуванням теми
         if self.is_dark_theme:
             self.calendar_button.setStyleSheet("""
                 QToolButton {
                     background-color: #7851A9;
                     color: white;
                     border: 1px solid #6a4697;
                     border-radius: 5px;
                     padding: 5px 10px;
                     min-width: 100px;
                     text-align: center;
                 }
                 QToolButton:hover {
                     background-color: #6a4697;
                 }
                 QToolButton:pressed {
                     background-color: #5d3d88;
                 }
             """)
         else:
             self.calendar_button.setStyleSheet("""
                 QToolButton {
                     background-color: #7851A9;
                     color: white;
                     border: 1px solid #6a4697;
                     border-radius: 5px;
                     padding: 5px 10px;
                     min-width: 100px;
                     text-align: center;
                 }
                 QToolButton:hover {
                     background-color: #6a4697;
                 }
                 QToolButton:pressed {
                     background-color: #5d3d88;
                 }
             """)
     else:
         self.calendar_button.setText("Дата")
         
         # Скидаємо стиль на стандартний з урахуванням теми
         if self.is_dark_theme:
             self.calendar_button.setStyleSheet("""
                 QToolButton {
                     border: 1px solid #444444;
                     background-color: #333333;
                     color: #ffffff;
                     border-radius: 5px;
                     padding: 5px 10px;
                     min-width: 100px;
                     text-align: center;
                 }
                 QToolButton:hover {
                     background-color: #3a3a3a;
                 }
                 QToolButton:pressed {
                     background-color: #2a2a2a;
                 }
             """)
         else:
             self.calendar_button.setStyleSheet("""
                 QToolButton {
                     border: 1px solid #cccccc;
                     background-color: #f0f0f0;
                     color: #000000;
                     border-radius: 5px;
                     padding: 5px 10px;
                     min-width: 100px;
                     text-align: center;
                 }
                 QToolButton:hover {
                     background-color: #e0e0e0;
                 }
                 QToolButton:pressed {
                     background-color: #d0d0d0;
                 }
             """)

 def on_checkbox_state_changed(self):
     """
     Обробник зміни стану чекбоксів фільтрів.
     Запускає відкладене оновлення фільтрів за допомогою search_timer.
     """
     # Запускаємо таймер, щоб не робити багато запитів при швидкій зміні кількох чекбоксів
     self.search_timer.start(300)
     
     # Взаємно виключаємо фільтри "Тільки неоплачені" та "Тільки оплачені"
     if self.sender() == self.unpaid_checkbox and self.unpaid_checkbox.isChecked():
                 self.paid_checkbox.setChecked(False)
     elif self.sender() == self.paid_checkbox and self.paid_checkbox.isChecked():
                 self.unpaid_checkbox.setChecked(False)

 def on_slider_value_changed(self):
     """
     Обробник зміни значення слайдера (місяців або років).
     Оновлює пов'язані спінбокси і запускає відкладене оновлення фільтрів.
     """
     sender = self.sender()
     if sender == self.month_slider:
         self.month_min.setValue(self.month_slider.low)
         self.month_max.setValue(self.month_slider.high)
     elif sender == self.year_slider:
         self.year_min.setValue(self.year_slider.low)
         self.year_max.setValue(self.year_slider.high)
     
     # Запускаємо відкладене оновлення фільтрів
         self.search_timer.start(300)

 def on_spinbox_value_changed(self):
     """
     Обробник зміни значення спінбокса (місяців або років).
     Оновлює пов'язані слайдери і запускає відкладене оновлення фільтрів.
     """
     sender = self.sender()
     if sender == self.month_min:
         self.month_slider.setLow(self.month_min.value())
     elif sender == self.month_max:
         self.month_slider.setHigh(self.month_max.value())
     elif sender == self.year_min:
         self.year_slider.setLow(self.year_min.value())
     elif sender == self.year_max:
         self.year_slider.setHigh(self.year_max.value())
     
     # Запускаємо відкладене оновлення фільтрів
         self.search_timer.start(300)

 def on_search_enter_pressed(self):
     """
     Обробник натискання клавіші Enter у полі пошуку.
     Запускає застосування фільтрів одразу, а не через відкладений таймер.
     """
     # Закриваємо випадаюче вікно з підказками, якщо воно відкрите
     self.fade_out_orders_popup()
     
     # Застосовуємо фільтри негайно
     asyncio.ensure_future(self.apply_orders_filters())

 async def apply_orders_filters(self, is_initial_load=False, is_auto_load=False):
     """
     Застосовує фільтри до замовлень і оновлює таблицю.
     
     :param is_initial_load: Чи це початкове завантаження даних.
     :param is_auto_load: Чи це автоматичне завантаження (при відображенні вкладки).
     """
     try:
         # Імпортуємо datetime і time прямо на початку методу, щоб уникнути конфліктів імен
         from datetime import datetime, time, timedelta
         
         # Відміняємо попереднє завдання фільтрації, якщо воно існує
         if self.current_filter_task and not self.current_filter_task.done():
             self.current_filter_task.cancel()
         
         # Показуємо індикатор завантаження
         if not is_auto_load:
            self.orders_opacity_effect.setOpacity(0.7)
            QApplication.processEvents()
         
         # Отримуємо параметри фільтрації
         query_params = self.get_orders_filter_params()
         
         # Скидаємо на першу сторінку лише при початковому завантаженні або при зміні фільтрів
         # але НЕ коли перемикаємо сторінки через пагінацію
         if is_initial_load or self.sender() == self.orders_filter_button or self.sender() == self.reset_button_orders:
             self.current_page = 1
         
         # Будуємо запит до бази даних на основі параметрів фільтрації
         # Змінено: build_orders_query_params тепер повертає словник параметрів, а не сам запит
         params = build_orders_query_params(self)
         
         # Створюємо базовий запит
         query = session.query(Order).options(
             joinedload(Order.order_details),
             joinedload(Order.client),
             joinedload(Order.order_status),
             joinedload(Order.payment_status),
             joinedload(Order.payment_method),
             joinedload(Order.delivery_method),
             # Recipient не має relationship, потрібно використовувати Address
             # joinedload(Order.recipient),
             # DeliveryStatus не має relationship, перевірте модель
             # joinedload(Order.delivery_status),
         ).order_by(desc(Order.order_date))
         
         # Застосовуємо фільтри на основі параметрів
         # Пошук
         if 'search_text' in params and params['search_text']:
             search_text = params['search_text'].lower()
             query = query.join(Client, Order.client_id == Client.id)
             query = query.filter(
                or_(
                    Client.first_name.ilike(f"%{search_text}%"),
                    Client.last_name.ilike(f"%{search_text}%"),
                    Order.tracking_number.ilike(f"%{search_text}%"),
                     Order.notes.ilike(f"%{search_text}%"),
                     Order.details.ilike(f"%{search_text}%"),
                     cast(Order.id, String).ilike(f"%{search_text}%")
                 )
             )
         
         # Фільтри дат (місяці та роки)
         if all(k in params for k in ['month_min', 'month_max', 'year_min', 'year_max']):
             start_date = datetime(params['year_min'], params['month_min'], 1)
             # Останній день місяця
             if params['month_max'] == 12:
                 end_date = datetime(params['year_max'] + 1, 1, 1) - timedelta(days=1)
             else:
                 end_date = datetime(params['year_max'], params['month_max'] + 1, 1) - timedelta(days=1)
             
             query = query.filter(Order.order_date.between(start_date, end_date))
         
         # Статуси відповіді
         if 'answer_statuses' in params and params['answer_statuses']:
             query = query.join(OrderStatus, Order.order_status_id == OrderStatus.id)
             query = query.filter(OrderStatus.status_name.in_(params['answer_statuses']))
         
         # Статуси оплати
         if 'payment_statuses' in params and params['payment_statuses']:
             query = query.join(PaymentStatus, Order.payment_status_id == PaymentStatus.id)
             query = query.filter(PaymentStatus.status_name.in_(params['payment_statuses']))
         
         # Методи доставки
         if 'delivery_methods' in params and params['delivery_methods']:
             query = query.join(DeliveryMethod, Order.delivery_method_id == DeliveryMethod.id)
             query = query.filter(DeliveryMethod.method_name.in_(params['delivery_methods']))
         
         # Пріоритет
         if 'priority' in params and params['priority'] not in ["Будь-який", "Пріоритет"]:
             query = query.filter(Order.priority == int(params['priority']))
         
         # Обробляємо спеціальні фільтри "Тільки неоплачені" та "Тільки оплачені"
         if self.unpaid_checkbox.isChecked():
             query = fix_unpaid_filter(query, session)
         elif self.paid_checkbox.isChecked():
             query = fix_paid_filter(query, session)
         
         # Застосовуємо фільтрацію за датою, якщо вона вибрана
         if hasattr(self, 'selected_filter_date') and self.selected_filter_date:
             # Перетворюємо QDate у Python datetime
             py_date = datetime(
                 self.selected_filter_date.year(),
                 self.selected_filter_date.month(),
                 self.selected_filter_date.day()
             )
             
             # Створюємо початок та кінець вибраного дня
             day_start = datetime.combine(py_date.date(), time.min)
             day_end = datetime.combine(py_date.date(), time.max)
             
             # Фільтруємо замовлення за вибраною датою
             query = query.filter(Order.order_date.between(day_start, day_end))
         
         # Отримуємо загальну кількість замовлень для пагінації
         total_count = await asyncio.to_thread(lambda q=query: q.count())
         
         # Обчислюємо загальну кількість сторінок
         self.total_pages = max(1, (total_count + self.page_size - 1) // self.page_size)
         
         # Застосовуємо ліміт і зсув для поточної сторінки
         offset = (self.current_page - 1) * self.page_size
         query = query.limit(self.page_size).offset(offset)
         
         # Отримуємо замовлення для поточної сторінки
         orders = await asyncio.to_thread(lambda q=query: q.all())
         
         # Зберігаємо всі замовлення для відображення
         self.all_orders = orders
         
         # Оновлюємо таблицю замовлень
         self.update_orders_table(orders)
         
         # Переконуємося, що таблиця розтягується правильно
         self.orders_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
         # Застосовуємо оптимальні розміри колонок замість встановлення всіх на Stretch
         self.apply_column_widths()
         
         # Оновлюємо кнопки пагінації
         self.update_orders_page_buttons()
         
         # Встановлюємо прапорець, що дані завантажені
         self.data_loaded = True
         
         # Відновлюємо прозорість таблиці
         self.orders_opacity_effect.setOpacity(1.0)
         
     except Exception as e:
         logging.error(f"Помилка при застосуванні фільтрів замовлень: {e}")
         logging.error(traceback.format_exc())
         self.orders_opacity_effect.setOpacity(1.0)
         if not is_auto_load:
             self.show_error_message(f"Помилка при застосуванні фільтрів: {e}")

 def get_orders_filter_params(self):
     """
     Збирає всі параметри фільтрації з інтерфейсу користувача.
     
     :return: Словник з параметрами фільтрації.
     """
     params = {}
     
     # Пошуковий запит
     search_text = self.orders_search_bar.text().strip()
     if search_text:
         params['search_text'] = search_text
     
     # Місяці
     params['month_min'] = self.month_min.value()
     params['month_max'] = self.month_max.value()
     
     # Роки
     params['year_min'] = self.year_min.value()
     params['year_max'] = self.year_max.value()
     
     # Статус відповіді
     selected_statuses = []
     for i, checkbox in enumerate(self.answer_status_checkboxes):
         if checkbox.isChecked():
             selected_statuses.append(checkbox.text())
     if selected_statuses:
         params['answer_statuses'] = selected_statuses
     
     # Статус оплати
     selected_payment_statuses = []
     for i, checkbox in enumerate(self.payment_status_checkboxes):
         if checkbox.isChecked():
             selected_payment_statuses.append(checkbox.text())
     if selected_payment_statuses:
         params['payment_statuses'] = selected_payment_statuses
     
     # Способи доставки
     selected_delivery_methods = []
     for i, checkbox in enumerate(self.delivery_checkboxes):
         if checkbox.isChecked():
             selected_delivery_methods.append(checkbox.text())
     if selected_delivery_methods:
         params['delivery_methods'] = selected_delivery_methods
     
     # Пріоритет
     if self.priority_combobox.currentIndex() > 1:  # Індекс > 1, бо 0 = "Пріоритет", 1 = "Будь-який"
         params['priority'] = self.priority_combobox.currentText()
     
     # Сортування
     if self.orders_sort_combobox.currentIndex() > 0:  # Індекс > 0, бо 0 = "Сортування"
         params['sort'] = self.orders_sort_combobox.currentIndex()
     
     return params

 async def reset_orders_filters(self):
     """
     Скидає всі фільтри до початкових значень і перезавантажує дані.
     """
     try:
         # Скидаємо пошуковий запит
         self.orders_search_bar.setText("")
         
         # Скидаємо чекбокси "Тільки неоплачені" та "Тільки оплачені"
         self.unpaid_checkbox.setChecked(False)
         self.paid_checkbox.setChecked(False)
         
         # Скидаємо значення місяців
         self.month_min.setValue(1)
         self.month_max.setValue(12)
         self.month_slider.setLow(1)
         self.month_slider.setHigh(12)
         
         # Скидаємо значення років
         self.year_min.setValue(2020)
         self.year_max.setValue(2030)
         self.year_slider.setLow(2020)
         self.year_slider.setHigh(2030)
         
         # Скидаємо вибрані статуси відповіді
         for checkbox in self.answer_status_checkboxes:
             checkbox.setChecked(False)
         
         # Скидаємо вибрані статуси оплати
         for checkbox in self.payment_status_checkboxes:
             checkbox.setChecked(False)
         
         # Скидаємо вибрані способи доставки
         for checkbox in self.delivery_checkboxes:
             checkbox.setChecked(False)
         
         # Скидаємо пріоритет
         self.priority_combobox.setCurrentIndex(0)
         
         # Скидаємо сортування
         self.orders_sort_combobox.setCurrentIndex(0)
         
         # Скидаємо фільтр дати
         self.selected_filter_date = None
         self._update_calendar_button_state()
         
         # Перезавантажуємо дані
         await self.apply_orders_filters()
         
     except Exception as e:
         logging.error(f"Помилка при скиданні фільтрів замовлень: {e}")
         logging.error(traceback.format_exc())
         self.show_error_message(f"Помилка при скиданні фільтрів: {e}")

 def update_orders_table(self, orders):
     """
     Оновлює таблицю замовлень новими даними.
     
     :param orders: Список об'єктів Order для відображення.
     """
     try:
         # Очищаємо таблицю
         self.orders_table.setRowCount(0)
         
         if not orders:
             return
         
         # Встановлюємо кількість рядків
         self.orders_table.setRowCount(len(orders))
         
         # Заповнюємо таблицю даними
         for row, order in enumerate(orders):
             # ID замовлення
             id_item = QTableWidgetItem(str(order.id))
             self.orders_table.setItem(row, 0, id_item)
             
             # Товари (отримуємо список з деталей замовлення)
             products_list = []
             if order.order_details:
                 for detail in order.order_details:
                     if detail.product:
                         # Виправлення: використовуємо productnumber замість name, яке відсутнє в моделі Product
                         product_info = f"{detail.product.productnumber or 'Невідомо'}<sup>{detail.quantity or 1}</sup>"
                         products_list.append(product_info)
             products_text = "; ".join(products_list) if products_list else "Немає товарів"
             
             # Створюємо QLabel з HTML-розміткою замість QTableWidgetItem
             from PyQt6.QtWidgets import QLabel
             products_label = QLabel(products_text)
             products_label.setTextFormat(Qt.TextFormat.RichText)
             
             # Зробимо стиль більш відповідним до стилю комірок таблиці "товари"
             # Збільшуємо лівий відступ для узгодження з таблицею товарів (з 10px до 15px)
             products_label.setFont(QFont("Arial", 13))
             # Встановлюємо стиль з урахуванням поточної теми
             text_color = "white" if self.is_dark_theme else "black"
             products_label.setStyleSheet(f"""
                 QLabel {{
                     padding: 5px 15px;
                     margin: 5px 0;
                     min-height: 30px;
                     background-color: transparent;
                     color: {text_color};
                 }}
             """)
             products_label.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
             
             # Встановлюємо внутрішні відступи для вирівнювання
             margins = products_label.contentsMargins()
             margins.setTop(-9)  # Збільшуємо від'ємний відступ зверху ще більше
             margins.setBottom(9)
             products_label.setContentsMargins(margins)
             
             # Зберігаємо мітку для можливості оновлення кольору при зміні теми
             products_label.setProperty("role", "product_cell")
             
             # Встановлюємо QLabel в комірку таблиці
             self.orders_table.setCellWidget(row, 1, products_label)
             
             # Клони номерів (якщо є)
             clones_item = QTableWidgetItem(order.alternative_order_number or "")
             self.orders_table.setItem(row, 2, clones_item)
             
             # Клієнт
             client_name = "Невідомо"
             if order.client:
                 client_name = f"{order.client.first_name or ''} {order.client.last_name or ''}".strip()
                 if not client_name:
                     client_name = "Без імені"
             client_item = QTableWidgetItem(client_name)
             self.orders_table.setItem(row, 3, client_item)
             
             # Ціна
             price_amount = order.total_amount or 0
             price_formatted = f"{int(price_amount)}" if price_amount == int(price_amount) else f"{price_amount}"
             price_item = QTableWidgetItem(price_formatted)
             self.orders_table.setItem(row, 4, price_item)
             
             # Додаткова операція
             extra_item = QTableWidgetItem("0")  # Цього поля немає в моделі Order
             self.orders_table.setItem(row, 5, extra_item)
             
             # Знижка
             discount_item = QTableWidgetItem("0")  # Цього поля немає в моделі Order
             self.orders_table.setItem(row, 6, discount_item)
             
             # Загальна сума
             total_amount = order.total_amount or 0
             total_formatted = f"{int(total_amount)}" if total_amount == int(total_amount) else f"{total_amount}"
             total_item = QTableWidgetItem(total_formatted)
             self.orders_table.setItem(row, 7, total_item)
             
             # Статус замовлення
             status_name = order.order_status.status_name if order.order_status else "Невідомо"
             status_item = QTableWidgetItem(status_name)
             self.orders_table.setItem(row, 8, status_item)
             
             # Статус оплати
             payment_status_name = order.payment_status.status_name if order.payment_status else "Невідомо"
             payment_status_item = QTableWidgetItem(payment_status_name)
             self.orders_table.setItem(row, 9, payment_status_item)
             
             # Метод оплати
             payment_method_name = order.payment_method.method_name if order.payment_method else "Невідомо"
             payment_method_item = QTableWidgetItem(payment_method_name)
             self.orders_table.setItem(row, 10, payment_method_item)
             
             # Уточнення
             specification_item = QTableWidgetItem(order.notes or "")  # Було 'specification', змінено на 'notes'
             self.orders_table.setItem(row, 11, specification_item)
             
             # Коментар
             comment_item = QTableWidgetItem(order.details or "")  # Було 'comment', змінено на 'details'
             self.orders_table.setItem(row, 12, comment_item)
             
             # Дата оплати
             payment_date = ""
             if order.payment_date:
                 payment_date = order.payment_date.strftime("%d.%m.%Y")
             payment_date_item = QTableWidgetItem(payment_date)
             self.orders_table.setItem(row, 13, payment_date_item)
             
             # Доставка
             delivery_method_name = order.delivery_method.method_name if order.delivery_method else "Невідомо"
             delivery_method_item = QTableWidgetItem(delivery_method_name)
             self.orders_table.setItem(row, 14, delivery_method_item)
             
             # Трек-номер
             tracking_item = QTableWidgetItem(order.tracking_number or "")
             self.orders_table.setItem(row, 15, tracking_item)
             
             # Отримувач
             recipient_name = order.recipient_name or "Невідомо"
             recipient_item = QTableWidgetItem(recipient_name)
             self.orders_table.setItem(row, 16, recipient_item)
             
             # Статус доставки - перевірте, чи є таке поле в моделі Order
             delivery_status_name = "Невідомо"  # Потрібно перевірити доступність цього поля
             delivery_status_item = QTableWidgetItem(delivery_status_name)
             self.orders_table.setItem(row, 17, delivery_status_item)
             
             # Дата замовлення
             order_date = ""
             if order.order_date:
                 order_date = order.order_date.strftime("%d.%m.%Y")
             order_date_item = QTableWidgetItem(order_date)
             self.orders_table.setItem(row, 18, order_date_item)
             
             # Пріоритет
             priority_item = QTableWidgetItem(str(order.priority or 0))
             self.orders_table.setItem(row, 19, priority_item)
         
         # Підганяємо ширину стовпців під вміст
         self.orders_table.resizeColumnsToContents()
         
         # Застосовуємо оптимальні розміри колонок
         self.apply_column_widths()
         
     except Exception as e:
         logging.error(f"Помилка при оновленні таблиці замовлень: {e}")
         logging.error(traceback.format_exc())
         self.show_error_message(f"Помилка при оновленні таблиці: {e}")

 def apply_column_widths(self):
     """
     Застосовує оптимальні розміри для колонок таблиці.
     """
     try:
         # Спочатку встановлюємо режим Stretch для всіх колонок
         self.orders_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
         
         # Колонки з коротким вмістом - ResizeToContents
         columns_to_optimize = [
             4,   # Ціна
             7,   # Сума
             8,   # Статус
             9,   # Статус оплати
             10,  # Метод оплати
             14,  # Доставка
             18   # Дата замовлення
         ]
         
         # Спочатку встановлюємо ResizeToContents для обчислення оптимальної ширини
         for col in columns_to_optimize:
             self.orders_table.horizontalHeader().setSectionResizeMode(col, QHeaderView.ResizeMode.ResizeToContents)
         
         # Потім фіксуємо ширину колонок, додаючи додатковий відступ
         for col in columns_to_optimize:
             # Отримуємо поточну ширину і додаємо до неї 25 пікселів
             current_width = self.orders_table.horizontalHeader().sectionSize(col)
             # Встановлюємо фіксовану ширину колонки з додатковим відступом
             self.orders_table.horizontalHeader().setSectionResizeMode(col, QHeaderView.ResizeMode.Fixed)
             self.orders_table.horizontalHeader().resizeSection(col, current_width + 25)
             
         # Колонки з детальною інформацією залишаються на Stretch
         detail_columns = [11, 12]  # Уточнення, Коментар
         for col in detail_columns:
             self.orders_table.horizontalHeader().setSectionResizeMode(col, QHeaderView.ResizeMode.Stretch)
     except Exception as e:
         logging.error(f"Помилка при застосуванні ширини колонок: {e}")

 def update_orders_page_buttons(self):
     """
     Оновлює кнопки пагінації на основі поточної сторінки та загальної кількості сторінок.
     """
     try:
         # Очищаємо поточні кнопки
         while self.orders_page_buttons_layout.count():
             item = self.orders_page_buttons_layout.takeAt(0)
             if item.widget():
                 item.widget().deleteLater()
         
         # Якщо сторінок менше 2, не показуємо пагінацію
         if self.total_pages < 2:
             return
         
         # Ультрамінімалістичний стиль пагінації
         if self.is_dark_theme:
             btn_style = """
                 QPushButton {
                     background-color: transparent;
                     color: #777777;
                     border: none;
                     font-size: 10pt;
                     min-width: 24px;
                     max-width: 24px;
                     min-height: 24px;
                     max-height: 24px;
                     margin: 0px;
                     padding: 0px;
                 }
                 QPushButton:hover {
                     color: #ffffff;
                 }
                 QPushButton:disabled {
                     color: #444444;
                 }
             """
             active_btn_style = """
                 QPushButton {
                     background-color: #7851A9;
                     color: white;
                     border: none;
                     border-radius: 2px;
                     font-size: 10pt;
                     font-weight: bold;
                     min-width: 24px;
                     max-width: 24px;
                     min-height: 24px;
                     max-height: 24px;
                     margin: 0px;
                     padding: 0px;
                 }
             """
         else:
             btn_style = """
                 QPushButton {
                     background-color: transparent;
                     color: #777777;
                     border: none;
                     font-size: 10pt;
                     min-width: 24px;
                     max-width: 24px;
                     min-height: 24px;
                     max-height: 24px;
                     margin: 0px;
                     padding: 0px;
                 }
                 QPushButton:hover {
                     color: #333333;
                 }
                 QPushButton:disabled {
                     color: #cccccc;
                 }
             """
             active_btn_style = """
                 QPushButton {
                     background-color: #7851A9;
                     color: white;
                     border: none;
                     border-radius: 2px;
                     font-size: 10pt;
                     font-weight: bold;
                     min-width: 24px;
                     max-width: 24px;
                     min-height: 24px;
                     max-height: 24px;
                     margin: 0px;
                     padding: 0px;
                 }
             """
         
         # Кнопка "на першу сторінку"
         first_button = QPushButton("«")
         first_button.setStyleSheet(btn_style)
         first_button.setEnabled(self.current_page > 1)
         first_button.clicked.connect(lambda: self.go_to_orders_page(1))
         self.orders_page_buttons_layout.addWidget(first_button)
         
         # Кнопка "назад"
         prev_button = QPushButton("‹")
         prev_button.setStyleSheet(btn_style)
         prev_button.setEnabled(self.current_page > 1)
         prev_button.clicked.connect(lambda: self.go_to_orders_page(self.current_page - 1))
         self.orders_page_buttons_layout.addWidget(prev_button)
         
         # Визначаємо діапазон сторінок для відображення
         # Показуємо максимально 5 сторінок: поточну, 2 до і 2 після
         start_page = max(1, self.current_page - 2)
         end_page = min(self.total_pages, start_page + 4)
         
         # Якщо показуємо менше 5 сторінок в кінці, то показуємо більше на початку
         if end_page - start_page < 4:
             start_page = max(1, end_page - 4)
         
         # Додаємо кнопки для кожної сторінки в діапазоні
         for page in range(start_page, end_page + 1):
             page_button = QPushButton(str(page))
             
             # Виділяємо поточну сторінку
             if page == self.current_page:
                 page_button.setStyleSheet(active_btn_style)
             else:
                 page_button.setStyleSheet(btn_style)
                 
             page_button.clicked.connect(lambda _, p=page: self.go_to_orders_page(p))
             self.orders_page_buttons_layout.addWidget(page_button)
         
         # Кнопка "вперед"
         next_button = QPushButton("›")
         next_button.setStyleSheet(btn_style)
         next_button.setEnabled(self.current_page < self.total_pages)
         next_button.clicked.connect(lambda: self.go_to_orders_page(self.current_page + 1))
         self.orders_page_buttons_layout.addWidget(next_button)
         
         # Кнопка "на останню сторінку"
         last_button = QPushButton("»")
         last_button.setStyleSheet(btn_style)
         last_button.setEnabled(self.current_page < self.total_pages)
         last_button.clicked.connect(lambda: self.go_to_orders_page(self.total_pages))
         self.orders_page_buttons_layout.addWidget(last_button)
         
     except Exception as e:
         logging.error(f"Помилка при оновленні кнопок пагінації: {e}")
         logging.error(traceback.format_exc())

 def go_to_orders_page(self, page):
     """
     Переходить на вказану сторінку замовлень.
     
     :param page: Номер сторінки, на яку потрібно перейти.
     """
     if page != self.current_page and 1 <= page <= self.total_pages:
         self.current_page = page
         # Перезавантажуємо дані для нової сторінки
         asyncio.ensure_future(self.apply_orders_filters())
         
         # Після оновлення даних переконуємося, що таблиця розтягується правильно
         self.orders_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
         # Застосовуємо оптимальні розміри колонок замість встановлення всіх на Stretch
         self.apply_column_widths()

 def show_error_message(self, message):
     """
     Показує повідомлення про помилку користувачу.
     
     :param message: Текст повідомлення про помилку.
     """
     QMessageBox.critical(self, "Помилка", message)

 def show_orders_cell_info(self, row, column):
     """
     Показує повну інформацію про вміст комірки при подвійному кліку.
     
     :param row: Індекс рядка.
     :param column: Індекс стовпця.
     """
     try:
         # Отримуємо текст з комірки
         item = self.orders_table.item(row, column)
         if not item:
             return
         
         text = item.text()
         if not text:
             return
         
         # Створюємо діалогове вікно для показу повного тексту
         detail_dialog = QDialog(self)
         detail_dialog.setWindowTitle(f"Інформація - {self.orders_column_names[column]}")
         detail_dialog.setMinimumSize(400, 300)
         
         layout = QVBoxLayout(detail_dialog)
         layout.setContentsMargins(20, 20, 20, 20)
         
         # Додаємо мітку з повним текстом
         label = QLabel(text)
         label.setWordWrap(True)
         label.setFont(QFont("Arial", 13))
         label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
         
         layout.addWidget(label)
         
         # Додаємо кнопку закриття
         close_button = QPushButton("Закрити")
         close_button.clicked.connect(detail_dialog.accept)
         close_button.setFont(QFont("Arial", 13))
         
         layout.addWidget(close_button, 0, Qt.AlignmentFlag.AlignCenter)
         
         # Показуємо діалогове вікно
         detail_dialog.exec()
         
     except Exception as e:
         logging.error(f"Помилка при показі інформації про комірку: {e}")
         logging.error(traceback.format_exc())
         self.show_error_message(f"Помилка при показі інформації: {e}")

 def select_orders_column(self, column_index):
     """
     Обробник кліку на заголовок стовпця.
     Виділяє всю колонку при натисканні на заголовок.
     
     :param column_index: Індекс вибраного стовпця.
     """
     # Тимчасово міняємо режим виділення на виділення колонок
     self.orders_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectColumns)
     # Виділяємо колонку
     self.orders_table.selectColumn(column_index)
     # Повертаємо режим виділення на виділення рядків
     self.orders_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)

 def toggle_orders_column(self, column_index, state):
     """
     Перемикає видимість стовпця в таблиці.
     
     :param column_index: Індекс стовпця.
     :param state: Новий стан (ввімкнено/вимкнено).
     """
     try:
         # Змінюємо видимість стовпця залежно від стану чекбокса
         self.orders_table.setColumnHidden(column_index, not state)
         
         # Підганяємо ширину стовпців під вміст, якщо стовпець став видимим
         if state:
             self.orders_table.resizeColumnToContents(column_index)
             
     except Exception as e:
         logging.error(f"Помилка при перемиканні видимості стовпця: {e}")
         logging.error(traceback.format_exc())
         self.show_error_message(f"Помилка при перемиканні видимості стовпця: {e}")

 async def run_orders_parsing_script(self):
     """
     Запускає скрипт парсингу замовлень з Google Sheets.
     """
     try:
         # Показуємо індикатор завантаження
         self.orders_opacity_effect.setOpacity(0.7)
         QApplication.processEvents()
         
         logging.info("Запуск парсингу замовлень з Google Sheets...")
         
         # Створюємо та запускаємо воркер для парсингу в окремому потоці
         worker = OrderParsingWorker()
         result = await asyncio.to_thread(worker.parse_orders)
         
         if result:
             logging.info("Парсинг замовлень завершено успішно. Оновлюємо таблицю...")
             # Оновлюємо таблицю після успішного парсингу
             await self.apply_orders_filters()
         else:
             logging.error("Помилка при парсингу замовлень")
             self.show_error_message("Помилка при парсингу замовлень")
         
         # Відновлюємо прозорість таблиці
         self.orders_opacity_effect.setOpacity(1.0)
         
     except Exception as e:
         logging.error(f"Помилка при запуску скрипта парсингу замовлень: {e}")
         logging.error(traceback.format_exc())
         self.orders_opacity_effect.setOpacity(1.0)
         self.show_error_message(f"Помилка при парсингу замовлень: {e}")
     
 def showEvent(self, event):
     """
     Обробник події відображення вкладки.
     Автоматично завантажує дані, якщо вони ще не завантажені.
     """
     super().showEvent(event)
     
     # Перевіряємо, чи дані вже завантажені
     if not self.data_loaded and hasattr(self, 'all_orders') and self.all_orders is not None and len(self.all_orders) == 0:
         logging.info("OrdersTab: автоматичне завантаження даних при відображенні")
         # Запускаємо асинхронне завантаження з параметром is_initial_load=True
         asyncio.ensure_future(self.apply_orders_filters(is_initial_load=True))
     else:
         logging.debug("OrdersTab: дані вже завантажені, автозавантаження не потрібне")

 def show_date_picker(self):
     """
     Відкриває діалогове вікно для вибору дати фільтрації.
     """
     try:
         # Створюємо діалогове вікно для вибору дати
         date_dialog = QDialog(self)
         date_dialog.setWindowTitle("Вибір дати")
         date_dialog.setFixedSize(380, 400)
         
         # Налаштовуємо стиль діалогу залежно від поточної теми
         if self.is_dark_theme:
             dialog_style = """
                 QDialog {
                     background-color: #333333;
                     color: #ffffff;
                 }
                 QLabel {
                     color: #ffffff;
                     font-size: 13pt;
                 }
                 QPushButton {
                     background-color: #444444;
                     color: #ffffff;
                     border: 1px solid #555555;
                     border-radius: 5px;
                     padding: 8px 15px;
                     font-size: 12pt;
                 }
                 QPushButton:hover {
                     background-color: #555555;
                 }
                 QPushButton#clearButton {
                     background-color: #555555;
                     color: #ffffff;
                 }
                 QPushButton#clearButton:hover {
                     background-color: #666666;
                 }
                 QPushButton#okButton {
                     background-color: #7851A9;
                     color: #ffffff;
                 }
                 QPushButton#okButton:hover {
                     background-color: #6a4697;
                 }
             """
         else:
             dialog_style = """
                 QDialog {
                     background-color: #f5f5f5;
                     color: #333333;
                 }
                 QLabel {
                     color: #333333;
                     font-size: 13pt;
                 }
                 QPushButton {
                     background-color: #f0f0f0;
                     color: #333333;
                     border: 1px solid #cccccc;
                     border-radius: 5px;
                     padding: 8px 15px;
                     font-size: 12pt;
                 }
                 QPushButton:hover {
                     background-color: #e0e0e0;
                 }
                 QPushButton#clearButton {
                     background-color: #e0e0e0;
                     color: #333333;
                 }
                 QPushButton#clearButton:hover {
                     background-color: #d0d0d0;
                 }
                 QPushButton#okButton {
                     background-color: #7851A9;
                     color: #ffffff;
                 }
                 QPushButton#okButton:hover {
                     background-color: #6a4697;
                 }
             """
         
         date_dialog.setStyleSheet(dialog_style)
         
         # Створюємо основний макет діалогу
         dialog_layout = QVBoxLayout(date_dialog)
         dialog_layout.setContentsMargins(20, 20, 20, 20)
         dialog_layout.setSpacing(15)
         
         # Додаємо віджет календаря
         calendar = QCalendarWidget()
         calendar.setGridVisible(True)
         calendar.setFont(QFont("Arial", 12))
         
         # Стилізуємо календар залежно від теми
         calendar_style = self._get_calendar_style()
         calendar.setStyleSheet(calendar_style)
         
         # Встановлюємо поточну вибрану дату, якщо вона є
         if hasattr(self, 'selected_filter_date') and self.selected_filter_date:
             calendar.setSelectedDate(self.selected_filter_date)
         
         dialog_layout.addWidget(calendar)
         
         # Додаємо рядок кнопок
         buttons_layout = QHBoxLayout()
         buttons_layout.setSpacing(10)
         
         # Кнопка "Очистити"
         clear_button = QPushButton("Очистити")
         clear_button.setFont(QFont("Arial", 12))
         clear_button.setObjectName("clearButton")
         clear_button.clicked.connect(lambda: self._on_date_cleared(date_dialog))
         buttons_layout.addWidget(clear_button)
         
         # Кнопка "OK"
         ok_button = QPushButton("OK")
         ok_button.setFont(QFont("Arial", 12))
         ok_button.setObjectName("okButton")
         ok_button.clicked.connect(lambda: self._on_date_selected(calendar.selectedDate(), date_dialog))
         buttons_layout.addWidget(ok_button)
         
         dialog_layout.addLayout(buttons_layout)
         
         # Показуємо діалогове вікно
         date_dialog.exec()
         
     except Exception as e:
         logging.error(f"Помилка при відкритті календаря: {e}")
         logging.error(traceback.format_exc())
         self.show_error_message(f"Помилка при відкритті календаря: {e}")

 def _get_calendar_style(self):
     """
     Повертає стиль для віджета календаря залежно від поточної теми.
     """
     if self.is_dark_theme:
         return """
             QCalendarWidget {
                 background-color: #333333;
                 color: white;
             }
             QCalendarWidget QToolButton {
                 color: white;
                 background-color: #444444;
                 border: 1px solid #555555;
                 border-radius: 4px;
                 padding: 3px;
                 font-size: 12pt;
             }
             QCalendarWidget QToolButton:hover {
                 background-color: #555555;
             }
             QCalendarWidget QMenu {
                 background-color: #444444;
                 color: white;
             }
             QCalendarWidget QSpinBox {
                 background-color: #444444;
                 color: white;
                 selection-background-color: #666666;
                 selection-color: white;
             }
             QCalendarWidget QTableView {
                 background-color: #333333;
                 selection-background-color: #7851A9;
                 selection-color: white;
                 alternate-background-color: #383838;
             }
             QCalendarWidget QAbstractItemView:enabled {
                 color: white;
                 background-color: #333333;
                 selection-background-color: #7851A9;
                 selection-color: white;
             }
             QCalendarWidget QAbstractItemView:disabled {
                 color: #666666;
             }
         """
     else:
         return """
             QCalendarWidget {
                 background-color: white;
                 color: #333333;
                 border: 1px solid #cccccc;
             }
             QCalendarWidget QToolButton {
                 color: #333333;
                 background-color: #f0f0f0;
                 border: 1px solid #cccccc;
                 border-radius: 4px;
                 padding: 3px;
                 font-size: 12pt;
             }
             QCalendarWidget QToolButton:hover {
                 background-color: #e0e0e0;
             }
             QCalendarWidget QMenu {
                 background-color: white;
                 color: #333333;
             }
             QCalendarWidget QSpinBox {
                 background-color: white;
                 color: #333333;
                 selection-background-color: #e0e0e0;
                 selection-color: #333333;
             }
             QCalendarWidget QTableView {
                 background-color: white;
                 selection-background-color: #7851A9;
                 selection-color: white;
                 alternate-background-color: #f5f5f5;
             }
             QCalendarWidget QAbstractItemView:enabled {
                 color: #333333;
                 background-color: white;
                 selection-background-color: #7851A9;
                 selection-color: white;
             }
             QCalendarWidget QAbstractItemView:disabled {
                 color: #aaaaaa;
             }
         """

 def _on_date_selected(self, date, dialog):
     """
     Обробник вибору дати у календарі.
     
     :param date: Вибрана дата (QDate).
     :param dialog: Діалогове вікно календаря.
     """
     self.selected_filter_date = date
     self._update_calendar_button_state()
     
     # Закриваємо діалогове вікно
     dialog.accept()
     
     # Застосовуємо фільтри з новою датою
     asyncio.ensure_future(self.apply_orders_filters())

 def _on_date_cleared(self, dialog):
     """
     Обробник очищення вибраної дати.
     
     :param dialog: Діалогове вікно календаря.
     """
     self.selected_filter_date = None
     self._update_calendar_button_state()
     
     # Закриваємо діалогове вікно
     dialog.accept()
     
     # Застосовуємо фільтри без дати
     asyncio.ensure_future(self.apply_orders_filters())

 def on_orders_selection_changed(self):
     """Оновлює колір тексту в QLabel при виділенні рядка в таблиці замовлень"""
     # Текст білого кольору для виділених рядків, звичайний колір для інших
     selected_rows = set(index.row() for index in self.orders_table.selectedIndexes())
     
     for row in range(self.orders_table.rowCount()):
         # Визначаємо, чи є рядок виділеним
         is_selected = row in selected_rows
         
         # Для колонки з товарами (колонка 1)
         label = self.orders_table.cellWidget(row, 1)
         if label and isinstance(label, QLabel) and label.property("role") == "product_cell":
             text_color = "white" if is_selected else ("white" if self.is_dark_theme else "black")
             label.setStyleSheet(f"""
                 QLabel {{
                     padding: 0px 15px;
                     margin: 0px;
                     min-height: 38px;
                     background-color: transparent;
                     color: {text_color};
                 }}
             """)
