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
 QThread
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

     for idx in self.orders_optional_indices:
         self.orders_table.setColumnHidden(idx, True)

     self.orders_opacity_effect = QGraphicsOpacityEffect()
     self.orders_table.setGraphicsEffect(self.orders_opacity_effect)

     center_layout.addWidget(self.orders_table, 10)

     # Пагінація
     self.orders_pagination_layout = QHBoxLayout()
     self.orders_pagination_layout.setContentsMargins(0, 0, 0, 0)
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

     self.set_orders_scroll_style()

     # Прив'язуємо сигнали
     self.month_slider.valueChanged.connect(self.on_slider_value_changed)
     self.year_slider.valueChanged.connect(self.on_slider_value_changed)
     self.month_min.valueChanged.connect(self.on_spinbox_value_changed)
     self.month_max.valueChanged.connect(self.on_spinbox_value_changed)
     self.year_min.valueChanged.connect(self.on_spinbox_value_changed)
     self.year_max.valueChanged.connect(self.on_spinbox_value_changed)

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
         transition: width 0.3s;
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
         transition: height 0.3s;
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
         transition: width 0.3s;
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
         transition: height 0.3s;
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

     self.update_orders_page_buttons()

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
     order_statuses = session.query(OrderStatus).order_by(OrderStatus.status_name).all()
     statuses_list = [st.status_name for st in order_statuses]
     self.answer_status_section = FilterSection(
         "Статус відповіді", items=statuses_list, columns=4, maxHeight=600
     )
     self.answer_status_section.toggle_animation.setDuration(500)
     self.answer_status_section.toggle_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)
     self.answer_status_section.toggle_button.setChecked(False)
     self.answer_status_section.on_toggle()
     self.answer_status_checkboxes = self.answer_status_section.all_checkboxes
     self.answer_status_section.checkbox_state_changed.connect(self.on_checkbox_state_changed)
     self.answer_status_section.toggle_animation_finished.connect(self.on_filters_panel_toggled)
     left_layout.addWidget(self.answer_status_section)

     # Статус оплати
     pay_statuses = session.query(PaymentStatus).order_by(PaymentStatus.status_name).all()
     pay_status_list = [p.status_name for p in pay_statuses]
     self.payment_status_section = FilterSection(
         "Статус оплати", items=pay_status_list, columns=4, maxHeight=600
     )
     self.payment_status_section.toggle_animation.setDuration(500)
     self.payment_status_section.toggle_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)
     self.payment_status_section.toggle_button.setChecked(False)
     self.payment_status_section.on_toggle()
     self.payment_status_checkboxes = self.payment_status_section.all_checkboxes
     self.payment_status_section.checkbox_state_changed.connect(self.on_checkbox_state_changed)
     self.payment_status_section.toggle_animation_finished.connect(self.on_filters_panel_toggled)
     left_layout.addWidget(self.payment_status_section)

     # Доставка
     delivery_methods = session.query(DeliveryMethod).order_by(DeliveryMethod.method_name).all()
     delivery_method_list = [dm.method_name for dm in delivery_methods]
     self.delivery_section = FilterSection(
         "Доставка", items=delivery_method_list, columns=4, maxHeight=600
     )
     self.delivery_section.toggle_animation.setDuration(500)
     self.delivery_section.toggle_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)
     self.delivery_section.toggle_button.setChecked(False)
     self.delivery_section.on_toggle()
     self.delivery_checkboxes = self.delivery_section.all_checkboxes
     self.delivery_section.checkbox_state_changed.connect(self.on_checkbox_state_changed)
     self.delivery_section.toggle_animation_finished.connect(self.on_filters_panel_toggled)
     left_layout.addWidget(self.delivery_section)

     left_layout.addStretch(0)

     # Права частина
     month_label = QLabel("По місяцях")
     month_label.setFont(QFont("Arial", 11))

     self.month_min = QSpinBox()
     self.month_min.setFont(QFont("Arial", 13))
     self.month_min.setPrefix("Від ")
     self.month_min.setMinimum(1)
     self.month_min.setMaximum(12)
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

     year_label = QLabel("Рік")
     year_label.setFont(QFont("Arial", 11))

     self.year_min = QSpinBox()
     self.year_min.setFont(QFont("Arial", 13))
     self.year_min.setPrefix("Від ")
     self.year_min.setMinimum(2020)
     self.year_min.setMaximum(2030)
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

     # Приклад "soon_slider" (неактивний)
     self.soon_slider = RangeSlider()
     self.soon_slider.setRange(0, 100)
     self.soon_slider.setLow(0)
     self.soon_slider.setHigh(100)
     self.soon_slider.setMinimumWidth(800)
     self.soon_slider.left_margin = 0
     self.soon_slider.right_margin = 9
     self.soon_slider.setEnabled(False)
     grey_style = "QSlider { background-color: #dcdcdc; }"
     self.soon_slider.setStyleSheet(grey_style)

     # combobox style
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

     def show_calendar():
         dlg = QDialog(self)
         dlg.setWindowTitle("Оберіть дату")
         layout = QVBoxLayout(dlg)
         cal = QCalendarWidget(dlg)
         cal.setGridVisible(True)
         layout.addWidget(cal)
         btn_box = QHBoxLayout()
         ok_btn = QPushButton("OK", dlg)
         cancel_btn = QPushButton("Скасувати", dlg)
         btn_box.addWidget(ok_btn)
         btn_box.addWidget(cancel_btn)
         layout.addLayout(btn_box)

         def on_ok():
             dlg.accept()

         def on_cancel():
             dlg.reject()

         ok_btn.clicked.connect(on_ok)
         cancel_btn.clicked.connect(on_cancel)
         dlg.exec()

     self.calendar_button.clicked.connect(show_calendar)

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

     # Повертаємо розташування елементів до одного рядка, як на вкладці Products
     filters_btn_layout = QHBoxLayout()
     filters_btn_layout.setSpacing(10)
     filters_btn_layout.setContentsMargins(0, 5, 0, 0)
     
     # Всі елементи в одному рядку
     filters_btn_layout.addWidget(self.orders_sort_combobox)
     filters_btn_layout.addWidget(self.calendar_button)
     filters_btn_layout.addWidget(self.priority_combobox)
     # Додає еластичний простір між елементами та кнопкою Reset
     filters_btn_layout.addStretch(1)
     # Кнопка Reset справа
     filters_btn_layout.addWidget(self.reset_button_orders)
     
     right_layout.addLayout(filters_btn_layout)

     # Прив'язуємо сигнали
     self.month_slider.valueChanged.connect(self.on_slider_value_changed)
     self.year_slider.valueChanged.connect(self.on_slider_value_changed)
     self.month_min.valueChanged.connect(self.on_spinbox_value_changed)
     self.month_max.valueChanged.connect(self.on_spinbox_value_changed)
     self.year_min.valueChanged.connect(self.on_spinbox_value_changed)
     self.year_max.valueChanged.connect(self.on_spinbox_value_changed)

 def on_filters_panel_toggled(self):
     self.toggle_animation_finished.emit()

 def on_checkbox_state_changed(self):
     """
     Обробляє зміну стану чекбокса в будь-якому фільтрі.
     Запускає фільтрацію даних.
     """
     # Перевіряємо, чи не обрані одночасно обидва чекбокси "Тільки оплачені" і "Тільки неоплачені"
     if hasattr(self, 'unpaid_checkbox') and hasattr(self, 'paid_checkbox'):
         if self.unpaid_checkbox.isChecked() and self.paid_checkbox.isChecked():
             # Якщо змінився paid_checkbox, вимикаємо unpaid_checkbox і навпаки
             sender = self.sender()
             if sender == self.unpaid_checkbox:
                 self.paid_checkbox.blockSignals(True)
                 self.paid_checkbox.setChecked(False)
                 self.paid_checkbox.blockSignals(False)
             else:
                 self.unpaid_checkbox.blockSignals(True)
                 self.unpaid_checkbox.setChecked(False)
                 self.unpaid_checkbox.blockSignals(False)
     
     # Запускаємо пошук, але переконуємося, що старі завдання скасовані
     asyncio.ensure_future(self.apply_orders_filters())

 def on_slider_value_changed(self):
     if self.data_loaded:
         self.search_timer.start(300)

 def on_spinbox_value_changed(self):
     if self.data_loaded:
         self.search_timer.start(300)

 async def run_orders_parsing_script(self):
     """
     Запускає процес оновлення даних через універсальний метод головного вікна.
     Цей метод залишено для сумісності.
     """
     if self.parent_window:
         self.parent_window.show_update_dialog_and_parse()
     else:
         # Якщо з якоїсь причини головне вікно недоступне, використовуємо запасний варіант
         self.parent_window.start_universal_parsing()

 async def apply_orders_filters(self, is_initial_load=False, is_auto_load=False):
    # Скасувати попереднє завдання, якщо воно існує та виконується
    if hasattr(self, 'filter_task') and self.filter_task and not self.filter_task.done():
        self.filter_task.cancel()
        try:
            await self.filter_task
        except asyncio.CancelledError:
            logging.debug("Попереднє завдання фільтрації скасовано")
        except Exception as e:
            logging.error(f"Помилка при скасуванні попереднього завдання: {str(e)}")
    
    # Показуємо індикатор прогресу, якщо це не автоматичне завантаження
    if not is_auto_load:
        self.parent_window.show_progress_bar(True)
    
    # Показуємо повідомлення про початок застосування фільтрів, якщо це не перше завантаження і не автоматичне
    if not is_initial_load and not is_auto_load:
        self.parent_window.set_status_message("Застосування фільтрів замовлень...")
    
    try:
        # Отримуємо параметри фільтрів
        from services.filter_service import build_orders_query_params
        filter_params = build_orders_query_params(self)
        logging.info(f"Застосовуємо фільтри замовлень з параметрами: {filter_params}")
        
        # Логуємо стан чекбоксів "Тільки неоплачені" та "Тільки оплачені"
        unpaid_checked = self.unpaid_checkbox.isChecked() if hasattr(self, 'unpaid_checkbox') else False
        paid_checked = self.paid_checkbox.isChecked() if hasattr(self, 'paid_checkbox') else False
        logging.info(f"Стан фільтрів оплати: unpaid_checkbox={unpaid_checked}, paid_checkbox={paid_checked}")
        
        # Завантажуємо всі замовлення з бази даних
        orders_data = await self.async_load_orders(filter_params)
        
        # Логуємо кількість отриманих замовлень
        logging.info(f"Отримано {len(orders_data)} замовлень для відображення")
        
        # Завантажуємо дані та оновлюємо інтерфейс
        self.load_orders(orders_data)
        
        # Якщо це не перше завантаження і не автоматичне завантаження, показуємо успішне повідомлення
        if not is_initial_load and not is_auto_load:
            self.parent_window.set_status_message("Фільтри застосовано успішно", 3000)
            logging.info("Фільтри замовлень застосовано успішно")
        
    except Exception as e:
        session.rollback()
        logging.error(f"Помилка при застосуванні фільтрів: {str(e)}")
        logging.error(traceback.format_exc())
        if not is_auto_load:  # Не показуємо помилку при автозавантаженні
            self.show_error_message(f"Помилка при застосуванні фільтрів: {str(e)}")
    finally:
        if not is_auto_load:  # Не приховуємо прогрес-бар при автозавантаженні
            self.parent_window.show_progress_bar(False)

 async def async_load_orders(self, filter_params=None):
    def blocking_query():
        from services.filter_service import build_orders_query_params
        
        # Отримуємо всі параметри фільтрів, якщо не передані
        if filter_params is None:
            params = build_orders_query_params(self)
        else:
            params = filter_params
        
        logging.info(f"Застосовую фільтри пошуку: {params}")
        
        session.rollback()
        q = session.query(Order).options(
            joinedload(Order.client),
            joinedload(Order.order_status),
            joinedload(Order.payment_status),
            joinedload(Order.payment_method),
            joinedload(Order.delivery_method),
            joinedload(Order.order_details).joinedload(OrderDetail.product)
        )
        
        # Застосовуємо текст пошуку, якщо він є
        search_text = params.get('search_text')
        if search_text:
            q = q.join(Client, Order.client_id == Client.id, isouter=True)
            q = q.outerjoin(OrderDetail, Order.id == OrderDetail.order_id)
            q = q.outerjoin(Product, OrderDetail.product_id == Product.id)
            
            q = q.filter(
                or_(
                    Client.first_name.ilike(f"%{search_text}%"),
                    Client.last_name.ilike(f"%{search_text}%"),
                    Order.tracking_number.ilike(f"%{search_text}%"),
                    Product.productnumber.ilike(f"%{search_text}%"),
                    Order.id.cast(String).like(f"%{search_text}%")
                )
            )
            # Distinct щоб уникнути дублікатів через JOIN
            q = q.distinct()
        
        # Застосовуємо фільтри відповідно до вибраних чекбоксів
        answer_statuses = params.get('answer_statuses')
        if answer_statuses:
            q = q.join(OrderStatus, Order.order_status_id == OrderStatus.id)
            q = q.filter(OrderStatus.status_name.in_(answer_statuses))
        
        payment_statuses = params.get('payment_statuses')
        if payment_statuses:
            q = q.join(PaymentStatus, Order.payment_status_id == PaymentStatus.id)
            q = q.filter(PaymentStatus.status_name.in_(payment_statuses))
        
        delivery_methods = params.get('delivery_methods')
        if delivery_methods:
            q = q.join(DeliveryMethod, Order.delivery_method_id == DeliveryMethod.id)
            q = q.filter(DeliveryMethod.method_name.in_(delivery_methods))
        
        # Фільтр по місяцях
        month_min = params.get('month_min', 1)
        month_max = params.get('month_max', 12)
        if month_min > 1 or month_max < 12:
            q = q.filter(func.extract('month', Order.order_date) >= month_min)
            q = q.filter(func.extract('month', Order.order_date) <= month_max)
        
        # Фільтр по роках
        year_min = params.get('year_min', 2020)
        year_max = params.get('year_max', 2030)
        if year_min > 2020 or year_max < 2030:
            q = q.filter(func.extract('year', Order.order_date) >= year_min)
            q = q.filter(func.extract('year', Order.order_date) <= year_max)
        
        # Сортування
        sort_option = params.get('sort_option')
        if sort_option == "Від дорожчого":
            q = q.order_by(Order.total_amount.desc())
        elif sort_option == "Від найдешевшого":
            q = q.order_by(Order.total_amount.asc())
        elif sort_option == "Від найбільшого (по кількості)":
            # Підрахунок кількості товарів у замовленні
            q = q.outerjoin(OrderDetail, Order.id == OrderDetail.order_id)
            q = q.group_by(Order.id)
            q = q.order_by(func.count(OrderDetail.id).desc())
        elif sort_option == "Від найдавнішого":
            q = q.order_by(Order.order_date.asc())
        elif sort_option == "Від найновішого":
            q = q.order_by(Order.order_date.desc())
        else:
            # За замовчуванням - від нових до старих
            q = q.order_by(Order.id.desc())
        
        # Фільтр за пріоритетом
        priority = params.get('priority')
        if priority and priority not in ["Будь-який", "Пріоритет"]:
            q = q.filter(Order.priority == int(priority))
        
        # Фільтр "Тільки неоплачені"
        if params.get('unpaid_only', False):
            q = fix_unpaid_filter(q, session)
        
        # Фільтр "Тільки оплачені"
        if params.get('paid_only', False):
            q = fix_paid_filter(q, session)
        
        results = q.all()

        order_list = []
        for od in results:
            client_obj = od.client
            if client_obj:
                client_display = f"{client_obj.first_name or ''} {client_obj.last_name or ''}".strip()
            else:
                client_display = ""

            order_status_text = od.order_status.status_name if od.order_status else ""
            payment_method_text = od.payment_method.method_name if od.payment_method else ""
            payment_status_text = od.payment_status.status_name if od.payment_status else (od.payment_status or "")
            delivery_method_text = od.delivery_method.method_name if od.delivery_method else ""

            dstat_obj = None
            if od.delivery_status_id:
                dstat_obj = session.query(DeliveryStatus).filter_by(id=od.delivery_status_id).first()
            delivery_status_text = dstat_obj.status_name if dstat_obj else ""

            product_numbers = []
            cloned_numbers = []
            product_prices = []
            discount_list = []
            additional_ops = []

            for det in od.order_details:
                pr = det.product
                if pr:
                    product_numbers.append(pr.productnumber or "")
                    cloned_numbers.append(pr.clonednumbers or "")
                else:
                    product_numbers.append("N/A")
                    cloned_numbers.append("")

                if det.price is not None:
                    product_prices.append(f"{float(det.price):.2f}".rstrip("0").rstrip("."))
                else:
                    product_prices.append("")

                ao = ""
                if det.additional_operation and det.additional_operation_value:
                    ao_val = float(det.additional_operation_value)
                    ao = f"{det.additional_operation} ({ao_val:+.2f})"
                additional_ops.append(ao)

                ds = ""
                if det.discount_type == "Відсоток" and det.discount_value is not None:
                    ds = f"{float(det.discount_value)}%"
                elif det.discount_type == "Фіксована" and det.discount_value is not None:
                    ds = f"{float(det.discount_value):.2f}"
                discount_list.append(ds)

            joined_products = ", ".join(product_numbers)
            joined_clones = ", ".join(cloned_numbers).strip().replace(";;", ";").replace("\n", " ")
            joined_prices = ", ".join(product_prices)
            joined_addops = ", ".join(a for a in additional_ops if a)
            joined_discounts = ", ".join(d for d in discount_list if d)

            odata = {
                'id': od.id,
                'client': client_display,
                'order_date': od.order_date,
                'order_status_text': order_status_text,
                'total_amount': od.total_amount,
                'payment_method_text': payment_method_text,
                'payment_status_text': payment_status_text,
                'payment_date': od.payment_date,
                'delivery_method_text': delivery_method_text,
                'delivery_status_text': delivery_status_text,
                'tracking_number': od.tracking_number or "",
                'recipient_name': od.recipient_name or "",
                'notes': od.notes or "",
                'priority': od.priority,
                'products_str': joined_products,
                'clones_str': joined_clones,
                'prices_str': joined_prices,
                'discount_str': joined_discounts,
                'addops_str': joined_addops,
                'order_date_str': od.order_date.strftime("%d.%m.%Y") if od.order_date else "",
                'payment_date_str': od.payment_date.strftime("%d.%m.%Y") if od.payment_date else ""
            }
            order_list.append(odata)
        return order_list

    return await asyncio.to_thread(blocking_query)

 def load_orders(self, orders_data):
     self.all_orders = orders_data
     total_orders = len(self.all_orders)
     self.total_pages = (total_orders // self.page_size) + (1 if total_orders % self.page_size != 0 else 0)
     if self.total_pages == 0:
         self.total_pages = 1
     if self.current_page > self.total_pages:
         self.current_page = self.total_pages
     self.data_loaded = True
     
     # Перевіряємо, чи активний пошук
     search_text = self.orders_search_bar.text().strip() if hasattr(self, 'orders_search_bar') else ""
     has_answer_filter = any(cb.isChecked() for cb in getattr(self, 'answer_status_checkboxes', []))
     has_payment_filter = any(cb.isChecked() for cb in getattr(self, 'payment_status_checkboxes', []))
     has_delivery_filter = any(cb.isChecked() for cb in getattr(self, 'delivery_checkboxes', []))
     
     # Показуємо інформацію про кількість знайдених результатів, якщо є активні фільтри
     if search_text or has_answer_filter or has_payment_filter or has_delivery_filter or self.unpaid_checkbox.isChecked() or self.paid_checkbox.isChecked():
         status_message = f"Знайдено замовлень: {total_orders}"
         if self.parent_window:
             self.parent_window.set_status_message(status_message, 5000)
         logging.info(status_message)
     
     # Прибираємо логіку зміни стилю пошукового поля
     # Поле завжди зберігатиме стандартний стиль відповідно до теми
     
     asyncio.ensure_future(self.animate_orders_page_change())

 async def animate_orders_page_change(self):
     await self.fade_orders_table(1.0, 0.0, 200)
     self.show_orders_page()
     await self.fade_orders_table(0.0, 1.0, 200)
     self.update_orders_page_buttons()

 async def fade_orders_table(self, start, end, duration):
     animation = QPropertyAnimation(self.orders_opacity_effect, b"opacity")
     animation.setDuration(duration)
     animation.setStartValue(start)
     animation.setEndValue(end)
     animation.setEasingCurve(QEasingCurve.Type.InOutCubic)
     loop = asyncio.get_event_loop()
     future = asyncio.Future()

     def on_finished():
         future.set_result(True)

     animation.finished.connect(on_finished)
     animation.start(QtCore.QAbstractAnimation.DeletionPolicy.DeleteWhenStopped)
     await future

 def show_orders_page(self):
     start_index = (self.current_page - 1) * self.page_size
     end_index = min(start_index + self.page_size, len(self.all_orders))
     self.orders_table.setRowCount(end_index - start_index)
     
     # Отримуємо пошуковий текст для виділення
     search_text = self.orders_search_bar.text().strip().lower() if hasattr(self, 'orders_search_bar') else ""
     highlight_color = QColor(255, 255, 0, 100)  # Світло-жовтий колір для виділення
     
     # Перевіряємо, чи є активне підсвічування рядка
     highlighted_row_global_index = -1
     if hasattr(self, 'highlighted_row') and self.highlighted_row is not None:
         highlighted_row_global_index = (self.current_page - 1) * self.page_size + self.highlighted_row
     
     for row_num, order in enumerate(self.all_orders[start_index:end_index]):
         try:
             global_row_index = start_index + row_num
             is_highlighted_row = global_row_index == highlighted_row_global_index
             
             item_0 = QTableWidgetItem(str(order['id']))
             # Виділяємо співпадіння в ID
             if (search_text and str(order['id']).lower().find(search_text) >= 0) or is_highlighted_row:
                 item_0.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 0, item_0)

             prods = order['products_str']
             item_1 = QTableWidgetItem(prods if prods else "N/A")
             # Виділяємо співпадіння в номерах продуктів
             if (search_text and prods.lower().find(search_text) >= 0) or is_highlighted_row:
                 item_1.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 1, item_1)

             clones = order['clones_str']
             item_2 = QTableWidgetItem(clones)
             if is_highlighted_row:
                 item_2.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 2, item_2)

             cli = order['client']
             item_3 = QTableWidgetItem(cli)
             # Виділяємо співпадіння в іменах клієнтів
             if (search_text and cli.lower().find(search_text) >= 0) or is_highlighted_row:
                 item_3.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 3, item_3)

             prices_str = order['prices_str']
             item_4 = QTableWidgetItem(prices_str)
             if is_highlighted_row:
                 item_4.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 4, item_4)

             addop_str = order['addops_str'] or ""
             item_5 = QTableWidgetItem(addop_str)
             if is_highlighted_row:
                 item_5.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 5, item_5)

             disc_str = order['discount_str'] or ""
             item_6 = QTableWidgetItem(disc_str)
             if is_highlighted_row:
                 item_6.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 6, item_6)

             total_str = ""
             if order['total_amount'] is not None:
                 total_str = f"{float(order['total_amount']):.2f}".rstrip('0').rstrip('.')
             item_7 = QTableWidgetItem(total_str)
             if is_highlighted_row:
                 item_7.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 7, item_7)

             st_text = (order['order_status_text'] or "").capitalize()
             item_8 = QTableWidgetItem(st_text)
             if is_highlighted_row:
                 item_8.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 8, item_8)

             pay_text = (order['payment_status_text'] or "").capitalize()
             item_9 = QTableWidgetItem(pay_text)
             if is_highlighted_row:
                 item_9.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 9, item_9)

             pm_text = (order['payment_method_text'] or "").capitalize()
             item_10 = QTableWidgetItem(pm_text)
             if is_highlighted_row:
                 item_10.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 10, item_10)

             item_11 = QTableWidgetItem("")
             if is_highlighted_row:
                 item_11.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 11, item_11)

             notes_str = order['notes'] or ""
             item_12 = QTableWidgetItem(notes_str)
             if is_highlighted_row:
                 item_12.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 12, item_12)

             pd_str = order['payment_date_str'] or ""
             item_13 = QTableWidgetItem(pd_str)
             if is_highlighted_row:
                 item_13.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 13, item_13)

             dm_text = (order['delivery_method_text'] or "").capitalize()
             item_14 = QTableWidgetItem(dm_text)
             if is_highlighted_row:
                 item_14.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 14, item_14)

             tracking = order['tracking_number']
             item_15 = QTableWidgetItem(tracking)
             # Виділяємо співпадіння в трек-номерах
             if (search_text and tracking.lower().find(search_text) >= 0) or is_highlighted_row:
                 item_15.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 15, item_15)

             item_16 = QTableWidgetItem(order['recipient_name'])
             if is_highlighted_row:
                 item_16.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 16, item_16)

             dst_text = (order['delivery_status_text'] or "").capitalize()
             item_17 = QTableWidgetItem(dst_text)
             if is_highlighted_row:
                 item_17.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 17, item_17)

             od_str = order['order_date_str'] or ""
             item_18 = QTableWidgetItem(od_str)
             if is_highlighted_row:
                 item_18.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 18, item_18)

             prio_str = ""
             if order['priority'] is not None:
                 prio_str = str(order['priority'])
             item_19 = QTableWidgetItem(prio_str)
             if is_highlighted_row:
                 item_19.setBackground(highlight_color)
             self.orders_table.setItem(row_num, 19, item_19)
             
             # Якщо це підсвічений рядок, оновлюємо його індекс на поточній сторінці
             if is_highlighted_row:
                 self.highlighted_row = row_num

         except Exception as e:
             logging.error(f"Помилка відображення рядка {row_num} замовлень: {e}")

     self.adjust_orders_table_columns()

 def adjust_orders_table_columns(self):
     self.orders_table.horizontalHeader().setStretchLastSection(False)
     self.orders_table.resizeColumnsToContents()

     if not self.orders_table.isColumnHidden(12):
         self.orders_table.horizontalHeader().setSectionResizeMode(
             12, QHeaderView.ResizeMode.Stretch
         )
     else:
         if not self.orders_table.isColumnHidden(3):
             self.orders_table.horizontalHeader().setSectionResizeMode(
                 3, QHeaderView.ResizeMode.Stretch
             )

     self.orders_table.updateGeometry()
     self.orders_table.viewport().update()

 async def reset_orders_filters(self):
     # Очищаємо підсвічування при скиданні фільтрів
     self.clear_highlight()
     
     # Очищаємо пошукове поле без зміни стилю
     self.orders_search_bar.clear()
     # Не змінюємо стиль, щоб зберігати стандартний вигляд відповідно до теми
     
     for chlist in [
         self.answer_status_checkboxes,
         self.payment_status_checkboxes,
         self.delivery_checkboxes
     ]:
         for cb in chlist:
             cb.blockSignals(True)
             cb.setChecked(False)
             cb.blockSignals(False)

     self.month_min.blockSignals(True)
     self.month_max.blockSignals(True)
     self.year_min.blockSignals(True)
     self.year_max.blockSignals(True)

     self.month_min.setValue(1)
     self.month_max.setValue(12)
     self.year_min.setValue(2020)
     self.year_max.setValue(2030)

     self.month_min.blockSignals(False)
     self.month_max.blockSignals(False)
     self.year_min.blockSignals(False)
     self.year_max.blockSignals(False)

     self.month_slider.blockSignals(True)
     self.month_slider.setLow(1)
     self.month_slider.setHigh(12)
     self.month_slider.blockSignals(False)

     self.year_slider.blockSignals(True)
     self.year_slider.setLow(2020)
     self.year_slider.setHigh(2030)
     self.year_slider.blockSignals(False)

     if "Сортування" not in [self.orders_sort_combobox.itemText(i) for i in range(self.orders_sort_combobox.count())]:
         self.orders_sort_combobox.blockSignals(True)
         self.orders_sort_combobox.insertItem(0, "Сортування")
         self.orders_sort_combobox.model().item(0).setEnabled(False)
         self.orders_sort_combobox.setCurrentIndex(0)
         self.orders_sort_combobox.blockSignals(False)
     else:
         self.orders_sort_combobox.blockSignals(True)
         self.orders_sort_combobox.setCurrentIndex(0)
         self.orders_sort_combobox.blockSignals(False)

     if "Пріоритет" not in [self.priority_combobox.itemText(i) for i in range(self.priority_combobox.count())]:
         self.priority_combobox.blockSignals(True)
         self.priority_combobox.insertItem(0, "Пріоритет")
         self.priority_combobox.model().item(0).setEnabled(False)
         self.priority_combobox.setCurrentIndex(0)
         self.priority_combobox.blockSignals(False)
     else:
         self.priority_combobox.blockSignals(True)
         self.priority_combobox.setCurrentIndex(0)
         self.priority_combobox.blockSignals(False)

     self.unpaid_checkbox.blockSignals(True)
     self.unpaid_checkbox.setChecked(False)
     self.unpaid_checkbox.blockSignals(False)
     
     self.paid_checkbox.blockSignals(True)
     self.paid_checkbox.setChecked(False)
     self.paid_checkbox.blockSignals(False)
     
     # Відображаємо інформацію про скидання фільтрів
     if self.parent_window and hasattr(self.parent_window, 'status_bar'):
         self.parent_window.status_bar.showMessage("Фільтри скинуто. Відображення всіх замовлень.", 3000)
     
     # Застосовуємо пошук без фільтрів
     await self.apply_orders_filters()

 def toggle_orders_column(self, index, state):
     is_checked = (state == Qt.CheckState.Checked.value)
     self.orders_table.setColumnHidden(index, not is_checked)
     self.adjust_orders_table_columns()

 def update_orders_page_buttons(self):
     for i in reversed(range(self.orders_page_buttons_layout.count())):
         w = self.orders_page_buttons_layout.takeAt(i).widget()
         if w:
             w.setParent(None)

     if self.total_pages < 1:
         return

     text_color = "#ffffff" if self.is_dark_theme else "#000000"
     base_style = f"""
         QPushButton {{
             border:none;
             background:transparent;
             color:{text_color};
             padding:5px;
         }}
         QPushButton:hover {{
             background-color:rgba(0,0,0,0.1);
             border-radius:3px;
         }}
     """

     prev_btn = QPushButton("◀")
     prev_btn.setFont(QFont("Arial", 13))
     prev_btn.setFixedHeight(35)
     prev_btn.setStyleSheet(base_style)
     prev_btn.setEnabled(self.current_page > 1)
     prev_btn.clicked.connect(lambda: self.go_to_orders_page(self.current_page - 1))
     self.orders_page_buttons_layout.addWidget(prev_btn)

     max_buttons = 7
     start_page = max(1, self.current_page - 3)
     end_page = min(start_page + max_buttons - 1, self.total_pages)
     if end_page - start_page < max_buttons - 1:
         start_page = max(1, end_page - max_buttons + 1)

     for p in range(start_page, end_page + 1):
         btn = QPushButton(str(p))
         btn.setFont(QFont("Arial", 13))
         btn.setFixedHeight(35)
         if p == self.current_page:
             btn.setStyleSheet(f"""
                 QPushButton {{
                     font-weight:bold;
                     border:none;
                     background:rgba(0,0,0,0.1);
                     padding:5px;
                     border-radius:3px;
                     color:{text_color};
                 }}
             """)
         else:
             btn.setStyleSheet(base_style)
         btn.clicked.connect(lambda _, page=p: self.go_to_orders_page(page))
         self.orders_page_buttons_layout.addWidget(btn)

     if end_page < self.total_pages:
         if end_page < self.total_pages - 1:
             ellips2 = QLabel("...")
             ellips2.setFont(QFont("Arial", 13))
             ellips2.setStyleSheet(f"color:{text_color};")
             self.orders_page_buttons_layout.addWidget(ellips2)

         last_btn = QPushButton(str(self.total_pages))
         last_btn.setFont(QFont("Arial", 13))
         last_btn.setFixedHeight(35)
         last_btn.setStyleSheet(base_style)
         last_btn.clicked.connect(lambda: self.go_to_orders_page(self.total_pages))
         self.orders_page_buttons_layout.addWidget(last_btn)

     next_btn = QPushButton("▶")
     next_btn.setFont(QFont("Arial", 13))
     next_btn.setFixedHeight(35)
     next_btn.setStyleSheet(base_style)
     next_btn.setEnabled(self.current_page < self.total_pages)
     next_btn.clicked.connect(lambda: self.go_to_orders_page(self.current_page + 1))
     self.orders_page_buttons_layout.addWidget(next_btn)

 def go_to_orders_page(self, page):
     if page != self.current_page and 1 <= page <= self.total_pages:
         self.current_page = page
         asyncio.ensure_future(self.animate_orders_page_change())

 def show_orders_cell_info(self, row, column):
     item = self.orders_table.item(row, column)
     if item:
         QMessageBox.information(
             self, "Деталі комірки (Замовлення)", f"Вміст:\n\n{item.text()}"
         )

 def select_orders_column(self, index):
     """
     Вибирає стовпець у таблиці замовлень.
     """
     self.orders_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectColumns)
     self.orders_table.selectColumn(index)
     self.orders_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)

 def clear_highlight(self):
     """Очищає підсвічування рядків у таблиці замовлень"""
     self.highlighted_row = None

 def on_search_enter_pressed(self):
     """
     Обробляє натискання Enter в полі пошуку.
     Запускає пошук із застосуванням фільтрів.
     """
     if self.orders_completer_list.isVisible():
         self.fade_out_orders_popup()
     
     # Очищаємо підсвічування при пошуку
     self.clear_highlight()
     
     # Запускаємо пошук з новим текстом
     logging.info(f"Початок пошуку за текстом: '{self.orders_search_bar.text().strip()}'")
     asyncio.ensure_future(self.apply_orders_filters())
     
 def show_error_message(self, message):
     """
     Показує повідомлення про помилку.
     """
     if hasattr(self, 'parent_window') and self.parent_window:
         self.parent_window.set_status_message(f"Помилка: {message}", 5000)
     
     QMessageBox.critical(self, "Помилка", message)
     
 def showEvent(self, event):
     """
     Обробник події відображення вкладки.
     Автоматично завантажує дані, якщо вони ще не завантажені.
     """
     super().showEvent(event)
     
     # Перевіряємо, чи дані вже завантажені
     if not self.data_loaded and hasattr(self, 'all_orders') and self.all_orders is not None and len(self.all_orders) == 0:
         logging.info("OrdersTab: автоматичне завантаження даних при відображенні")
         # Запускаємо асинхронне завантаження з параметром is_auto_load=True
         asyncio.ensure_future(self.apply_orders_filters(is_initial_load=True, is_auto_load=True))
     else:
         logging.debug("OrdersTab: дані вже завантажені, автозавантаження не потрібне")

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

# Додаємо функції, які ми оголосили раніше
def parse_google_sheets(self):
    """Функція запускає парсинг Google Sheets у фоновому режимі"""
    try:
        # Отримуємо URL-адреси таблиць для парсингу
        sheets_urls = [self.google_sheets_url_edit.text()]
        
        if not sheets_urls[0]:
            self.show_error_message("Помилка", "Не вказана URL-адреса Google Sheets")
            return
            
        # Запускаємо асинхронний парсинг
        result = parsing_api.start_parsing(sheets_urls, force_process=False)
        
        if result["success"]:
            # Показуємо повідомлення про успішний запуск
            self.show_info_message("Парсинг запущено", result["message"])
            
            # Запускаємо фоновий потік для оновлення інтерфейсу під час парсингу
            self.start_ui_update_thread()
        else:
            # Показуємо повідомлення про помилку
            self.show_error_message("Помилка", result["message"])
    
    except Exception as e:
        self.show_error_message("Помилка", f"Помилка при запуску парсингу: {str(e)}")
        logger.error(f"Помилка при запуску парсингу: {e}")
        import traceback
        logger.error(traceback.format_exc())

def start_ui_update_thread(self):
    """Запускає фоновий потік для оновлення інтерфейсу під час парсингу"""
    # Створюємо елемент для відображення статусу парсингу, якщо його ще немає
    if not hasattr(self, "parsing_status_widget"):
        from PyQt5.QtWidgets import QLabel
        self.parsing_status_widget = QLabel(self)
        self.parsing_status_widget.setStyleSheet("background-color: #f8f9fa; padding: 10px; border-radius: 5px;")
        self.parsing_status_widget.setOpenExternalLinks(True)
        self.parsing_status_widget.setTextFormat(QtCore.Qt.RichText)
        self.parsing_status_widget.setWordWrap(True)
        
        # Додаємо віджет статусу до інтерфейсу над таблицею замовлень
        self.orders_tab_layout.insertWidget(0, self.parsing_status_widget)
    
    # Функція оновлення інтерфейсу
    def update_ui():
        try:
            while True:
                # Отримуємо поточний статус парсингу
                status_html = parsing_api.get_status_html()
                
                # Оновлюємо віджет статусу в головному потоці
                QtCore.QMetaObject.invokeMethod(
                    self.parsing_status_widget,
                    "setText",
                    QtCore.Qt.QueuedConnection,
                    QtCore.Q_ARG(str, status_html)
                )
                
                # Оновлюємо таблицю замовлень, якщо парсинг виконується
                status = parsing_api.get_status()
                if status["is_running"]:
                    # Оновлюємо таблицю замовлень кожні 5 секунд
                    if status.get("processed_rows", 0) % 50 == 0:
                        QtCore.QMetaObject.invokeMethod(
                            self,
                            "refresh_orders_table",
                            QtCore.Qt.QueuedConnection
                        )
                else:
                    # Якщо парсинг завершено, оновлюємо таблицю і виходимо з циклу
                    if status.get("end_time"):
                        QtCore.QMetaObject.invokeMethod(
                            self,
                            "refresh_orders_table",
                            QtCore.Qt.QueuedConnection
                        )
                        
                        # Якщо пройшло більше 10 секунд після завершення, виходимо з циклу
                        if "end_time" in status and status["end_time"]:
                            end_time = status["end_time"]
                            elapsed_since_end = (datetime.now() - end_time).total_seconds()
                            if elapsed_since_end > 10:
                                break
                
                # Пауза перед наступним оновленням
                time.sleep(1)
                
            # Приховуємо віджет статусу після завершення
            QtCore.QMetaObject.invokeMethod(
                self.parsing_status_widget,
                "hide",
                QtCore.Qt.QueuedConnection
            )
                
        except Exception as e:
            logger.error(f"Помилка в потоці оновлення інтерфейсу: {e}")
    
    # Запускаємо потік
    ui_thread = threading.Thread(target=update_ui, daemon=True)
    ui_thread.start()
    
    # Показуємо віджет статусу
    self.parsing_status_widget.show()

# Замінюємо функцію оновлення таблиці замовлень, щоб використовувати безблокуючий метод
def refresh_orders_table(self):
    """Оновлює таблицю замовлень використовуючи безблокуючий метод під час парсингу"""
    try:
        # Перевіряємо, чи виконується парсинг
        status = parsing_api.get_status()
        
        if status.get("is_running", False):
            # Якщо парсинг виконується, використовуємо безблокуючий метод
            orders = parsing_api.get_orders(
                limit=100,  # Обмежуємо кількість замовлень для кращої продуктивності
                filter_text=self.search_orders_edit.text() if self.search_orders_edit.text() else None
            )
            
            # Очищаємо таблицю
            self.orders_table.setRowCount(0)
            
            # Заповнюємо таблицю даними
            for order in orders:
                self.add_order_to_table(order)
                
            # Додаємо примітку, що дані можуть бути неповними
            if not hasattr(self, "parsing_note_label"):
                from PyQt5.QtWidgets import QLabel
                self.parsing_note_label = QLabel("Примітка: під час парсингу відображаються останні 100 замовлень", self)
                self.parsing_note_label.setStyleSheet("color: #6c757d; font-style: italic;")
                self.orders_tab_layout.insertWidget(2, self.parsing_note_label)
            
            self.parsing_note_label.show()
        else:
            # Якщо парсинг не виконується, використовуємо стандартний метод
            # Приховуємо примітку, якщо вона існує
            if hasattr(self, "parsing_note_label") and self.parsing_note_label:
                self.parsing_note_label.hide()
            
            # Оригінальний код оновлення таблиці
            if hasattr(self, "_original_refresh_orders_table"):
                self._original_refresh_orders_table()
            else:
                # Якщо оригінальна функція не збережена, використовуємо стандартний метод
                # Тут можна додати базову реалізацію або викликати виключення
                logger.error("Оригінальна функція оновлення таблиці не знайдена")
    
    except Exception as e:
        logger.error(f"Помилка при оновленні таблиці замовлень: {e}")
        import traceback
        logger.error(traceback.format_exc())
