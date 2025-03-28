#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import logging
import asyncio
import subprocess
import re

from PyQt6 import QtCore
from PyQt6.QtWidgets import (
   QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QCheckBox, QLabel,
   QSpinBox, QPushButton, QTableWidget, QSizePolicy, QAbstractScrollArea,
   QMessageBox, QHeaderView, QTableWidgetItem, QAbstractItemView, 
   QListWidget, QListWidgetItem, QGraphicsOpacityEffect, QGroupBox,
   QGraphicsDropShadowEffect, QComboBox, QScrollArea, QSpacerItem, QMenu,
   QStyledItemDelegate, QStyle, QApplication, QCalendarWidget, QDialog
)
from PyQt6.QtGui import QFont, QPixmap, QColor, QCursor, QIcon, QMouseEvent, QAction
from PyQt6.QtCore import (
   Qt, QTimer, QEvent, QEasingCurve, QPoint, pyqtSignal, QPropertyAnimation, QDate, QMargins, QTime, QDateTime,
   QRect, QSize, QMargins, pyqtSignal
)

import qtawesome as qta

from db import session
from models import (
   Product, Type, Subtype, Brand, Gender, Color, Country, Status, Condition, Import, OrderDetails
)
from widgets import (
   RangeSlider, CollapsibleWidget, CollapsibleSection, FilterSection, FocusableSearchLineEdit
)
from services.theme_service import (
   apply_theme, update_text_colors
)
from services.filter_service import (
   remember_query, get_suggestions, get_suppliers,
   build_query_params, update_filter_counts
)

from sqlalchemy.orm import joinedload, aliased
from sqlalchemy import or_, desc, func, Float, cast

from .scripts import parsing_api
import threading
import time
import types

# Ініціалізуємо логер
logger = logging.getLogger(__name__)


def fix_sold_filter(q, session):
   """
   Фільтр «Тільки Непродані» (statusid=2 => «Непродано»).
   """
   q = q.filter(Product.statusid == 2)
   return q


class ProductsTab(QWidget):
   """
   Вкладка "Товари":
     - Фільтри (Бренд, Стать, Тип, Колір, Країна) ліворуч,
     - Слайдери (Ціна, Розмір, Розміри (см)) праворуч,
     - При великому вікні слайдери дійсно «прилипають» до правого краю,
       а при малому — з'являється горизонтальна прокрутка (QScrollArea).
     - Кнопки (Скинути Фільтри тощо) мають фіксовану maxWidth,
       щоб не розтягуватись надміру.
     - Без контурів на groupbox, отже більш «чистий» вигляд.
   """
   toggle_animation_finished = pyqtSignal()

   def __init__(self, parent=None):
       super().__init__(parent)
       self.parent_window = parent
       self.is_dark_theme = False
       self.data_loaded = False
       
       # Отримуємо доступ до вкладки замовлень з головного вікна
       if parent and hasattr(parent, 'orders_tab'):
           self.orders_tab = parent.orders_tab
       else:
           self.orders_tab = None
           
       self.page_size = 50
       self.current_page = 1
       self.all_products = []
       self.total_pages = 1
       
       # Змінна для лічильника вже знайдених товарів
       self.found_count = 0

       # Змінна для поточного виділеного стовпця
       self.highlighted_column = None

       self.logo_label = None
       self.theme_toggle_button = None

       # «Тільки Непродані»
       self.unsold_checkbox = QCheckBox("Тільки Непродані")
       self.unsold_checkbox.setFont(QFont("Arial", 13))
       self.unsold_checkbox.setChecked(True)
       self.unsold_checkbox.stateChanged.connect(self.on_filter_value_changed)

       # Таймери
       self.search_timer = QTimer()
       self.search_timer.setSingleShot(True)
       self.search_timer.timeout.connect(lambda: asyncio.ensure_future(self.apply_filters()))

       self.completer_timer = QTimer()
       self.completer_timer.setSingleShot(True)
       self.completer_timer.setInterval(500)
       self.completer_timer.timeout.connect(self.update_completer)
       self.popup_fade_animation = None
       self.current_suggestion_index = -1

       self.setup_ui()
       self.setMouseTracking(True)

       # Встановлюємо асинхронний парсинг в кінці ініціалізації
       self._install_async_parsing()

   def _install_async_parsing(self):
       """Встановлює асинхронне оновлення таблиці продуктів під час парсингу"""
       try:
           # Зберігаємо оригінальну функцію оновлення таблиці
           if not hasattr(self, "_original_refresh_products_table"):
               self._original_refresh_products_table = self.refresh_products_table
               
           # Замінюємо функції
           self.refresh_products_table = types.MethodType(self.refresh_products_table, self)
           
           logger.info("Встановлено асинхронне оновлення таблиці продуктів")
       except Exception as e:
           logger.error(f"Помилка при встановленні асинхронного оновлення таблиці продуктів: {e}")
           import traceback
           logger.error(traceback.format_exc())

   def mousePressEvent(self, event):
       """
       При натисканні будь-де на вкладці забираємо фокус із search_bar, якщо він активний.
       """
       if self.search_bar and self.search_bar.hasFocus():
           self.search_bar.clearFocus()
       super().mousePressEvent(event)

   def setup_ui(self):
       main_layout = QVBoxLayout(self)
       main_layout.setContentsMargins(10, 10, 10, 10)
       main_layout.setSpacing(0)

       # Верхня панель
       top_widget = QWidget()
       top_layout = QHBoxLayout(top_widget)
       top_layout.setContentsMargins(0, 0, 0, 10)
       top_layout.setSpacing(5)

       # Логотип
       self.logo_label = QLabel()
       logo_pixmap = QPixmap("style/images/icons/logo.png")
       logo_pixmap = logo_pixmap.scaled(
           50, 50,
           Qt.AspectRatioMode.KeepAspectRatio,
           Qt.TransformationMode.SmoothTransformation
       )
       self.logo_label.setPixmap(logo_pixmap)
       self.logo_label.setFixedSize(60, 60)

       # Пошук
       search_layout = QHBoxLayout()
       search_layout.setSpacing(5)
       self.search_bar = FocusableSearchLineEdit()
       self.search_bar.setObjectName("searchBar")
       self.search_bar.setPlaceholderText("Пошук продукту...")
       self.search_bar.setClearButtonEnabled(True)
       self.search_bar.setFont(QFont("Arial", 13))
       search_icon = qta.icon('fa5s.search', color='#888888')
       self.search_bar.addAction(search_icon, QLineEdit.ActionPosition.LeadingPosition)

       # completer_list
       self.completer_list = QListWidget()
       self.completer_list.setFocusPolicy(Qt.FocusPolicy.NoFocus)
       self.completer_list.setMouseTracking(True)
       self.completer_list.setWindowFlags(
           QtCore.Qt.WindowType.FramelessWindowHint
           | QtCore.Qt.WindowType.Tool
           | QtCore.Qt.WindowType.NoDropShadowWindowHint
           | QtCore.Qt.WindowType.WindowStaysOnTopHint
       )
       self.completer_list.setAttribute(QtCore.Qt.WidgetAttribute.WA_TranslucentBackground, True)
       self.completer_list.setMaximumHeight(250)
       self.popup_opacity_effect = QGraphicsOpacityEffect(self.completer_list)
       self.completer_list.setGraphicsEffect(self.popup_opacity_effect)
       self.popup_fade_animation = QPropertyAnimation(self.popup_opacity_effect, b"opacity")
       self.popup_fade_animation.setDuration(250)
       self.popup_fade_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)

       self.search_bar.textChanged.connect(self.debounced_completer_update)
       self.completer_list.itemClicked.connect(self.insert_completion)
       self.search_bar.installEventFilter(self)
       self.completer_list.installEventFilter(self)

       search_layout.addWidget(self.search_bar)

       # Кнопка теми
       self.theme_toggle_button = QPushButton()
       self.theme_toggle_button.setObjectName("themeToggleButton")
       self.theme_toggle_button.setFixedSize(35, 35)
       self.theme_toggle_button.setFont(QFont("Arial", 13))
       self.theme_toggle_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
       self.theme_toggle_button.clicked.connect(self.on_theme_button_clicked)
       search_layout.addWidget(self.theme_toggle_button, 0, Qt.AlignmentFlag.AlignRight)

       top_layout.addWidget(self.logo_label, alignment=Qt.AlignmentFlag.AlignVCenter)
       top_layout.addLayout(search_layout)
       main_layout.addWidget(top_widget, 0)

       # Фільтри + Таблиця + "Відображувані" в одному контейнері
       center_widget = QWidget()
       center_layout = QVBoxLayout(center_widget)
       center_layout.setContentsMargins(0, 0, 0, 0)
       center_layout.setSpacing(8)
       main_layout.addWidget(center_widget, 1)

       # CollapsibleWidget "Фільтри"
       self.filters_panel = CollapsibleWidget("Фільтри Пошуку (1)")
       self.filters_panel.toggle_animation.setDuration(500)
       self.filters_panel.toggle_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)
       self.filters_panel.content_area.setMaximumHeight(0)
       self.filters_panel.content_area.setVisible(False)
       self.filters_panel.toggle_button.setChecked(False)
       self.filters_panel.toggle_animation_finished.connect(self.on_filters_panel_toggled)

       self.create_filters_panel()
       center_layout.addWidget(self.filters_panel, 0)

       # Таблиця
       self.column_names = [
           "Номер", "Номери Клонів", "Тип", "Підтип", "Бренд", "Модель",
           "Маркування", "Рік", "Стать", "Колір", "Опис", "Країна Власник",
           "Країна Виробник", "Розмір", "СМ", "Ціна", "Стара Ціна",
           "Статус", "Стан", "Додаткова Нотатка", "Імпорт", "Кількість"
       ]
       self.optional_columns_indices = [1, 3, 5, 6, 7, 11, 12, 16, 19, 21]
       self.mandatory_columns_indices = [0, 2, 4, 10, 13, 15, 8, 14, 17, 18, 20]

       self.table = QTableWidget()
       self.table.setObjectName("productsTable")
       self.table.setColumnCount(len(self.column_names))
       self.table.setHorizontalHeaderLabels(self.column_names)
       self.table.verticalHeader().setVisible(False)
       self.table.setAlternatingRowColors(True)
       self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
       self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
       self.table.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustToContents)
       self.table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
       self.table.setFont(QFont("Arial", 13))
       self.table.setShowGrid(True)
       self.table.setGridStyle(Qt.PenStyle.SolidLine)
       self.table.horizontalHeader().setSectionsMovable(True)
       self.table.verticalHeader().setSectionsMovable(False)
       self.table.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
       self.table.setHorizontalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
       self.table.horizontalHeader().setFixedHeight(35)
       
       # Додаємо делегат для першої колонки
       self.number_delegate = NumberColumnDelegate(self.table, self.is_dark_theme)
       self.table.setItemDelegateForColumn(0, self.number_delegate)

       for idx in self.optional_columns_indices:
           self.table.setColumnHidden(idx, True)

       # Контекстне меню
       self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
       self.table.customContextMenuRequested.connect(self.show_table_context_menu)

       # Ефект прозорості
       self.table_opacity_effect = QGraphicsOpacityEffect()
       self.table.setGraphicsEffect(self.table_opacity_effect)

       center_layout.addWidget(self.table, 10)

       # Пагінація
       self.pagination_layout = QHBoxLayout()
       self.pagination_layout.setContentsMargins(0, 0, 0, 0)
       self.pagination_layout.setSpacing(8)
       self.pagination_layout.setAlignment(Qt.AlignmentFlag.AlignHCenter)

       self.page_buttons_layout = QHBoxLayout()
       self.page_buttons_layout.setSpacing(5)
       self.page_buttons_layout.setContentsMargins(0, 0, 0, 0)
       self.pagination_layout.addLayout(self.page_buttons_layout)
       center_layout.addLayout(self.pagination_layout, 0)

       # CollapsibleSection "Відображувані"
       self.displayed_section = CollapsibleSection("Відображувані")
       self.displayed_section.toggle_button.setFont(QFont("Arial", 14, QFont.Weight.Bold))
       self.displayed_section.toggle_animation.setDuration(500)
       self.displayed_section.toggle_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)

       displayed_layout = QVBoxLayout()
       displayed_layout.setSpacing(8)
       self.displayed_checkboxes = []
       for index, column_name in enumerate(self.column_names):
           if index in self.mandatory_columns_indices:
               continue
           checkbox = QCheckBox(column_name)
           checkbox.setFont(QFont("Arial", 12))
           checkbox.setChecked(not self.table.isColumnHidden(index))
           checkbox.stateChanged.connect(lambda state, idx=index: self.toggle_column(idx, state))
           self.displayed_checkboxes.append(checkbox)
           displayed_layout.addWidget(checkbox)

       self.displayed_section.setContentLayout(displayed_layout)
       self.displayed_section.toggle_button.setChecked(False)
       self.displayed_section.on_toggle()
       self.displayed_section.toggle_animation_finished.connect(self.on_filters_panel_toggled)
       center_layout.addWidget(self.displayed_section, 0)

       # Нижній блок: чекбокс + кнопки
       bottom_widget = QWidget()
       bottom_layout = QHBoxLayout(bottom_widget)
       bottom_layout.setContentsMargins(0, 10, 0, 0)
       bottom_layout.setSpacing(10)

       bottom_layout.addWidget(self.unsold_checkbox, alignment=Qt.AlignmentFlag.AlignLeft)
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

       self.filter_button = QPushButton("Застосувати")
       self.filter_button.setObjectName("applyFilterButton")
       self.filter_button.setFont(QFont("Arial", 13))
       self.filter_button.setFixedHeight(35)
       # Щоб кнопки не «розтягувалися»
       self.filter_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
       self.filter_button.setStyleSheet(common_button_style)
       buttons_layout.addWidget(self.filter_button)

       self.refresh_button = QPushButton()
       self.refresh_button.setObjectName("refreshButton")
       self.refresh_button.setFont(QFont("Arial", 13))
       self.refresh_button.setFixedHeight(35)
       self.refresh_button.setFixedWidth(35)
       self.refresh_button.setIcon(qta.icon('fa5s.sync', color='#000000'))
       self.refresh_button.setIconSize(QtCore.QSize(18, 18))
       self.refresh_button.setStyleSheet("""
           QPushButton {
               background-color: #f8f8f8;
               border: 1px solid #dcdcdc;
               color: #000000;
               border-radius: 5px;
               padding: 5px;
               max-width: 35px;
           }
           QPushButton:hover {
               background-color: #e8e8e8;
           }
           QPushButton:pressed {
               background-color: #d0d0d0;
           }
       """)
       self.refresh_button.clicked.connect(lambda: self.run_refresh_from_shared_button())
       buttons_layout.addWidget(self.refresh_button)

       bottom_layout.addLayout(buttons_layout)
       center_layout.addWidget(bottom_widget, 0)

       # Сигнали
       self.filter_button.clicked.connect(lambda: asyncio.ensure_future(self.apply_filters()))
       self.table.cellDoubleClicked.connect(self.show_cell_info)
       self.table.horizontalHeader().sectionClicked.connect(self.select_column)
       self.table.itemSelectionChanged.connect(self.on_selection_changed)

       self.set_scroll_style()

   def set_scroll_style(self):
       """
       Минималистичный автоскрывающийся скроллбар:
       - Полупрозрачный (появляется только при наведении)
       - Без рейла/фона
       - Накладывается на край таблицы
       - Плавное появление/исчезновение
       """
       # Стиль для основной таблицы продуктов
       table_scroll_style = """
       QTableWidget {
           border: 1px solid #cccccc;
           color: #000000; /* Додаємо явно чорний колір тексту для клітинок таблиці */
       }
       
       QTableWidgetItem {
           color: #000000; /* Чорний колір тексту для всіх елементів у таблиці */
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
       self.table.setStyleSheet(table_scroll_style)
       
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
           
       if hasattr(self, 'completer_list'):
           self.completer_list.setStyleSheet(global_scroll_style)
           
       # Применяем стиль для фильтров секций
       for section_name in ['brand_section', 'gender_section', 'type_section', 
                            'color_section', 'country_section']:
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
       self.update_theme_icon_only()
       update_filter_counts(self)
       
       # Логотип
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
       
       self.update_page_buttons()
       
       # Оновлюємо кольори тексту в таблиці
       if self.data_loaded and hasattr(self, 'table'):
           self.update_table_text_color()
     
       # Оновлюємо кольори тексту для QLabel в комірках таблиці
       self.update_product_labels_color() 
       
       # Оновлюємо стан делегата з новою темою
       if hasattr(self, 'number_delegate'):
           self.number_delegate.setDarkTheme(is_dark)

   def update_theme_icon_only(self):
       from services.theme_service import update_theme_icon_for_button
       if self.theme_toggle_button:
           update_theme_icon_for_button(self.theme_toggle_button, self.is_dark_theme)

   def eventFilter(self, obj, event):
       if obj == self.search_bar:
           if event.type() == QEvent.Type.FocusOut:
               if self.completer_list.isVisible():
                   mouse_pos = QCursor.pos()
                   if not self.completer_list.geometry().contains(mouse_pos):
                       self.fade_out_popup()
           elif event.type() == QEvent.Type.KeyPress:
               if event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
                   if 0 <= self.current_suggestion_index < self.completer_list.count():
                       item = self.completer_list.item(self.current_suggestion_index)
                       self.insert_completion(item)
                   else:
                       remember_query(self.search_bar.text())
                       asyncio.ensure_future(self.apply_filters())
                       self.fade_out_popup()
                   return True
               elif event.key() == Qt.Key.Key_Escape:
                   self.fade_out_popup()
                   return True
               elif event.key() == Qt.Key.Key_Down:
                   if self.completer_list.count() > 0:
                       new_index = self.current_suggestion_index + 1
                       if new_index < self.completer_list.count():
                           self.current_suggestion_index = new_index
                           self.completer_list.setCurrentRow(self.current_suggestion_index)
                   return True
               elif event.key() == Qt.Key.Key_Up:
                   if self.completer_list.count() > 0:
                       new_index = self.current_suggestion_index - 1
                       if new_index >= 0:
                           self.current_suggestion_index = new_index
                           self.completer_list.setCurrentRow(self.current_suggestion_index)
                   return True
       elif obj == self.completer_list:
           if event.type() == QEvent.Type.MouseButtonPress:
               item = self.completer_list.itemAt(event.pos())
               if item:
                   self.insert_completion(item)
           elif event.type() == QEvent.Type.KeyPress:
               if event.key() == Qt.Key.Key_Escape:
                   self.fade_out_popup()
                   self.search_bar.setFocus()
                   return True
               elif event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
                   item = self.completer_list.currentItem()
                   self.insert_completion(item)
                   return True
               elif event.key() == Qt.Key.Key_Down:
                   new_index = self.current_suggestion_index + 1
                   if new_index < self.completer_list.count():
                       self.current_suggestion_index = new_index
                       self.completer_list.setCurrentRow(self.current_suggestion_index)
                   return True
               elif event.key() == Qt.Key.Key_Up:
                   new_index = self.current_suggestion_index - 1
                   if new_index >= 0:
                       self.current_suggestion_index = new_index
                       self.completer_list.setCurrentRow(self.current_suggestion_index)
                   return True
       return False

   def debounced_completer_update(self, text: str):
       self.current_suggestion_index = -1
       if text.strip():
           self.completer_timer.start()
       else:
           self.fade_out_popup()

   def update_completer(self):
       text = self.search_bar.text().strip()
       if not text:
           self.fade_out_popup()
           return

       suggestions = get_suggestions(text, session)
       self.completer_list.clear()

       categories_order = ["Останні Пошуки", "Рекомендації", "Бренд", "Модель", "Опис"]
       found_suggestions = False
       for category in categories_order:
           suggestion_list = suggestions.get(category, [])
           for s in suggestion_list:
               found_suggestions = True
               item = QListWidgetItem(s)
               self.completer_list.addItem(item)

       if not found_suggestions:
           no_item = QListWidgetItem("Немає підказок…")
           no_item.setFlags(Qt.ItemFlag.NoItemFlags)
           self.completer_list.addItem(no_item)

       self.position_completer_list()
       if not self.completer_list.isVisible():
           self.fade_in_popup()
       else:
           self.completer_list.update()

   def position_completer_list(self):
       self.completer_list.setFixedWidth(self.search_bar.width())
       list_pos = self.search_bar.mapToGlobal(QtCore.QPoint(0, self.search_bar.height()))
       self.completer_list.move(list_pos)

   def fade_in_popup(self):
       self.popup_fade_animation.stop()
       self.popup_opacity_effect.setOpacity(0.0)
       self.completer_list.show()
       self.popup_fade_animation.setStartValue(0.0)
       self.popup_fade_animation.setEndValue(1.0)
       self.popup_fade_animation.start()

   def fade_out_popup(self):
       def on_finished():
           self.completer_list.hide()

       if not self.completer_list.isVisible():
           return

       self.popup_fade_animation.stop()
       start_opacity = self.popup_opacity_effect.opacity()
       try:
           self.popup_fade_animation.finished.disconnect()
       except TypeError:
           pass
       self.popup_fade_animation.finished.connect(on_finished)
       self.popup_fade_animation.setStartValue(start_opacity)
       self.popup_fade_animation.setEndValue(0.0)
       self.popup_fade_animation.start()

   def insert_completion(self, item):
       if item and item.flags() != Qt.ItemFlag.NoItemFlags:
           text = item.text()
           remember_query(text)
           self.search_bar.setText(text)
       self.fade_out_popup()
       asyncio.ensure_future(self.apply_filters())
       self.search_bar.setFocus()

   def create_filters_panel(self):
       """
       Використовуємо QScrollArea для фільтрів, без зайвих контурів.
       Слайдери: мін.ширина ~800, вирівняні вправо.
       """
       # Зовнішній контейнер
       scroll_area = QScrollArea()
       scroll_area.setWidgetResizable(True)
       scroll_area.setStyleSheet("QScrollArea { border: none; }")  # Без контуру
       container_widget = QWidget()
       container_widget.setStyleSheet("QGroupBox { border: none; }")  # Прибрати контури groupbox
       container_layout = QHBoxLayout(container_widget)
       container_layout.setContentsMargins(0, 0, 0, 0)
       container_layout.setSpacing(20)

       # Ліва частина (фільтри-бокси)
       self.left_group = QGroupBox()
       self.left_group.setTitle("")
       self.left_group.setStyleSheet("border: none;")
       left_layout = QVBoxLayout(self.left_group)
       left_layout.setContentsMargins(10, 5, 10, 10)
       left_layout.setSpacing(15)

       # Права частина (слайдери)
       self.right_group = QGroupBox()
       self.right_group.setTitle("")
       self.right_group.setStyleSheet("border: none;")
       right_layout = QVBoxLayout(self.right_group)
       right_layout.setContentsMargins(10, 10, 10, 10)
       right_layout.setSpacing(15)

       container_layout.addWidget(self.left_group, stretch=1)
       container_layout.addWidget(self.right_group, stretch=1)

       # Додаємо в scroll_area
       scroll_area.setWidget(container_widget)

       # Вставляємо scroll_area у фільтри
       self.filters_panel.setContentLayout(QVBoxLayout())
       self.filters_panel.content_area.layout().addWidget(scroll_area)

       # Формуємо фільтри
       self.populate_filters(left_layout, right_layout)

   def populate_filters(self, left_layout, right_layout):
       """
       Ліва частина: Brand, Gender, Type, Color, Country.
       Права частина: Price slider, Size slider, Dimension slider, comboboxes (Стан, Постачальник, Сортування).
       """
       # Ліва частина
       brands = session.query(Brand).order_by(Brand.brandname).all()
       brand_list = [b.brandname for b in brands]
       self.brand_section = FilterSection("Бренд", items=brand_list, columns=4, maxHeight=600)
       self.brand_section.toggle_animation.setDuration(500)
       self.brand_section.toggle_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)
       self.brand_section.toggle_button.setChecked(False)
       self.brand_section.on_toggle()
       self.brand_checkboxes = self.brand_section.all_checkboxes
       self.brand_section.checkbox_state_changed.connect(self.on_checkbox_state_changed)
       self.brand_section.toggle_animation_finished.connect(self.on_filters_panel_toggled)
       left_layout.addWidget(self.brand_section)

       genders = session.query(Gender).order_by(Gender.gendername).all()
       gender_list = [g.gendername for g in genders]
       self.gender_section = FilterSection("Стать", items=gender_list, columns=4, maxHeight=600)
       self.gender_section.toggle_animation.setDuration(500)
       self.gender_section.toggle_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)
       self.gender_section.toggle_button.setChecked(False)
       self.gender_section.on_toggle()
       self.gender_checkboxes = self.gender_section.all_checkboxes
       self.gender_section.checkbox_state_changed.connect(self.on_checkbox_state_changed)
       self.gender_section.toggle_animation_finished.connect(self.on_filters_panel_toggled)
       left_layout.addWidget(self.gender_section)

       types = session.query(Type).order_by(Type.typename).all()
       subtypes = session.query(Subtype).order_by(Subtype.subtypename).all()
       all_type_items = list(set([t.typename for t in types] + [st.subtypename for st in subtypes]))
       all_type_items.sort(key=str.lower)
       self.type_section = FilterSection("Тип", items=all_type_items, columns=4, maxHeight=600)
       self.type_section.toggle_animation.setDuration(500)
       self.type_section.toggle_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)
       self.type_section.toggle_button.setChecked(False)
       self.type_section.on_toggle()
       self.type_checkboxes = self.type_section.all_checkboxes
       self.type_section.checkbox_state_changed.connect(self.on_checkbox_state_changed)
       self.type_section.toggle_animation_finished.connect(self.on_filters_panel_toggled)
       left_layout.addWidget(self.type_section)

       colors = session.query(Color).order_by(Color.colorname).all()
       color_list = [c.colorname for c in colors]
       self.color_section = FilterSection("Колір", items=color_list, columns=4, maxHeight=600)
       self.color_section.toggle_animation.setDuration(500)
       self.color_section.toggle_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)
       self.color_section.toggle_button.setChecked(False)
       self.color_section.on_toggle()
       self.color_checkboxes = self.color_section.all_checkboxes
       self.color_section.checkbox_state_changed.connect(self.on_checkbox_state_changed)
       self.color_section.toggle_animation_finished.connect(self.on_filters_panel_toggled)
       left_layout.addWidget(self.color_section)

       countries = session.query(Country).order_by(Country.countryname).all()
       country_list = [cn.countryname for cn in countries]
       self.country_section = FilterSection("Країна", items=country_list, columns=4, maxHeight=600)
       self.country_section.toggle_animation.setDuration(500)
       self.country_section.toggle_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)
       self.country_section.toggle_button.setChecked(False)
       self.country_section.on_toggle()
       self.country_checkboxes = self.country_section.all_checkboxes
       self.country_section.checkbox_state_changed.connect(self.on_checkbox_state_changed)
       self.country_section.toggle_animation_finished.connect(self.on_filters_panel_toggled)
       left_layout.addWidget(self.country_section)

       left_layout.addStretch(0)

       # Права частина
       label_font = QFont("Arial", 11)

       price_label = QLabel("Ціна")
       price_label.setFont(label_font)
       self.price_min = self.create_spinbox(0, 9999, "Від ")
       self.price_max = self.create_spinbox(0, 9999, "До ")
       self.price_max.setValue(9999)

       self.price_slider = RangeSlider()
       self.price_slider.setObjectName("priceSlider")
       self.price_slider.left_margin = 0
       self.price_slider.right_margin = 9
       self.price_slider.setRange(0, 20000)
       self.price_slider.setLow(0)
       self.price_slider.setHigh(20000)
       self.price_slider.setMinimumWidth(800)

       price_input_layout = QHBoxLayout()
       price_input_layout.setSpacing(5)
       self.price_min.setFixedWidth(80)
       self.price_max.setFixedWidth(80)
       price_input_layout.addWidget(price_label)
       price_input_layout.addWidget(self.price_min)
       price_input_layout.addWidget(self.price_max)

       price_slider_layout = QHBoxLayout()
       price_slider_layout.setContentsMargins(0, 0, 0, 0)
       price_slider_layout.setSpacing(0)
       price_slider_layout.addStretch(1)
       price_slider_layout.addWidget(self.price_slider, 0, Qt.AlignmentFlag.AlignRight)

       right_layout.addLayout(price_input_layout)
       right_layout.addLayout(price_slider_layout)

       size_label = QLabel("Розмір")
       size_label.setFont(label_font)
       self.size_min = self.create_spinbox(14, 60, "Від ")
       self.size_max = self.create_spinbox(14, 60, "До ")
       self.size_max.setValue(60)

       self.size_slider = RangeSlider()
       self.size_slider.setObjectName("sizeSlider")
       self.size_slider.left_margin = 0
       self.size_slider.right_margin = 9
       self.size_slider.setRange(14, 60)
       self.size_slider.setLow(14)
       self.size_slider.setHigh(60)
       self.size_slider.setMinimumWidth(800)

       size_input_layout = QHBoxLayout()
       size_input_layout.setSpacing(5)
       self.size_min.setFixedWidth(80)
       self.size_max.setFixedWidth(80)
       size_input_layout.addWidget(size_label)
       size_input_layout.addWidget(self.size_min)
       size_input_layout.addWidget(self.size_max)

       size_slider_layout = QHBoxLayout()
       size_slider_layout.setContentsMargins(0, 0, 0, 0)
       size_slider_layout.setSpacing(0)
       size_slider_layout.addStretch(1)
       size_slider_layout.addWidget(self.size_slider, 0, Qt.AlignmentFlag.AlignRight)

       right_layout.addLayout(size_input_layout)
       right_layout.addLayout(size_slider_layout)

       dimensions_label = QLabel("Розміри (см)")
       dimensions_label.setFont(label_font)
       self.dimensions_min = self.create_spinbox(5, 40, "Від ")
       self.dimensions_max = self.create_spinbox(5, 40, "До ")
       self.dimensions_max.setValue(40)

       self.dimensions_slider = RangeSlider()
       self.dimensions_slider.setObjectName("dimensionsSlider")
       self.dimensions_slider.left_margin = 0
       self.dimensions_slider.right_margin = 9
       self.dimensions_slider.setRange(5, 40)
       self.dimensions_slider.setLow(5)
       self.dimensions_slider.setHigh(40)
       self.dimensions_slider.setMinimumWidth(800)

       dim_input_layout = QHBoxLayout()
       dim_input_layout.setSpacing(5)
       self.dimensions_min.setFixedWidth(80)
       self.dimensions_max.setFixedWidth(80)
       dim_input_layout.addWidget(dimensions_label)
       dim_input_layout.addWidget(self.dimensions_min)
       dim_input_layout.addWidget(self.dimensions_max)

       dim_slider_layout = QHBoxLayout()
       dim_slider_layout.setContentsMargins(0, 0, 0, 0)
       dim_slider_layout.setSpacing(0)
       dim_slider_layout.addStretch(1)
       dim_slider_layout.addWidget(self.dimensions_slider, 0, Qt.AlignmentFlag.AlignRight)

       right_layout.addLayout(dim_input_layout)
       right_layout.addLayout(dim_slider_layout)

       # Combobox: Стан, Постачальник, Сортування
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

       self.condition_combobox = QComboBox()
       self.condition_combobox.setFont(QFont("Arial", 13))
       self.condition_combobox.setFixedHeight(35)
       self.condition_combobox.setStyleSheet(combobox_style)
       self.condition_combobox.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
       self.condition_combobox.addItem("Стан")
       self.condition_combobox.model().item(0).setEnabled(False)
       self.condition_combobox.addItem("Всі")
       self.condition_combobox.addItem("Новий")
       self.condition_combobox.addItem("Хороший")
       self.condition_combobox.addItem("Вживаний")
       self.condition_combobox.addItem("Пошкоджений")
       self.condition_combobox.setCurrentIndex(0)
       self.condition_combobox.currentIndexChanged.connect(self.on_filter_value_changed)

       supplier_list = get_suppliers(session)
       self.supplier_combobox = QComboBox()
       self.supplier_combobox.setFont(QFont("Arial", 13))
       self.supplier_combobox.setFixedHeight(35)
       self.supplier_combobox.setStyleSheet(combobox_style)
       self.supplier_combobox.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
       self.supplier_combobox.addItem("Постачальник")
       self.supplier_combobox.model().item(0).setEnabled(False)
       self.supplier_combobox.addItem("Всі")
       for sup in supplier_list:
           self.supplier_combobox.addItem(sup)
       self.supplier_combobox.setCurrentIndex(0)
       self.supplier_combobox.currentIndexChanged.connect(self.on_filter_value_changed)

       self.sort_combobox = QComboBox()
       self.sort_combobox.setFont(QFont("Arial", 13))
       self.sort_combobox.setFixedHeight(35)
       self.sort_combobox.setStyleSheet(combobox_style)
       self.sort_combobox.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
       self.sort_combobox.addItem("Сортування")
       self.sort_combobox.model().item(0).setEnabled(False)
       self.sort_combobox.addItem("По імені")
       self.sort_combobox.addItem("За часом додавання")
       self.sort_combobox.addItem("Від дешевого")
       self.sort_combobox.addItem("Від найдорожчого")
       self.sort_combobox.setCurrentIndex(0)
       self.sort_combobox.currentIndexChanged.connect(self.on_filter_value_changed)

       combo_bottom_layout = QHBoxLayout()
       combo_bottom_layout.setSpacing(10)
       combo_bottom_layout.addWidget(self.condition_combobox)
       combo_bottom_layout.addWidget(self.supplier_combobox)
       combo_bottom_layout.addWidget(self.sort_combobox)
       combo_bottom_layout.addStretch(1)

       self.reset_button = QPushButton("Скинути Фільтри")
       self.reset_button.setObjectName("resetFiltersButton")
       self.reset_button.setFont(QFont("Arial", 13))
       self.reset_button.setFixedHeight(35)
       self.reset_button.setStyleSheet("""
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

       self.reset_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)

       def add_shadow():
           shadow = QGraphicsDropShadowEffect()
           shadow.setBlurRadius(10)
           shadow.setColor(QColor(0, 0, 0, 80))
           shadow.setOffset(0, 0)
           self.reset_button.setGraphicsEffect(shadow)

       def remove_shadow():
           self.reset_button.setGraphicsEffect(None)

       self.reset_button.enterEvent = lambda e: add_shadow()
       self.reset_button.leaveEvent = lambda e: remove_shadow()
       self.reset_button.clicked.connect(lambda: asyncio.ensure_future(self.reset_filters()))

       btn_layout = QHBoxLayout()
       btn_layout.setSpacing(10)
       btn_layout.addLayout(combo_bottom_layout)
       btn_layout.addWidget(self.reset_button)

       right_layout.addLayout(btn_layout)
       right_layout.addStretch(1)

       # Прив'язка сигналів
       self.price_slider.valueChanged.connect(self.update_price_spinboxes)
       self.size_slider.valueChanged.connect(self.update_size_spinboxes)
       self.dimensions_slider.valueChanged.connect(self.update_dimensions_spinboxes)
       self.price_min.valueChanged.connect(self.update_price_slider)
       self.price_max.valueChanged.connect(self.update_price_slider)
       self.size_min.valueChanged.connect(self.update_size_slider)
       self.size_max.valueChanged.connect(self.update_size_slider)
       self.dimensions_min.valueChanged.connect(self.update_dimensions_slider)
       self.dimensions_max.valueChanged.connect(self.update_dimensions_slider)
       self.price_min.valueChanged.connect(self.on_filter_value_changed)
       self.price_max.valueChanged.connect(self.on_filter_value_changed)
       self.size_min.valueChanged.connect(self.on_filter_value_changed)
       self.size_max.valueChanged.connect(self.on_filter_value_changed)
       self.dimensions_min.valueChanged.connect(self.on_filter_value_changed)
       self.dimensions_max.valueChanged.connect(self.on_filter_value_changed)

       update_filter_counts(self)
       self.update_slider_locks()

   def on_filters_panel_toggled(self):
       self.adjust_table_columns()
       self.toggle_animation_finished.emit()

   def create_spinbox(self, minimum, maximum, prefix=""):
       spin = QSpinBox()
       spin.setFont(QFont("Arial", 13))
       spin.setPrefix(prefix)
       spin.setMinimum(minimum)
       spin.setMaximum(maximum)
       spin.setFixedWidth(80)
       return spin

   def on_checkbox_state_changed(self):
       if self.data_loaded:
           self.search_timer.start(300)
           update_filter_counts(self)

   def on_filter_value_changed(self):
       if self.data_loaded:
           self.search_timer.start(300)
       self.update_slider_locks()

   async def run_parsing_script(self):
       self.parent_window.show_progress_bar(True)
       self.refresh_button.setEnabled(False)
       
       # Встановлюємо початковий статус
       self.parent_window.set_status_message("Оновлення бази товарів...")
       
       try:
           # Запускаємо googlesheets_pars.py
           def blocking_script_products():
               script_path = os.path.join(
                   os.path.dirname(os.path.abspath(__file__)),
                   'scripts',
                   'googlesheets_pars.py'
               )
               result = subprocess.run([sys.executable, script_path],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
               return result

           result_products = await asyncio.to_thread(blocking_script_products)
           
           if result_products.returncode != 0:
               error_message = result_products.stderr.strip()
               self.show_error_message(f"Помилка оновлення товарів: {error_message}")
               return
               
           # Оновлюємо статус
           self.parent_window.set_status_message("Оновлення бази замовлень...")
           
           # Запускаємо orders_pars.py
           def blocking_script_orders():
               script_path = os.path.join(
                   os.path.dirname(os.path.abspath(__file__)),
                   'scripts',
                   'orders_pars.py'
               )
               result = subprocess.run([sys.executable, script_path],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
               return result
               
           result_orders = await asyncio.to_thread(blocking_script_orders)
           
           if result_orders.returncode != 0:
               error_message = result_orders.stderr.strip()
               self.show_error_message(f"Помилка оновлення замовлень: {error_message}")
               return
           
           # Оновлюємо статус
           self.parent_window.set_status_message("Оновлення завершено, застосовую фільтри...")
           
           # Застосовуємо фільтри для оновлення відображення
           await self.apply_filters()
           
           # Показуємо повідомлення про успіх
           self.parent_window.set_status_message("Оновлення бази даних успішно завершено", 5000)
           
       except Exception as e:
           session.rollback()
           self.show_error_message(str(e))
           self.parent_window.set_status_message("Помилка оновлення бази даних", 5000)
       finally:
           self.parent_window.show_progress_bar(False)
           self.refresh_button.setEnabled(True)

   async def apply_filters(self, is_initial_load=False):
       """
       Застосовує фільтри до даних та оновлює таблицю.
       Параметр is_initial_load вказує, що це перше завантаження при старті програми.
       """
       session.rollback()
       self.parent_window.show_progress_bar(True)

       query_params = build_query_params(self)
       query_params['unsold_only'] = self.unsold_checkbox.isChecked()

       try:
           logging.info("Починаю завантаження продуктів...")
           products = await self.async_load_products(query_params)
           logging.info(f"Завантажено {len(products)} продуктів")
           self.load_data(products)
           
           # Якщо це перше завантаження, переконуємося, що дані відображаються без анімації і затримок
           if is_initial_load:
               self.table_opacity_effect.setOpacity(1.0)  # Встановлюємо повну непрозорість при першому завантаженні
               self.show_page()
               self.update_page_buttons()
               logging.info("Перше завантаження завершено")
       except Exception as e:
           session.rollback()
           logging.error(f"Помилка при завантаженні даних: {str(e)}")
           self.show_error_message(str(e))
       finally:
           self.parent_window.show_progress_bar(False)
           if not is_initial_load:  # Не забираємо фокус при першому завантаженні
               self.search_bar.setFocus()

   async def async_load_products(self, query_params):
       def blocking_query():
           unsold_only = query_params.get('unsold_only')
           search_text = query_params.get('search_text')
           selected_brands = query_params.get('selected_brands')
           selected_genders = query_params.get('selected_genders')
           selected_types = query_params.get('selected_types')
           selected_colors = query_params.get('selected_colors')
           selected_countries = query_params.get('selected_countries')
           price_min = query_params.get('price_min')
           price_max = query_params.get('price_max')
           size_min = query_params.get('size_min')
           size_max = query_params.get('size_max')
           dim_min = query_params.get('dim_min')
           dim_max = query_params.get('dim_max')
           selected_condition = query_params.get('selected_condition')
           selected_supplier = query_params.get('selected_supplier')
           sort_option = query_params.get('sort_option')

           owner_alias = aliased(Country)
           manuf_alias = aliased(Country)

           q = session.query(
               Product.productnumber,
               Product.clonednumbers,
               Product.model,
               Product.marking,
               Product.year,
               Product.description,
               Product.extranote,
               Product.price,
               Product.oldprice,
               Product.dateadded,
               Product.sizeeu,
               Product.sizeua,
               Product.sizeusa,
               Product.sizeuk,
               Product.sizejp,
               Product.sizecn,
               Product.measurementscm,
               Product.quantity,
               Type.typename,
               Subtype.subtypename,
               Brand.brandname,
               Gender.gendername,
               Color.colorname,
               owner_alias.countryname.label('ownercountryname'),
               manuf_alias.countryname.label('manufacturercountryname'),
               Status.statusname,
               Condition.conditionname,
               Import.importname
           ).join(
               Type, Product.typeid == Type.id, isouter=True
           ).join(
               Subtype, Product.subtypeid == Subtype.id, isouter=True
           ).join(
               Brand, Product.brandid == Brand.id, isouter=True
           ).join(
               Gender, Product.genderid == Gender.id, isouter=True
           ).join(
               Color, Product.colorid == Color.id, isouter=True
           ).join(
               owner_alias, Product.ownercountryid == owner_alias.id, isouter=True
           ).join(
               manuf_alias, Product.manufacturercountryid == manuf_alias.id, isouter=True
           ).join(
               Status, Product.statusid == Status.id, isouter=True
           ).join(
               Condition, Product.conditionid == Condition.id, isouter=True
           ).join(
               Import, Product.importid == Import.id, isouter=True
           )

           # Не показуємо статус "видалено" (наприклад, id=7)
           q = q.filter(Product.statusid != 7)

           if unsold_only:
               q = fix_sold_filter(q, session)

           # Пошук
           if search_text:
               st_like = f"%{search_text.strip()}%"
               search_fields = [
                   Product.productnumber,
                   Product.description,
                   Product.extranote,
                   Brand.brandname,
                   Product.model,
                   Product.marking,
                   Type.typename,
                   Subtype.subtypename,
                   Color.colorname,
                   Gender.gendername
               ]
               conds = [f.ilike(st_like) for f in search_fields]
               q = q.filter(or_(*conds))

           # Бренд
           if selected_brands:
               br_subq = session.query(Brand.id).filter(Brand.brandname.in_(selected_brands)).subquery()
               q = q.filter(Product.brandid.in_(br_subq))

           # Стать
           if selected_genders:
               gd_subq = session.query(Gender.id).filter(Gender.gendername.in_(selected_genders)).subquery()
               q = q.filter(Product.genderid.in_(gd_subq))

           # Тип / Підтип
           if selected_types:
               tp_subq = session.query(Type.id).filter(Type.typename.in_(selected_types)).subquery()
               st_subq = session.query(Subtype.id).filter(Subtype.subtypename.in_(selected_types)).subquery()
               q = q.filter(or_(Product.typeid.in_(tp_subq), Product.subtypeid.in_(st_subq)))

           # Колір
           if selected_colors:
               cl_subq = session.query(Color.id).filter(Color.colorname.in_(selected_colors)).subquery()
               q = q.filter(Product.colorid.in_(cl_subq))

           # Країна
           if selected_countries:
               c_subq = session.query(Country.id).filter(Country.countryname.in_(selected_countries)).subquery()
               q = q.filter(
                   or_(
                       Product.ownercountryid.in_(c_subq),
                       Product.manufacturercountryid.in_(c_subq)
                   )
               )

           # Ціна
           if price_min > 0 or price_max < 9999:
               q = q.filter(Product.price >= price_min, Product.price <= price_max)

           def get_sizeeu_clean_expression(field):
               replacements = [
                   (',', '.'),
                   ('-', '.'),
                   ('/', '.'),
                   ('½', '.5'),
                   ('⅓', '.33'),
                   ('⅔', '.66'),
                   ('¼', '.25'),
                   ('¾', '.75')
               ]
               expr = field
               for old, new in replacements:
                   expr = func.replace(expr, old, new)
               expr = func.regexp_replace(expr, r'[^0-9\.]', '', 'g')
               expr = func.nullif(expr, '')
               expr = cast(expr, Float)
               return expr

           # Розмір (EU)
           if size_min > 14 or size_max < 60:
               size_expr = get_sizeeu_clean_expression(Product.sizeeu)
               q = q.filter(size_expr >= size_min, size_expr <= size_max)

           # Розмір (см)
           if dim_min > 5 or dim_max < 40:
               dim_expr = get_sizeeu_clean_expression(Product.measurementscm)
               q = q.filter(dim_expr >= dim_min, dim_expr <= dim_max)

           # Стан
           if selected_condition not in ["Стан", "Всі", None, ""]:
               c_obj = session.query(Condition).filter(
                   Condition.conditionname.ilike(selected_condition.lower())
               ).first()
               if c_obj:
                   q = q.filter(Product.conditionid == c_obj.id)

           # Постачальник
           if selected_supplier not in ["Постачальник", "Всі", None, ""]:
               imp_obj = session.query(Import).filter(Import.importname.ilike(selected_supplier)).first()
               if imp_obj:
                   q = q.filter(Product.importid == imp_obj.id)

           # Сортування
           if sort_option == "По імені":
               q = q.order_by(Product.productnumber.asc())
           elif sort_option == "За часом додавання":
               q = q.order_by(desc(Product.dateadded))
           elif sort_option == "Від дешевого":
               q = q.order_by(Product.price.asc())
           elif sort_option == "Від найдорожчого":
               q = q.order_by(Product.price.desc())

           results = q.all()

           products_list = []
           for row in results:
               product_data = {
                   'productnumber': row.productnumber,
                   'clonednumbers': row.clonednumbers,
                   'model': row.model,
                   'marking': row.marking,
                   'year': row.year,
                   'description': row.description,
                   'extranote': row.extranote,
                   'price': row.price,
                   'oldprice': row.oldprice,
                   'dateadded': row.dateadded,
                   'sizeeu': row.sizeeu,
                   'sizeua': row.sizeua,
                   'sizeusa': row.sizeusa,
                   'sizeuk': row.sizeuk,
                   'sizejp': row.sizejp,
                   'sizecn': row.sizecn,
                   'measurementscm': row.measurementscm,
                   'quantity': row.quantity,
                   'typename': row.typename if row.typename else '',
                   'subtypename': row.subtypename if row.subtypename else '',
                   'brandname': row.brandname if row.brandname else '',
                   'gendername': row.gendername if row.gendername else '',
                   'colorname': row.colorname if row.colorname else '',
                   'ownercountryname': row.ownercountryname if row.ownercountryname else '',
                   'manufacturercountryname': row.manufacturercountryname if row.manufacturercountryname else '',
                   'statusname': row.statusname if row.statusname else '',
                   'conditionname': row.conditionname if row.conditionname else '',
                   'importname': row.importname if row.importname else '',
               }
               products_list.append(product_data)
           return products_list

       return await asyncio.to_thread(blocking_query)

   def load_data(self, products):
       self.all_products = products
       total_products = len(self.all_products)
       self.total_pages = (total_products // self.page_size) + (1 if total_products % self.page_size != 0 else 0)
       if self.total_pages == 0:
           self.total_pages = 1
       if self.current_page > self.total_pages:
           self.current_page = self.total_pages
       self.data_loaded = True

       # Одразу показуємо дані без анімації, щоб уникнути проблем із opacity
       self.table_opacity_effect.setOpacity(1.0)
       self.show_page()
       self.update_page_buttons()
       update_filter_counts(self)

   # Не використовуємо анімацію fade, щоб уникнути проблем з кольором тексту
   async def animate_page_change(self):
       # Встановлюємо повну непрозорість
       self.table_opacity_effect.setOpacity(1.0)
       # Показуємо дані
       self.show_page()
       # Оновлюємо кнопки
       self.update_page_buttons()
       
   async def fade_table(self, start, end, duration):
       """Збережено для сумісності, але не використовується для уникнення проблем з кольором тексту"""
       animation = QPropertyAnimation(self.table_opacity_effect, b"opacity")
       animation.setDuration(duration)
       animation.setStartValue(start)
       animation.setEndValue(end)
       animation.setEasingCurve(QEasingCurve.Type.InOutCubic)
       loop = asyncio.get_event_loop()
       fut = asyncio.Future()

       def on_finished():
           fut.set_result(True)

       animation.finished.connect(on_finished)
       animation.start(QtCore.QAbstractAnimation.DeletionPolicy.DeleteWhenStopped)
       await fut

   def show_page(self):
       logging.info(f"Відображення сторінки {self.current_page} з {self.total_pages}")
       start_index = (self.current_page - 1) * self.page_size
       end_index = min(start_index + self.page_size, len(self.all_products))
       logging.info(f"Встановлення к-сті рядків таблиці: {end_index - start_index}")
       self.table.setRowCount(end_index - start_index)

       for row_num, product in enumerate(self.all_products[start_index:end_index]):
           try:
               # Замість QLabel використовуємо QTableWidgetItem з правильними даними для делегата
               productnumber = product['productnumber'] or ""
               if productnumber.startswith("#"):
                   productnumber = productnumber[1:]
               
               # Отримуємо кількість товару
               quantity = product['quantity'] or 1
               
               # Створюємо звичайний QTableWidgetItem з текстом, який буде оброблений делегатом
               item_0 = QTableWidgetItem(f"{productnumber}<sup>{quantity}</sup>")
               item_0.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
               self.table.setItem(row_num, 0, item_0)

               clonednumbers = product['clonednumbers'] or ""
               item_1 = QTableWidgetItem(clonednumbers)
               item_1.setToolTip(clonednumbers)
               self.table.setItem(row_num, 1, item_1)

               typename = product['typename'] or ""
               tooltip_typename = typename
               if '/' in typename:
                   typename = typename.split('/')[0]
               item_2 = QTableWidgetItem(typename)
               item_2.setToolTip(tooltip_typename)
               self.table.setItem(row_num, 2, item_2)

               subtypename = product['subtypename'] or ""
               item_3 = QTableWidgetItem(subtypename)
               item_3.setToolTip(subtypename)
               self.table.setItem(row_num, 3, item_3)

               brandname = (product['brandname'] or "").capitalize()
               item_4 = QTableWidgetItem(brandname)
               item_4.setToolTip(brandname)
               self.table.setItem(row_num, 4, item_4)

               model = product['model'] or ""
               item_5 = QTableWidgetItem(model)
               item_5.setToolTip(model)
               self.table.setItem(row_num, 5, item_5)

               marking = product['marking'] or ""
               item_6 = QTableWidgetItem(marking)
               item_6.setToolTip(marking)
               self.table.setItem(row_num, 6, item_6)

               year = str(product['year']) if product['year'] else ""
               item_7 = QTableWidgetItem(year)
               item_7.setToolTip(year)
               self.table.setItem(row_num, 7, item_7)

               gendername = product['gendername'] or ""
               item_8 = QTableWidgetItem(gendername)
               item_8.setToolTip(gendername)
               self.table.setItem(row_num, 8, item_8)

               colorname = product['colorname'] or ""
               tooltip_color = colorname
               if '/' in colorname:
                   colorname = colorname.split('/')[0]
               item_9 = QTableWidgetItem(colorname)
               item_9.setToolTip(tooltip_color)
               self.table.setItem(row_num, 9, item_9)

               description = product['description'] or ""
               tooltip_description = description
               if '/' in description:
                   description = description.split('/')[0]
               item_10 = QTableWidgetItem(description)
               item_10.setToolTip(tooltip_description)
               self.table.setItem(row_num, 10, item_10)

               ownercountry = product['ownercountryname'] or ""
               item_11 = QTableWidgetItem(ownercountry)
               item_11.setToolTip(ownercountry)
               self.table.setItem(row_num, 11, item_11)

               manufcountry = product['manufacturercountryname'] or ""
               item_12 = QTableWidgetItem(manufcountry)
               item_12.setToolTip(manufcountry)
               self.table.setItem(row_num, 12, item_12)

               size_str = product['sizeeu'] or ""
               size_str = self.format_size(size_str)
               item_13 = QTableWidgetItem(size_str)
               item_13.setToolTip(size_str)
               self.table.setItem(row_num, 13, item_13)

               meas_str = product['measurementscm'] or ""
               if meas_str:
                   meas_str = meas_str.replace(',', '.')
               item_14 = QTableWidgetItem(meas_str)
               item_14.setToolTip(meas_str)
               self.table.setItem(row_num, 14, item_14)

               price_value = product['price']
               if price_value is not None:
                   # Визначаємо, чи є число цілим
                   if price_value == int(price_value):
                       price_str = f"{int(price_value)}"
                   else:
                       price_str = f"{price_value}"
               else:
                   price_str = ""
               item_15 = QTableWidgetItem(price_str)
               item_15.setToolTip(price_str)
               self.table.setItem(row_num, 15, item_15)

               old_price_value = product['oldprice']
               if old_price_value is not None:
                   # Визначаємо, чи є число цілим
                   if old_price_value == int(old_price_value):
                       old_price_str = f"{int(old_price_value)}"
                   else:
                       old_price_str = f"{old_price_value}"
               else:
                   old_price_str = ""
               item_16 = QTableWidgetItem(old_price_str)
               item_16.setToolTip(old_price_str)
               self.table.setItem(row_num, 16, item_16)

               status_name = (product['statusname'] or "").capitalize()
               item_17 = QTableWidgetItem(status_name)
               item_17.setToolTip(status_name)
               self.table.setItem(row_num, 17, item_17)

               condition_name = (product['conditionname'] or "").capitalize()
               item_18 = QTableWidgetItem(condition_name)
               item_18.setToolTip(condition_name)
               self.table.setItem(row_num, 18, item_18)

               extranote = product['extranote'] or ""
               item_19 = QTableWidgetItem(extranote)
               item_19.setToolTip(extranote)
               self.table.setItem(row_num, 19, item_19)

               importn = product['importname'] or ""
               item_20 = QTableWidgetItem(importn)
               item_20.setToolTip(importn)
               self.table.setItem(row_num, 20, item_20)

               # Додаємо нове поле "Кількість"
               quantity_val = product['quantity']
               quantity_str = str(quantity_val) if quantity_val is not None else ""
               item_21 = QTableWidgetItem(quantity_str)
               item_21.setToolTip(quantity_str)
               self.table.setItem(row_num, 21, item_21)

           except Exception as e:
               logging.error(f"Помилка завантаження рядка {row_num}: {e}")

       self.adjust_table_columns()
       self.update_table_text_color()

   def update_table_text_color(self):
       """Встановлює колір тексту для всіх елементів таблиці на основі поточної теми"""
       # Вибираємо колір відповідно до теми (білий для темної теми, чорний для світлої)
       text_color = QColor(255, 255, 255) if self.is_dark_theme else QColor(0, 0, 0)
       
       for row in range(self.table.rowCount()):
           for col in range(self.table.columnCount()):
               item = self.table.item(row, col)
               if item:
                   item.setForeground(text_color)
     
       # Також оновлюємо колір тексту для QLabel в комірках таблиці
       self.update_product_labels_color()

   def update_product_labels_color(self):
       """Оновлює колір тексту для QLabel з номерами товарів при зміні теми"""
       # Отримуємо виділені рядки
       selected_rows = set(index.row() for index in self.table.selectedIndexes())
       
       # Проходимо по всіх комірках і оновлюємо стиль QLabel
       for row in range(self.table.rowCount()):
           label = self.table.cellWidget(row, 0)
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

   def adjust_table_columns(self):
       self.table.horizontalHeader().setStretchLastSection(False)
       total_width = self.table.viewport().width()
       fixed_width = sum(
           self.table.columnWidth(i)
           for i in range(self.table.columnCount())
           if i not in [10, 19]
       )
       remaining_width = total_width - fixed_width
       if remaining_width > 0:
           desc_width = remaining_width // 2
           extranote_width = remaining_width - desc_width
           self.table.setColumnWidth(10, desc_width)
           self.table.setColumnWidth(19, extranote_width)
       else:
           self.table.resizeColumnsToContents()
       for i in range(self.table.columnCount()):
           if i in [10, 19]:
               self.table.horizontalHeader().setSectionResizeMode(i, QHeaderView.ResizeMode.Stretch)
           else:
               self.table.horizontalHeader().setSectionResizeMode(i, QHeaderView.ResizeMode.ResizeToContents)
       self.table.updateGeometry()
       self.table.viewport().update()

   async def reset_filters(self, keep_search=False):
       """
       Скидає всі фільтри та оновлює таблицю
       
       :param keep_search: Якщо True, зберігає поточний пошуковий запит
       """
       # Зберігаємо пошуковий запит, якщо потрібно
       search_text = self.search_bar.text() if keep_search else ""
       
       self.search_bar.clear()
       for checkbox in (
           self.brand_checkboxes + self.gender_checkboxes +
           self.type_checkboxes + self.color_checkboxes + self.country_checkboxes
       ):
           checkbox.blockSignals(True)
           checkbox.setChecked(False)
           checkbox.blockSignals(False)

       self.price_min.blockSignals(True)
       self.price_max.blockSignals(True)
       self.size_min.blockSignals(True)
       self.size_max.blockSignals(True)
       self.dimensions_min.blockSignals(True)
       self.dimensions_max.blockSignals(True)

       self.price_min.setValue(self.price_min.minimum())
       self.price_max.setValue(self.price_max.maximum())
       self.size_min.setValue(self.size_min.minimum())
       self.size_max.setValue(self.size_max.maximum())
       self.dimensions_min.setValue(self.dimensions_min.minimum())
       self.dimensions_max.setValue(self.dimensions_max.maximum())

       self.price_min.blockSignals(False)
       self.price_max.blockSignals(False)
       self.size_min.blockSignals(False)
       self.size_max.blockSignals(False)
       self.dimensions_min.blockSignals(False)
       self.dimensions_max.blockSignals(False)

       self.update_price_slider()
       self.update_size_slider()
       self.update_dimensions_slider()

       if "Стан" not in [self.condition_combobox.itemText(i) for i in range(self.condition_combobox.count())]:
           self.condition_combobox.blockSignals(True)
           self.condition_combobox.insertItem(0, "Стан")
           self.condition_combobox.model().item(0).setEnabled(False)
           self.condition_combobox.setCurrentIndex(0)
           self.condition_combobox.blockSignals(False)
       else:
           self.condition_combobox.blockSignals(True)
           self.condition_combobox.setCurrentIndex(0)
           self.condition_combobox.blockSignals(False)

       if "Постачальник" not in [self.supplier_combobox.itemText(i) for i in range(self.supplier_combobox.count())]:
           self.supplier_combobox.blockSignals(True)
           self.supplier_combobox.insertItem(0, "Постачальник")
           self.supplier_combobox.model().item(0).setEnabled(False)
           self.supplier_combobox.setCurrentIndex(0)
           self.supplier_combobox.blockSignals(False)
       else:
           self.supplier_combobox.blockSignals(True)
           self.supplier_combobox.setCurrentIndex(0)
           self.supplier_combobox.blockSignals(False)

       if "Сортування" not in [self.sort_combobox.itemText(i) for i in range(self.sort_combobox.count())]:
           self.sort_combobox.blockSignals(True)
           self.sort_combobox.insertItem(0, "Сортування")
           self.sort_combobox.model().item(0).setEnabled(False)
           self.sort_combobox.setCurrentIndex(0)
           self.sort_combobox.blockSignals(False)
       else:
           self.sort_combobox.blockSignals(True)
           self.sort_combobox.setCurrentIndex(0)
           self.sort_combobox.blockSignals(False)

       self.unsold_checkbox.blockSignals(True)
       self.unsold_checkbox.setChecked(True)
       self.unsold_checkbox.blockSignals(False)

       update_filter_counts(self)
       self.update_slider_locks()
       
       # Відновлюємо пошуковий запит, якщо потрібно
       if keep_search and search_text:
           self.search_bar.setText(search_text)
           
       await self.apply_filters()

   def update_price_spinboxes(self):
       self.price_min.blockSignals(True)
       self.price_max.blockSignals(True)
       self.price_min.setValue(self.price_slider.low)
       self.price_max.setValue(self.price_slider.high)
       self.price_min.blockSignals(False)
       self.price_max.blockSignals(False)

   def update_size_spinboxes(self):
       self.size_min.blockSignals(True)
       self.size_max.blockSignals(True)
       self.size_min.setValue(self.size_slider.low)
       self.size_max.setValue(self.size_slider.high)
       self.size_min.blockSignals(False)
       self.size_max.blockSignals(False)
       self.update_slider_locks()

   def update_dimensions_spinboxes(self):
       self.dimensions_min.blockSignals(True)
       self.dimensions_max.blockSignals(True)
       self.dimensions_min.setValue(self.dimensions_slider.low)
       self.dimensions_max.setValue(self.dimensions_slider.high)
       self.dimensions_min.blockSignals(False)
       self.dimensions_max.blockSignals(False)
       self.update_slider_locks()

   def update_price_slider(self):
       self.price_slider.blockSignals(True)
       self.price_slider.setLow(self.price_min.value())
       self.price_slider.setHigh(self.price_max.value())
       self.price_slider.blockSignals(False)

   def update_size_slider(self):
       self.size_slider.blockSignals(True)
       self.size_slider.setLow(self.size_min.value())
       self.size_slider.setHigh(self.size_max.value())
       self.size_slider.blockSignals(False)
       self.update_slider_locks()

   def update_dimensions_slider(self):
       self.dimensions_slider.blockSignals(True)
       self.dimensions_slider.setLow(self.dimensions_min.value())
       self.dimensions_slider.setHigh(self.dimensions_max.value())
       self.dimensions_slider.blockSignals(False)
       self.update_slider_locks()

   def update_slider_locks(self):
       size_changed = (
           self.size_min.value() != self.size_min.minimum() or
           self.size_max.value() != self.size_max.maximum()
       )
       dim_changed = (
           self.dimensions_min.value() != self.dimensions_min.minimum() or
           self.dimensions_max.value() != self.dimensions_max.maximum()
       )

       if size_changed and not dim_changed:
           self.dimensions_slider.setEnabled(False)
           self.dimensions_min.setEnabled(False)
           self.dimensions_max.setEnabled(False)
           grey_style = "QSlider { background-color: #dcdcdc; }"
           self.dimensions_slider.setStyleSheet(grey_style)
           self.dimensions_slider.setToolTip("Заблоковано (вибрано 'Розмір (EU)')")
           self.dimensions_min.setToolTip("Заблоковано (вибрано 'Розмір (EU)')")
           self.dimensions_max.setToolTip("Заблоковано (вибрано 'Розмір (EU)'')")
           self.size_slider.setStyleSheet("")
           self.size_slider.setToolTip("")
           self.size_min.setToolTip("")
           self.size_max.setToolTip("")
       elif dim_changed and not size_changed:
           self.size_slider.setEnabled(False)
           self.size_min.setEnabled(False)
           self.size_max.setEnabled(False)
           grey_style = "QSlider { background-color: #dcdcdc; }"
           self.size_slider.setStyleSheet(grey_style)
           self.size_slider.setToolTip("Заблоковано (вибрано 'СМ')")
           self.size_min.setToolTip("Заблоковано (вибрано 'СМ')")
           self.size_max.setToolTip("Заблоковано (вибрано 'СМ')")
           self.dimensions_slider.setStyleSheet("")
           self.dimensions_slider.setToolTip("")
           self.dimensions_min.setToolTip("")
           self.dimensions_max.setToolTip("")
       else:
           self.dimensions_slider.setEnabled(True)
           self.dimensions_min.setEnabled(True)
           self.dimensions_max.setEnabled(True)
           self.dimensions_slider.setStyleSheet("")
           self.dimensions_slider.setToolTip("")
           self.dimensions_min.setToolTip("")
           self.dimensions_max.setToolTip("")
           self.size_slider.setEnabled(True)
           self.size_min.setEnabled(True)
           self.size_max.setEnabled(True)
           self.size_slider.setStyleSheet("")
           self.size_slider.setToolTip("")
           self.size_min.setToolTip("")
           self.size_max.setToolTip("")

   def format_size(self, size_str):
       if not size_str:
           return ""
       size_str = size_str.replace(',', '.')
       parts = size_str.split('-')
       formatted = []
       for p in parts:
           try:
               val = float(p)
               if val.is_integer():
                   formatted.append(str(int(val)))
               else:
                   formatted.append(self.decimal_to_fraction_symbol(val))
           except ValueError:
               formatted.append(p)
       return '-'.join(formatted)

   def decimal_to_fraction_symbol(self, value):
       integer_part = int(value)
       decimal_part = value - integer_part
       if abs(decimal_part - 0.25) < 0.05:
           fraction = '¼'
       elif abs(decimal_part - 0.3333) < 0.05 or abs(decimal_part - 0.3) < 0.05:
           fraction = '⅓'
       elif abs(decimal_part - 0.5) < 0.05:
           fraction = '½'
       elif abs(decimal_part - 0.6666) < 0.05 or abs(decimal_part - 0.6) < 0.05:
           fraction = '⅔'
       elif abs(decimal_part - 0.75) < 0.05:
           fraction = '¾'
       else:
           if decimal_part == 0:
               return str(integer_part)
           else:
               return f"{value:.2f}".rstrip('0').rstrip('.').replace(",", ".")
       if integer_part == 0:
           return fraction
       else:
           return f"{integer_part}{fraction}"

   def show_cell_info(self, row, column):
       item = self.table.item(row, column)
       if item:
           QMessageBox.information(self, "Деталі комірки", f"Вміст:\n\n{item.text()}")

   def select_column(self, index):
       self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectColumns)
       self.table.selectColumn(index)
       self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)

   def toggle_column(self, index, state):
       is_checked = (state == Qt.CheckState.Checked.value)
       self.table.setColumnHidden(index, not is_checked)
       self.adjust_table_columns()

   def show_table_context_menu(self, pos):
       try:
           row = self.table.indexAt(pos).row()
           if row < 0:
               return

           menu = QMenu(self)
           # Отримуємо дані про продукт
           status_item = self.table.item(row, 17)  # колонка status
           product_item = self.table.item(row, 0)  # колонка productnumber
           
           if not status_item or not product_item:
               return
               
           st_text = status_item.text().strip().lower() if status_item.text() else ""
           pnum = product_item.text().strip() if product_item.text() else ""
           
           # Опція «Показати в замовленні» (тільки якщо продано)
           if st_text == "продано":
               show_in_orders_action = QAction("Показати в замовленні", self)
               show_in_orders_action.triggered.connect(lambda checked=False, pn=pnum: self.show_in_orders(pn))
               menu.addAction(show_in_orders_action)
           
           # Опція «Показати в Google Sheets» (для всіх товарів)
           show_in_sheets_action = QAction("Показати в Google Sheets", self)
           show_in_sheets_action.triggered.connect(lambda checked=False, pn=pnum: self.show_in_google_sheets(pn))
           menu.addAction(show_in_sheets_action)
           
           # Опція «Видалити» (для всіх товарів)
           delete_action = QAction("Видалити", self)
           delete_action.triggered.connect(lambda checked=False, r=row: self.delete_product(r))
           menu.addAction(delete_action)

           # Використовуємо exec_ замість exec для сумісності з новішими версіями PyQt
           try:
               # Перевіряємо наявність методу exec_ або exec
               if hasattr(menu, 'exec_'):
                   menu.exec_(self.table.viewport().mapToGlobal(pos))
               else:
                   menu.exec(self.table.viewport().mapToGlobal(pos))
           except Exception as e:
               logging.error(f"Помилка при відображенні контекстного меню: {e}")
       except Exception as e:
           logging.error(f"Помилка в методі show_table_context_menu: {e}")

   def show_in_orders(self, product_number):
       try:
           if not product_number:
               logging.warning("Спроба показати порожній номер продукту в замовленнях")
               return
               
           if self.parent_window:
               self.parent_window.show_order_for_product(product_number)
           else:
               logging.error("Неможливо перейти до замовлень: відсутнє головне вікно")
       except Exception as e:
           logging.error(f"Помилка при переході до замовлення: {e}")
           QMessageBox.warning(self, "Помилка", f"Не вдалося показати продукт у замовленнях: {str(e)}")
           
   def show_in_google_sheets(self, product_number):
       try:
           if not product_number:
               logging.warning("Спроба показати порожній номер продукту в Google Sheets")
               return
               
           # TODO: Реалізувати логіку відкриття Google Sheets з товаром
           QMessageBox.information(self, "Google Sheets", f"Відкриття продукту {product_number} в Google Sheets (функція в розробці)")
       except Exception as e:
           logging.error(f"Помилка при відкритті Google Sheets: {e}")
           QMessageBox.warning(self, "Помилка", f"Не вдалося відкрити Google Sheets: {str(e)}")

   def delete_product(self, row):
       item = self.table.item(row, 0)
       if not item:
           return
       product_number = item.text()
       
       # Перевіряємо, чи продукт є проданим або в замовленнях
       status_item = self.table.item(row, 17)  # колонка status
       if status_item and status_item.text().strip().lower() == "продано":
           reply = QMessageBox.warning(
               self,
               "Увага!",
               f"Товар {product_number} має статус 'Продано' і може бути в активних замовленнях. "
               f"Видалення може призвести до порушення цілісності даних. Ви впевнені, що хочете продовжити?",
               QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
               QMessageBox.StandardButton.No
           )
           if reply != QMessageBox.StandardButton.Yes:
               return
       else:
           reply = QMessageBox.question(
               self,
               "Підтвердження",
               f"Ви впевнені, що хочете видалити товар: {product_number}?",
               QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
               QMessageBox.StandardButton.No
           )
           if reply != QMessageBox.StandardButton.Yes:
               return
           
       asyncio.ensure_future(self.remove_product_from_db(product_number))

   async def remove_product_from_db(self, product_number):
       def blocking_delete():
           session.rollback()
           prod = session.query(Product).filter_by(productnumber=product_number).first()
           if not prod:
               return False, "Товар не знайдено в базі даних"
               
           # Перевіряємо, чи товар є у замовленнях
           order_details = session.query(OrderDetails).filter_by(product_id=prod.id).all()
           if order_details:
               return False, f"Неможливо видалити товар {product_number}, оскільки він міститься в замовленнях. Спочатку видаліть товар із замовлень."
               
           # Якщо все гаразд, видаляємо товар
           session.delete(prod)
           session.commit()
           return True, f"Товар {product_number} успішно видалено"

       try:
           self.parent_window.show_progress_bar(True)
           success, message = await asyncio.to_thread(blocking_delete)
           if success:
               await self.apply_filters()
               QMessageBox.information(self, "Успіх", message)
           else:
               QMessageBox.warning(self, "Помилка", message)
       except Exception as e:
           self.show_error_message(str(e))
       finally:
           self.parent_window.show_progress_bar(False)

   def show_error_message(self, error_message):
       QMessageBox.critical(self, "Помилка", f"Сталася помилка:\n{error_message}")

   def update_page_buttons(self):
       """
       Оновлює кнопки пагінації на основі поточної сторінки та загальної кількості сторінок.
       """
       try:
           # Очищаємо поточні кнопки
           for i in reversed(range(self.page_buttons_layout.count())):
               w = self.page_buttons_layout.takeAt(i).widget()
               if w:
                   w.setParent(None)
                   
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
           first_button.clicked.connect(lambda: self.go_to_page(1))
           self.page_buttons_layout.addWidget(first_button)
           
           # Кнопка "назад"
           prev_button = QPushButton("‹")
           prev_button.setStyleSheet(btn_style)
           prev_button.setEnabled(self.current_page > 1)
           prev_button.clicked.connect(lambda: self.go_to_page(self.current_page - 1))
           self.page_buttons_layout.addWidget(prev_button)
           
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
               
               page_button.clicked.connect(lambda _, p=page: self.go_to_page(p))
               self.page_buttons_layout.addWidget(page_button)
           
           # Кнопка "вперед"
           next_button = QPushButton("›")
           next_button.setStyleSheet(btn_style)
           next_button.setEnabled(self.current_page < self.total_pages)
           next_button.clicked.connect(lambda: self.go_to_page(self.current_page + 1))
           self.page_buttons_layout.addWidget(next_button)
           
           # Кнопка "на останню сторінку"
           last_button = QPushButton("»")
           last_button.setStyleSheet(btn_style)
           last_button.setEnabled(self.current_page < self.total_pages)
           last_button.clicked.connect(lambda: self.go_to_page(self.total_pages))
           self.page_buttons_layout.addWidget(last_button)
           
       except Exception as e:
           logging.error(f"Помилка при оновленні кнопок пагінації: {e}")
           logging.error(traceback.format_exc())

   def go_to_page(self, page):
       if page != self.current_page and 1 <= page <= self.total_pages:
           self.current_page = page
           asyncio.ensure_future(self.animate_page_change())

   def hide_refresh_button(self):
       """Приховує кнопку оновлення на вкладці товарів"""
       if hasattr(self, 'refresh_button'):
           self.refresh_button.setVisible(False)
     
   def refresh_table(self):
       """Оновлює таблицю товарів"""
       # Використовуємо існуючий метод для оновлення товарів
       asyncio.ensure_future(self.apply_filters(is_initial_load=True))

   # Метод для обробки натискання кнопки оновлення
   def run_refresh_from_shared_button(self):
       """Викликає універсальний метод оновлення з головного вікна"""
       if self.parent_window:
           self.parent_window.show_update_dialog_and_parse()
       else:
           # Якщо вікно недоступне, використовуємо старий метод універсального парсингу
           self.window().start_universal_parsing()

   def refresh_products_table(self):
       """Оновлює таблицю продуктів використовуючи безблокуючий метод під час парсингу"""
       try:
           # Перевіряємо, чи виконується парсинг
           status = parsing_api.get_status()
           
           if status.get("is_running", False):
               # Якщо парсинг виконується, використовуємо безблокуючий метод
               only_available = self.only_available_checkbox.isChecked() if hasattr(self, "only_available_checkbox") else False
               
               products = parsing_api.get_products(
                   limit=100,  # Обмежуємо кількість продуктів для кращої продуктивності
                   filter_text=self.search_products_edit.text() if hasattr(self, "search_products_edit") and self.search_products_edit.text() else None,
                   only_available=only_available
               )
               
               # Очищаємо таблицю
               self.products_table.setRowCount(0)
               
               # Заповнюємо таблицю даними
               for product in products:
                   self.add_product_to_table(product)
                   
               # Додаємо примітку, що дані можуть бути неповними
               if not hasattr(self, "parsing_note_label"):
                   from PyQt5.QtWidgets import QLabel
                   self.parsing_note_label = QLabel("Примітка: під час парсингу відображаються останні 100 продуктів", self)
                   self.parsing_note_label.setStyleSheet("color: #6c757d; font-style: italic;")
                   
                   # Додаємо віджет у верхню частину таблиці
                   if hasattr(self, "products_tab_layout"):
                       self.products_tab_layout.insertWidget(2, self.parsing_note_label)
                   else:
                       # Якщо layout недоступний, додаємо віджет безпосередньо
                       self.parsing_note_label.setGeometry(10, 60, 400, 20)
                       self.parsing_note_label.show()
               
               if hasattr(self, "parsing_note_label"):
                   self.parsing_note_label.show()
           else:
               # Якщо парсинг не виконується, використовуємо стандартний метод
               # Приховуємо примітку, якщо вона існує
               if hasattr(self, "parsing_note_label") and self.parsing_note_label:
                   self.parsing_note_label.hide()
               
               # Оригінальний код оновлення таблиці
               if hasattr(self, "_original_refresh_products_table"):
                   self._original_refresh_products_table()
               else:
                   logger.error("Оригінальна функція оновлення таблиці продуктів не знайдена")
       
       except Exception as e:
           logger.error(f"Помилка при оновленні таблиці продуктів: {e}")
           import traceback
           logger.error(traceback.format_exc())

   def add_product_to_table(self, product):
       """Додає продукт в таблицю (для використання з асинхронним методом)"""
       try:
           row_position = self.table.rowCount()
           self.table.insertRow(row_position)
           
           # Заповнюємо комірки на основі структури даних, отриманої з API
           # ID
           id_item = QTableWidgetItem(str(product.get('id', '')))
           self.table.setItem(row_position, 0, id_item)
           
           # Номер продукту
           from PyQt6.QtWidgets import QLabel
           number_label = QLabel(product.get('product_number', ''))
           number_label.setFont(QFont("Arial", 13))
           
           # Встановлюємо стиль з урахуванням поточної теми
           text_color = "white" if self.is_dark_theme else "black"
           number_label.setStyleSheet(f"""
               QLabel {{
                   padding: 0px 15px;
                   margin: 0px;
                   min-height: 38px;
                   background-color: transparent;
                   color: {text_color};
               }}
           """)
           number_label.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
           number_label.setProperty("role", "product_cell")
           
           # Встановлюємо QLabel в комірку таблиці замість QTableWidgetItem
           self.table.setCellWidget(row_position, 1, number_label)
           
           # Клони
           clones_item = QTableWidgetItem(product.get('clones', ''))
           self.table.setItem(row_position, 2, clones_item)
           
           # Ціна
           price_value = product.get('price')
           if price_value is not None:
               # Визначаємо, чи є число цілим
               if price_value == int(price_value):
                   price_str = f"{int(price_value)}"
               else:
                   price_str = f"{price_value}"
           else:
               price_str = ""
           price_item = QTableWidgetItem(price_str)
           self.table.setItem(row_position, 3, price_item)
           
           # Стара ціна
           old_price_value = product.get('old_price')
           if old_price_value is not None:
               # Визначаємо, чи є число цілим
               if old_price_value == int(old_price_value):
                   old_price_str = f"{int(old_price_value)}"
               else:
                   old_price_str = f"{old_price_value}"
           else:
               old_price_str = ""
           old_price_item = QTableWidgetItem(old_price_str)
           self.table.setItem(row_position, 4, old_price_item)
           
           # Статус
           status_item = QTableWidgetItem(product.get('status_name', ''))
           self.table.setItem(row_position, 5, status_item)
           
           # Дата створення
           created_at_item = QTableWidgetItem(product.get('created_at', ''))
           self.table.setItem(row_position, 6, created_at_item)
           
           # Дата оновлення
           updated_at_item = QTableWidgetItem(product.get('updated_at', ''))
           self.table.setItem(row_position, 7, updated_at_item)
           
       except Exception as e:
           logger.error(f"Помилка при додаванні продукту до таблиці: {e}")
           import traceback
           logger.error(traceback.format_exc())

   def on_selection_changed(self):
       """Оновлює колір тексту в QLabel при виділенні рядка"""
       # Текст білого кольору для виділених рядків, звичайний колір для інших
       selected_rows = set(index.row() for index in self.table.selectedIndexes())
       
       for row in range(self.table.rowCount()):
           # Визначаємо, чи є рядок виділеним
           is_selected = row in selected_rows
           
           # Для колонки з номером товару (колонка 0)
           label = self.table.cellWidget(row, 0)
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

   async def show_products_for_order(self, order_id):
       """
       Фільтрує таблицю товарів для відображення товарів конкретного замовлення
       і підсвічує їх фіолетовою рамкою
       
       :param order_id: ID замовлення
       """
       try:
           if not order_id:
               logging.warning("Спроба показати товари для пустого ID замовлення")
               return
               
           logging.info(f"Пошук товарів для замовлення #{order_id}")
           
           # Очищаємо поточні фільтри
           await self.reset_filters()
           
           # Отримуємо список номерів товарів для замовлення з бази даних
           product_numbers = await self._get_product_numbers_for_order(order_id)
           
           if not product_numbers:
               QMessageBox.information(self, "Інформація", f"В замовленні #{order_id} не знайдено товарів")
               return
               
           # Встановлюємо фільтр по знайдених номерах товарів
           # Використовуємо оператор OR для відображення всіх товарів з замовлення
           search_text = " OR ".join(product_numbers)
           self.search_bar.setText(search_text)
           
           # Оновлюємо таблицю з новим фільтром
           await self.apply_filters()
           
           # Чекаємо завершення завантаження даних
           await asyncio.sleep(0.3)
           
           # Підсвічуємо знайдені товари фіолетовою рамкою
           self._highlight_products(product_numbers)
           
           # Показуємо інформаційне повідомлення
           self.set_status_message(f"Відображено товари з замовлення #{order_id}", 5000)
           
       except Exception as e:
           logging.error(f"Помилка при показі товарів для замовлення: {e}")
           QMessageBox.warning(self, "Помилка", f"Не вдалося показати товари для замовлення: {str(e)}")

   async def _get_product_numbers_for_order(self, order_id):
       """
       Отримує список номерів товарів для вказаного замовлення
       
       :param order_id: ID замовлення
       :return: Список номерів товарів
       """
       try:
           from db import connect_to_db
           
           def blocking_query():
               conn = connect_to_db()
               cursor = conn.cursor()
               
               cursor.execute("""
                   SELECT p.productnumber
                   FROM products p
                   JOIN order_details od ON p.id = od.product_id
                   WHERE od.order_id = %s
               """, (order_id,))
               
               results = cursor.fetchall()
               cursor.close()
               conn.close()
               
               return [row[0] for row in results if row[0]]
               
           return await asyncio.to_thread(blocking_query)
       except Exception as e:
           logging.error(f"Помилка при отриманні товарів замовлення: {e}")
           return []

   def _highlight_products(self, product_numbers):
       """
       Підсвічує рядки товарів з вказаними номерами
       
       :param product_numbers: Список номерів товарів для підсвічування
       """
       try:
           # Фіолетовий колір для підсвічування (аналогічно корпоративному кольору)
           highlight_color = QColor("#7851A9")
           
           # Скидаємо попереднє підсвічування
           for row in range(self.table.rowCount()):
               for col in range(self.table.columnCount()):
                   item = self.table.item(row, col)
                   if item:
                       item.setBackground(QColor("transparent"))
           
           # Шукаємо і підсвічуємо потрібні товари
           highlighted_rows = []
           for row in range(self.table.rowCount()):
               product_item = self.table.item(row, 0)  # колонка productnumber
               if not product_item:
                   continue
                   
               product_number = product_item.text().strip()
               if product_number in product_numbers:
                   highlighted_rows.append(row)
                   
                   # Встановлюємо стиль підсвічування (фіолетова рамка)
                   for col in range(self.table.columnCount()):
                       item = self.table.item(row, col)
                       if item:
                           item.setData(Qt.ItemDataRole.UserRole, "highlighted")
                           # Підсвічуємо фоном всі комірки
                           item.setBackground(QColor(highlight_color.red(), highlight_color.green(), highlight_color.blue(), 30))
           
           # Якщо знайдені товари, прокручуємо до першого
           if highlighted_rows:
               self.table.scrollToItem(
                   self.table.item(highlighted_rows[0], 0),
                   QAbstractItemView.ScrollHint.PositionAtCenter
               )
       except Exception as e:
           logging.error(f"Помилка при підсвічуванні товарів: {e}")

   def set_status_message(self, message, timeout=0):
       """
       Встановлює статусне повідомлення у статус-бар
       
       :param message: Текст повідомлення
       :param timeout: Час в мс, через який повідомлення буде автоматично очищено (0 - не очищати)
       """
       if self.parent_window:
           self.parent_window.set_status_message(message, timeout)
       else:
           logging.debug(f"Статусне повідомлення (без батьківського вікна): {message}")

# Додаємо спеціальний клас делегата для першої колонки з номерами
class NumberColumnDelegate(QStyledItemDelegate):
    def __init__(self, parent=None, is_dark_theme=False):
        super().__init__(parent)
        self.is_dark_theme = is_dark_theme
        self.highlight_color = QColor("#7851A9")  # Корпоративний фіолетовий колір
        
    def paint(self, painter, option, index):
        if index.column() == 0:  # Тільки для першої колонки з номерами
            # Отримуємо дані
            text = index.model().data(index, Qt.ItemDataRole.DisplayRole)
            if not text:
                return
                
            # Визначаємо стиль відображення
            painter.save()
            
            # Зафарбовуємо фон
            if option.state & QStyle.StateFlag.State_Selected:
                # Використовуємо корпоративний фіолетовий колір замість стандартного
                painter.fillRect(option.rect, self.highlight_color)
                text_color = QColor("white")  # Білий текст на фіолетовому фоні
            else:
                painter.fillRect(option.rect, option.palette.base())
                text_color = QColor("white") if self.is_dark_theme else QColor("black")
                
            # Налаштовуємо шрифт
            font = QFont("Arial", 13)
            painter.setFont(font)
            
            # Встановлюємо колір тексту
            painter.setPen(text_color)
            
            # Малюємо текст з відступами
            text_rect = option.rect.adjusted(15, 0, -15, 0)  # Горизонтальні відступи по 15px
            
            # Розбиваємо текст на основну частину і надстроковий індекс
            if "<sup>" in text:
                number, quantity = text.split("<sup>")
                quantity = quantity.replace("</sup>", "")
                
                # Розраховуємо розміри для основного тексту
                font_metrics = painter.fontMetrics()
                number_width = font_metrics.horizontalAdvance(number)
                
                # Малюємо основний текст
                painter.drawText(
                    text_rect,
                    Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft,
                    number
                )
                
                # Налаштовуємо шрифт для надстрокового індексу
                sup_font = QFont("Arial", 10)
                painter.setFont(sup_font)
                
                # Покращуємо позиціонування надстрокового індексу
                sup_rect = QRect(
                    text_rect.x() + number_width,
                    text_rect.y() + (text_rect.height() // 4) - 2,  # Краще вертикальне позиціонування
                    font_metrics.horizontalAdvance(quantity) + 5,
                    font_metrics.height() // 2
                )
                
                painter.drawText(
                    sup_rect,
                    Qt.AlignmentFlag.AlignLeft,
                    quantity
                )
            else:
                painter.drawText(
                    text_rect,
                    Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft,
                    text
                )
                
            painter.restore()
        else:
            super().paint(painter, option, index)
    
    def setDarkTheme(self, is_dark):
        self.is_dark_theme = is_dark
        