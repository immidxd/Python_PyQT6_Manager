#!/usr/bin/env python3
# -*- coding: utf-8 -*-




import os
import logging
import asyncio
import subprocess
import sys
import time
import json
import traceback
import re
import datetime
import random
from pathlib import Path




from PyQt6 import QtCore
from PyQt6.QtWidgets import (
 QMainWindow, QTabWidget, QStatusBar, QProgressBar, QMessageBox,
 QAbstractItemView, QLabel, QWidget, QVBoxLayout, QPushButton, QHBoxLayout, QFrame, QApplication, QDialog
)
from PyQt6.QtCore import Qt, QPropertyAnimation, QEasingCurve, QThread, pyqtSignal, QSize, QRect, QTimer, QMargins, QEvent, QCoreApplication
import qasync
from PyQt6.QtGui import QColor, QFont, QIcon, QPixmap, QAction, QPalette
import qtawesome as qta




from db import session
from models import (
 Product, OrderDetails
)

from services.theme_service import apply_theme
from services.notification_service import NotificationManager
from workers import UniversalParsingWorker, AsyncUniversalParsingWorker, OrderParsingWorker




logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')








class MainWindow(QMainWindow):
 """
 Головне вікно, що містить вкладки:
   1) "Товари"
   2) "Замовлення"




 Тут є:
   - Один спільний прогрес-бар (self.progress_bar).
   - Метод show_order_for_product() для переходу на вкладку «Замовлення»
     та виділення конкретного замовлення, в якому фігурує цей товар.
 """
 def __init__(self):
     print("Початок __init__ MainWindow")
     super().__init__()
     logging.debug("Запуск програми... у MainWindow")

     # Додаємо змінну для відстеження статусу оновлення таблиць
     self._is_refreshing_tables = False
     
     # Додаємо змінну для відстеження асинхронного оновлення
     self._is_async_updating = False
     
     # Змінні для відстеження завершення оновлення таблиць
     self._products_done = False
     self._orders_done = False
     
     # Змінна для збереження поточного активного статусу процесу
     self._current_process_status = ""
     # Таймер для повернення статусу активного процесу
     self._status_return_timer = None
     
     # Змінна для відстеження стану прогрес-бару
     self._is_progress_indeterminate = False
     
     # Для плавної анімації прогрес-бару
     self._indeterminate_value = 0.0
     self._animation_speed = 1.0  # швидкість анімації
     
     # Змінні для стабільної індикації прогресу
     self._progress_direction = 1  # 1 - вправо, -1 - вліво
     self._progress_position = 0
     
     # Прапорець для відстеження активного стану анімації
     self._progress_animation_is_active = False
     
     # Таймер для постійної анімації
     self._continuous_animation_timer = QtCore.QTimer(self)
     self._continuous_animation_timer.timeout.connect(self._animate_progress_step)
     self._continuous_animation_timer.setInterval(50)  # 50 мс між оновленнями - плавна анімація
     
     self.setWindowTitle("Менеджер Продуктів та Замовлень")
     self.resize(1200, 800)

     # Прибираємо контури з вікна
     self.setStyleSheet("""
         QMainWindow {
             border: none;
             background-color: transparent;
         }
         QTabWidget::pane { 
             border-top: 0px;
             top: -1px;
         }
     """)

     # Початково – світла тема
     self.is_dark_theme = False

     # Створюємо менеджера сповіщень
     self.notification_manager = NotificationManager(self)

     # Створюємо QTabWidget з двома вкладками
     self.tab_widget = QTabWidget()
     self.tab_widget.setMovable(False)
     self.tab_widget.setTabPosition(QTabWidget.TabPosition.North)
     self.tab_widget.setDocumentMode(True)
     self.tab_widget.setTabsClosable(False)
     
     # Прибираємо товстий контур між заголовками вкладок і вмістом
     self.tab_widget.setStyleSheet("""
         QTabWidget::pane { 
             border-top: 0px solid #C2C7CB;
             position: absolute;
             top: -0.5em; 
         }
         QTabWidget::tab-bar {
             alignment: left;
         }
         QTabBar::tab {
             background: transparent;
             border: none;
             padding: 8px 12px;
             margin-right: 4px;
         }
         QTabBar::tab:!selected {
             color: #888888;  /* Сірий колір тексту для неактивних вкладок */
         }
     """)

     self.products_tab = None
     self.orders_tab = None

     # Створюємо статус-бар
     status_bar = QStatusBar()
     self.setStatusBar(status_bar)
     
     # Створюємо спеціальний лейбл для статусних повідомлень (праворуч)
     self.status_message_label = QLabel("")
     self.status_message_label.setAlignment(Qt.AlignmentFlag.AlignRight)
     font = self.status_message_label.font()
     font.setPointSize(font.pointSize() - 4)  # Ще менший шрифт (з -3 на -4)
     font.setItalic(True)  # Курсив
     self.status_message_label.setFont(font)
     # Налаштовуємо поведінку тексту - автоматично обрізати текст і показувати трикрапку
     self.status_message_label.setTextFormat(Qt.TextFormat.PlainText)
     self.status_message_label.setTextInteractionFlags(Qt.TextInteractionFlag.NoTextInteraction)
     self.status_message_label.setWordWrap(False)
     # Налаштовуємо автоматичне скорочення (еліпсизацію) тексту в середині
     self.status_message_label.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
     # Додаємо відступ праворуч для тексту і зміщуємо трохи лівіше
     self.status_message_label.setContentsMargins(0, 0, 20, 0)
     # Встановлюємо максимальну ширину для лейбла - 55% від ширини вікна (трохи менше)
     self.status_message_label.setMaximumWidth(int(self.width() * 0.55))
     status_bar.addPermanentWidget(self.status_message_label, 1)  # Додаємо stretch=1 для розтягування
     
     # Налаштування прогрес-бару
     self.progress_bar_widget = QFrame(self)
     self.progress_bar_widget.setGeometry(0, self.height() - 3, self.width(), 3)
     self.progress_bar_widget.setStyleSheet("QFrame { background-color: rgb(220, 220, 220); }")
     self.progress_bar_widget.setVisible(False)
     
     self.progress_bar = QProgressBar(self.progress_bar_widget)
     self.progress_bar.setGeometry(0, 0, self.width(), 3)
     self.progress_bar.setRange(0, 100)
     self.progress_bar.setValue(0)
     self.progress_bar.setTextVisible(False)
     self.progress_bar.setStyleSheet("QProgressBar { border: none; background-color: transparent; } "
                                   "QProgressBar::chunk { background-color: #7851A9; }")
     
     # Ініціалізуємо зміни для прогрес-бару
     self._is_progress_indeterminate = False
     self._progress_animation = QPropertyAnimation(self.progress_bar, b"value")
     self._progress_animation.setDuration(300)  # Тривалість анімації 300 мс
     self._progress_animation.setEasingCurve(QEasingCurve.Type.InOutQuad)
     
     # Створюємо анімацію для індетермінованого режиму
     self._indeterminate_animation = QPropertyAnimation(self.progress_bar, b"value")
     self._indeterminate_animation.setDuration(800)  # Швидша тривалість для постійного руху
     self._indeterminate_animation.setEasingCurve(QEasingCurve.Type.Linear)  # Лінійна анімація для плавного руху
     self._indeterminate_animation.setStartValue(0)
     self._indeterminate_animation.setEndValue(100)
     
     # Спочатку ініціалізуємо вкладки
     self.init_tabs()
     
     # Встановлюємо віджет вкладок як центральний віджет
     self.setCentralWidget(self.tab_widget)
     
     # Встановлюємо позицію віджета з прогрес-баром
     self.progress_bar_widget.setParent(self)
     self.progress_bar_widget.setGeometry(0, self.height() - 3, self.width(), 3)
     
     # Перед показом переконуємося, що прогрес-бар з'явиться поверх інших віджетів
     self.progress_bar_widget.raise_()

     from services.theme_service import apply_theme
     apply_theme(self, self.is_dark_theme)

     self.products_tab.apply_external_theme(self.is_dark_theme)
     self.orders_tab.apply_external_theme(self.is_dark_theme)

     # Не викликаємо async методи тут, а перемістимо їх до showEvent
     # qasync.create_task(self.products_tab.apply_filters(is_initial_load=True))
     # qasync.create_task(self.orders_tab.apply_orders_filters(is_initial_load=True))

     print("Кінець __init__ MainWindow")

 def init_tabs(self):
     from views.products_tab import ProductsTab
     from views.orders_tab import OrdersTab

     self.products_tab = ProductsTab(parent=self)
     self.orders_tab = OrdersTab(parent=self)

     self.tab_widget.addTab(self.products_tab, "Товари")
     self.tab_widget.addTab(self.orders_tab, "Замовлення")
     
     # Приховуємо кнопки оновлення безпосередньо на вкладках
     # if hasattr(self.products_tab, 'refresh_button'):
     #     self.products_tab.refresh_button.setVisible(False)
     
     # Додаємо методи оновлення таблиць, якщо вони відсутні
     if not hasattr(self.products_tab, 'refresh_table'):
         setattr(self.products_tab, 'refresh_table', lambda: asyncio.ensure_future(self.products_tab.apply_filters(is_initial_load=True)))
     
     if not hasattr(self.orders_tab, 'refresh_orders_table'):
         setattr(self.orders_tab, 'refresh_orders_table', lambda: asyncio.ensure_future(self.orders_tab.apply_orders_filters()))

 def toggle_theme_global(self):
     """Перемикає тему між темною та світлою для всього додатку."""
     self.is_dark_theme = not self.is_dark_theme
     
     # Застосовуємо нову тему
     from services.theme_service import apply_theme
     apply_theme(self, self.is_dark_theme)
     
     # Передаємо нове значення is_dark_theme до вкладок
     self.products_tab.apply_external_theme(self.is_dark_theme)
     self.orders_tab.apply_external_theme(self.is_dark_theme)

 async def force_unfiltered_orders_refresh(self):
     if self.orders_tab:
         await self.orders_tab.reset_orders_filters()

 def show_order_for_product(self, product_number):
     try:
         if not product_number:
             QMessageBox.warning(
                 self, 
                 "Помилка", 
                 "Не вказано номер продукту"
             )
             return
         
         # Переходимо на вкладку "Замовлення"
         self.tab_widget.setCurrentWidget(self.orders_tab)
         
         # Встановлюємо номер продукту в пошукове поле
         if hasattr(self.orders_tab, 'orders_search_bar'):
             self.orders_tab.orders_search_bar.setText(product_number)
             self.orders_tab.orders_search_bar.setFocus()
         
         # Запускаємо пошук
         if hasattr(self.orders_tab, 'apply_orders_filters'):
             asyncio.ensure_future(self.orders_tab.apply_orders_filters())
         
         # Більше не потрібно шукати замовлення через складну асинхронну логіку, 
         # оскільки ми просто передали номер продукту в пошук
         # і система сама покаже відповідні замовлення
     except Exception as e:
         logging.error(f"Помилка в методі show_order_for_product: {e}")
         QMessageBox.warning(self, "Помилка", f"Не вдалося перейти до замовлення: {str(e)}")

 def showEvent(self, event):
     """
     Метод викликається при першому відображенні вікна.
     Гарантуємо, що дані завантажуються та відображаються.
     """
     super().showEvent(event)
     # Запускаємо завантаження даних у вкладках
     if hasattr(self, 'products_tab'):
         asyncio.ensure_future(self.products_tab.apply_filters(is_initial_load=True))
     if hasattr(self, 'orders_tab'):
         asyncio.ensure_future(self.orders_tab.apply_orders_filters(is_initial_load=True))

 async def _highlight_and_show_details(self, row, product_number):
     """Метод для виділення рядка замовлення та відображення деталей з підсвіченим продуктом"""
     try:
         logging.info(f"ЯСКРАВЕ ПІДСВІЧЕННЯ рядка {row}")
         
         # Очищаємо попередні підсвічування
         for r in range(self.orders_tab.orders_table.rowCount()):
             for col in range(self.orders_tab.orders_table.columnCount()):
                 item = self.orders_tab.orders_table.item(r, col)
                 if item:
                     item.setBackground(QColor(0, 0, 0, 0))
                     # Встановлюємо колір тексту відповідно до поточної теми
                     text_color = QColor(255, 255, 255) if self.is_dark_theme else QColor(0, 0, 0)
                     item.setForeground(text_color)
                     # Скидаємо жирний шрифт
                     font = item.font()
                     font.setBold(False)
                     item.setFont(font)
         
         # Виділяємо рядок з виділенням через вбудований механізм таблиці
         self.orders_tab.orders_table.clearSelection()
         self.orders_tab.orders_table.selectRow(row)
         
         # Прокручуємо до рядка
         id_item = self.orders_tab.orders_table.item(row, 0)
         if id_item:
             self.orders_tab.orders_table.scrollToItem(
                 id_item, 
                 QAbstractItemView.ScrollHint.PositionAtCenter
             )
         
         # Використовуємо такий же колір підсвічування як і для пошуку
         highlight_color = QColor(255, 255, 0, 100)  # Світло-жовтий з напівпрозорістю
         
         # Встановлюємо колір фону для комірок рядка
         for col in range(self.orders_tab.orders_table.columnCount()):
             item = self.orders_tab.orders_table.item(row, col)
             if item:
                 item.setBackground(highlight_color)
                 # Не змінюємо колір тексту та жирність для відповідності стилю пошуку
         
         # Зберігаємо інформацію про підсвічений рядок
         self.orders_tab.highlighted_row = row
         
         # Відкриваємо деталі замовлення
         if hasattr(self.orders_tab, 'show_order_details'):
             logging.info(f"Відкриваємо деталі замовлення для рядка {row}")
             try:
                 await self.orders_tab.show_order_details(row)
                 
                 # Даємо час на відкриття діалогу
                 await asyncio.sleep(0.5)
                 
                 # Підсвічуємо продукт у деталях
                 if hasattr(self.orders_tab, 'order_details_dialog') and self.orders_tab.order_details_dialog:
                     details_table = self.orders_tab.order_details_dialog.details_table
                     
                     # Шукаємо потрібний рядок - використовуємо case-insensitive пошук
                     for detail_row in range(details_table.rowCount()):
                         product_number_item = details_table.item(detail_row, 1)  # Колонка з номером продукту
                         if product_number_item:
                             table_product_number = product_number_item.text().strip()
                             # Порівнюємо без урахування регістру
                             if table_product_number.lower() == product_number.lower() or \
                                product_number.lower() in table_product_number.lower():
                                logging.info(f"Знайдено продукт {product_number} у рядку {detail_row} деталей")
                                details_table.clearSelection()
                                details_table.selectRow(detail_row)
                                details_table.scrollToItem(
                                    product_number_item,
                                    QAbstractItemView.ScrollHint.PositionAtCenter
                                )
                                
                                # Підсвічуємо рядок з продуктом тим самим кольором
                                for col in range(details_table.columnCount()):
                                    item = details_table.item(detail_row, col)
                                    if item:
                                        item.setBackground(highlight_color)
                                break
             except Exception as details_error:
                 logging.error(f"Помилка при відкритті деталей: {details_error}")
         
         # Оновлюємо таблицю
         self.orders_tab.orders_table.update()
         
         logging.info("✅ Підсвічування та показ деталей завершено успішно")
         
     except Exception as e:
         logging.error(f"Помилка при виділенні рядка та відображенні деталей: {e}")

 def set_status_message(self, message, timeout=5000, is_process_status=False):
     """
     Встановлює повідомлення у статус-бар праворуч, меншим шрифтом і курсивом.
     
     Параметри:
     - message (str): Текст повідомлення для відображення
     - timeout (int): Час у мілісекундах, після якого повідомлення зникне.
       Значення 0 означає, що повідомлення не буде автоматично очищено.
       За замовчуванням: 5000 мс (5 секунд)
     - is_process_status (bool): Якщо True, повідомлення вважається статусом активного процесу
       і буде зберігатися до завершення процесу або перекриття іншим статусом (з поверненням)
     
     ВАЖЛИВО:
     - При встановленні фінального повідомлення "Базу даних оновлено" воно не повинно мати "..."
       в кінці, на відміну від проміжних повідомлень, які можуть мати "..." під час обробки.
     - Фінальне повідомлення має зникати автоматично через timeout (5 секунд),
       тоді як проміжні повідомлення залишатимуться до наступного оновлення.
     - Статуси активних процесів будуть повертатися після перекриття через 10 секунд.
     """
     logging.info(f"Встановлюю повідомлення статусу: '{message}', таймаут: {timeout} мс, is_process_status: {is_process_status}")
     
     # Перевіряємо, чи існує status_message_label
     if not hasattr(self, 'status_message_label') or not self.status_message_label:
         logging.warning("set_status_message: status_message_label не знайдено")
         # Запасний варіант - використовуємо стандартний status_bar
         if hasattr(self, 'statusBar'):
             self.statusBar().showMessage(message, timeout)
         return
     
     # Якщо повідомлення задовге, обрізаємо його для відображення
     max_length = 100
     if len(message) > max_length:
         message = message[:max_length] + "..."
         logging.debug(f"Повідомлення обрізано до {max_length} символів")
     
     # Визначаємо фактичний таймаут для автоматичного очищення
     actual_timeout = timeout
     
     # Якщо це статус активного процесу, зберігаємо його
     if is_process_status and message != "Базу даних оновлено":
         self._current_process_status = message
         logging.debug(f"Зберігаємо статус активного процесу: '{message}'")
         
         # Відміняємо попередній таймер повернення статусу, якщо він існує
         if hasattr(self, '_status_return_timer') and self._status_return_timer:
             self._status_return_timer.stop()
             self._status_return_timer = None
     
     # Для "Базу даних оновлено" не додаємо "..." і встановлюємо таймаут 5 секунд
     if message == "Базу даних оновлено":
         # Не додаємо "..." для кінцевого повідомлення
         display_message = message
         actual_timeout = 5000  # 5 секунд
         logging.debug("Фінальне повідомлення: без '...' з таймаутом 5 секунд")
         
         # Очищаємо поточний статус активного процесу
         self._current_process_status = ""
     else:
         # Додаємо "..." для проміжних повідомлень, якщо їх там ще немає
         if not message.endswith("..."):
             display_message = message + "..."
             logging.debug(f"Проміжне повідомлення: додано '...' -> '{display_message}'")
         else:
             display_message = message
     
     # Встановлюємо текст у status_message_label
     self.status_message_label.setText(display_message)
     
     # Якщо встановлено таймаут і це не статус активного процесу, 
     # створюємо таймер для очищення повідомлення
     if actual_timeout > 0 and (not is_process_status or message == "Базу даних оновлено"):
         logging.debug(f"Встановлено таймер очищення повідомлення через {actual_timeout} мс")
         QtCore.QTimer.singleShot(actual_timeout, lambda: self._clear_status_and_return())
     
 def _clear_status_and_return(self):
     """
     Очищає поточне повідомлення в status_message_label і повертає
     статус активного процесу (якщо такий є) через 10 секунд.
     """
     self.status_message_label.clear()
     logging.debug("Повідомлення в status_message_label очищено")
     
     # Якщо є поточний статус активного процесу, встановлюємо таймер для його повернення
     if hasattr(self, '_current_process_status') and self._current_process_status:
         logging.debug(f"Заплановано повернення статусу активного процесу через 10 секунд: '{self._current_process_status}'")
         
         # Створюємо новий таймер для повернення статусу
         self._status_return_timer = QtCore.QTimer()
         self._status_return_timer.setSingleShot(True)
         self._status_return_timer.timeout.connect(
             lambda: self.set_status_message(self._current_process_status, is_process_status=True)
         )
         self._status_return_timer.start(10000)  # 10 секунд

 def show_progress_bar(self, visible=True):
     """
     Показує або приховує прогрес-бар з плавною анімацією.
     
     Параметри:
     - visible (bool): Якщо True, показує прогрес-бар, інакше приховує
     """
     logging.info(f"show_progress_bar: visible={visible}")
     
     # Перевіряємо, чи існують необхідні об'єкти
     if not hasattr(self, 'progress_bar') or not self.progress_bar:
         logging.warning("show_progress_bar: progress_bar не знайдено")
         return
         
     if not hasattr(self, 'progress_bar_widget') or not self.progress_bar_widget:
         logging.warning("show_progress_bar: progress_bar_widget не знайдено")
         return
     
     try:
         if visible:
             # Зупиняємо всі попередні анімації
             if hasattr(self, '_progress_animation') and self._progress_animation:
                 self._progress_animation.stop()
             if hasattr(self, '_indeterminate_animation') and self._indeterminate_animation:
                 self._indeterminate_animation.stop()
                 
                 # Відключаємо та перепідключаємо сигнал для уникнення множинних з'єднань
                 try:
                     self._indeterminate_animation.finished.disconnect()
                 except:
                     pass
             
             # Скидаємо прогрес-бар до початкового стану
             self.progress_bar.setValue(0)
             
             # Встановлюємо правильне положення
             self.progress_bar_widget.setGeometry(0, self.height() - 3, self.width(), 3)
             
             # Показуємо віджети ПЕРЕД запуском анімації
             self.progress_bar_widget.setVisible(True)
             self.progress_bar.setVisible(True)
             self.progress_bar_widget.raise_()
             
             # Включаємо режим індетермінованого прогресу
             self._is_progress_indeterminate = True
             
             # Запускаємо постійну анімацію через таймер
             if not self._continuous_animation_timer.isActive():
                 self._continuous_animation_timer.start()
             
             # Забезпечуємо оновлення інтерфейсу
             QtCore.QCoreApplication.processEvents()
             
             logging.debug("show_progress_bar: Показано прогрес-бар з плавною анімацією")
         else:
             # Вимикаємо індетермінований режим
             self._is_progress_indeterminate = False
             
             # Зупиняємо всі анімації
             if hasattr(self, '_indeterminate_animation') and self._indeterminate_animation:
                 self._indeterminate_animation.stop()
                 try:
                     self._indeterminate_animation.finished.disconnect()
                 except:
                     pass
             
             # Зупиняємо таймер анімації
             if self._continuous_animation_timer.isActive():
                 self._continuous_animation_timer.stop()
             
             # Плавно заповнюємо прогрес-бар до 100% перед приховуванням
             if hasattr(self, '_progress_animation') and self._progress_animation:
                 self._progress_animation.stop()
                 
                 # Спроба відключити попередні сигнали
                 try:
                     self._progress_animation.finished.disconnect()
                 except:
                     pass
                     
                 current_value = self.progress_bar.value()
                 self._progress_animation.setStartValue(current_value)
                 self._progress_animation.setEndValue(100)
                 self._progress_animation.finished.connect(self._hide_progress_bar_completed)
                 self._progress_animation.start()
                 self._progress_animation_is_active = True
                 logging.debug(f"Запущено анімацію завершення прогрес-бару (від {current_value} до 100)")
             else:
                 # Якщо анімація недоступна, одразу приховуємо
                 self._hide_progress_bar_completed()
             
             logging.debug("show_progress_bar: Ініційовано приховування прогрес-бару")
     except Exception as e:
         logging.error(f"show_progress_bar: Помилка при показі/приховуванні прогрес-бару: {e}")
         # Запасний варіант - просто показуємо/приховуємо без анімації
         if hasattr(self, 'progress_bar_widget') and self.progress_bar_widget:
             self.progress_bar_widget.setVisible(visible)
         if hasattr(self, 'progress_bar') and self.progress_bar:
             self.progress_bar.setVisible(visible)
             
         # У випадку помилки при показі, примусово робимо прогрес-бар видимим
         if visible:
             if hasattr(self, 'progress_bar_widget'):
                 self.progress_bar_widget.setVisible(True)
             if hasattr(self, 'progress_bar'):
                 self.progress_bar.setVisible(True)
             if hasattr(self, 'progress_bar_widget'):
                 self.progress_bar_widget.raise_()

 def _hide_progress_bar_completed(self):
     """Завершує приховування прогрес-бару після анімації"""
     try:
         logging.debug("_hide_progress_bar_completed: приховування прогрес-бару завершено")
         self._progress_animation_is_active = False
         self._is_progress_indeterminate = False
         
         if self.progress_bar_widget:
             self.progress_bar_widget.setVisible(False)
         if self.progress_bar:
             self.progress_bar.setVisible(False)
     except Exception as e:
         logging.error(f"_hide_progress_bar_completed: Помилка при приховуванні прогрес-бару: {e}")

 def resizeEvent(self, event):
     """
     Обробляє подію зміни розміру вікна.
     
     Важливо:
     - Метод викликається Qt автоматично при кожній зміні розміру вікна
     - Оновлює розмір і позицію progress_bar_widget
     - Налаштовує максимальну ширину status_message_label
     - Змінює позицію анімаційного блоку, якщо він існує
     """
     super().resizeEvent(event)
     
     # Переконуємося, що прогрес-бар завжди розтягується на всю ширину вікна
     if hasattr(self, 'progress_bar_widget'):
         self.progress_bar_widget.setGeometry(0, self.height() - 3, self.width(), 3)
         
     if hasattr(self, 'progress_bar'):
         self.progress_bar.setGeometry(0, 0, self.width(), 3)
     
     # Лімітуємо максимальну ширину лейблу повідомлень
     if hasattr(self, 'status_message_label'):
         self.status_message_label.setMaximumWidth(int(self.width() * 0.55))
     
     # Переконатись, що поточне повідомлення не буде обрізано при зміні розміру
     current_text = self.status_message_label.text() if hasattr(self, 'status_message_label') else ""
     if current_text and "Базу даних оновлено" not in current_text and "..." not in current_text:
         self.status_message_label.setText(current_text + "...")
     
     # Якщо є анімація прогрес-бару, потрібно оновити стиль для нової ширини вікна
     if hasattr(self, '_animation_position') and self.progress_bar and self.progress_bar.isVisible():
         # Викликаємо _animate_progress_step для оновлення стилю з новою шириною
         self._animate_progress_step()

 def update_parsing_progress(self, progress, status_message=None):
     """
     Оновлює стан прогрес-бару і статусного повідомлення.
     
     Параметри:
     - progress (int, float або None): відсоток виконання (0-100) або None для індетермінованого режиму
     - status_message (str, опціонально): повідомлення для показу в статус-барі
     """
     try:
         # Перевіряємо, чи існують необхідні віджети
         if not hasattr(self, 'progress_bar') or not self.progress_bar:
             logging.warning("update_parsing_progress: progress_bar не знайдено")
             return
             
         if not hasattr(self, 'progress_bar_widget') or not self.progress_bar_widget:
             logging.warning("update_parsing_progress: progress_bar_widget не знайдено")
             return
         
         # Використовуємо QTimer.singleShot для оновлення прогрес-бару в основному потоці подій
         # Це допомагає уникнути блокування інтерфейсу під час оновлень з фонового потоку
         if progress is None:
             # Для невизначеного прогресу використовуємо індетермінований режим
             QtCore.QTimer.singleShot(0, lambda: self._update_indeterminate_progress(status_message))
             logging.debug("update_parsing_progress: Встановлено індетермінований режим")
         else:
             # Перевіряємо тип progress
             try:
                 progress_value = float(progress)
                 # Для конкретного значення прогресу використовуємо звичайний режим
                 QtCore.QTimer.singleShot(0, lambda: self._update_determinate_progress(progress_value, status_message))
                 logging.debug(f"update_parsing_progress: Встановлено визначений прогрес {progress_value}%")
             except (ValueError, TypeError):
                 # Якщо не вдалося конвертувати до числа, використовуємо індетермінований режим
                 logging.warning(f"update_parsing_progress: Неможливо конвертувати progress={progress} до числа")
                 QtCore.QTimer.singleShot(0, lambda: self._update_indeterminate_progress(status_message))
         
     except Exception as e:
         logging.error(f"update_parsing_progress: Помилка при оновленні прогресу: {e}")
         # Запасний варіант - просто встановлюємо значення без анімації
         try:
             if progress is not None:
                 try:
                     self.progress_bar.setValue(int(progress))
                 except (ValueError, TypeError):
                     pass
         except:
             pass

 def _animate_progress_step(self):
     """
     Анімує крок прогрес-бару в індетермінованому режимі.
     Плавна безперервна анімація без ривків, що рухається від лівого до правого краю.
     """
     try:
         if not hasattr(self, 'progress_bar') or not self.progress_bar:
             logging.warning("_animate_progress_step: progress_bar не знайдено")
             return
             
         if not self.progress_bar.isVisible():
             # Прогрес-бар невидимий, зупиняємо анімацію
             if hasattr(self, '_continuous_animation_timer') and self._continuous_animation_timer.isActive():
                 self._continuous_animation_timer.stop()
             return
         
         # Ініціалізуємо змінні плавної анімації, якщо їх ще немає
         if not hasattr(self, '_animation_position'):
             self._animation_position = -40
             self._animation_speed = 0.7
             self._animation_width = 40
         
         # Збільшуємо позицію для руху вправо
         self._animation_position += self._animation_speed
         
         # Якщо блок повністю вийшов за межі правого краю, починаємо знову з лівого
         if self._animation_position > 100 + self._animation_width:
             self._animation_position = -self._animation_width
         
         # Нормалізуємо позицію для градієнта (всі значення мають бути від 0 до 1)
         pos1 = max(0, min(1, (self._animation_position-self._animation_width)/100))
         pos2 = max(0, min(1, (self._animation_position-self._animation_width*0.7)/100))
         pos3 = max(0, min(1, (self._animation_position-self._animation_width*0.3)/100))
         pos4 = max(0, min(1, self._animation_position/100))
         pos5 = max(0, min(1, (self._animation_position+self._animation_width*0.3)/100))
         pos6 = max(0, min(1, (self._animation_position+self._animation_width*0.7)/100))
         pos7 = max(0, min(1, (self._animation_position+self._animation_width)/100))
         
         # Створюємо градієнт з плавними краями
         style = f"""
             QProgressBar {{
                 border: none;
                 background-color: transparent;
                 text-align: center;
             }}
             QProgressBar::chunk {{
                 background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                     stop:0 transparent,
                     stop:{pos1} transparent,
                     stop:{pos2} rgba(120, 81, 169, 0.2),
                     stop:{pos3} rgba(120, 81, 169, 0.7),
                     stop:{pos4} #7851A9,
                     stop:{pos5} rgba(120, 81, 169, 0.7),
                     stop:{pos6} rgba(120, 81, 169, 0.2),
                     stop:{pos7} transparent,
                     stop:1 transparent
                 );
                 width: {self.width()}px;
             }}
         """
         
         # Встановлюємо стиль
         self.progress_bar.setStyleSheet(style)
         
     except Exception as e:
         logging.error(f"_animate_progress_step: Помилка при анімації прогресу: {e}")

 def _update_indeterminate_progress(self, status_message=None):
     """
     Встановлює прогрес-бар в режим невизначеного прогресу.
     Використовує плавну безперервну анімацію з градієнтом.
     
     Параметри:
     - status_message (str, опціонально): повідомлення для показу в статус-барі
     """
     try:
         # Перевіряємо, чи прогрес-бар видимий
         if not self.progress_bar_widget.isVisible():
             # Показуємо прогрес-бар з анімацією
             self.show_progress_bar(True)
         
         # Скидаємо змінні анімації для плавного старту
         self._animation_position = -40
         self._animation_speed = 0.7
         self._animation_width = 40
         
         # Якщо таймер для анімації вже існує, просто переконаємось, що він запущений
         if hasattr(self, '_continuous_animation_timer'):
             if not self._continuous_animation_timer.isActive():
                 self._continuous_animation_timer.start(20)  # 20мс для дуже плавної анімації (~50fps)
         else:
             # Створюємо таймер для анімації
             self._continuous_animation_timer = QTimer()
             self._continuous_animation_timer.timeout.connect(self._animate_progress_step)
             self._continuous_animation_timer.start(20)  # 20мс для дуже плавної анімації
         
         # Оновлюємо статусне повідомлення, якщо воно надано
         if status_message is not None:
             self.set_status_message(status_message, is_process_status=True)
         
         logging.debug("_update_indeterminate_progress: Встановлено індетермінований режим прогрес-бару")
         
     except Exception as e:
         logging.error(f"_update_indeterminate_progress: Помилка при встановленні індетермінованого режиму: {e}")

 def _update_determinate_progress(self, progress, status_message=None):
     """
     Оновлює прогрес-бар з конкретним значенням прогресу.
     
     Параметри:
     - progress (float): відсоток виконання (0-100)
     - status_message (str, опціонально): повідомлення для показу в статус-барі
     """
     try:
         # Перевіряємо, чи прогрес-бар видимий
         if not self.progress_bar_widget.isVisible():
             # Показуємо прогрес-бар з анімацією
             self.show_progress_bar(True)
         
         # Переконуємося, що значення прогресу у допустимому діапазоні і є цілим числом
         progress_value = max(0, min(100, progress))
         
         # Змінюємо стиль для звичайного режиму
         self.progress_bar.setFormat("")
         self.progress_bar.setRange(0, 100)
         self.progress_bar.setTextVisible(False)
         
         # Встановлюємо значення прогресу - ВАЖЛИВО: setValue приймає тільки int!
         self.progress_bar.setValue(int(progress_value))
         
         # Якщо досягнуто 100%, приховуємо прогрес-бар з анімацією
         if progress_value >= 100:
             # Використовуємо затримку щоб показати 100% перед приховуванням
             QtCore.QTimer.singleShot(500, lambda: self.show_progress_bar(False))
             
         # Оновлюємо статусне повідомлення, якщо воно надано
         if status_message is not None:
             self.set_status_message(status_message, is_process_status=True)
         
         # Зупиняємо таймер анімації, якщо він запущений
         if hasattr(self, '_continuous_animation_timer') and self._continuous_animation_timer.isActive():
             self._continuous_animation_timer.stop()
             logging.debug("_update_determinate_progress: Зупинено таймер анімації")
     
     except Exception as e:
         logging.error(f"_update_determinate_progress: Помилка при оновленні прогресу: {e}")

 def update_progress_value(self, value):
     """
     Оновлює значення прогрес-бару з певним відсотком виконання.
     
     Параметри:
     - value (int): відсоток виконання (0-100)
     """
     try:
         progress_value = max(0, min(100, value))
         # Викликаємо метод оновлення прогрес-бару
         self._update_determinate_progress(progress_value)
     except Exception as e:
         logging.error(f"update_progress_value: Помилка при оновленні значення прогресу: {e}")

 def show_notification(self, message, error=False, timeout=10000):
     """
     Показує повідомлення у вигляді спливаючого вікна
     """
     # Завжди використовуємо 10000 мс (10 секунд) для всіх повідомлень, особливо для повідомлень про завершення процесів
     timeout = 10000  # 10 секунд для всіх повідомлень
     
     # Валідація таймауту
     if timeout is not None:
         try:
             timeout = int(timeout)
             if timeout < 0:
                 timeout = 10000  # значення за замовчуванням
         except (ValueError, TypeError):
             timeout = 10000
     else:
         timeout = 10000  # Якщо None, використовуємо стандартне значення

     NotificationManager.instance.show_notification(message, error, timeout)
     self.update()

 def start_universal_parsing(self):
     """Запускає універсальний процес парсингу для обох типів даних"""
     # Перевіряємо, чи вже виконується процес парсингу
     try:
         if hasattr(self, 'parsing_thread') and self.parsing_thread and self.parsing_thread.isRunning():
             self.set_status_message("Процес вже запущено", is_process_status=True)
             self.show_notification("Процес вже виконується. Зачекайте.", error=True)
             return
     except RuntimeError:
         # Обробляємо випадок, коли QThread було видалено
         self.parsing_thread = None
     
     # Перевіряємо, чи не відбувається оновлення таблиць
     if hasattr(self, '_is_refreshing_tables') and self._is_refreshing_tables:
         self.set_status_message("Таблиці оновлюються", is_process_status=True)
         self.show_notification("Таблиці оновлюються. Спробуйте пізніше.", error=True)
         return
     
     # Встановлюємо прапорець асинхронного оновлення
     self._is_async_updating = True
     
     # Скидаємо статуси завершення
     self._products_done = False
     self._orders_done = False
     
     logging.info("Розпочато парсинг з головного вікна")
     
     # Показуємо прогрес-бар перед запуском потоку
     self.show_progress_bar(True)
     
     # Затримуємося для оновлення інтерфейсу перед запуском важкого процесу
     QtCore.QTimer.singleShot(100, self._start_parsing_thread)
     
     # Встановлюємо початковий статус з позначкою активного процесу
     self.set_status_message("Підготовка до оновлення", is_process_status=True)

 def _start_parsing_thread(self):
     """Запускає потік парсингу після короткої затримки для оновлення інтерфейсу"""
     # Створюємо потік і СИНХРОННИЙ універсальний воркер
     self.parsing_thread = QThread()
     
     # Використовуємо синхронний воркер, він більш надійний
     self.parsing_worker = UniversalParsingWorker()
     self.parsing_worker.moveToThread(self.parsing_thread)
     
     # Підключаємо сигнали
     self.parsing_thread.started.connect(self.parsing_worker.run)
     self.parsing_worker.finished.connect(self.parsing_thread.quit)
     self.parsing_worker.finished.connect(self.parsing_worker.deleteLater)
     self.parsing_thread.finished.connect(self.parsing_thread.deleteLater)
     self.parsing_thread.finished.connect(self.on_parsing_finished)
     
     # Підключаємо сигнали для оновлення статусу, передаючи статус активного процесу
     self.parsing_worker.status_update.connect(
         lambda msg: self.set_status_message(msg, is_process_status=True)
     )
     self.parsing_worker.progress.connect(self.update_parsing_progress)
     self.parsing_worker.error.connect(self.show_parsing_error)
     
     # Запускаємо потік
     self.parsing_thread.start()
     logging.debug("Потік парсингу запущено після затримки інтерфейсу")

 def on_parsing_finished(self):
     """
     Обробляє подію завершення парсингу.
     
     ВАЖЛИВО:
     - Метод обов'язково має приховати прогрес-бар
     - Метод має скинути всі прапорці (_products_done, _orders_done)
     - Метод має очистити посилання на об'єкт потоку парсингу, щоб уникнути витоку пам'яті
     - Метод оновлює статус-бар з повідомленням "Базу даних оновлено" без "..."
     """
     logging.info("on_parsing_finished: Обробка завершення парсингу")
     
     try:
         # Скидаємо прапорець асинхронного оновлення
         self._is_async_updating = False
         
         # Плавно приховуємо прогрес-бар (спочатку довершуємо до 100%)
         logging.debug("on_parsing_finished: Приховування прогрес-бару")
         
         # Зупиняємо таймер безперервної анімації
         if hasattr(self, '_continuous_animation_timer') and self._continuous_animation_timer.isActive():
             self._continuous_animation_timer.stop()
             
         # Показуємо фінальну анімацію і приховуємо прогрес-бар
         self._finalize_progress_animation()
         
         # Скидаємо прапорці
         logging.debug("on_parsing_finished: Скидання прапорців _products_done та _orders_done")
         self._products_done = False
         self._orders_done = False
         
         # Оновлюємо статус
         logging.debug("on_parsing_finished: Оновлення статусу до 'Базу даних оновлено'")
         self.set_status_message("Базу даних оновлено", 5000)  # 5 секунд таймаут
         
         # Очищаємо посилання на об'єкт потоку
         if hasattr(self, 'parsing_thread') and self.parsing_thread:
             logging.debug("on_parsing_finished: Очищення посилання на потік парсингу")
             self.parsing_thread = None
           
         logging.info("on_parsing_finished: Завершення парсингу успішно оброблено")
     except Exception as e:
         logging.error(f"on_parsing_finished: Помилка при обробці завершення парсингу: {e}")
         # Аварійне очищення ресурсів
         self.parsing_thread = None
         self._is_async_updating = False
         self.show_progress_bar(False)

 def _finalize_progress_animation(self):
     """Плавно завершує анімацію прогрес-бару, заповнюючи його до 100%, а потім приховує."""
     try:
         # Переконуємося, що прогрес-бар видимий
         if not hasattr(self, 'progress_bar') or not self.progress_bar:
             return
             
         if not self.progress_bar_widget.isVisible():
             return
         
         # Встановлюємо нормальний режим
         self.progress_bar.setRange(0, 100)
         
         # Плавно заповнюємо до 100%
         if hasattr(self, '_progress_animation') and self._progress_animation:
             # Зупиняємо існуючу анімацію
             self._progress_animation.stop()
             
             # Очищаємо старі з'єднання
             try:
                 self._progress_animation.finished.disconnect()
             except:
                 pass
             
             # Встановлюємо фіксований стиль для фінальної анімації
             self.progress_bar.setStyleSheet("""
                 QProgressBar { 
                     border: none; 
                     background-color: transparent; 
                 }
                 QProgressBar::chunk { 
                     background-color: #7851A9; 
                 }
             """)
             
             # Поточне значення
             current_value = self.progress_bar.value()
             if current_value > 80:
                 current_value = 0
                 
             # Налаштовуємо анімацію
             self._progress_animation.setStartValue(current_value)
             self._progress_animation.setEndValue(100)
             self._progress_animation.setDuration(500)  # 500 мс на заповнення
             self._progress_animation.setEasingCurve(QEasingCurve.Type.OutQuad)
             
             # Підключаємо сигнал завершення для приховування
             self._progress_animation.finished.connect(self._hide_progress_bar_completed)
             
             # Запускаємо анімацію
             self._progress_animation.start()
         else:
             # Якщо анімація недоступна, просто приховуємо
             self._hide_progress_bar_completed()
     except Exception as e:
         logging.error(f"_finalize_progress_animation: Помилка при фіналізації анімації: {e}")
         self._hide_progress_bar_completed()

 def show_parsing_error(self, error_msg):
     """Показує сповіщення про помилку парсингу"""
     self.show_notification(f"Помилка парсингу: {error_msg}", error=True)

 def show_update_dialog_and_parse(self):
     """
     Показує діалогове вікно вибору типу оновлення та запускає процес парсингу.
     Універсальний метод, який викликається з обох вкладок.
     """
     try:
         # Перевіряємо, чи вже виконується процес парсингу
         if hasattr(self, 'parsing_thread') and self.parsing_thread and self.parsing_thread.isRunning():
             self.set_status_message("Процес вже запущено", is_process_status=True)
             self.show_notification("Процес вже виконується. Зачекайте.", error=True)
             return
     except RuntimeError:
         # Обробляємо випадок, коли QThread було видалено
         self.parsing_thread = None
     
     # Перевіряємо, чи не відбувається оновлення таблиць
     if hasattr(self, '_is_refreshing_tables') and self._is_refreshing_tables:
         self.set_status_message("Таблиці оновлюються", is_process_status=True)
         self.show_notification("Таблиці оновлюються. Спробуйте пізніше.", error=True)
         return
     
     # Показуємо прогрес-бар перед запуском діалогу
     self.show_progress_bar(True)
     
     # Показуємо діалогове вікно для вибору типу оновлення
     from workers import UpdateTypeDialog
     dialog = UpdateTypeDialog(self)
     result = dialog.exec()
     
     # Якщо користувач скасував операцію
     if result != QDialog.DialogCode.Accepted:
         self.set_status_message("Оновлення скасовано користувачем", 5000)
         self.show_progress_bar(False)
         return
         
     # Отримуємо обраний тип оновлення
     force_update = dialog.is_full_update_selected()
     
     # Встановлюємо прапорець асинхронного оновлення
     self._is_async_updating = True
     
     # Скидаємо статуси завершення
     self._products_done = False
     self._orders_done = False
     
     # Встановлюємо статус відповідно до вибраного типу оновлення
     if force_update:
         self.set_status_message("Розпочато повне оновлення бази даних...", is_process_status=True)
     else:
         self.set_status_message("Розпочато стандартне оновлення бази даних...", is_process_status=True)
     
     # Створюємо потік і робітника
     self.parsing_thread = QThread()
     self.parsing_worker = OrderParsingWorker(force_process=force_update)
     self.parsing_worker.moveToThread(self.parsing_thread)
     
     # Підключаємо сигнали
     self.parsing_worker.status_update.connect(lambda msg: self.set_status_message(msg, is_process_status=True))
     self.parsing_worker.progress.connect(lambda value: self.update_progress_value(value))
     self.parsing_worker.parsing_error.connect(self.handle_parsing_error)
     self.parsing_worker.finished.connect(self.on_parsing_finished)
     self.parsing_thread.started.connect(self.parsing_worker.run)
     
     # Запускаємо потік з обробником
     self.parsing_thread.start()
     
 def handle_parsing_error(self, error_info):
     """Обробка помилок парсингу від робітника"""
     sheet = error_info.get("sheet", "Невідомий аркуш")
     row = error_info.get("row", "?")
     error = error_info.get("error", "Невідома помилка")
     client = error_info.get("client", "Невідомий клієнт")
     
     logging.error(f"Помилка парсингу: Аркуш '{sheet}', Рядок {row}, Клієнт '{client}': {error}")
     self.show_notification(f"Помилка парсингу в аркуші '{sheet}': {error}", error=True)

 def on_parsing_finished(self):
     """Обробка завершення парсингу"""
     try:
         # Відключаємо потік
         if hasattr(self, 'parsing_thread') and self.parsing_thread and self.parsing_thread.isRunning():
             self.parsing_thread.quit()
             self.parsing_thread.wait()
         
         # Оновлюємо таблиці в обох вкладках
         if hasattr(self, 'products_tab') and self.products_tab:
             asyncio.ensure_future(self.products_tab.apply_filters())
         
         if hasattr(self, 'orders_tab') and self.orders_tab:
             asyncio.ensure_future(self.orders_tab.apply_orders_filters())
         
         # Показуємо повідомлення про успіх
         self.set_status_message("Базу даних успішно оновлено", 5000)
         
     except Exception as e:
         logging.error(f"Помилка при завершенні оновлення бази даних: {str(e)}")
         self.show_notification(f"Помилка: {str(e)}", error=True)
     finally:
         self.show_progress_bar(False)


