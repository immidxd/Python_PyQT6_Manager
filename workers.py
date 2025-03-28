import sys
import os
import subprocess
import logging
import asyncio
import qasync
import time
import datetime
from PyQt6.QtCore import QObject, pyqtSignal, QThread, pyqtSlot, QCoreApplication
from PyQt6.QtWidgets import QApplication, QMessageBox, QDialog, QVBoxLayout, QLabel, QRadioButton, QPushButton, QDialogButtonBox
from sqlalchemy.orm import aliased
from sqlalchemy import or_, and_, func, String, cast, Float
from models import (
   Product, Type, Subtype, Brand, Gender, Color, Country, Status, Condition, Import
)
from db import Session
from views.scripts.orders_pars import process_orders_sheet_data, get_parsing_errors
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import traceback
from time import sleep
import random
from gspread.exceptions import APIError
from services.google_sheets_service import google_sheets_service


# Налаштування більш детального логування
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

# Налаштування основного логера
LOG_FILE = os.path.join(LOG_DIR, f"sheets_api_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Створюємо логер для API запитів з окремим файлом
api_logger = logging.getLogger('google_sheets_api')
api_handler = logging.FileHandler(os.path.join(LOG_DIR, f"api_requests_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"), encoding='utf-8')
api_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
api_logger.addHandler(api_handler)
api_logger.setLevel(logging.DEBUG)

# Логер для операцій квоти
quota_logger = logging.getLogger('quota_management')
quota_handler = logging.FileHandler(os.path.join(LOG_DIR, f"quota_management_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"), encoding='utf-8')
quota_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
quota_logger.addHandler(quota_handler)
quota_logger.setLevel(logging.DEBUG)

# Логер для винятків та помилок
error_logger = logging.getLogger('error_tracking')
error_handler = logging.FileHandler(os.path.join(LOG_DIR, f"errors_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"), encoding='utf-8')
error_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s]\n%(message)s\n%(pathname)s:%(lineno)d\n\n'))
error_logger.addHandler(error_handler)
error_logger.setLevel(logging.ERROR)

logging.getLogger('asyncio').setLevel(logging.DEBUG)


class UpdateTypeDialog(QDialog):
    """
    Діалогове вікно для вибору типу оновлення бази даних.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Вибір типу оновлення бази даних")
        self.setMinimumWidth(400)
        
        layout = QVBoxLayout()
        
        # Опис
        description = QLabel(
            "Виберіть тип оновлення бази даних з Google Sheets:\n\n"
            "1. Стандартне оновлення - обробляє тільки нові або змінені рядки.\n"
            "2. Повне оновлення - очищає кеш хешів і обробляє всі рядки заново."
        )
        description.setWordWrap(True)
        layout.addWidget(description)
        
        # Радіокнопки для вибору
        self.standard_radio = QRadioButton("Стандартне оновлення")
        self.standard_radio.setChecked(True)
        self.full_radio = QRadioButton("Повне оновлення (займає більше часу)")
        
        layout.addWidget(self.standard_radio)
        layout.addWidget(self.full_radio)
        
        # Кнопки OK/Cancel
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        
        layout.addWidget(buttons)
        self.setLayout(layout)
    
    def is_full_update_selected(self):
        """Повертає True, якщо вибрано повне оновлення"""
        return self.full_radio.isChecked()


class Worker(QObject):
   """
   Використовувався для фонової (асинхронної) завантаження продуктів
   (застосування фільтрів). У разі потреби можна досі використовувати
   (напр. у комбінації з QThread). 
   Наразі може бути необов'язковим, якщо ми робимо to_thread у main.py напряму.
   """

   finished = pyqtSignal(list)
   error = pyqtSignal(str)

   def __init__(self, query_params):
       super().__init__()
       self.query_params = query_params
       self._is_running = True

   @pyqtSlot()
   def run(self):
       try:
           session_worker = Session()
           products = self.get_products(session_worker)
           session_worker.close()
           if self._is_running:
               self.finished.emit(products)
       except Exception as e:
           if self._is_running:
               logging.error(f"Помилка в робітнику: {e}")
               self.error.emit(str(e))

   def stop(self):
       self._is_running = False

   def parse_size(self, size_str):
       """
       Парсинг розміру й повернення числа. Наприклад, може конвертувати '10½' -> 10.5 і т.д.
       """
       if not size_str:
           return None
       size_str = size_str.replace(',', '.').replace('⅓', '.33').replace('⅔', '.66') \
           .replace('¼', '.25').replace('½', '.5').replace('¾', '.75')
       try:
           return float(size_str)
       except ValueError:
           if '-' in size_str or '/' in size_str:
               delimiter = '-' if '-' in size_str else '/'
               parts = size_str.split(delimiter)
               try:
                   numbers = []
                   for p in parts:
                       p = p.strip()
                       p = p.replace('⅓', '.33').replace('⅔', '.66') \
                            .replace('¼', '.25').replace('½', '.5').replace('¾', '.75')
                       numbers.append(float(p))
                   return sum(numbers) / len(numbers)
               except ValueError:
                   return None
           else:
               return None

   def get_sizeeu_clean_expression(self, field):
       """
       Повертає вираз для очищення та конвертації поля розміру у числове значення.
       """
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

       # Використовуємо regexp_replace для видалення непотрібних символів
       expr = func.regexp_replace(expr, '[^0-9\.]', '', 'g')
       expr = func.nullif(expr, '')
       expr = cast(expr, Float)
       return expr

   def get_products(self, session):
       """
       Основний запит до БД із застосуванням усіх фільтрів self.query_params.
       Уникаємо дублювання join(Country,...).
       """
       from sqlalchemy.orm import aliased

       # Створюємо псевдоніми для країн
       owner_alias = aliased(Country, name='owner_alias')
       manuf_alias = aliased(Country, name='manuf_alias')

       # Розпаковуємо фільтр-параметри
       unsold_only = self.query_params.get('unsold_only')
       search_text = self.query_params.get('search_text')
       selected_brands = self.query_params.get('selected_brands')
       selected_genders = self.query_params.get('selected_genders')
       selected_types = self.query_params.get('selected_types')
       selected_colors = self.query_params.get('selected_colors')
       selected_countries = self.query_params.get('selected_countries')
       price_min = self.query_params.get('price_min')
       price_max = self.query_params.get('price_max')
       size_min = self.query_params.get('size_min')
       size_max = self.query_params.get('size_max')
       dim_min = self.query_params.get('dim_min')
       dim_max = self.query_params.get('dim_max')
       selected_condition = self.query_params.get('selected_condition')
       selected_supplier = self.query_params.get('selected_supplier')
       sort_option = self.query_params.get('sort_option')

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

       # Якщо потрібен лише "Непроданий"
       if unsold_only:
           sold_statuses = session.query(Status).filter(Status.statusname.ilike('%продан%')).all()
           if sold_statuses:
               sold_ids = [s.id for s in sold_statuses]
               q = q.filter(~Product.statusid.in_(sold_ids))

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
           brand_ids_subq = session.query(Brand.id).filter(Brand.brandname.in_(selected_brands)).subquery()
           q = q.filter(Product.brandid.in_(brand_ids_subq))

       # Стать
       if selected_genders:
           gender_ids_subq = session.query(Gender.id).filter(Gender.gendername.in_(selected_genders)).subquery()
           q = q.filter(Product.genderid.in_(gender_ids_subq))

       # Тип/Підтип
       if selected_types:
           type_ids_subq = session.query(Type.id).filter(Type.typename.in_(selected_types)).subquery()
           sub_ids_subq = session.query(Subtype.id).filter(Subtype.subtypename.in_(selected_types)).subquery()
           q = q.filter(
               or_(
                   Product.typeid.in_(type_ids_subq),
                   Product.subtypeid.in_(sub_ids_subq)
               )
           )

       # Колір
       if selected_colors:
           color_ids_subq = session.query(Color.id).filter(Color.colorname.in_(selected_colors)).subquery()
           q = q.filter(Product.colorid.in_(color_ids_subq))

       # Країна
       if selected_countries:
           country_ids_subq = session.query(Country.id).filter(Country.countryname.in_(selected_countries)).subquery()
           q = q.filter(
               or_(
                   Product.ownercountryid.in_(country_ids_subq),
                   Product.manufacturercountryid.in_(country_ids_subq)
               )
           )

       # Ціна
       if price_min is not None and price_max is not None:
           if price_min > 0 or price_max < 9999:
               q = q.filter(Product.price >= price_min, Product.price <= price_max)

       # Розмір EU
       if size_min is not None and size_max is not None:
           if size_min > 14 or size_max < 60:
               size_expr = self.get_sizeeu_clean_expression(Product.sizeeu)
               q = q.filter(size_expr >= size_min, size_expr <= size_max)

       # Розмір (см)
       if dim_min is not None and dim_max is not None:
           if dim_min > 5 or dim_max < 40:
               dim_expr = self.get_sizeeu_clean_expression(Product.measurementscm)
               q = q.filter(dim_expr >= dim_min, dim_expr <= dim_max)

       # Стан
       if selected_condition not in (None, "Стан", "Всі"):
           c_obj = session.query(Condition).filter(
               Condition.conditionname.ilike(selected_condition.lower())
           ).first()
           if c_obj:
               q = q.filter(Product.conditionid == c_obj.id)

       # Постачальник
       if selected_supplier not in (None, "Постачальник", "Всі"):
           imp_obj = session.query(Import).filter(Import.importname.ilike(selected_supplier)).first()
           if imp_obj:
               q = q.filter(Product.importid == imp_obj.id)

       # Сортування
       if sort_option == "По імені":
           q = q.order_by(Product.productnumber.asc())
       elif sort_option == "За часом додавання":
           q = q.order_by(Product.dateadded.desc())
       elif sort_option == "Від дешевого":
           q = q.order_by(Product.price.asc())
       elif sort_option == "Від найдорожчого":
           q = q.order_by(Product.price.desc())

       results = q.all()
       logging.debug(f"Знайдено {len(results)} продуктів за фільтрами.")

       products_list = []
       for row in results:
           if not self._is_running:
               break
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


class ParsingWorker(QObject):
   """
   Приклад воркера, який запускає скрипт парсингу (googlesheets_pars.py)
   у фоновому потоці.
   """
   finished = pyqtSignal()
   error = pyqtSignal(str)

   def __init__(self):
       super().__init__()
       self._is_running = True

   @pyqtSlot()
   def run(self):
       try:
           print("Запуск скрипту парсингу...")
           script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts', 'googlesheets_pars.py')
           result = subprocess.run([sys.executable, script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
           if result.returncode != 0:
               error_message = result.stderr.strip()
               if self._is_running:
                   self.error.emit(error_message)
           else:
               print("Скрипт парсингу успішно завершився.")
               if self._is_running:
                   self.finished.emit()
       except Exception as e:
           if self._is_running:
               self.error.emit(str(e))

   def stop(self):
       self._is_running = False


class OrderParsingWorker(QObject):
    finished = pyqtSignal()
    progress = pyqtSignal(int)
    status_update = pyqtSignal(str)
    parsing_error = pyqtSignal(dict)  # Новий сигнал для помилок парсингу
    
    def __init__(self, force_process=False):
        super().__init__()
        self.force_process = force_process
        self.logger = logging.getLogger('OrderParsingWorker')
        self.logger.info(f"Ініціалізація OrderParsingWorker з force_process={force_process}")
        
    def parse_orders(self):
        """
        Метод для парсингу замовлень, який викликається з orders_tab.py.
        Запускає метод run і повертає результат обробки.
        
        Returns:
            bool: True, якщо парсинг завершився успішно, False у випадку помилки.
        """
        try:
            self.logger.info("Початок parse_orders()")
            # Запускаємо основний метод обробки замовлень
            self.run()
            self.logger.info("Завершення parse_orders()")
            return True
        except Exception as e:
            self.logger.error(f"Помилка в parse_orders: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False
        
    def show_update_type_dialog(self):
        """
        Показує діалогове вікно для вибору типу оновлення бази даних.
        Повертає True, якщо користувач вибрав повне оновлення, False для стандартного.
        """
        # Переконуємося, що є активне вікно програми
        app = QApplication.instance()
        if not app:
            self.logger.warning("QApplication екземпляр не знайдено, діалог не може бути показаний")
            return self.force_process
            
        self.logger.info("Відображення діалогу вибору типу оновлення")    
        dialog = UpdateTypeDialog()
        result = dialog.exec()
        
        if result == QDialog.DialogCode.Accepted:
            selection = dialog.is_full_update_selected()
            self.logger.info(f"Користувач вибрав: {'Повне оновлення' if selection else 'Стандартне оновлення'}")
            return selection
        else:
            # Користувач скасував, припиняємо операцію
            self.logger.info("Користувач скасував діалог вибору типу оновлення")
            return None
        
    def run(self):
        start_time = datetime.datetime.now()
        self.logger.info(f"===== ПОЧАТОК ПРОЦЕСУ ІМПОРТУ ({start_time.strftime('%Y-%m-%d %H:%M:%S')}) =====")
        self.logger.info(f"РЕЖИМ ПАРСИНГУ при початку run(): {'ПОВНИЙ' if self.force_process else 'СТАНДАРТНИЙ'}")
        
        # Створюємо окремі логи для цього запуску
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs', 'parse_sessions')
        os.makedirs(log_dir, exist_ok=True)
        
        timestamp = start_time.strftime('%Y%m%d_%H%M%S')
        session_log_file = os.path.join(log_dir, f"parse_session_{timestamp}.log")
        
        session_logger = logging.getLogger(f'session_{timestamp}')
        session_logger.setLevel(logging.DEBUG)
        session_handler = logging.FileHandler(session_log_file, encoding='utf-8')
        session_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        session_logger.addHandler(session_handler)
        
        # Налаштовуємо моніторинг обробки таблиць
        session_logger.info(f"===== ПОЧАТОК СЕАНСУ ПАРСИНГУ =====")
        session_logger.info(f"Режим парсингу: {'ПОВНИЙ' if self.force_process else 'СТАНДАРТНИЙ'}")
        session_logger.info(f"Force process: {self.force_process}")
        
        # Створюємо список для збору всіх помилок
        collected_errors = []
        
        try:
            # Перевіряємо, чи Google Sheets API доступний
            self.logger.info("Перевірка доступності Google Sheets API")
            session_logger.info("Перевірка доступності Google Sheets API")
            if not google_sheets_service.is_ready():
                error_msg = "Неможливо отримати доступ до Google Sheets API"
                self.logger.error(error_msg)
                session_logger.error(error_msg)
                self.status_update.emit(f"Помилка: {error_msg}")
                error_data = {"sheet": "API_ERROR", "row": 0, "error": error_msg, "client": "Немає", "error_type": "ConnectionError"}
                self.parsing_error.emit(error_data)
                collected_errors.append(error_data)
                return
                
            # Отримуємо список аркушів для обробки
            self.logger.info("Отримання аркушів замовлень з Google Sheets API")
            session_logger.info("Отримання аркушів замовлень з Google Sheets API")
            sheets_list = google_sheets_service.get_orders_worksheets()
            
            # Якщо немає доступу до таблиці
            if sheets_list is None:
                error_msg = "Неможливо отримати доступ до таблиці Google Sheets"
                self.logger.error(error_msg)
                session_logger.error(error_msg)
                self.status_update.emit(f"Помилка: {error_msg}")
                error_data = {"sheet": "API_ERROR", "row": 0, "error": error_msg, "client": "Немає", "error_type": "ConnectionError"}
                self.parsing_error.emit(error_data)
                collected_errors.append(error_data)
                return

            # Перевіряємо наявність аркушів
            if not sheets_list:
                error_msg = "Не знайдено жодного аркуша в таблиці Google Sheets"
                self.logger.error(error_msg)
                session_logger.error(error_msg)
                self.status_update.emit(f"Помилка: {error_msg}")
                error_data = {"sheet": "API_ERROR", "row": 0, "error": error_msg, "client": "Немає", "error_type": "NoSheetsError"}
                self.parsing_error.emit(error_data)
                collected_errors.append(error_data)
                return
                
            # Аналіз отриманих аркушів
            total_sheets = len(sheets_list)
            session_logger.info(f"Отримано {total_sheets} аркушів для обробки")
            session_logger.info(f"Аркуші: {', '.join([ws.title for ws in sheets_list])}")
            self.logger.info(f"Отримано {total_sheets} аркушів для обробки")
            
            # Ініціалізуємо лічильники для статистики
            total_sheets_processed = 0
            total_sheets_with_errors = 0
            total_sheets_skipped = 0
            
            total_rows_processed = 0
            total_rows_skipped = 0
            total_rows_with_errors = 0
            
            total_orders_processed = 0
            total_orders_skipped = 0
            total_orders_updated = 0
            total_products_added = 0
            
            # Встановлюємо початковий статус і прогрес
            self.progress.emit(0)
            self.status_update.emit(f"Початок обробки {total_sheets} аркушів...")
            
            # Додамо лічильники для відстеження API помилок
            total_api_errors = 0
            total_quota_exceeded = 0
            total_other_errors = 0
            
            # Проходимо по всіх аркушах
            for index, worksheet in enumerate(sheets_list):
                sheet_name = worksheet.title
                sheet_start_time = datetime.datetime.now()
                
                # Оновлюємо прогрес і статус
                progress_percent = int((index / total_sheets) * 100)
                self.progress.emit(progress_percent)
                self.status_update.emit(f"Обробка аркуша {sheet_name} ({index+1}/{total_sheets})...")
                
                # Логуємо початок обробки аркуша
                self.logger.info(f"===== ПОЧАТОК ОБРОБКИ АРКУША {sheet_name} ({index+1}/{total_sheets}) =====")
                session_logger.info(f"===== ПОЧАТОК ОБРОБКИ АРКУША {sheet_name} ({index+1}/{total_sheets}) =====")
                
                # Отримуємо дані з аркуша з повторними спробами при помилках квоти API
                retry_count = 0
                max_retries = 20  # Збільшуємо кількість спроб до 20
                backoff_time = 1  # Початкова затримка в секундах
                max_backoff_time = 180  # Збільшуємо максимальну затримку до 3 хвилин (було 60 секунд)
                
                data = None
                while retry_count < max_retries:
                    try:
                        session_logger.info(f"Спроба #{retry_count+1} отримання даних з аркуша {sheet_name}")
                        data = worksheet.get_all_values()
                        self.logger.info(f"[Аркуш {sheet_name}] Отримано {len(data)} рядків даних")
                        session_logger.info(f"[Аркуш {sheet_name}] Отримано {len(data)} рядків даних")
                        break  # Дані отримано успішно, виходимо з циклу
                    except gspread.exceptions.APIError as api_error:
                        error_str = str(api_error)
                        retry_count += 1
                        
                        # Виявляємо, чи це помилка перевищення квоти
                        if "Quota exceeded" in error_str or "Rate Limit Exceeded" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                            total_quota_exceeded += 1
                            wait_time = min(backoff_time * 2 ** (retry_count - 1), max_backoff_time)  # Експоненціальне збільшення часу очікування
                            jitter = random.uniform(0.5, 1.5)  # Рандомізація для уникнення синхронізації запитів
                            wait_time = wait_time * jitter
                            
                            error_msg = f"[Аркуш {sheet_name}] Перевищено квоту API, спроба {retry_count}/{max_retries}, очікування {wait_time:.1f} сек"
                            self.logger.warning(error_msg)
                            session_logger.warning(error_msg)
                            quota_logger.warning(f"[Аркуш {sheet_name}] Quota exceeded: {error_str}")
                            self.status_update.emit(f"Перевищено квоту API, очікування {wait_time:.1f} сек...")
                            
                            # Очікуємо перед наступною спробою
                            sleep(wait_time)
                        else:
                            # Інші помилки API
                            total_other_errors += 1
                            error_msg = f"Помилка API при отриманні даних з аркуша {sheet_name}: {api_error}"
                            
                            self.logger.error(error_msg)
                            session_logger.error(f"{error_msg}\n{traceback.format_exc()}")
                            error_logger.error(f"{error_msg}\n{traceback.format_exc()}")
                            api_logger.error(f"[Аркуш {sheet_name}] Невідома помилка API: {error_str}")
                            
                            self.status_update.emit(error_msg)
                            error_data = {
                                "sheet": sheet_name, 
                                "row": 0, 
                                "error": f"Помилка API: {api_error}",
                                "client": "Немає",
                                "error_type": "APIError",
                                "traceback": traceback.format_exc()
                            }
                            self.parsing_error.emit(error_data)
                            collected_errors.append(error_data)
                            retry_count = max_retries  # Завершуємо цикл спроб
                            break
                    except Exception as e:
                        # Інші неочікувані помилки
                        total_other_errors += 1
                        error_msg = f"Помилка при отриманні даних з аркуша {sheet_name}: {e}"
                        self.logger.error(error_msg)
                        session_logger.error(f"{error_msg}\n{traceback.format_exc()}")
                        error_logger.error(f"{error_msg}\n{traceback.format_exc()}")
                        
                        self.status_update.emit(error_msg)
                        error_data = {
                            "sheet": sheet_name, 
                            "row": 0, 
                            "error": f"Неочікувана помилка: {e}",
                            "client": "Немає",
                            "error_type": "Exception",
                            "traceback": traceback.format_exc()
                        }
                        self.parsing_error.emit(error_data)
                        collected_errors.append(error_data)
                        retry_count = max_retries  # Завершуємо цикл спроб
                        break
                
                # Перевіряємо, чи отримали дані аркуша
                if not data:
                    error_msg = f"Не вдалося отримати дані з аркуша {sheet_name} після {max_retries} спроб"
                    self.logger.error(error_msg)
                    session_logger.error(error_msg)
                    self.status_update.emit(error_msg)
                    total_sheets_with_errors += 1
                    continue  # Перейти до наступного аркуша
                    
                # Якщо в аркуші менше 2 рядків (тільки заголовки або порожній), пропускаємо його
                if len(data) < 2:
                    info_msg = f"[Аркуш {sheet_name}] Містить менше 2 рядків (порожній або тільки заголовки), пропускаємо"
                    self.logger.info(info_msg)
                    session_logger.info(info_msg)
                    total_sheets_skipped += 1
                    continue
                
                # Логуємо інформацію про отримані дані
                self.logger.info(f"[Аркуш {sheet_name}] Отримано {len(data)} рядків даних")
                session_logger.info(f"[Аркуш {sheet_name}] Отримано {len(data)} рядків даних")
                
                # Перетворюємо список рядків у словники
                headers = [h.strip() for h in data[0]]
                rows = []
                
                for row_data in data[1:]:  # Пропускаємо заголовки
                    row_dict = {}
                    for j, cell_value in enumerate(row_data):
                        if j < len(headers):
                            row_dict[j] = cell_value
                    rows.append(row_dict)
                
                # Обробляємо дані аркуша
                try:
                    # Логуємо початок обробки даних
                    self.logger.info(f"[Аркуш {sheet_name}] Початок обробки {len(rows)} рядків даних в режимі {'ПОВНИЙ' if self.force_process else 'СТАНДАРТНИЙ'}")
                    session_logger.info(f"[Аркуш {sheet_name}] Початок обробки {len(rows)} рядків даних в режимі {'ПОВНИЙ' if self.force_process else 'СТАНДАРТНИЙ'}")
                    
                    # Викликаємо функцію обробки даних з модуля orders_pars.py
                    self.logger.info(f"Виклик process_orders_sheet_data з force_process={self.force_process}")
                    session_logger.info(f"Виклик process_orders_sheet_data з force_process={self.force_process}")
                    
                    # Обробка даних з функції має бути у форматі словника
                    result = process_orders_sheet_data(
                        rows, sheet_name, self.force_process
                    )
                    
                    # Перетворюємо результат до словника, якщо потрібно
                    result = ensure_dict_result(result)
                    
                    # Отримуємо значення зі словника результатів
                    orders_processed = result.get("orders_processed", 0)
                    orders_skipped = result.get("orders_skipped", 0)
                    orders_updated = result.get("orders_updated", 0)
                    products_added = result.get("products_added", 0)
                    errors = result.get("parsing_errors", [])
                    
                    # Оновлюємо загальну статистику
                    total_orders_processed += orders_processed
                    total_orders_skipped += orders_skipped
                    total_orders_updated += orders_updated
                    total_products_added += products_added
                    total_rows_processed += orders_processed + orders_updated
                    total_rows_skipped += orders_skipped
                    total_sheets_processed += 1
                    
                    # Логуємо результати обробки
                    result_msg = (f"[Аркуш {sheet_name}] Результати обробки: "
                                f"оброблено {orders_processed} замовлень, "
                                f"пропущено {orders_skipped}, "
                                f"оновлено {orders_updated}, "
                                f"додано {products_added} продуктів")
                    
                    self.logger.info(result_msg)
                    session_logger.info(result_msg)
                    
                    # Логуємо помилки
                    if errors:
                        for err in errors:
                            self.parsing_error.emit(err)
                            collected_errors.append(err)
                            error_logger.warning(
                                f"[Аркуш {sheet_name}] Помилка обробки рядка {err.get('row', 'Н/Д')}: " +
                                f"{err.get('error', 'Невідома помилка')}"
                            )
                            
                        total_rows_with_errors += len(errors)
                        self.logger.warning(f"[Аркуш {sheet_name}] Знайдено {len(errors)} помилок обробки")
                        session_logger.warning(f"[Аркуш {sheet_name}] Знайдено {len(errors)} помилок обробки")
                        if len(errors) > 0:
                            total_sheets_with_errors += 1
                    
                    # Вимірюємо час обробки аркуша
                    sheet_end_time = datetime.datetime.now()
                    sheet_duration = sheet_end_time - sheet_start_time
                    self.logger.info(f"[Аркуш {sheet_name}] Час обробки: {str(sheet_duration).split('.')[0]}")
                    session_logger.info(f"[Аркуш {sheet_name}] Час обробки: {str(sheet_duration).split('.')[0]}")
                    
                    # Додаємо паузу між аркушами для уникнення послідовних перевищень квоти
                    if index < total_sheets - 1:  # Якщо це не останній аркуш
                        pause_time = 5.0  # Стандартна пауза 5 секунд
                        
                        # Якщо був хоча б один випадок перевищення квоти під час обробки цього аркуша, збільшуємо паузу
                        if total_quota_exceeded > 0:
                            quota_pause = min(30.0, 5.0 * (total_quota_exceeded / 10 + 1))  # Збільшуємо паузу залежно від кількості перевищень квоти, максимум 30 секунд
                            pause_time = quota_pause
                            self.logger.info(f"Збільшена пауза між аркушами через {total_quota_exceeded} перевищень квоти: {pause_time:.1f} секунд")
                            session_logger.info(f"Збільшена пауза між аркушами через {total_quota_exceeded} перевищень квоти: {pause_time:.1f} секунд")
                        
                        self.logger.info(f"Пауза {pause_time:.1f} секунд перед обробкою наступного аркуша...")
                        session_logger.info(f"Пауза {pause_time:.1f} секунд перед обробкою наступного аркуша...")
                        self.status_update.emit(f"Пауза {pause_time:.1f} секунд перед наступним аркушем...")
                        
                        # Використовуємо затримку з обробкою подій Qt
                        for i in range(int(pause_time * 10)):  # 10 кроків на секунду для плавності
                            if not self._is_running:  # Перевіряємо, чи не треба зупинитися
                                break
                            QCoreApplication.processEvents()  # Обробляємо події Qt
                            sleep(0.1)  # Чекаємо 100 мс
                    
                except Exception as e:
                    # Обробка помилок, що виникли під час виклику process_orders_sheet_data
                    error_type = type(e).__name__
                    error_msg = f"Помилка обробки даних аркуша {sheet_name}: {str(e)}"
                    self.logger.error(error_msg)
                    session_logger.error(f"{error_msg}\n{traceback.format_exc()}")
                    error_logger.error(f"{error_msg}\n{traceback.format_exc()}")
                    
                    self.status_update.emit(error_msg)
                    self.parsing_error.emit({
                        "sheet": sheet_name, 
                        "row": 0, 
                        "error": error_msg, 
                        "client": "Немає", 
                        "error_type": error_type
                    })
                    total_sheets_with_errors += 1
                
            # Видаляємо дублікати замовлень після обробки всіх аркушів
            try:
                self.logger.info("Видалення дублікатів замовлень...")
                session_logger.info("Початок процесу видалення дублікатів замовлень")
                
                # Імпортуємо функцію з оновлення статусу, щоб не дублювати код
                from views.scripts.orders_pars import remove_redundant_order_duplicates
                remove_redundant_order_duplicates()
                
                self.logger.info("Дублікати замовлень успішно видалені")
                session_logger.info("Дублікати замовлень успішно видалені")
                
            except Exception as e:
                error_type = type(e).__name__
                error_msg = f"Помилка видалення дублікатів: {str(e)}"
                
                self.logger.error(error_msg)
                session_logger.error(f"{error_msg}\n{traceback.format_exc()}")
                error_logger.error(f"{error_msg}\n{traceback.format_exc()}")
                
                self.status_update.emit(f"Помилка видалення дублікатів: {error_type} - {str(e)}")
                error_data = {
                    "sheet": "Загальна помилка", 
                    "row": 0, 
                    "error": error_msg, 
                    "client": "Немає", 
                    "error_type": error_type,
                    "traceback": traceback.format_exc()
                }
                self.parsing_error.emit(error_data)
                collected_errors.append(error_data)
                
            # Логування помилок у файл
            try:
                self.logger.info("Запис помилок парсингу у файл логів")
                from views.scripts.orders_pars import log_sheets_issues
                
                # Передаємо список, а не метод
                log_sheets_issues(collected_errors)
                
                if collected_errors:
                    error_stats = ", ".join([f"{type_name}: {count}" for type_name, count in error_types.items()])
                    error_summary = f"Імпорт завершено з {len(collected_errors)} помилками. Типи помилок: {error_stats}. Деталі у файлі логів."
                    
                    self.logger.warning(error_summary)
                    self.status_update.emit(error_summary)
                else:
                    success_msg = "Імпорт успішно завершено без помилок."
                    self.logger.info(success_msg)
                    self.status_update.emit(success_msg)
            except Exception as e:
                error_msg = f"Помилка логування помилок: {str(e)}"
                self.logger.error(error_msg)
                session_logger.error(f"{error_msg}\n{traceback.format_exc()}")
                self.status_update.emit(error_msg)
            
            # Завершальна статистика та час виконання
            end_time = datetime.datetime.now()
            total_duration = end_time - start_time
            
            # Формуємо підсумковий звіт
            summary = f"""
===== ПІДСУМКОВИЙ ЗВІТ ПРО ІМПОРТ ({end_time.strftime('%Y-%m-%d %H:%M:%S')}) =====
Час виконання: {str(total_duration).split('.')[0]}
Режим: {'ПОВНИЙ ПАРСИНГ' if self.force_process else 'СТАНДАРТНИЙ ПАРСИНГ'}

Статистика аркушів:
- Всього аркушів: {total_sheets}
- Успішно оброблено: {total_sheets_processed}
- З помилками: {total_sheets_with_errors}
- Пропущено: {total_sheets_skipped}

Статистика рядків:
- Оброблено: {total_rows_processed}
- Пропущено: {total_rows_skipped}
- З помилками: {total_rows_with_errors}

Статистика замовлень:
- Додано нових: {total_orders_processed}
- Оновлено існуючих: {total_orders_updated}
- Пропущено: {total_orders_skipped}
- Додано продуктів: {total_products_added}

Статистика API:
- Помилок перевищення квоти: {total_quota_exceeded}
- Інших помилок API: {total_other_errors}

Загальна кількість помилок: {len(collected_errors)}
Деталі у файлі: {session_log_file}
"""
            
            self.logger.info(summary)
            session_logger.info(summary)
            self.status_update.emit(f"Імпорт завершено. Всього оброблено {total_orders_processed + total_orders_updated} замовлень, {len(collected_errors)} помилок.")
            
            # Закриваємо логи сеансу
            for handler in session_logger.handlers:
                handler.close()
                session_logger.removeHandler(handler)
                
            # Відсилаємо сигнал про завершення
            self.progress.emit(100)
            self.finished.emit()
            
        except Exception as e:
            error_type = type(e).__name__
            error_msg = f"Загальна помилка імпорту: {error_type} - {str(e)}"
            
            self.logger.error(error_msg)
            error_logger.error(f"{error_msg}\n{traceback.format_exc()}")
            
            self.status_update.emit(error_msg)
            error_data = {
                "sheet": "Загальна помилка", 
                "row": 0, 
                "error": error_msg, 
                "client": "Немає",
                "error_type": error_type,
                "traceback": traceback.format_exc()
            }
            self.parsing_error.emit(error_data)
            collected_errors.append(error_data)
            logging.error(traceback.format_exc())
            
            self.finished.emit()


class AsyncUniversalParsingWorker(QObject):
    """
    АСИНХРОННИЙ воркер для парсингу даних (товарів і замовлень).
    
    Особливості:
    - Використовує asyncio для неблокуючого виконання
    - Запускає обидва процеси паралельно та чекає їх завершення
    - Використовує indeterminate progress mode для кращої анімації прогрес-бару
    - Інтерфейс залишається чутливим під час виконання
    
    Сигнали:
    - status_update: Відправляє оновлення статусу парсингу
    - progress: Відправляє значення прогресу (0-100) або None для індетермінованого режиму
    - error: Відправляє повідомлення про помилку
    - finished: Відправляється після завершення обох парсингів
    - products_finished: Відправляється після завершення парсингу товарів
    - orders_finished: Відправляється після завершення парсингу замовлень
    """
    status_update = pyqtSignal(str)
    progress = pyqtSignal(object)  # Може бути int або None для індетермінованого режиму
    error = pyqtSignal(str)
    finished = pyqtSignal()
    products_finished = pyqtSignal()
    orders_finished = pyqtSignal()
    
    def __init__(self):
        """Ініціалізує воркера і встановлює початкові значення змінних."""
        super().__init__()
        self._is_running = True
        self._products_process = None
        self._orders_process = None
        logging.debug("AsyncUniversalParsingWorker створено")

    def run(self):
        """
        Запускає обидва процеси парсингу асинхронно.
        
        ВАЖЛИВО:
        - Використовує asyncio.run щоб правильно керувати циклом подій
        - Запускає _async_runner як основну асинхронну функцію
        - Обробляє помилки, які можуть виникнути в асинхронному контексті
        """
        try:
            # Виставляємо індетермінований режим прогрес-бару
            self.progress.emit(None)
            
            # Використовуємо qasync.run для запуску асинхронної функції у середовищі Qt
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self._async_runner())
        except Exception as e:
            # Ловимо всі помилки і надсилаємо сигнал про помилку
            error_message = f"Помилка при асинхронному парсингу: {str(e)}\n{traceback.format_exc()}"
            logging.error(error_message)
            self.status_update.emit("Помилка при оновленні")
            self.error.emit(error_message)
            self.finished.emit()

    async def _async_runner(self):
        """
        Основна асинхронна функція, яка запускає і керує парсингом.
        """
        try:
            self.status_update.emit("Підготовка до оновлення")
            
            # Запускаємо обидва процеси паралельно
            products_task = asyncio.create_task(self._run_products_parser())
            # Маленька пауза для UI
            await asyncio.sleep(0.1)
            orders_task = asyncio.create_task(self._run_orders_parser())
            
            # Очікуємо завершення обох задач
            try:
                # Спершу чекаємо на завершення продуктів з індетермінованим прогресом
                self.progress.emit(None)
                await products_task
                self.products_finished.emit()
                self.status_update.emit("Товари оновлено")
                
                # Встановлюємо проміжний прогрес після завершення товарів
                self.progress.emit(50)
                
                # Потім чекаємо на завершення замовлень
                self.progress.emit(None)  # Повертаємо індетермінований режим
                await orders_task
                self.orders_finished.emit()
                self.status_update.emit("Замовлення оновлено")
                
                # Завершальний прогрес
                self.progress.emit(100)
                
                # Перевіряємо, чи процес не було зупинено
                if self._is_running:
                    self.status_update.emit("Оновлення бази завершено")
                else:
                    self.status_update.emit("Процес зупинено користувачем")
                
            except asyncio.CancelledError:
                # Якщо асинхронну задачу було скасовано
                self.status_update.emit("Процес зупинено")
                
            except Exception as e:
                # Якщо виникла помилка при виконанні задач
                logging.error(f"Помилка при виконанні асинхронних задач: {str(e)}")
                self.status_update.emit(f"Помилка: {str(e)}")
                self.error.emit(str(e))
            
            # Фінальний сигнал про завершення роботи
            self.finished.emit()
            
        except Exception as e:
            # Обробка помилок в основній асинхронній функції
            logging.error(f"Помилка в _async_runner: {str(e)}")
            self.status_update.emit(f"Помилка: {str(e)}")
            self.error.emit(str(e))
            self.finished.emit()

    async def _run_products_parser(self):
        """
        Запускає парсинг товарів асинхронно.
        
        Повертає:
            bool: True якщо парсинг завершився успішно, False у випадку помилки.
        """
        # Перевіряємо, чи процес не зупинено
        if not self._is_running:
            return False
        
        try:
            # Знаходимо шлях до скрипту товарів
            script_name = 'googlesheets_pars.py'
            base_dir = os.path.dirname(os.path.abspath(__file__))
            
            path_variants = [
                os.path.join(base_dir, 'views', 'scripts', script_name),
                os.path.join(base_dir, 'scripts', script_name),
                os.path.join(base_dir, '..', 'views', 'scripts', script_name),
                os.path.join(base_dir, '..', 'scripts', script_name),
                os.path.join(os.path.dirname(base_dir), 'views', 'scripts', script_name),
                os.path.join(os.path.dirname(base_dir), 'scripts', script_name),
            ]
            
            products_script_path = None
            for path in path_variants:
                if os.path.isfile(path):
                    products_script_path = path
                    break
            
            if not products_script_path:
                error_msg = f"Не знайдено скрипт {script_name} в жодному з можливих місць"
                logging.error(error_msg)
                self.status_update.emit(error_msg)
                self.error.emit(error_msg)
                return False
            
            logging.info(f"Знайдено скрипт товарів: {products_script_path}")
            
            # Відправляємо статус
            self.status_update.emit("Оновлення товарів")
            
            # Створюємо процес
            cmd = [sys.executable, products_script_path]
            self._products_process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                text=True
            )
            
            # Чекаємо на завершення процесу
            stdout, stderr = await self._products_process.communicate()
            
            # Перевіряємо код завершення
            if self._products_process.returncode != 0:
                error_message = stderr.strip() if stderr else "Невідома помилка"
                logging.error(f"Помилка парсингу товарів: {error_message}")
                self.status_update.emit(f"Помилка оновлення товарів")
                self.error.emit(f"Помилка при парсингу товарів: {error_message}")
                return False
            
            # Успішне завершення
            logging.info("Парсинг товарів успішно завершено")
            return True
            
        except asyncio.CancelledError:
            # Якщо корутину було скасовано, завершуємо процес
            logging.info("Парсинг товарів скасовано")
            if self._products_process and self._products_process.returncode is None:
                try:
                    self._products_process.terminate()
                    # Даємо час на коректне завершення
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logging.error(f"Помилка при завершенні процесу товарів: {e}")
            return False
            
        except Exception as e:
            # Логуємо помилку і повертаємо статус
            logging.error(f"Помилка при парсингу товарів: {str(e)}")
            self.status_update.emit(f"Помилка при оновленні товарів")
            self.error.emit(f"Помилка при парсингу товарів: {str(e)}")
            return False

    async def _run_orders_parser(self):
        """
        Запускає парсинг замовлень асинхронно.
        
        Повертає:
            bool: True якщо парсинг завершився успішно, False у випадку помилки.
        """
        # Перевіряємо, чи процес не зупинено
        if not self._is_running:
            return False
            
        try:
            # Знаходимо шлях до скрипту замовлень
            script_name = 'orders_pars.py'
            base_dir = os.path.dirname(os.path.abspath(__file__))
            
            path_variants = [
                os.path.join(base_dir, 'views', 'scripts', script_name),
                os.path.join(base_dir, 'scripts', script_name),
                os.path.join(base_dir, '..', 'views', 'scripts', script_name),
                os.path.join(base_dir, '..', 'scripts', script_name),
                os.path.join(os.path.dirname(base_dir), 'views', 'scripts', script_name),
                os.path.join(os.path.dirname(base_dir), 'scripts', script_name),
            ]
            
            orders_script_path = None
            for path in path_variants:
                if os.path.isfile(path):
                    orders_script_path = path
                    break
            
            if not orders_script_path:
                error_msg = f"Не знайдено скрипт {script_name} в жодному з можливих місць"
                logging.error(error_msg)
                self.status_update.emit(error_msg)
                self.error.emit(error_msg)
                return False
            
            logging.info(f"Знайдено скрипт замовлень: {orders_script_path}")
            
            # Відправляємо статус
            self.status_update.emit("Оновлення замовлень")
            
            # Створюємо процес
            cmd = [sys.executable, orders_script_path]
            self._orders_process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                text=True
            )
            
            # Чекаємо на завершення процесу
            stdout, stderr = await self._orders_process.communicate()
            
            # Перевіряємо код завершення
            if self._orders_process.returncode != 0:
                error_message = stderr.strip() if stderr else "Невідома помилка"
                logging.error(f"Помилка парсингу замовлень: {error_message}")
                self.status_update.emit(f"Помилка оновлення замовлень: {error_message}")
                self.error.emit(f"Помилка при парсингу замовлень: {error_message}")
                return False
            
            # Успішне завершення
            logging.info("Парсинг замовлень успішно завершено")
            self.status_update.emit("Замовлення успішно оновлено")
            return True
            
        except asyncio.CancelledError:
            # Якщо корутину було скасовано, завершуємо процес
            logging.info("Парсинг замовлень скасовано")
            if self._orders_process and self._orders_process.returncode is None:
                try:
                    self._orders_process.terminate()
                    # Даємо час на коректне завершення
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logging.error(f"Помилка при завершенні процесу замовлень: {e}")
            return False
            
        except Exception as e:
            # Логуємо помилку і повертаємо статус
            logging.error(f"Помилка при парсингу замовлень: {str(e)}")
            self.status_update.emit(f"Помилка при оновленні замовлень: {error_type} - {str(e)}")
            self.error.emit(f"Помилка при парсингу замовлень: {str(e)}")
            return False

    def stop(self):
        """
        Зупиняє процес парсингу.
        
        ВАЖЛИВО:
        - Встановлює self._is_running = False для сигналізації зупинки
        - Завершує всі запущені процеси, якщо вони активні
        """
        logging.info("AsyncUniversalParsingWorker: отримано запит на зупинку")
        self._is_running = False
        
        # Зупиняємо процес товарів, якщо він існує і працює
        if self._products_process and self._products_process.returncode is None:
            try:
                logging.info("Завершую процес парсингу товарів")
                self._products_process.terminate()
            except Exception as e:
                logging.error(f"Помилка при завершенні процесу товарів: {e}")
        
        # Зупиняємо процес замовлень, якщо він існує і працює
        if self._orders_process and self._orders_process.returncode is None:
            try:
                logging.info("Завершую процес парсингу замовлень")
                self._orders_process.terminate()
            except Exception as e:
                logging.error(f"Помилка при завершенні процесу замовлень: {e}")
        
        # Емітуємо сигнал про завершення
        self.status_update.emit("Процес зупинено користувачем")
        self.finished.emit()


class UniversalParsingWorker(QObject):
    """
    СИНХРОННИЙ воркер для парсингу даних (товарів і замовлень).
    
    Особливості:
    - Виконує процеси послідовно в поточному потоці
    - Використовує subprocess.Popen для неблокуючого виклику зовнішніх скриптів
    - Інтерфейс залишається чутливим завдяки обробці QCoreApplication.processEvents()
    - Працює в режимі індетермінованого прогрес-бару для кращого UX
    
    Сигнали:
    - status_update: Відправляє оновлення статусу парсингу
    - progress: Відправляє значення прогресу (0-100) або None для індетермінованого режиму
    - error: Відправляє повідомлення про помилку
    - finished: Відправляється після завершення обох парсингів
    """
    status_update = pyqtSignal(str)
    progress = pyqtSignal(object)  # Може бути int або None
    error = pyqtSignal(str)
    finished = pyqtSignal()
    
    def __init__(self):
        """Ініціалізує воркера і встановлює початкові значення змінних."""
        super().__init__()
        self._is_running = True
        self.force_process = False  # Параметр для повного оновлення
        logging.debug("UniversalParsingWorker створено")
    
    def show_update_type_dialog(self):
        """
        Показує діалогове вікно для вибору типу оновлення бази даних.
        Повертає True, якщо користувач вибрав повне оновлення, False для стандартного, None якщо скасував.
        """
        # Переконуємося, що є активне вікно програми
        app = QApplication.instance()
        if not app:
            logging.warning("QApplication екземпляр не знайдено, діалог не може бути показаний")
            return self.force_process
            
        logging.info("Відображення діалогу вибору типу оновлення")
        dialog = UpdateTypeDialog()
        result = dialog.exec()
        
        if result == QDialog.DialogCode.Accepted:
            selection = dialog.is_full_update_selected()
            logging.info(f"Користувач вибрав: {'Повне оновлення' if selection else 'Стандартне оновлення'}")
            return selection
        else:
            # Користувач скасував, припиняємо операцію
            logging.info("Користувач скасував діалог вибору типу оновлення")
            return None

    def run(self):
        """
        Запускає обидва процеси парсингу в синхронному режимі.
        
        ВАЖЛИВО:
        - Метод шукає скрипти парсингу в різних можливих розташуваннях
        - Якщо скрипти не знайдено, метод поверне помилку і завершиться
        - Кожен зовнішній скрипт запускається через subprocess.Popen з обробкою подій Qt
        - Прогрес-бар працює в режимі постійної анімації для кращого UX
        """
        try:
            # Активуємо індетермінований прогрес-бар
            self.progress.emit(None)
            
            # Виводимо інформацію про режим оновлення
            mode_text = "ПОВНИЙ" if self.force_process else "СТАНДАРТНИЙ"
            logging.info(f"Запуск універсального парсингу в режимі: {mode_text}")
            self.status_update.emit(f"Режим оновлення: {mode_text}")
            
            # ПЕРША ФАЗА: ПАРСИНГ ТОВАРІВ
            self.status_update.emit("Підготовка до оновлення товарів")
            
            # Шлях до скрипту товарів - пробуємо різні варіанти
            script_name = 'googlesheets_pars.py'
            base_dir = os.path.dirname(os.path.abspath(__file__))
            
            path_variants = [
                os.path.join(base_dir, 'views', 'scripts', script_name),
                os.path.join(base_dir, 'scripts', script_name),
                os.path.join(base_dir, '..', 'views', 'scripts', script_name),
                os.path.join(base_dir, '..', 'scripts', script_name),
                os.path.join(os.path.dirname(base_dir), 'views', 'scripts', script_name),
                os.path.join(os.path.dirname(base_dir), 'scripts', script_name),
            ]
            
            products_script_path = None
            for path in path_variants:
                if os.path.isfile(path):
                    products_script_path = path
                    break
            
            if not products_script_path:
                error_msg = f"Не знайдено скрипт {script_name} в жодному з можливих місць"
                logging.error(error_msg)
                self.status_update.emit(error_msg)
                self.error.emit(error_msg)
                self.finished.emit()
                return
                
            logging.info(f"Знайдено скрипт товарів: {products_script_path}")
            
            # Сповіщаємо користувача
            self.status_update.emit("Початок оновлення товарів")
            
            # Запускаємо скрипт товарів
            command = [sys.executable, products_script_path]
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, 
                text=True,
                bufsize=1
            )
            
            # Очікуємо на завершення процесу з обробкою подій Qt
            current_status = ""
            while process.poll() is None and self._is_running:
                # Обробляємо події інтерфейсу, щоб уникнути зависання
                QCoreApplication.processEvents()
                
                # Читаємо вивід скрипта для відображення статусів
                output_line = process.stdout.readline().strip()
                if output_line:
                    # Шукаємо інформаційні повідомлення про обробку аркушів у виводі
                    if "Обробка:" in output_line:
                        sheet_name = output_line.split("Обробка:", 1)[1].strip()
                        status_msg = f"Оновлення товарів: обробка аркуша {sheet_name}"
                        if status_msg != current_status:
                            current_status = status_msg
                            self.status_update.emit(status_msg)
                            logging.info(status_msg)
                    # Шукаємо інформацію про запуск orders_pars.py
                    elif "Запускаємо orders_pars.py" in output_line:
                        status_msg = "Завершення оновлення товарів, підготовка до оновлення замовлень"
                        self.status_update.emit(status_msg)
                        logging.info(status_msg)
                    # Додаємо інформацію про обробку даних
                    elif "Документ:" in output_line:
                        document_name = output_line.split("Документ:", 1)[1].strip()
                        status_msg = f"Оновлення товарів: документ {document_name}"
                        if status_msg != current_status:
                            current_status = status_msg
                            self.status_update.emit(status_msg)
                            logging.info(status_msg)
                    # Логуємо інформацію про пропуск аркушів
                    elif "Пропуск" in output_line and "Постачальники" not in output_line:
                        sheet_name = output_line.split("Пропуск", 1)[1].strip().replace("'", "")
                        status_msg = f"Оновлення товарів: пропуск аркуша {sheet_name}"
                        if status_msg != current_status:
                            current_status = status_msg
                            self.status_update.emit(status_msg)
                            logging.info(status_msg)
                
                # Перевіряємо помилки
                error_line = process.stderr.readline().strip()
                if error_line:
                    logging.error(f"STDERR from products script: {error_line}")
                    if "Помилка" in error_line:
                        self.status_update.emit(f"Помилка оновлення товарів: {error_line}")
                
                # Коротка пауза для зменшення навантаження на CPU
                time.sleep(0.05)
            
            # Перевіряємо, чи процес все ще виконується
            if process.poll() is None:
                # Якщо процес ще не завершився, але воркер отримав сигнал зупинки
                process.terminate()
                # Даємо час для завершення
                time.sleep(0.5)
            
            # Отримуємо результат
            stdout, stderr = process.communicate()
            
            if process.returncode != 0:
                error_message = stderr.strip()
                logging.error(f"Помилка парсингу товарів: {error_message}")
                self.status_update.emit(f"Помилка оновлення товарів")
                self.error.emit(f"Помилка при парсингу товарів: {error_message}")
            else:
                logging.info("Парсинг товарів успішно завершено")
                self.status_update.emit("Товари оновлено")
            
            # Встановлюємо проміжну відмітку прогресу
            self.progress.emit(50)
            
            # Перевіряємо, чи потрібно продовжувати
            if not self._is_running:
                self.status_update.emit("Процес зупинено користувачем")
                self.finished.emit()
                return
            
            # Коротка пауза для оновлення інтерфейсу
            QCoreApplication.processEvents()
            time.sleep(0.1)
            
            # ДРУГА ФАЗА: ПАРСИНГ ЗАМОВЛЕНЬ
            self.status_update.emit("Підготовка до оновлення замовлень")
            # Повертаємося до індетермінованого режиму
            self.progress.emit(None)
            
            # Шлях до скрипту замовлень - пробуємо різні варіанти
            script_name = 'orders_pars.py'
            
            path_variants = [
                os.path.join(base_dir, 'views', 'scripts', script_name),
                os.path.join(base_dir, 'scripts', script_name),
                os.path.join(base_dir, '..', 'views', 'scripts', script_name),
                os.path.join(base_dir, '..', 'scripts', script_name),
                os.path.join(os.path.dirname(base_dir), 'views', 'scripts', script_name),
                os.path.join(os.path.dirname(base_dir), 'scripts', script_name),
            ]
            
            orders_script_path = None
            for path in path_variants:
                if os.path.isfile(path):
                    orders_script_path = path
                    break
            
            if not orders_script_path:
                error_msg = f"Не знайдено скрипт {script_name} в жодному з можливих місць"
                logging.error(error_msg)
                self.status_update.emit(error_msg)
                self.error.emit(error_msg)
                self.finished.emit()
                return
            
            logging.info(f"Знайдено скрипт замовлень: {orders_script_path}")
            
            # Сповіщаємо користувача
            self.status_update.emit("Оновлення замовлень")
            
            # Запускаємо скрипт замовлень з параметром force, якщо потрібно
            command = [sys.executable, orders_script_path]
            if self.force_process:
                command.append("--force")
                logging.info("Запуск orders_pars.py з параметром --force (повне оновлення)")
            
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, 
                text=True,
                bufsize=1
            )
            
            # Очікуємо на завершення процесу з обробкою подій Qt
            current_status = ""
            while process.poll() is None and self._is_running:
                # Обробляємо події інтерфейсу, щоб уникнути зависання
                QCoreApplication.processEvents()
                
                # Читаємо вивід скрипта для відображення статусів
                output_line = process.stdout.readline().strip()
                if output_line:
                    # Шукаємо інформаційні повідомлення про обробку аркушів у виводі
                    if "Обробка:" in output_line:
                        sheet_name = output_line.split("Обробка:", 1)[1].strip()
                        status_msg = f"Оновлення замовлень: обробка аркуша {sheet_name}"
                        if status_msg != current_status:
                            current_status = status_msg
                            self.status_update.emit(status_msg)
                            logging.info(status_msg)
                    # Шукаємо інформацію про запуск orders_pars.py
                    elif "Запускаємо orders_pars.py" in output_line:
                        status_msg = "Завершення оновлення замовлень, підготовка до оновлення товарів"
                        self.status_update.emit(status_msg)
                        logging.info(status_msg)
                    # Додаємо інформацію про обробку даних
                    elif "Документ:" in output_line:
                        document_name = output_line.split("Документ:", 1)[1].strip()
                        status_msg = f"Оновлення замовлень: документ {document_name}"
                        if status_msg != current_status:
                            current_status = status_msg
                            self.status_update.emit(status_msg)
                            logging.info(status_msg)
                    # Логуємо інформацію про пропуск аркушів
                    elif "Пропуск" in output_line and "Постачальники" not in output_line:
                        sheet_name = output_line.split("Пропуск", 1)[1].strip().replace("'", "")
                        status_msg = f"Оновлення замовлень: пропуск аркуша {sheet_name}"
                        if status_msg != current_status:
                            current_status = status_msg
                            self.status_update.emit(status_msg)
                            logging.info(status_msg)
                
                # Перевіряємо помилки
                error_line = process.stderr.readline().strip()
                if error_line:
                    logging.error(f"STDERR from orders script: {error_line}")
                    if "Помилка" in error_line:
                        self.status_update.emit(f"Помилка оновлення замовлень: {error_line}")
                
                # Коротка пауза для зменшення навантаження на CPU
                time.sleep(0.05)
            
            # Перевіряємо, чи процес все ще виконується
            if process.poll() is None:
                # Якщо процес ще не завершився, але воркер отримав сигнал зупинки
                process.terminate()
                # Даємо час для завершення
                time.sleep(0.5)
            
            # Отримуємо результат
            stdout, stderr = process.communicate()
            
            if process.returncode != 0:
                error_message = stderr.strip()
                logging.error(f"Помилка парсингу замовлень: {error_message}")
                self.status_update.emit(f"Помилка оновлення замовлень")
                self.error.emit(f"Помилка при парсингу замовлень: {error_message}")
            else:
                logging.info("Парсинг замовлень успішно завершено")
                self.status_update.emit("Замовлення оновлено")
            
            # Завершальна фаза
            if not self._is_running:
                self.status_update.emit("Процес зупинено користувачем")
            else:
                # Встановлюємо 100% для завершення
                self.progress.emit(100)
                self.status_update.emit("Оновлення бази завершено")
            
            # Даємо час для оновлення інтерфейсу перед емісією сигналу завершення
            QCoreApplication.processEvents()
            time.sleep(0.1)
            
            # Сигнал завершення - важливо, це викличе on_parsing_finished() в MainWindow
            self.finished.emit()
            
        except Exception as e:
            logging.exception(f"Помилка при парсингу: {str(e)}")
            self.status_update.emit(f"Помилка: {str(e)}")
            self.error.emit(str(e))
            self.finished.emit()
    
    def stop(self):
        """Зупиняє процес парсингу."""
        self._is_running = False
        logging.info("UniversalParsingWorker: отримано запит на зупинку")


def ensure_dict_result(result):
    """
    Перетворює результат функції process_orders_sheet_data до формату словника 
    для забезпечення зворотної сумісності. Якщо результат вже є словником, 
    повертає його без змін.
    """
    if isinstance(result, dict):
        return result
    elif isinstance(result, tuple) and len(result) >= 5:
        # Це старий формат - кортеж з 5 значень
        return {
            "orders_processed": result[0],
            "orders_skipped": result[1],
            "orders_updated": result[2],
            "products_added": result[3],
            "parsing_errors": result[4] if len(result) > 4 else []
        }
    else:
        # Якщо щось інше - повертаємо порожній словник з нулями
        return {
            "orders_processed": 0,
            "orders_skipped": 0,
            "orders_updated": 0,
            "products_added": 0,
            "parsing_errors": []
        }