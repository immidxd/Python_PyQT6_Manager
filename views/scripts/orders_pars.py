#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import logging
import time
import hashlib
import argparse
import threading
import queue
import json
from datetime import datetime
from collections import defaultdict

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

# Встановлюємо більш детальне логування для моніторингу процесу парсингу
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
                    handlers=[
                        logging.StreamHandler(),
                        logging.FileHandler("orders_import_logs.log", mode="a")
                    ])
logger = logging.getLogger(__name__)

# Додаємо глобальний список для збереження помилок парсингу
parsing_errors = []

# Додаємо глобальні змінні для відстеження прогресу парсингу
parsing_status = {
    "is_running": False,
    "total_sheets": 0,
    "processed_sheets": 0,
    "total_rows": 0,
    "processed_rows": 0,
    "current_sheet": "",
    "start_time": None,
    "end_time": None,
    "errors": 0,
    "orders_processed": 0,
    "orders_updated": 0,
    "memory_usage": 0,
    "progress_percent": 0
}

# Черга для обробки аркушів асинхронно
parsing_queue = queue.Queue()

# -------------------------------------------------------
#   Дані для підключення до БД
# -------------------------------------------------------
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")

# -------------------------------------------------------
#   Дані для Google Sheets
# -------------------------------------------------------
GOOGLE_SHEETS_JSON_KEY = os.getenv("GOOGLE_SHEETS_JSON_KEY")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GOOGLE_SHEETS_CREDENTIALS_FILE = os.path.join(SCRIPT_DIR, GOOGLE_SHEETS_JSON_KEY)
SPREADSHEET_NAME = os.getenv("GOOGLE_SHEETS_DOCUMENT_NAME_ORDERS", "Замовлення")

# -------------------------------------------------------
#   Статичні довідники / константи
# -------------------------------------------------------
# genders.id: 1=чоловічий, 2=жіночий, 3=унісекс
GENDER_ID_MALE = 1
GENDER_ID_FEMALE = 2
GENDER_ID_UNISEX = 3

# order_statuses.id
ORDER_STATUS_MAP = {
   "підтверджено": 1,
   "очікується": 2,
   "уточнити": 3,
   "фото": 4,
   "відміна": 5,
   "ігнорування": 6,
   "подарунок": 7,
   "в черзі": 8,
   "повернення": 9,
   "обмін": 10,
   "передати": 11
}

# payment_statuses.id
PAYMENT_STATUS_MAP = {
   "оплачено": 1,
   "доплатити": 2,
   "відкладено": 3,
   "не оплачено": 4
}

# delivery_methods.id
DELIVERY_METHOD_MAP = {
   "нп": 1,
   "уп": 2,
   "міст": 3,
   "самовивіз": 4,
   "місцевий": 5,
   "відкладено": 6,
   "магазин": 7
}

# delivery_statuses.id
DELIVERY_STATUS_MAP = {
   "створено": 1,
   "відправлено": 2,
   "в дорозі": 3,
   "доставлено": 4,
   "повернуто": 5
}

# statuses.id (для products.statusid): 1=Продано, 2=Непродано
PRODUCT_STATUS_SOLD = 1
PRODUCT_STATUS_NOT_SOLD = 2

# Глобальні опції, які можна змінити через аргументи командного рядка
FORCE_PROCESS_ALL = False

# Шлях до файлу логування проблем з парсингом аркушів
SHEETS_ISSUES_LOG_FILE = os.path.join(SCRIPT_DIR, "sheets_parsing_issues.log")

# -------------------------------------------------------
#   Ініціалізація таблиць відстеження прогресу обробки
# -------------------------------------------------------
def log_sheets_issues(issues_list):
    """
    Функція для логування проблем із парсингом аркушів Google Sheets.
    Групує проблеми за аркушами для зручного перегляду.
    
    :param issues_list: список словників з інформацією про проблеми
    """
    if not issues_list:
        return
        
    grouped_issues = {}
    issue_types_stats = {}
    rows_with_issues = set()
    sheets_with_issues = set()
    clients_with_issues = set()
    
    # Групуємо проблеми за аркушами та типами
    for issue in issues_list:
        sheet_name = issue.get('sheet_name', 'Unknown')
        if sheet_name not in grouped_issues:
            grouped_issues[sheet_name] = []
        
        grouped_issues[sheet_name].append(issue)
        
        # Збираємо статистику за типами проблем
        issue_type = issue.get('issue', '').split(':')[0] if ':' in issue.get('issue', '') else issue.get('issue', 'Unknown')
        issue_types_stats[issue_type] = issue_types_stats.get(issue_type, 0) + 1
        
        # Збираємо унікальні рядки з проблемами
        row_num = issue.get('row_num')
        if row_num:
            rows_with_issues.add((sheet_name, row_num))
        
        # Збираємо унікальні аркуші з проблемами
        sheets_with_issues.add(sheet_name)
        
        # Збираємо унікальних клієнтів з проблемами
        client = issue.get('client', 'Unknown')
        if client and client != 'Немає':
            clients_with_issues.add(client)
    
    try:
        # Відкриваємо файл для запису логів
        with open(SHEETS_ISSUES_LOG_FILE, 'w', encoding='utf-8') as f:
            # Записуємо заголовок і час
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"--- Проблеми з парсингом аркушів Google Sheets ({now}) ---\n\n")
            
            # Загальна статистика по проблемах
            f.write("## Загальна статистика\n")
            f.write(f"- Загальна кількість проблем: {len(issues_list)}\n")
            f.write(f"- Кількість аркушів з проблемами: {len(sheets_with_issues)}\n")
            f.write(f"- Кількість унікальних рядків з проблемами: {len(rows_with_issues)}\n")
            f.write(f"- Кількість унікальних клієнтів з проблемами: {len(clients_with_issues)}\n\n")
            
            # Статистика за типами проблем
            f.write("## Статистика за типами проблем\n")
            for issue_type, count in sorted(issue_types_stats.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / len(issues_list)) * 100
                f.write(f"- {issue_type}: {count} випадків ({percentage:.2f}%)\n")
            f.write("\n")
            
            # Записуємо деталі проблем по кожному аркушу
            for sheet_name, issues in grouped_issues.items():
                f.write(f"\n### Аркуш: {sheet_name} ({len(issues)} проблем)\n")
                
                # Групуємо проблеми за типами для цього аркуша
                sheet_issue_types = {}
                for issue in issues:
                    issue_type = issue.get('issue', '').split(':')[0] if ':' in issue.get('issue', '') else issue.get('issue', 'Unknown')
                    if issue_type not in sheet_issue_types:
                        sheet_issue_types[issue_type] = []
                    sheet_issue_types[issue_type].append(issue)
                
                # Для кожного типу проблеми виводимо статистику
                for issue_type, type_issues in sheet_issue_types.items():
                    f.write(f"\n#### {issue_type} ({len(type_issues)} випадків)\n")
                    
                    # Обмежуємо кількість детальних прикладів
                    example_limit = 10
                    for i, issue in enumerate(type_issues):
                        if i >= example_limit:
                            f.write(f"\n... і ще {len(type_issues) - example_limit} подібних проблем\n")
                            break
                            
                        row_num = issue.get('row_num', 'Невідомо')
                        client = issue.get('client', 'Немає')
                        issue_text = issue.get('issue', 'Невідома проблема')
                        
                        f.write(f"- Рядок {row_num}, Клієнт: {client}\n")
                        f.write(f"  Проблема: {issue_text}\n")
                        f.write(f"  Рекомендація: Перевірте дані в Google Sheets та виправте проблему.\n\n")
            
            # Записуємо підсумок
            f.write("\n## Рекомендації\n")
            f.write("1. Перевірте правильність введення номерів продуктів та клонів\n")
            f.write("2. Переконайтеся, що дати замовлень вказані в правильному форматі\n")
            f.write("3. Перевірте наявність усіх обов'язкових полів для замовлень\n")
            
        logger.info(f"Лог проблем з парсингом аркушів збережено у файл: {SHEETS_ISSUES_LOG_FILE}")
        
    except Exception as e:
        logger.error(f"Помилка при збереженні логу проблем з парсингом аркушів: {e}")

def init_tracking_tables(conn):
    """Створює таблиці для відстеження прогресу обробки, якщо вони не існують"""
    cur = conn.cursor()
    try:
        # Таблиця для відстеження прогресу по аркушах
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processing_progress (
                sheet_name VARCHAR(255) PRIMARY KEY,
                last_processed_timestamp TIMESTAMP WITH TIME ZONE,
                last_row_index INTEGER
            )
        """)
        
        # Таблиця для збереження хешів рядків (для ефективного оновлення)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS row_hashes (
                sheet_name VARCHAR(255) NOT NULL,
                row_index INTEGER NOT NULL,
                row_hash VARCHAR(32) NOT NULL,
                client_name VARCHAR(255),
                is_processed BOOLEAN DEFAULT FALSE,
                error_message TEXT,
                PRIMARY KEY (sheet_name, row_index)
            )
        """)
        
        # Перевіряємо, чи є колонка is_processed у таблиці row_hashes
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='row_hashes' AND column_name='is_processed'
        """)
        
        if not cur.fetchone():
            # Додаємо колонку is_processed, якщо її ще немає
            cur.execute("""
                ALTER TABLE row_hashes 
                ADD COLUMN is_processed BOOLEAN DEFAULT FALSE
            """)
            logger.info("Додано колонку is_processed до таблиці row_hashes")
        
        # Перевіряємо, чи є колонка error_message у таблиці row_hashes
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='row_hashes' AND column_name='error_message'
        """)
        
        if not cur.fetchone():
            # Додаємо колонку error_message, якщо її ще немає
            cur.execute("""
                ALTER TABLE row_hashes 
                ADD COLUMN error_message TEXT
            """)
            logger.info("Додано колонку error_message до таблиці row_hashes")
            
        conn.commit()
        logger.debug("Таблиці відстеження прогресу створені або вже існують")
    except Exception as e:
        conn.rollback()
        logger.error(f"Помилка при створенні таблиць відстеження: {e}")
    finally:
        cur.close()

# -------------------------------------------------------
#   Функції для відстеження змін рядків
# -------------------------------------------------------
def compute_row_hash(row_data):
    """
    Обчислює хеш для рядка даних, щоб порівнювати зміни.
    Використовується для визначення, чи змінилися дані рядка.
    """
    # Перетворюємо всі значення в рядки і об'єднуємо їх
    # Ігноруємо порожні значення для зменшення хибних спрацьовувань
    try:
        row_str = "||".join([str(val).strip() if val else "" for val in row_data])
        logger.debug(f"Обчислення хешу для рядка: {row_str[:100]}...")
        return hashlib.md5(row_str.encode('utf-8')).hexdigest()
    except Exception as e:
        logger.error(f"Помилка при обчисленні хешу рядка: {e}")
        return hashlib.md5("error_computing_hash".encode('utf-8')).hexdigest()

def get_existing_row_hash(cursor, sheet_name, row_index):
    """Отримує збережений хеш рядка з бази даних"""
    cursor.execute("""
        SELECT row_hash, is_processed, error_message
        FROM row_hashes
        WHERE sheet_name = %s AND row_index = %s
    """, (sheet_name, row_index))
    
    result = cursor.fetchone()
    if result:
        return {'hash': result[0], 'is_processed': result[1], 'error_message': result[2]}
    return None

def update_row_hash(cursor, connection, sheet_name, row_index, row_hash, client_name, is_processed=True, error_message=None):
    """Оновлює або додає запис хешу рядка в базу даних"""
    try:
        cursor.execute("""
            INSERT INTO row_hashes (sheet_name, row_index, row_hash, client_name, is_processed, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (sheet_name, row_index) 
            DO UPDATE SET row_hash = %s, client_name = %s, is_processed = %s, error_message = %s
        """, (sheet_name, row_index, row_hash, client_name, is_processed, error_message, 
              row_hash, client_name, is_processed, error_message))
        
        connection.commit()
    except Exception as e:
        connection.rollback()
        logger.error(f"Помилка при оновленні хешу рядка: {e}")

def update_sheet_progress(cursor, connection, sheet_name, total_rows):
    """Оновлює інформацію про прогрес обробки аркуша"""
    try:
        cursor.execute("""
            INSERT INTO processing_progress (sheet_name, last_processed_timestamp, last_row_index)
            VALUES (%s, now(), %s)
            ON CONFLICT (sheet_name) 
            DO UPDATE SET last_processed_timestamp = now(), last_row_index = %s
        """, (sheet_name, total_rows, total_rows))
        
        connection.commit()
    except Exception as e:
        connection.rollback()
        logger.error(f"Помилка при оновленні прогресу: {e}")

# -------------------------------------------------------
#   Підключення до Google Sheets
# -------------------------------------------------------
def get_google_sheet_client():
    """Отримує клієнт для роботи з Google Sheets API"""
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_json = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'newproject2024-419923-8aec36a3b0ce.json')
        credentials = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scope)
        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        logger.error(f"Помилка при отриманні клієнта Google Sheets: {e}")
        return None

# -------------------------------------------------------
#   Підключення до PostgreSQL
# -------------------------------------------------------
def connect_to_db():
   try:
       return psycopg2.connect(
           host=DB_HOST,
           port=DB_PORT,
           database=DB_NAME,
           user=DB_USER,
           password=DB_PASSWORD
       )
   except psycopg2.Error as e:
       logger.error(f"Помилка підключення до бази даних: {e}")
       return None

# -------------------------------------------------------
#   Сортування аркушів за датою в імені
# -------------------------------------------------------
def sort_worksheets_by_date(worksheets):
    """
    Сортує аркуші Google Sheets за датою (якщо дата вказана в імені аркуша)
    Формат: 'DD.MM.YYYY Тема' або просто ім'я без дати
    
    :param worksheets: список аркушів Google Sheets
    :return: відсортований список аркушів
    """
    def extract_date(worksheet):
        """Витягує дату з імені аркуша, якщо вона є"""
        name = worksheet.title
        # Шукаємо дату у форматі DD.MM.YYYY на початку назви
        match = re.match(r'(\d{2}\.\d{2}\.\d{4})', name)
        if match:
            date_str = match.group(1)
            try:
                # Парсимо дату
                return datetime.strptime(date_str, "%d.%m.%Y")
            except ValueError:
                # Якщо не вдалося розпарсити дату, повертаємо мінімальну дату
                return datetime.min
        # Якщо дати немає, повертаємо мінімальну дату
        return datetime.min
    
    # Сортуємо аркуші за датами (нові аркуші спочатку)
    return sorted(worksheets, key=extract_date, reverse=True)

# -------------------------------------------------------
#   Утиліти
# -------------------------------------------------------
def validate_text(value, max_length=None):
   if not value:
       return None
   text = str(value).strip()
   # Видаляємо деякі невидимі символи (emoji, Variation Selectors тощо)
   text = re.sub(r'[\uFE0F\u200B-\u200D]+', '', text)
   if max_length and len(text) > max_length:
       text = text[:max_length]
   return text or None

def validate_decimal(value):
   if not value:
       return None
   try:
       val = str(value).replace(",", ".")
       return float(val)
   except:
       return None

def validate_integer(value):
   if not value:
       return None
   try:
       return int(float(str(value).strip()))
   except:
       return None

def parse_date_dd_mm_yyyy(date_str):
   if not date_str:
       return None
   for fmt in ("%d.%m.%Y", "%d.%m.%y"):
       try:
           return datetime.strptime(date_str, fmt).date()
       except ValueError:
           pass
   return None

def guess_gender_by_last_name(last_name):
   if not last_name:
       return GENDER_ID_UNISEX
   ln = last_name.lower()
   female_endings = ["ова", "ева", "єва", "іна", "ська", "зька", "цька", "єнко", "юненко", "юженко"]
   for fe in female_endings:
       if ln.endswith(fe):
           return GENDER_ID_FEMALE
   male_endings = ["ов", "ев", "єв", "ін", "ий", "ський", "ко", "енко"]
   for me in male_endings:
       if ln.endswith(me):
           return GENDER_ID_MALE
   return GENDER_ID_UNISEX

def parse_broadcast_sheet_name(sheet_name):
   date_match = re.search(r"\d{1,2}\.\d{1,2}\.\d{2,4}", sheet_name)
   broadcast_date = None
   broadcast_topic = sheet_name.strip()

   if date_match:
       date_str = date_match.group(0)
       broadcast_date = parse_date_dd_mm_yyyy(date_str)
       broadcast_topic = broadcast_topic.replace(date_str, "").strip(" ()\t")

   bracket_match = re.search(r"\((.*?)\)", broadcast_topic)
   if bracket_match:
       broadcast_topic = bracket_match.group(1).strip()

   return broadcast_date, broadcast_topic or None

# -------------------------------------------------------
#   Робота з клієнтами
# -------------------------------------------------------
def get_or_create_client(cursor, connection, full_name):
   if not full_name:
       return None
   parts = full_name.split()
   if len(parts) == 1:
       first_name = parts[0]
       last_name = None
       middle_name = None
   elif len(parts) == 2:
       first_name, last_name = parts
       middle_name = None
   else:
       first_name = parts[0]
       last_name = parts[1]
       middle_name = " ".join(parts[2:])

   guessed_gender_id = guess_gender_by_last_name(last_name) if last_name else GENDER_ID_UNISEX

   cursor.execute("""
       SELECT id, gender_id
         FROM clients
        WHERE lower(trim(first_name)) = lower(trim(%s))
          AND lower(trim(coalesce(last_name, ''))) = lower(trim(%s))
          AND lower(trim(coalesce(middle_name, ''))) = lower(trim(%s))
        LIMIT 1
   """, (first_name or "", last_name or "", middle_name or ""))
   row = cursor.fetchone()
   if row:
       cid, old_gender_id = row
       if old_gender_id == GENDER_ID_UNISEX and guessed_gender_id != GENDER_ID_UNISEX:
           cursor.execute("""
               UPDATE clients
                  SET gender_id=%s,
                      updated_at=now()
                WHERE id=%s
           """,(guessed_gender_id, cid))
           connection.commit()
       return cid

   cursor.execute("""
       INSERT INTO clients (first_name, last_name, middle_name, gender_id, created_at, updated_at)
       VALUES (%s, %s, %s, %s, now(), now())
       RETURNING id
   """,(first_name, last_name, middle_name, guessed_gender_id))
   new_cid = cursor.fetchone()[0]
   connection.commit()
   return new_cid

def get_or_create_default_client(cursor, connection):
    """Отримати або створити клієнта за замовчуванням для замовлень без вказаного клієнта"""
    cursor.execute("""
        SELECT id FROM clients
        WHERE first_name = 'Невідомий' AND last_name IS NULL AND middle_name IS NULL
        LIMIT 1
    """)
    row = cursor.fetchone()
    if row:
        return row[0]
    
    # Створити клієнта за замовчуванням
    cursor.execute("""
        INSERT INTO clients (first_name, last_name, middle_name, gender_id, created_at, updated_at)
        VALUES ('Невідомий', NULL, NULL, %s, now(), now())
        RETURNING id
    """, (GENDER_ID_UNISEX,))
    new_id = cursor.fetchone()[0]
    connection.commit()
    return new_id

# -------------------------------------------------------
#   Робота з продуктами
# -------------------------------------------------------
def get_or_create_product(cursor, connection, product_number):
   if not product_number:
       product_number = "???"
   cursor.execute("""
       SELECT id, price, oldprice, statusid
         FROM products
        WHERE productnumber=%s
        LIMIT 1
   """,(product_number,))
   row = cursor.fetchone()
   if row:
       return row[0]

   cursor.execute("""
       INSERT INTO products (productnumber, created_at, updated_at, statusid)
       VALUES (%s, now(), now(), %s)
       RETURNING id
   """,(product_number, PRODUCT_STATUS_NOT_SOLD))
   pid = cursor.fetchone()[0]
   connection.commit()
   return pid

def update_product_price(cursor, connection, product_id, new_price):
   if not product_id or new_price is None:
       return
   cursor.execute("""
       SELECT price, oldprice
         FROM products
        WHERE id=%s
   """,(product_id,))
   row = cursor.fetchone()
   if not row:
       return
   current_price, current_oldprice = row

   if current_price is not None:
       if current_oldprice is None:
           cursor.execute("""
               UPDATE products
                  SET oldprice=%s
                WHERE id=%s
           """,(current_price, product_id))
       else:
           if current_oldprice > new_price:
               cursor.execute("""
                   UPDATE products
                      SET oldprice=%s
                    WHERE id=%s
               """,(current_price, product_id))

   cursor.execute("""
       UPDATE products
          SET price=%s,
              updated_at=now()
        WHERE id=%s
   """,(new_price, product_id))
   connection.commit()

def append_clonednumbers(cursor, connection, product_id, new_clones_list):
   if not new_clones_list:
       return
   cursor.execute("""
       SELECT clonednumbers
         FROM products
        WHERE id=%s
   """,(product_id,))
   row = cursor.fetchone()
   if not row:
       return
   existing = row[0] if row[0] else ""
   existing_str = existing.strip()

   appended = []
   for c in new_clones_list:
       c = c.strip()
       if c and c not in existing_str:
           appended.append(c)

   if not appended:
       return

   if existing_str:
       new_val = existing_str + "; " + "; ".join(appended)
   else:
       new_val = "; ".join(appended)

   cursor.execute("""
       UPDATE products
          SET clonednumbers=%s,
              updated_at=now()
        WHERE id=%s
   """,(new_val, product_id))
   connection.commit()

# -------------------------------------------------------
#   Парсинг дод. операцій і знижок
# -------------------------------------------------------
def parse_additional_operation(op_str):
   if not op_str:
       return (None, 0.0)
   s = op_str.replace(" ", "")
   sign = 1
   if s.startswith("-"):
       sign = -1
       s = s[1:]
   elif s.startswith("+"):
       s = s[1:]
   val = validate_decimal(s)
   if val is None:
       return (None, 0.0)
   return ("Додаткова операція", sign * val)

def parse_discount_str(discount_str):
   if not discount_str:
       return (None, None)
   ds = discount_str.strip().lower()
   if "%" in ds:
       discount_type = "Відсоток"
       found_nums = re.findall(r"([\d.,]+%)", ds)
       if not found_nums:
           found_nums = re.findall(r"([\d.,]+)", ds)
       total_val = 0.0
       for fn in found_nums:
           fn_clean = fn.replace("%","")
           val = validate_decimal(fn_clean)
           if val is not None:
               total_val += val
       return (discount_type, total_val if total_val else None)
   else:
       discount_type = "Фіксована"
       found_nums = re.findall(r"([\d.,]+)", ds)
       total_val = 0.0
       for fn in found_nums:
           val = validate_decimal(fn)
           if val is not None:
               total_val += val
       return (discount_type, total_val if total_val else None)

# -------------------------------------------------------
#   Робота з таблицями orders, order_details
# -------------------------------------------------------
def find_exact_order(
   cursor,
   client_id,
   order_date,
   order_status_id,
   payment_status_id,
   payment_status_text,
   delivery_method_id,
   delivery_status_id,
   tracking_number,
   deferred_until,
   priority_val,
   notes
):
   if tracking_number:
       cursor.execute("""
           SELECT id
             FROM orders
            WHERE tracking_number=%s
            LIMIT 1
       """, (tracking_number,))
       row = cursor.fetchone()
       if row:
           return row[0]

   cursor.execute("""
       SELECT id
         FROM orders
        WHERE client_id=%s
          AND coalesce(order_date,'1970-01-01'::date) = coalesce(%s::date,'1970-01-01'::date)
          AND coalesce(order_status_id,0) = coalesce(%s,0)
          AND coalesce(payment_status_id,0) = coalesce(%s,0)
          AND coalesce(payment_status,'') = coalesce(%s,'')
          AND coalesce(delivery_method_id,0) = coalesce(%s,0)
          AND coalesce(delivery_status_id,0) = coalesce(%s,0)
          AND (deferred_until IS NULL AND %s IS NULL OR deferred_until = %s::date)
          AND coalesce(priority,0) = coalesce(%s,0)
          AND coalesce(notes,'') = coalesce(%s,'')
        LIMIT 1
   """, (
       client_id,
       order_date,
       order_status_id,
       payment_status_id,
       payment_status_text or "",
       delivery_method_id,
       delivery_status_id,
       deferred_until,
       deferred_until,
       priority_val or 0,
       notes or ""
   ))
   row2 = cursor.fetchone()
   if row2:
       return row2[0]
   
   return None

def find_duplicate_order(cursor, client_id, product_numbers, order_date=None, payment_status_text=None, total_amount=None, ignore_unknown_check=False, exact_order_id=None):
    """
    Знаходить існуюче замовлення, яке може бути дублікатом на основі:
    1. Однаковий клієнт (client_id)
    2. Ті самі номери продуктів (product_numbers як список)
    3. Опційно - та сама дата замовлення (order_date)
    4. Опційно - той самий статус оплати (payment_status_text)
    5. Опційно - та сама сума (total_amount)
    
    Тепер також розпізнає замовлення, де "???" міг бути замінений на реальний номер.
    
    Параметри:
        ignore_unknown_check (bool): Якщо True, не пропускає перевірку для "Невідомого" клієнта
        exact_order_id (int): Якщо вказано, шукає лише по цьому order_id
    
    Повертає id замовлення, якщо знайдено дублікат, інакше None
    """
    if exact_order_id is not None:
        # Перевіряємо існування замовлення з вказаним ID
        cursor.execute("""
            SELECT o.id, client_id
            FROM orders o
            WHERE o.id = %s AND o.client_id = %s
        """, (exact_order_id, client_id))
        
        row = cursor.fetchone()
        if row:
            logger.info(f"Знайдено замовлення за точним ID={exact_order_id}")
            return exact_order_id
        else:
            logger.warning(f"Замовлення з ID={exact_order_id} не знайдено або належить іншому клієнту")
    
    if not client_id or not product_numbers:
        logger.debug(f"find_duplicate_order: Недостатньо даних для пошуку дублікатів (client_id={client_id}, product_numbers=None або порожній)")
        return None
    
    # Переконаємося, що product_numbers - це список рядків
    product_numbers = [str(p).strip() for p in product_numbers if p]
    if not product_numbers:
        logger.debug("find_duplicate_order: Після очищення список product_numbers порожній")
        return None
    
    # Сортуємо номери продуктів для стабільного порівняння
    product_numbers.sort()
    
    # Отримуємо ім'я клієнта для логів
    cursor.execute("SELECT first_name, last_name FROM clients WHERE id = %s", (client_id,))
    client_row = cursor.fetchone()
    client_name = f"{client_row[0] or ''} {client_row[1] or ''}".strip() if client_row else f"ID: {client_id}"
    
    logger.info(f"Пошук дублікатів замовлення для клієнта '{client_name}' з продуктами: {', '.join(product_numbers)}")
    
    # ВАЖЛИВА ЗМІНА: Перевірка на випадок із "Невідомим" клієнтом або відсутнім ім'ям
    # У цьому випадку, ми ЗАВЖДИ створюємо нове замовлення
    is_unknown_client = client_name.strip() == "Невідомий" or client_name.strip() == "ID: None" or not client_name.strip()
    if is_unknown_client and not ignore_unknown_check:
        logger.info(f"Клієнт '{client_name}' є Невідомим або без імені - створюємо нове замовлення")
        return None
    
    # 1. Спочатку шукаємо за точним збігом номерів продуктів
    # Будуємо SQL-запит, який враховує додаткові критерії, якщо вони надані
    base_query = """
        SELECT o.id, o.total_amount, o.order_date, o.payment_status as payment_status, COUNT(od.id) as product_count
        FROM orders o
        LEFT JOIN order_details od ON o.id = od.order_id
        WHERE o.client_id = %s
    """
    
    params = [client_id]
    
    # Додаємо умови для дати замовлення, якщо вказана
    if order_date:
        base_query += " AND o.order_date = %s"
        params.append(order_date)
    
    # Додаємо умови для статусу оплати, якщо вказаний
    if payment_status_text:
        base_query += " AND o.payment_status = %s"
        params.append(payment_status_text)
    
    # Додаємо GROUP BY після всіх умов WHERE
    base_query += """
        GROUP BY o.id, o.total_amount, o.order_date, o.payment_status
    """
    
    # Виконуємо запит для отримання всіх потенційних замовлень клієнта
    cursor.execute(base_query, params)
    potential_orders = cursor.fetchall()
    
    # Результати перевірки за типом дублікатів для логу
    found_dupes = {
        "exact_match": None,     # Точний збіг номерів продуктів
        "unknown_replaced": None # Замовлення, де "???" були замінені на реальні номери
    }
    
    if not potential_orders:
        logger.info(f"find_duplicate_order: Клієнт '{client_name}' не має жодних замовлень, що відповідають критеріям")
    else:
        logger.info(f"find_duplicate_order: Знайдено {len(potential_orders)} потенційних замовлень для клієнта '{client_name}'")
        
        # Для кожного замовлення перевіряємо продукти (спочатку шукаємо точні збіги)
        for order_id, order_total, order_date_val, payment_status_val, product_count in potential_orders:
            # Додаткова перевірка кількості продуктів перед порівнянням деталей
            if product_count != len(product_numbers):
                logger.info(f"find_duplicate_order: Замовлення ID={order_id} має {product_count} продуктів, але шукаємо {len(product_numbers)} продуктів")
                continue
                
            cursor.execute("""
                SELECT p.productnumber 
                FROM order_details od
                JOIN products p ON p.id = od.product_id
                WHERE od.order_id = %s
            """, (order_id,))
            
            order_products = [row[0] for row in cursor.fetchall()]
            if not order_products:
                logger.info(f"find_duplicate_order: Замовлення ID={order_id} не має продуктів")
                continue
            
            # Сортуємо номери продуктів замовлення для порівняння
            order_products.sort()
            
            logger.info(f"find_duplicate_order: Порівнюємо продукти для замовлення ID={order_id}: {', '.join(order_products)} з {', '.join(product_numbers)}")
            
            # Перевіряємо, чи збігаються номери продуктів
            products_match = set(product_numbers) == set(order_products)
            
            if products_match:
                logger.info(f"find_duplicate_order: Знайдено потенційний дублікат: замовлення ID={order_id} для клієнта '{client_name}' має такі самі продукти")
                
                # Якщо замовлення має ту саму дату, статус оплати та суму, це майже напевно дублікат
                date_match = True if not order_date else (order_date_val == order_date)
                status_match = True if not payment_status_text else (payment_status_val == payment_status_text)
                amount_match = True if total_amount is None else (abs(float(order_total or 0) - float(total_amount or 0)) < 0.01)
                
                if date_match and status_match and amount_match:
                    logger.info(f"find_duplicate_order: Замовлення ID={order_id} відповідає всім критеріям для дубліката")
                    found_dupes["exact_match"] = order_id
                    break  # Знайдений точний дублікат - завершуємо пошук
                else:
                    # Якщо якийсь із критеріїв не збігається, записуємо причину для налагодження
                    reasons = []
                    if not date_match:
                        reasons.append(f"різні дати ({order_date_val} vs {order_date})")
                    if not status_match:
                        reasons.append(f"різні статуси оплати ({payment_status_val} vs {payment_status_text})")
                    if not amount_match:
                        reasons.append(f"різні суми ({order_total} vs {total_amount})")
                        
                    logger.info(f"find_duplicate_order: Замовлення ID={order_id} має однакові продукти, але: {', '.join(reasons)}")
    
    # 2. Якщо точний збіг не знайдено, перевіряємо випадок із заміною "???" на реальний номер
    # Перевіряємо обидва випадки:
    # а) Коли в поточному замовленні є "???" і його треба порівняти з існуючими замовленнями з реальними номерами
    # б) Коли в поточному замовленні є реальні номери, а в базі є замовлення з "???"
    
    has_unknown = "???" in product_numbers
    
    if not found_dupes["exact_match"] and not is_unknown_client:  # Додаємо перевірку на Невідомого клієнта
        # Перевіряємо усі замовлення цього клієнта знову, але з іншим фокусом
        for order_id, order_total, order_date_val, payment_status_val, product_count in potential_orders:
            # Пропускаємо замовлення з різною кількістю продуктів, якщо різниця понад 2
            if abs(product_count - len(product_numbers)) > 2:
                continue
                
            cursor.execute("""
                SELECT p.productnumber, p.id, od.price
                FROM order_details od
                JOIN products p ON p.id = od.product_id
                WHERE od.order_id = %s
            """, (order_id,))
            
            order_product_details = cursor.fetchall()
            order_products = [row[0] for row in order_product_details]
            
            # Якщо у поточному замовленні є "???", а у знайденому замовленні всі реальні номери
            if has_unknown and all(p != "???" for p in order_products):
                # Перевіряємо чіткі збіги за іншими критеріями (дата, сума, статус)
                date_match = True if not order_date else (order_date_val == order_date)
                status_match = True if not payment_status_text else (payment_status_val == payment_status_text)
                amount_match = True if total_amount is None else (abs(float(order_total or 0) - float(total_amount or 0)) < 0.01)
                
                if date_match and status_match and amount_match:
                    logger.info(f"find_duplicate_order: Знайдено замовлення ID={order_id} з реальними номерами, яке відповідає замовленню з '???' за іншими критеріями")
                    found_dupes["unknown_replaced"] = order_id
                    break
            
            # Якщо у знайденому замовленні є "???", а у поточному всі реальні номери
            elif "???" in order_products and not has_unknown:
                # Перевіряємо чіткі збіги за іншими критеріями (дата, сума, статус)
                date_match = True if not order_date else (order_date_val == order_date)
                status_match = True if not payment_status_text else (payment_status_val == payment_status_text)
                amount_match = True if total_amount is None else (abs(float(order_total or 0) - float(total_amount or 0)) < 0.01)
                
                if date_match and status_match and amount_match:
                    logger.info(f"find_duplicate_order: Знайдено замовлення ID={order_id} з '???', яке відповідає замовленню з реальними номерами за іншими критеріями")
                    found_dupes["unknown_replaced"] = order_id
                    break
    
    # Пріоритет для повернення результату: точний збіг > заміна невідомих номерів
    if found_dupes["exact_match"]:
        logger.info(f"find_duplicate_order: Повертаю знайдений точний дублікат ID={found_dupes['exact_match']}")
        return found_dupes["exact_match"]
    elif found_dupes["unknown_replaced"]:
        logger.info(f"find_duplicate_order: Повертаю знайдений дублікат із заміненими невідомими номерами ID={found_dupes['unknown_replaced']}")
        return found_dupes["unknown_replaced"]
    
    logger.info(f"find_duplicate_order: Не знайдено дублікатів для клієнта '{client_name}' з продуктами: {', '.join(product_numbers)}")
    return None

def create_or_update_order_details(
   cursor,
   connection,
   order_id,
   product_id,
   price,
   discount_type,
   discount_value,
   additional_operation_name,
   additional_operation_value
):
   cursor.execute("""
       SELECT id, price, discount_type, discount_value,
              additional_operation, additional_operation_value, quantity
         FROM order_details
        WHERE order_id=%s AND product_id=%s
        LIMIT 1
   """,(order_id, product_id))
   existing = cursor.fetchone()
   if existing:
       detail_id, old_price, old_d_type, old_d_val, old_aop, old_aop_val, old_qty = existing
       new_price = price if (price is not None) else old_price
       new_d_type = discount_type if discount_type else old_d_type
       new_d_val = discount_value if discount_value else old_d_val
       new_aop = additional_operation_name if additional_operation_name else old_aop
       new_aop_val = additional_operation_value if additional_operation_value else old_aop_val
       new_qty = old_qty if old_qty else 1

       cursor.execute("""
           UPDATE order_details
              SET price=%s,
                  discount_type=%s,
                  discount_value=%s,
                  additional_operation=%s,
                  additional_operation_value=%s,
                  quantity=%s,
                  updated_at=now()
            WHERE id=%s
       """,(new_price, new_d_type, new_d_val, new_aop, new_aop_val, new_qty, detail_id))
       connection.commit()
   else:
       cursor.execute("""
           INSERT INTO order_details (
               order_id,
               product_id,
               quantity,
               price,
               discount_type,
               discount_value,
               additional_operation,
               additional_operation_value,
               created_at,
               updated_at
           )
           VALUES (
               %s,%s,1,
               %s,%s,%s,
               %s,%s,
               now(), now()
           )
       """,(order_id, product_id,
            price if price is not None else 0.0,
            discount_type, discount_value,
            additional_operation_name, additional_operation_value))
       connection.commit()

def recalc_order_total(cursor, connection, order_id, order_status_id):
   if not order_id:
       return
   if order_status_id in (7, 9):  # Подарунок / Повернення
       cursor.execute("""
           UPDATE orders
              SET total_amount=0,
                  updated_at=now()
            WHERE id=%s
       """,(order_id,))
       connection.commit()
       return

   cursor.execute("""
       SELECT COALESCE(SUM(
           CASE
               WHEN discount_type='Відсоток' THEN (price * quantity) * (1 - discount_value/100)
               WHEN discount_type='Фіксована' THEN (price * quantity) - discount_value
               ELSE (price * quantity)
           END
           + additional_operation_value
       ), 0)
       FROM order_details
       WHERE order_id=%s
   """,(order_id,))
   sm = cursor.fetchone()[0] or 0
   if sm<0:
       sm=0
   cursor.execute("""
       UPDATE orders
          SET total_amount=%s,
              updated_at=now()
        WHERE id=%s
   """,(sm, order_id))
   connection.commit()

def set_products_sold_if_paid(cursor, connection, order_id, payment_status_text):
   if not order_id or not payment_status_text:
       return
   if payment_status_text.strip().lower() == "оплачено":
       cursor.execute("""
           UPDATE products
              SET statusid=%s,
                  updated_at=now()
            WHERE id IN (
              SELECT product_id
                FROM order_details
               WHERE order_id=%s
            )
       """,(PRODUCT_STATUS_SOLD, order_id))
       connection.commit()

# -------------------------------------------------------
#   Створення / оновлення замовлення (orders)
# -------------------------------------------------------
def upsert_order(
   cursor,
   connection,
   client_id,
   order_date,
   order_status_id,
   payment_status_id,
   payment_status_text,
   delivery_method_id,
   delivery_status_id,
   tracking_number,
   deferred_until,
   priority_val,
   notes
):
   if not client_id:
       client_id = get_or_create_default_client(cursor, connection)
   if not order_date:
       order_date = datetime.now().date()

   payment_method_id_for_demo = None

   existing_id = find_exact_order(
       cursor,
       client_id,
       order_date,
       order_status_id,
       payment_status_id,
       payment_status_text,
       delivery_method_id,
       delivery_status_id,
       tracking_number,
       deferred_until,
       priority_val,
       notes
   )
   if existing_id:
       cursor.execute("""
           UPDATE orders
              SET client_id=%s,
                  order_date=%s,
                  order_status_id=%s,
                  payment_status_id=%s,
                  payment_status=%s,
                  delivery_method_id=%s,
                  delivery_status_id=%s,
                  tracking_number=%s,
                  deferred_until=%s,
                  priority=%s,
                  notes=%s,
                  payment_method_id=%s,
                  updated_at=now()
            WHERE id=%s
       """,(
           client_id,
           order_date,
           order_status_id,
           payment_status_id,
           payment_status_text or "Не оплачено",
           delivery_method_id,
           delivery_status_id,
           tracking_number,
           deferred_until,
           priority_val or 0,
           notes,
           payment_method_id_for_demo,
           existing_id
       ))
       connection.commit()
       return existing_id
   else:
       cursor.execute("""
           INSERT INTO orders (
               client_id,
               order_date,
               order_status_id,
               payment_status_id,
               payment_status,
               delivery_method_id,
               delivery_status_id,
               tracking_number,
               deferred_until,
               priority,
               notes,
               payment_method_id,
               total_amount,
               created_at,
               updated_at
           )
           VALUES (
               %s,%s,%s,
               %s,%s,
               %s,%s,
               %s,%s,%s,
               %s,%s,
               0,
               now(), now()
           )
           RETURNING id
       """,(
           client_id,
           order_date,
           order_status_id,
           payment_status_id,
           payment_status_text or "Не оплачено",
           delivery_method_id,
           delivery_status_id,
           tracking_number,
           deferred_until,
           priority_val or 0,
           notes,
           payment_method_id_for_demo
       ))
       new_id = cursor.fetchone()[0]
       connection.commit()
       return new_id

# -------------------------------------------------------
#   Обробка аркуша "Клієнти" (за потреби)
# -------------------------------------------------------
def process_clients_sheet_data(rows, sheet_name):
   conn = connect_to_db()
   if not conn:
       logger.error("Не вдалося підключитися до БД (processing 'Клієнти').")
       return
   
   # Ініціалізуємо таблиці відстеження
   init_tracking_tables(conn)
   
   cur = conn.cursor()

   for i, row in enumerate(rows[1:], start=2):
       if len(row) < 8:
           continue
       
       # Обчислюємо хеш рядка для перевірки змін
       row_hash = compute_row_hash(row)
       existing_hash_info = get_existing_row_hash(cur, sheet_name, i)
       
       # Пропускаємо рядок якщо хеш не змінився і він був успішно оброблений раніше
       if existing_hash_info and existing_hash_info['hash'] == row_hash and existing_hash_info['is_processed']:
           continue
       
       full_name = validate_text(row[0])
       phone     = validate_text(row[1], max_length=20)
       facebook  = validate_text(row[2], max_length=255)
       viber     = validate_text(row[3], max_length=255)
       telegram  = validate_text(row[4], max_length=255)
       instagram = validate_text(row[5], max_length=255)
       olx       = validate_text(row[6], max_length=255)
       email     = validate_text(row[7], max_length=255)

       if not full_name:
           update_row_hash(cur, conn, sheet_name, i, row_hash, None, False, "Відсутнє ім'я клієнта")
           continue
           
       try:
           client_id = get_or_create_client(cur, conn, full_name)
           if not client_id:
               update_row_hash(cur, conn, sheet_name, i, row_hash, full_name, False, "Не вдалося створити клієнта")
               continue

           cur.execute("""
               SELECT phone_number, facebook, viber, telegram, instagram, olx, email
                 FROM clients
                WHERE id=%s
           """,(client_id,))
           ex = cur.fetchone()
           if not ex:
               update_row_hash(cur, conn, sheet_name, i, row_hash, full_name, False, "Клієнт не знайдений після створення")
               continue
               
           ex_phone, ex_fb, ex_vb, ex_tg, ex_ig, ex_olx, ex_em = ex

           update_fields = []
           update_vals = []

           def maybe_update_phone(new_phone, old_phone):
               if new_phone and (not old_phone or not old_phone.strip()):
                   cur.execute("SELECT id FROM clients WHERE phone_number=%s",(new_phone,))
                   conf = cur.fetchone()
                   if conf and conf[0] != client_id:
                       logger.warning(
                           f"[{sheet_name} row={i}] Телефон {new_phone} вже зайнятий іншим (id={conf[0]})."
                       )
                       return
                   update_fields.append("phone_number=%s")
                   update_vals.append(new_phone)

           def maybe_update(field_name, new_val, old_val):
               if new_val and (not old_val or not old_val.strip()):
                   update_fields.append(f"{field_name}=%s")
                   update_vals.append(new_val)

           maybe_update_phone(phone, ex_phone)
           maybe_update("facebook", facebook, ex_fb)
           maybe_update("viber", viber, ex_vb)
           maybe_update("telegram", telegram, ex_tg)
           maybe_update("instagram", instagram, ex_ig)
           maybe_update("olx", olx, ex_olx)
           maybe_update("email", email, ex_em)

           if update_fields:
               sql_str = "UPDATE clients SET " + ", ".join(update_fields) + ", updated_at=now() WHERE id=%s"
               update_vals.append(client_id)
               cur.execute(sql_str, tuple(update_vals))
               conn.commit()
               
           # Оновлюємо хеш рядка після успішної обробки
           update_row_hash(cur, conn, sheet_name, i, row_hash, full_name, True)

       except Exception as e:
           logger.error(f"[{sheet_name}] Рядок {i} => Помилка: {e}")
           conn.rollback()
           update_row_hash(cur, conn, sheet_name, i, row_hash, full_name, False, str(e))
   
   # Оновлюємо прогрес обробки аркуша
   update_sheet_progress(cur, conn, sheet_name, len(rows))
   cur.close()
   conn.close()

# -------------------------------------------------------
#   Обробка замовлень (основна логіка)
# -------------------------------------------------------
def process_orders_sheet_data(rows, sheet_name, force_process=False):
    """
    Обробляє дані з аркуша Google Sheets і додає/оновлює замовлення в базі даних.
    
    Args:
        rows: Рядки даних з аркуша
        sheet_name: Назва аркуша
        force_process: Якщо True, обробляє всі рядки незалежно від хешу
    """
    global parsing_errors
    parsing_errors = []  # Очищаємо список помилок перед новим парсингом
    
    # Загальне підключення до БД для операцій з хешами рядків
    conn = connect_to_db_with_isolation(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
    if not conn:
        error_msg = "Не вдалося підключитися до БД (processing 'Замовлення')."
        logger.error(error_msg)
        parsing_errors.append({"sheet": sheet_name, "row": 0, "error": error_msg, "client": "Немає"})
        return parsing_errors
    
    # Ініціалізуємо таблиці відстеження
    init_tracking_tables(conn)
    
    # Лічильники для логування
    orders_skipped_duplicate = 0
    orders_added = 0
    orders_updated = 0
    rows_no_changes = 0
    rows_invalid = 0
    rows_errors = 0
    rows_processed = 0

    broadcast_date, broadcast_topic = parse_broadcast_sheet_name(sheet_name)
    logger.info(f"Обробляємо аркуш '{sheet_name}' з датою {broadcast_date} та темою '{broadcast_topic}'")

    # Оновлюємо статус парсингу
    update_parsing_status("current_sheet", sheet_name)
    update_parsing_status("total_rows", len(rows))
    update_parsing_status("processed_rows", 0)

    logger.info(f"[{sheet_name}] Всього рядків для обробки: {len(rows)}")
    
    # Перевірка, чи аркуш є новим (від 07.03.2025)
    is_new_sheet = False
    try:
        if broadcast_date:
            sheet_date = datetime.strptime(broadcast_date.strftime("%d.%m.%Y"), "%d.%m.%Y")
            cutoff_date = datetime(2025, 3, 7)  # 07.03.2025
            is_new_sheet = sheet_date >= cutoff_date
            if is_new_sheet:
                logger.info(f"[{sheet_name}] Це новий аркуш (після 07.03.2025). Всі рядки будуть оброблені примусово.")
    except Exception as e:
        logger.warning(f"Не вдалося порівняти дати для аркуша {sheet_name}: {e}")

    # Створюємо курсор для основного з'єднання
    cur = conn.cursor()

    # У workers.py рядки створюються з data[1:], тому індекс 0 у rows[] фактично є другим рядком в xlsx
    for i, row in enumerate(rows, start=1):
        # Оновлюємо статус обробки
        update_parsing_status("processed_rows", i)
        
        actual_row_index = i + 1  # Справжній індекс рядка в таблиці (з урахуванням заголовків)
        client_name = None  # Ініціалізуємо для коректної обробки помилок
        
        # Створюємо окреме підключення для кожного рядка
        # Це дозволяє іншим частинам програми отримувати доступ до даних поки ми оновлюємо
        transaction_conn = None
        transaction_cur = None
        
        try:
            if len(row) < 26:
                error_msg = f"[{sheet_name}] Рядок {actual_row_index}: мало колонок (очікувалось ~26, отримано {len(row)}). Пропуск."
                logger.warning(error_msg)
                parsing_errors.append({"sheet": sheet_name, "row": actual_row_index, "error": error_msg, "client": "Немає"})
                rows_invalid += 1
                continue

            # Перевіряємо, чи не пустий рядок
            row_has_data = False
            for cell in row:
                if cell:
                    row_has_data = True
                    break
            
            if not row_has_data:
                logger.debug(f"[{sheet_name}] Рядок {actual_row_index}: порожній рядок, пропускаємо")
                rows_invalid += 1
                continue

            # Обчислюємо хеш рядка
            row_hash = compute_row_hash(row)
            existing_hash_info = get_existing_row_hash(cur, sheet_name, actual_row_index)
            
            # Якщо це новий аркуш (від 07.03.2025) або увімкнено режим примусової обробки,
            # обробляємо рядок в будь-якому разі
            force_row_process = force_process or (is_new_sheet and actual_row_index == 2)
            
            if force_row_process and actual_row_index == 2:
                logger.info(f"[{sheet_name}] Примусово обробляємо рядок 2 (перший після заголовків)")
            
            # Якщо хеш не змінився і був успішно оброблений раніше, пропускаємо рядок, 
            # але тільки якщо не увімкнений форсований режим і це не примусово оброблюваний рядок
            if not force_row_process and existing_hash_info and existing_hash_info['hash'] == row_hash and existing_hash_info['is_processed']:
                logger.debug(f"[{sheet_name}] Рядок {actual_row_index}: хеш не змінився, пропускаємо")
                rows_no_changes += 1
                continue
            
            # Якщо хеш не змінився, але була помилка раніше і не увімкнений примусовий режим - пропускаємо
            if not force_row_process and existing_hash_info and existing_hash_info['hash'] == row_hash and not existing_hash_info['is_processed']:
                error_msg = f"[{sheet_name}] Рядок {actual_row_index}: хеш не змінився, але раніше була помилка: {existing_hash_info['error_message']}"
                logger.info(error_msg)
                parsing_errors.append({"sheet": sheet_name, "row": actual_row_index, "error": existing_hash_info['error_message'], "client": existing_hash_info.get('client_name', 'Немає')})
                rows_errors += 1
                continue

            # Тепер створюємо окреме підключення для транзакції обробки рядка
            transaction_conn = connect_to_db_with_isolation(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            if not transaction_conn:
                error_msg = f"[{sheet_name}] Не вдалося створити з'єднання для обробки рядка {actual_row_index}"
                logger.error(error_msg)
                parsing_errors.append({"sheet": sheet_name, "row": actual_row_index, "error": error_msg, "client": "Немає"})
                continue
                
            transaction_cur = transaction_conn.cursor()
            
            # Отримуємо дані з рядка
            raw_products        = validate_text(row[0])
            raw_clones          = validate_text(row[1])
            client_name         = validate_text(row[2])

            raw_prices          = validate_text(row[10])
            op_str              = validate_text(row[11])
            disc_str            = validate_text(row[12])

            raw_order_status    = validate_text(row[14])
            raw_payment_status  = validate_text(row[15])
            raw_delivery_method = validate_text(row[16])

            note_r              = validate_text(row[17])
            note_s              = validate_text(row[18])

            raw_delivery_status = validate_text(row[21])
            tracking_number     = validate_text(row[22])
            raw_deferred_until  = validate_text(row[24])
            raw_priority        = validate_text(row[25])

            # Логування важливих деталей рядка
            logger.info(f"[{sheet_name}] Рядок {actual_row_index}: Клієнт='{client_name}', " +
                        f"Продукти='{raw_products}', Клони='{raw_clones}', " +
                        f"Статус='{raw_order_status}', Оплата='{raw_payment_status}', " +
                        f"Доставка='{raw_delivery_method}', " +
                        f"Статус доставки='{raw_delivery_status}'")
            
            # Перевірка наявності продуктів
            if not raw_products and not raw_clones:
                error_msg = f"[{sheet_name}] Рядок {actual_row_index}: відсутні номери продуктів та клонів"
                logger.warning(error_msg)
                parsing_errors.append({"sheet": sheet_name, "row": actual_row_index, "error": error_msg, "client": client_name or "Немає"})
                rows_invalid += 1
                continue
                
            # Обробка клієнта (порожнє поле - це нормально, створюємо "Невідомий")
            if not client_name:
                logger.info(f"[{sheet_name}] Рядок {actual_row_index}: відсутнє ім'я клієнта, використовуємо клієнта за замовчуванням")
                client_id = get_or_create_default_client(transaction_cur, transaction_conn)
            else:
                client_id = get_or_create_client(transaction_cur, transaction_conn, client_name)
                if not client_id:
                    logger.warning(f"[{sheet_name}] Рядок {actual_row_index}: не вдалося створити клієнта '{client_name}', використовуємо клієнта за замовчуванням")
                    client_id = get_or_create_default_client(transaction_cur, transaction_conn)

            # Обробка дати відкладення
            deferred_until = parse_date_dd_mm_yyyy(raw_deferred_until)
            if deferred_until:
                logger.debug(f"[{sheet_name}] Рядок {actual_row_index}: замовлення відкладене до {deferred_until}")
                low_ps = (raw_payment_status or "").strip().lower()
                if low_ps in ("оплачено","доплатити"):
                    logger.debug(f"[{sheet_name}] Рядок {actual_row_index}: оплачене відкладене замовлення, встановлюємо метод доставки 'відкладено'")
                    raw_delivery_method = "відкладено"
                else:
                    if not raw_payment_status or not raw_payment_status.strip():
                        logger.debug(f"[{sheet_name}] Рядок {actual_row_index}: неоплачене відкладене замовлення, встановлюємо статус оплати 'Відкладено'")
                        raw_payment_status = "Відкладено"

            # Обробка статусів замовлення
            order_status_id = None
            if raw_order_status:
                st = raw_order_status.strip().lower()
                if st == "підтвердженно":
                    st = "підтверджено"
                order_status_id = ORDER_STATUS_MAP.get(st)
                if not order_status_id:
                    logger.warning(f"[{sheet_name}] Рядок {actual_row_index}: невідомий статус замовлення '{raw_order_status}'")

            payment_status_id = None
            payment_status_text = "Не оплачено"
            if raw_payment_status:
                ps = raw_payment_status.strip().lower()
                payment_status_id = PAYMENT_STATUS_MAP.get(ps)
                payment_status_text = raw_payment_status
                if not payment_status_id:
                    logger.warning(f"[{sheet_name}] Рядок {actual_row_index}: невідомий статус оплати '{raw_payment_status}'")

            delivery_method_id = None
            if raw_delivery_method:
                dm = raw_delivery_method.strip().lower()
                delivery_method_id = DELIVERY_METHOD_MAP.get(dm)
                if not delivery_method_id:
                    logger.warning(f"[{sheet_name}] Рядок {actual_row_index}: невідомий метод доставки '{raw_delivery_method}'")

            delivery_status_id = None
            if raw_delivery_status:
                ds = raw_delivery_status.strip().lower()
                delivery_status_id = DELIVERY_STATUS_MAP.get(ds)
                if not delivery_status_id:
                    logger.warning(f"[{sheet_name}] Рядок {actual_row_index}: невідомий статус доставки '{raw_delivery_status}'")

            # Додаткові примітки можуть містити ID замовлення
            notes = ""
            exact_order_id = None
            
            if note_r:
                notes += note_r
                # Перевіряємо, чи є в примітці ID замовлення
                order_id_match = re.search(r'OrderID[:=](\d+)', note_r)
                if order_id_match:
                    try:
                        exact_order_id = int(order_id_match.group(1))
                        logger.info(f"[{sheet_name}] Рядок {actual_row_index}: знайдено ID замовлення {exact_order_id} в примітці")
                    except ValueError:
                        logger.warning(f"[{sheet_name}] Рядок {actual_row_index}: некоректний ID замовлення в примітці")
                    
            if note_s:
                if notes:
                    notes += " | " + note_s
                else:
                    notes = note_s
                # Перевіряємо, чи є в примітці ID замовлення, якщо ще не знайдено
                if exact_order_id is None:
                    order_id_match = re.search(r'OrderID[:=](\d+)', note_s)
                    if order_id_match:
                        try:
                            exact_order_id = int(order_id_match.group(1))
                            logger.info(f"[{sheet_name}] Рядок {actual_row_index}: знайдено ID замовлення {exact_order_id} в примітці")
                        except ValueError:
                            logger.warning(f"[{sheet_name}] Рядок {actual_row_index}: некоректний ID замовлення в примітці")
            
            priority_val = validate_integer(raw_priority)
            order_date = broadcast_date

            # Розбір номерів продуктів та номерів-клонів
            product_numbers = []
            processed_clone_numbers = []  # Для зберігання оброблених номерів клонів
            
            # Обробка стандартних номерів продуктів
            if raw_products:
                split_prods = re.split(r"[;,]", raw_products)
                product_numbers = [p.strip() for p in split_prods if p.strip()]
            
            # Обробка номерів-клонів
            clones_list = []
            clone_originals = {}  # Зберігаємо оригінальні номери для клонів
            
            if raw_clones:
                # Розділяємо рядок клонів за комою або крапкою з комою
                cln = re.split(r"[;,]", raw_clones)
                
                for c in cln:
                    c = c.strip()
                    if not c:
                        continue
                    
                    # Перевіряємо формат "НОМЕР(ОРИГІНАЛ)" або "НОМЕР(???)"
                    clone_match = re.match(r"(.+?)\((.+?)\)", c)
                    
                    if clone_match:
                        clone_number = clone_match.group(1).strip()
                        original_number = clone_match.group(2).strip()
                        
                        # Зберігаємо клон та його оригінал
                        clones_list.append(clone_number)
                        clone_originals[clone_number] = original_number
                        
                        # Якщо оригінал "???", додаємо клон як основний номер
                        if original_number == "???" and not raw_products:
                            processed_clone_numbers.append(clone_number)
                        # Інакше додаємо клон до clonednumbers відповідного продукту
                        elif original_number != "???":
                            # Будемо додавати клон до оригінального продукту пізніше
                            pass
                    else:
                        # Якщо формат не відповідає "НОМЕР(ОРИГІНАЛ)", просто додаємо як клон
                        clones_list.append(c)
                        
                        # Якщо основних номерів немає, використовуємо клон як основний
                        if not raw_products:
                            processed_clone_numbers.append(c)
            
            # Об'єднуємо стандартні номери та номери-клони, що мають стати основними
            final_product_numbers = product_numbers + processed_clone_numbers
            
            # Якщо немає номерів продуктів і клонів, створюємо "???" номери на основі цін
            if not final_product_numbers:
                prices_text = raw_prices or ''
                if prices_text:
                    # Розбиваємо ціни по роздільнику і рахуємо їх
                    prices = [p.strip() for p in re.split('[,;]', prices_text) if p.strip()]
                    # Якщо є хоча б одна ціна, додаємо відповідну кількість "???" номерів
                    if prices:
                        final_product_numbers = ["???" for _ in prices]
                        logger.warning(f"[{sheet_name}] Рядок {actual_row_index}: відсутні номери продуктів та клонів, створюємо {len(prices)} товарів з номером '???' на основі цін")
                    else:
                        # Якщо немає цін, додаємо один "???"
                        final_product_numbers = ["???"]
                        logger.warning(f"[{sheet_name}] Рядок {actual_row_index}: відсутні номери продуктів та клонів і цін, створюємо один товар з номером '???'")
                else:
                    # Якщо немає цін, додаємо один "???"
                    final_product_numbers = ["???"]
                    logger.warning(f"[{sheet_name}] Рядок {actual_row_index}: відсутні номери продуктів, клонів і цін, створюємо один товар з номером '???'")
                
                issue_text = f"[{sheet_name}] Рядок {actual_row_index}: відсутні номери продуктів та клонів, використовуємо '???' ({len(final_product_numbers)} шт.)"
                parsing_errors.append({
                    'row_num': actual_row_index,
                    'sheet_name': sheet_name,
                    'client': client_name,
                    'issue': issue_text
                })
            
            logger.debug(f"[{sheet_name}] Рядок {actual_row_index}: Фінальні номери продуктів: {', '.join(final_product_numbers)}")
            
            # Перевірка на дублікати замовлень з урахуванням додаткових критеріїв
            duplicate_order_id = find_duplicate_order(
                transaction_cur, 
                client_id, 
                final_product_numbers, 
                order_date, 
                payment_status_text,
                ignore_unknown_check=True,  # Дозволяємо оновлювати замовлення для Невідомого клієнта
                exact_order_id=exact_order_id  # Якщо в примітці був ID замовлення, використовуємо його
            )
            
            if duplicate_order_id:
                logger.info(f"[{sheet_name}] Рядок {actual_row_index}: знайдено дублікат замовлення (ID={duplicate_order_id}) для клієнта {client_name} з продуктами {', '.join(final_product_numbers)}. Оновлюємо існуюче замовлення.")
                order_id = duplicate_order_id
                orders_skipped_duplicate += 1
                
                # Додаємо ID замовлення в примітки, якщо його там немає
                if not re.search(r'OrderID[:=](\d+)', notes):
                    notes = f"{notes} | OrderID={duplicate_order_id}".strip('| ')
                
                # Оновлюємо існуюче замовлення замість створення нового
                transaction_cur.execute("""
                    UPDATE orders
                      SET client_id=%s,
                          order_date=%s,
                          order_status_id=%s,
                          payment_status_id=%s,
                          payment_status=%s,
                          delivery_method_id=%s,
                          delivery_status_id=%s,
                          tracking_number=%s,
                          deferred_until=%s,
                          priority=%s,
                          notes=%s,
                          updated_at=now()
                    WHERE id=%s
                """,(
                    client_id,
                    order_date,
                    order_status_id,
                    payment_status_id,
                    payment_status_text or "Не оплачено",
                    delivery_method_id,
                    delivery_status_id,
                    tracking_number,
                    deferred_until,
                    priority_val or 0,
                    notes,
                    duplicate_order_id
                ))
                
                # Видаляємо старі деталі замовлення, щоб замінити їх на нові
                transaction_cur.execute("""
                    DELETE FROM order_details
                    WHERE order_id = %s
                """, (duplicate_order_id,))
                
                transaction_conn.commit()
                orders_updated += 1
                logger.debug(f"[{sheet_name}] Рядок {actual_row_index}: оновлено існуюче замовлення ID={order_id} та видалено старі деталі для повного оновлення")
            else:
                # Створюємо нове замовлення
                logger.debug(f"[{sheet_name}] Рядок {actual_row_index}: створюємо нове замовлення")
                order_id = upsert_order(
                    transaction_cur,
                    transaction_conn,
                    client_id,
                    order_date,
                    order_status_id,
                    payment_status_id,
                    payment_status_text,
                    delivery_method_id,
                    delivery_status_id,
                    tracking_number,
                    deferred_until,
                    priority_val,
                    notes
                )
                if order_id:
                    # Додаємо ID замовлення в примітки і оновлюємо замовлення
                    if not re.search(r'OrderID[:=](\d+)', notes):
                        notes = f"{notes} | OrderID={order_id}".strip('| ')
                        transaction_cur.execute("""
                            UPDATE orders
                              SET notes=%s
                            WHERE id=%s
                        """, (notes, order_id))
                        transaction_conn.commit()
                    
                    orders_added += 1
                    logger.info(f"[{sheet_name}] Рядок {actual_row_index}: створено нове замовлення ID={order_id}")
                else:
                    error_msg = "Не вдалося створити нове замовлення"
                    logger.error(f"[{sheet_name}] Рядок {actual_row_index}: {error_msg}")
                    update_row_hash(cur, conn, sheet_name, actual_row_index, row_hash, client_name, False, error_msg)
                    if transaction_conn:
                        transaction_conn.close()
                    continue
            
            if not order_id:
                error_msg = "Не отримано ID замовлення, пропускаємо обробку деталей"
                logger.error(f"[{sheet_name}] Рядок {actual_row_index}: {error_msg}")
                update_row_hash(cur, conn, sheet_name, actual_row_index, row_hash, client_name, False, error_msg)
                if transaction_conn:
                    transaction_conn.close()
                continue

            # Обробка цін
            price_values = []
            if raw_prices:
                split_prices = re.split(r"[;,]", raw_prices)
                price_values = [x.strip() for x in split_prices]

            # Обробка знижок і додаткових операцій
            addop_name, addop_val = parse_additional_operation(op_str)
            discount_type, discount_value = parse_discount_str(disc_str)
            used_addop = False
            used_discount = False

            # Обробка товарів замовлення
            for idx3, pnum in enumerate(final_product_numbers):
                try:
                    product_id = get_or_create_product(transaction_cur, transaction_conn, pnum)

                    # Обробка клонів для цього продукту
                    if pnum in clone_originals:
                        # Якщо це клон з оригіналом "???", він вже був доданий як основний номер
                        if clone_originals[pnum] == "???":
                            pass
                        # Інакше знаходимо оригінальний продукт і додаємо до нього клон
                        else:
                            original_product_number = clone_originals[pnum]
                            try:
                                transaction_cur.execute("""
                                    SELECT id FROM products 
                                    WHERE productnumber = %s
                                """, (original_product_number,))
                                original_product_row = transaction_cur.fetchone()
                                
                                if original_product_row:
                                    original_product_id = original_product_row[0]
                                    # Додаємо клон до оригінального продукту
                                    append_clonednumbers(transaction_cur, transaction_conn, original_product_id, [pnum])
                                    logger.debug(f"[{sheet_name}] Рядок {actual_row_index}: додано клон {pnum} до продукту {original_product_number}")
                            except Exception as clone_error:
                                logger.warning(f"[{sheet_name}] Рядок {actual_row_index}: не вдалося додати клон {pnum} до продукту {original_product_number}: {clone_error}")
                    
                    # Додаємо клони для всіх продуктів (якщо є)
                    if idx3 < len(product_numbers) and clones_list:
                        clones_to_add = []
                        for clone in clones_list:
                            # Додаємо клони до продукту, якщо вони не є самостійними продуктами
                            if clone not in processed_clone_numbers:
                                clones_to_add.append(clone)
                        
                        if clones_to_add:
                            append_clonednumbers(transaction_cur, transaction_conn, product_id, clones_to_add)
                            logger.debug(f"[{sheet_name}] Рядок {actual_row_index}: додано клони {', '.join(clones_to_add)} до продукту {pnum}")

                    # Обробка цін
                    this_price = None
                    if idx3 < len(price_values):
                        pr = validate_decimal(price_values[idx3])
                        if pr is not None:
                            this_price = pr

                    if this_price is not None:
                        update_product_price(transaction_cur, transaction_conn, product_id, this_price)

                    # Застосування знижки і додаткових операцій
                    aop_name = None
                    aop_val = 0.0
                    d_type = None
                    d_val = None
                    if not used_addop and addop_name:
                        aop_name = addop_name
                        aop_val = addop_val
                        used_addop = True
                    if not used_discount and discount_type:
                        d_type = discount_type
                        d_val = discount_value
                        used_discount = True

                    # Додавання деталей замовлення
                    create_or_update_order_details(
                        transaction_cur,
                        transaction_conn,
                        order_id,
                        product_id,
                        this_price,
                        d_type,
                        d_val,
                        aop_name,
                        aop_val
                    )
                    logger.debug(f"[{sheet_name}] Рядок {actual_row_index}: додано/оновлено деталь замовлення для продукту {pnum}")
                except Exception as product_error:
                    logger.error(f"[{sheet_name}] Рядок {actual_row_index}: помилка при обробці продукту {pnum}: {product_error}")
                    # Продовжуємо з наступним продуктом, але не робимо rollback всієї транзакції

            # Перерахунок загальної суми замовлення
            recalc_order_total(transaction_cur, transaction_conn, order_id, order_status_id)
            
            # Позначення товарів як проданих, якщо замовлення оплачене
            set_products_sold_if_paid(transaction_cur, transaction_conn, order_id, payment_status_text)
            
            # Оновлюємо хеш рядка як успішно оброблений
            update_row_hash(cur, conn, sheet_name, actual_row_index, row_hash, client_name, True)
            logger.debug(f"[{sheet_name}] Рядок {actual_row_index}: оновлено хеш рядка")
            transaction_conn.commit()
            
            # Інкрементуємо лічильник успішно оброблених рядків
            rows_processed += 1
            
            # Даємо можливість інтерфейсу оновитися, вивільняючи процесор
            if i % 10 == 0:  # Кожні 10 рядків
                time.sleep(0.01)  # Маленька пауза для роботи інтерфейсу

        except Exception as e:
            try:
                if transaction_conn:
                    transaction_conn.rollback()
            except:
                pass
                
            error_msg = f"[{sheet_name}] Рядок {actual_row_index} => Помилка: {e}"
            logger.error(error_msg)
            import traceback
            logger.error(traceback.format_exc())
            
            # Додаємо помилку до списку для відображення в UI
            parsing_errors.append({"sheet": sheet_name, "row": actual_row_index, "error": str(e), "client": client_name or "Немає"})
            
            # Зберігаємо інформацію про помилку в хеш-таблиці, але позначаємо як не оброблений
            update_row_hash(cur, conn, sheet_name, actual_row_index, row_hash, client_name, False, str(e))
            rows_errors += 1
        finally:
            # Закриваємо транзакційне з'єднання для цього рядка
            if transaction_conn:
                transaction_conn.close()
    
    # Оновлюємо прогрес обробки аркуша
    update_sheet_progress(cur, conn, sheet_name, len(rows))
    
    logger.info(f"""
[{sheet_name}] Результати парсингу:
    Оброблено рядків: {rows_processed}
    Пропущено порожніх/неповних рядків: {rows_invalid}
    Пропущено рядків (немає змін): {rows_no_changes}
    Пропущено дублікатів замовлень: {orders_skipped_duplicate}
    Додано нових замовлень: {orders_added}
    Оновлено існуючих замовлень: {orders_updated}
    Помилок при обробці: {rows_errors}
""")
    cur.close()
    conn.close()
    
    # Оновлюємо статус парсингу
    update_parsing_status("processed_sheets", parsing_status["processed_sheets"] + 1)
    update_parsing_status("orders_processed", parsing_status["orders_processed"] + orders_added)
    update_parsing_status("orders_updated", parsing_status["orders_updated"] + orders_updated)
    update_parsing_status("errors", parsing_status["errors"] + rows_errors)
    
    # Повертаємо список помилок для відображення в UI
    return parsing_errors

# -------------------------------------------------------
#   Видалення дубльованих замовлень
# -------------------------------------------------------
def remove_redundant_order_duplicates():
   """
   Знаходить і видаляє дублікати замовлень за такими критеріями:
   1. Дублі в order_details (однакові (order_id, product_id))
   2. Замовлення з однаковими клієнтами та однаковими наборами продуктів
   3. Замовлення з однаковими клієнтами, продуктами та сумами
   """
   conn = connect_to_db()
   if not conn:
       logger.error("Не вдалося підключитися для видалення дублювань у замовленнях")
       return
   cur = conn.cursor()
   try:
       # 1. Видалення дублікатів у order_details
       logger.info("Видаляємо дублікати у order_details (за (order_id, product_id))...")
       cur.execute("""
           WITH duplicates AS (
               SELECT
                 id,
                 order_id,
                 product_id,
                 ROW_NUMBER() OVER(PARTITION BY order_id, product_id ORDER BY id) AS rn
               FROM order_details
           )
           DELETE FROM order_details
           WHERE id IN (
               SELECT id
               FROM duplicates
               WHERE rn>1
           )
           RETURNING id, order_id, product_id
       """)
       deleted_details = cur.fetchall()
       dd_count = len(deleted_details)
       logger.info(f"Видалено дублів у order_details: {dd_count}")
       
       if dd_count > 0:
           for detail_id, order_id, product_id in deleted_details:
               logger.debug(f"Видалено дубль деталі ID={detail_id} (замовлення={order_id}, продукт={product_id})")
       
       # 2. Знаходимо замовлення, які можуть бути дублікатами
       logger.info("Шукаємо замовлення з однаковими клієнтами та продуктами...")
       
       # Отримуємо список усіх клієнтів
       cur.execute("SELECT id, first_name, last_name FROM clients")
       clients = cur.fetchall()
       logger.debug(f"Знайдено {len(clients)} клієнтів для перевірки дублікатів замовлень")
       
       duplicates_deleted = 0
       
       for client_row in clients:
           client_id = client_row[0]
           client_name = f"{client_row[1] or ''} {client_row[2] or ''}".strip() if client_row else f"ID: {client_id}"
           
           # Отримуємо всі замовлення цього клієнта
           cur.execute("""
               SELECT o.id, o.total_amount, o.created_at, o.payment_status, 
                      o.order_status_id, o.delivery_method_id, o.order_date
               FROM orders o
               WHERE o.client_id = %s
               ORDER BY o.created_at DESC
           """, (client_id,))
           
           client_orders = cur.fetchall()
           if len(client_orders) <= 1:
               continue
               
           logger.debug(f"Клієнт '{client_name}' має {len(client_orders)} замовлень, перевіряємо на дублікати")
               
           # Для кожного замовлення отримаємо список продуктів
           orders_with_products = []
           for order_id, total, created_at, payment_status, order_status_id, delivery_method_id, order_date in client_orders:
               cur.execute("""
                   SELECT p.productnumber, p.id
                   FROM order_details od
                   JOIN products p ON p.id = od.product_id
                   WHERE od.order_id = %s
               """, (order_id,))
               
               order_products = cur.fetchall()
               if not order_products:
                   logger.debug(f"Замовлення ID={order_id} клієнта '{client_name}' не має продуктів, пропускаємо")
                   continue
                   
               # Розділяємо номери продуктів та їх ID
               product_numbers = [row[0] for row in order_products]
               product_ids = [row[1] for row in order_products]
               
               # Перевіряємо статус оплати
               is_paid = payment_status.lower() == "оплачено" if payment_status else False
               
               # Додаємо інформацію про статус для логування
               status_info = ""
               if order_status_id:
                   cur.execute("SELECT status_name FROM order_statuses WHERE id = %s", (order_status_id,))
                   status_row = cur.fetchone()
                   if status_row:
                       status_info = status_row[0]
               
               # Додаємо інформацію про метод доставки для логування
               delivery_info = ""
               if delivery_method_id:
                   cur.execute("SELECT method_name FROM delivery_methods WHERE id = %s", (delivery_method_id,))
                   delivery_row = cur.fetchone()
                   if delivery_row:
                       delivery_info = delivery_row[0]
               
               orders_with_products.append((
                   order_id, 
                   product_numbers, 
                   product_ids, 
                   total, 
                   created_at, 
                   is_paid, 
                   status_info, 
                   delivery_info,
                   order_date
               ))
               
               logger.debug(f"Замовлення ID={order_id} клієнта '{client_name}': {len(product_numbers)} продуктів, " +
                           f"оплачено: {is_paid}, статус: {status_info}, доставка: {delivery_info}, " +
                           f"дата: {order_date}, створено: {created_at}, сума: {total}")
           
           # Групуємо замовлення за однаковими наборами продуктів, статусом оплати І датою замовлення
           product_groups = {}
           for order_id, product_numbers, product_ids, total, created_at, is_paid, status_info, delivery_info, order_date in orders_with_products:
               key = (tuple(sorted(product_numbers)), is_paid, order_date)
               if key not in product_groups:
                   product_groups[key] = []
               product_groups[key].append((order_id, product_ids, total, created_at, is_paid, status_info, delivery_info, order_date))
           
           # Для кожної групи з однаковими продуктами, залишаємо найновіше замовлення
           for product_key, order_group in product_groups.items():
               product_numbers, is_paid_key, order_date_key = product_key
               
               if len(order_group) <= 1:
                   continue
                   
               logger.info(f"Знайдено {len(order_group)} замовлень з однаковими продуктами для клієнта '{client_name}', " +
                          f"оплачено: {is_paid_key}, дата: {order_date_key}, продукти: {', '.join(product_numbers)}")
                   
               # Сортуємо за датою (найновіші спочатку)
               order_group.sort(key=lambda x: -int(x[3].timestamp() if x[3] else 0))
               
               # Залишаємо найновіше замовлення, видаляємо всі інші
               keep_order_id, keep_product_ids, keep_total, keep_created_at, is_paid, keep_status, keep_delivery, keep_date = order_group[0]
               
               logger.info(f"Залишаємо замовлення ID={keep_order_id} (оплачено: {is_paid}, статус: {keep_status}, " + 
                          f"доставка: {keep_delivery}, дата: {keep_date}, створено: {keep_created_at}, сума: {keep_total})")
               
               for order_id, product_ids, total, created_at, order_is_paid, status, delivery, order_date in order_group[1:]:
                   try:
                       logger.warning(f"Видаляємо дублікат замовлення ID={order_id} (оплачено: {order_is_paid}, статус: {status}, " +
                                     f"доставка: {delivery}, дата: {order_date}, створено: {created_at}, сума: {total})")
                       
                       # Перевіряємо статус продуктів, які видаляємо
                       # Якщо залишене замовлення оплачене, статус продуктів має бути "Продано"
                       if is_paid:
                           for prod_id in product_ids:
                               cur.execute("""
                                   UPDATE products 
                                   SET statusid = %s, updated_at = now()
                                   WHERE id = %s
                               """, (PRODUCT_STATUS_SOLD, prod_id))
                               logger.debug(f"Встановлено статус 'Продано' для продукту ID={prod_id} у видаленому замовленні")
                       
                       # Спочатку видаляємо деталі замовлення
                       cur.execute("DELETE FROM order_details WHERE order_id = %s RETURNING id, product_id", (order_id,))
                       deleted_details = cur.fetchall()
                       details_deleted = len(deleted_details)
                       
                       for det_id, prod_id in deleted_details:
                           logger.debug(f"Видалено деталь ID={det_id} (продукт={prod_id}) замовлення ID={order_id}")
                       
                       # Потім видаляємо саме замовлення
                       cur.execute("DELETE FROM orders WHERE id = %s", (order_id,))
                       order_deleted = cur.rowcount
                       
                       if details_deleted > 0 or order_deleted > 0:
                           duplicates_deleted += 1
                           logger.info(f"Видалено дублікат замовлення ID={order_id}, залишено ID={keep_order_id}")
                   except Exception as delete_err:
                       logger.error(f"Помилка при видаленні дубліката ID={order_id}: {delete_err}")
                       conn.rollback()
                       # Продовжуємо з наступним замовленням
       
       if duplicates_deleted > 0:
           logger.info(f"Видалено дублікатів замовлень: {duplicates_deleted}")
       else:
           logger.info("Дублікатів замовлень не знайдено")
           
       conn.commit()

   except Exception as e:
       logger.error(f"Помилка remove_redundant_order_duplicates(): {e}")
       conn.rollback()
   finally:
       cur.close()
       conn.close()

# -------------------------------------------------------
#   Отримання рядків з помилками для повторної обробки
# -------------------------------------------------------
def list_failed_rows():
    """
    Виводить список рядків з помилками по кожному аркушу.
    """
    conn = connect_to_db()
    if not conn:
        logger.error("Не вдалося підключитися до БД для отримання списку помилок.")
        return
        
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT sheet_name, row_index, client_name, error_message, row_hash
            FROM row_hashes
            WHERE is_processed = FALSE AND error_message IS NOT NULL
            ORDER BY sheet_name, row_index
        """)
        
        failed_rows = cur.fetchall()
        if not failed_rows:
            logger.info("Немає рядків з помилками для повторної обробки.")
            return []
        
        logger.info(f"Знайдено {len(failed_rows)} рядків з помилками для повторної обробки:")
        
        # Групуємо помилки за аркушами
        by_sheet = {}
        for sheet_name, row_index, client_name, error_message, row_hash in failed_rows:
            if sheet_name not in by_sheet:
                by_sheet[sheet_name] = []
            by_sheet[sheet_name].append({
                'sheet_name': sheet_name,
                'row_index': row_index,
                'client_name': client_name,
                'error_message': error_message,
                'row_hash': row_hash
            })
        
        # Виводимо інформацію по кожному аркушу
        for sheet_name, rows in by_sheet.items():
            logger.info(f"Аркуш '{sheet_name}': {len(rows)} рядків з помилками")
            for row in rows:
                logger.info(f"  - Рядок {row['row_index']}: {row['client_name'] or 'Невідомий клієнт'}, Помилка: {row['error_message']}")
                
        return failed_rows
        
    except Exception as e:
        logger.error(f"Помилка при отриманні списку рядків з помилками: {e}")
        return []
    finally:
        cur.close()
        conn.close()

# -------------------------------------------------------
#   Повторна обробка рядків з помилками
# -------------------------------------------------------
def retry_failed_rows(sheet_name=None):
    """
    Спроба повторної обробки рядків з помилками.
    Якщо sheet_name вказано, обробляє тільки рядки з цього аркуша.
    """
    conn = connect_to_db()
    if not conn:
        logger.error("Не вдалося підключитися до БД для повторної обробки рядків з помилками.")
        return
        
    cur = conn.cursor()
    try:
        # Отримуємо список аркушів з помилками
        if sheet_name:
            cur.execute("""
                SELECT DISTINCT sheet_name
                FROM row_hashes
                WHERE is_processed = FALSE AND error_message IS NOT NULL AND sheet_name = %s
            """, (sheet_name,))
        else:
            cur.execute("""
                SELECT DISTINCT sheet_name
                FROM row_hashes
                WHERE is_processed = FALSE AND error_message IS NOT NULL
            """)
            
        sheets_with_errors = [row[0] for row in cur.fetchall()]
        
        if not sheets_with_errors:
            logger.info(f"Немає аркушів з помилками{' для ' + sheet_name if sheet_name else ''}.")
            return
            
        # Підключаємося до Google Sheets
        client = get_google_sheet_client()
        if not client:
            logger.error("Не вдалося отримати клієнт Google Sheets.")
            return
            
        # Відкриваємо документ
        try:
            doc = client.open(SPREADSHEET_NAME)
        except Exception as e:
            logger.error(f"Помилка відкриття Google Sheets: {e}")
            return
            
        # Для кожного аркуша з помилками
        for sheet_name in sheets_with_errors:
            try:
                # Знаходимо аркуш
                worksheet = None
                for ws in doc.worksheets():
                    if ws.title.strip() == sheet_name:
                        worksheet = ws
                        break
                        
                if not worksheet:
                    logger.error(f"Аркуш '{sheet_name}' не знайдено в документі.")
                    continue
                    
                # Отримуємо дані аркуша
                data = worksheet.get_all_values()
                if not data:
                    logger.error(f"Аркуш '{sheet_name}' порожній.")
                    continue
                    
                # Отримуємо список рядків з помилками для цього аркуша
                cur.execute("""
                    SELECT row_index
                    FROM row_hashes
                    WHERE is_processed = FALSE AND error_message IS NOT NULL AND sheet_name = %s
                    ORDER BY row_index
                """, (sheet_name,))
                
                error_row_indices = [row[0] for row in cur.fetchall()]
                
                if not error_row_indices:
                    logger.info(f"Немає рядків з помилками для аркуша '{sheet_name}'.")
                    continue
                    
                logger.info(f"Повторна обробка {len(error_row_indices)} рядків з помилками для аркуша '{sheet_name}'...")
                
                # Створюємо дані для повторної обробки тільки з рядками, що мали помилки
                error_rows = [data[0]]  # Заголовок
                for row_index in error_row_indices:
                    if row_index < len(data):
                        error_rows.append(data[row_index])
                
                # Обробляємо тільки рядки з помилками, включаючи заголовок
                process_orders_sheet_data(error_rows, sheet_name, force_process=True)
                
            except Exception as e:
                logger.error(f"Помилка при повторній обробці аркуша '{sheet_name}': {e}")
                
    except Exception as e:
        logger.error(f"Помилка при повторній обробці рядків з помилками: {e}")
    finally:
        cur.close()
        conn.close()

# -------------------------------------------------------
#   Імпорт головний
# -------------------------------------------------------
def import_data(sheet_links, force_process=False):
    """
    Оновлена функція для імпорту даних з Google Sheets
    :param sheet_links: список посилань на Google Sheets
    :param force_process: якщо True, примусово обробляємо всі замовлення, незважаючи на хеш
    :return: tuple з к-стю оброблених замовлень, продуктів і моніторингу
    """
    orders_processed = 0
    orders_skipped = 0
    orders_updated = 0
    products_added = 0
    tracking_added = 0
    
    all_parsing_errors = []  # Збираємо всі помилки парсингу з усіх аркушів
    
    start_time = datetime.now()
    logger.info(f"Починаємо імпорт даних з Google Sheets ({len(sheet_links)} файлів)")
    
    # Підрахунок загальної кількості рядків у всіх файлах для відображення прогресу
    try:
        total_rows_count = 0
        total_sheets_count = 0
        
        for sheet_url in sheet_links:
            try:
                # Отримуємо дані з Google Sheets
                gc = get_google_sheet_client()
                if not gc:
                    error_msg = f"Не вдалося отримати клієнт Google Sheets для {sheet_url}"
                    logger.error(error_msg)
                    all_parsing_errors.append({
                        'row_num': 'N/A',
                        'sheet_name': 'N/A',
                        'client': 'N/A',
                        'issue': error_msg
                    })
                    continue
                    
                try:
                    sheet = gc.open_by_url(sheet_url)
                    logger.info(f"Відкрито файл: {sheet.title}")
                except Exception as e:
                    error_msg = f"Помилка при відкритті файлу {sheet_url}: {str(e)}"
                    logger.error(error_msg)
                    all_parsing_errors.append({
                        'row_num': 'N/A',
                        'sheet_name': 'N/A',
                        'client': 'N/A',
                        'issue': error_msg
                    })
                    continue
                    
                # Отримуємо список аркушів
                try:
                    worksheets = sheet.worksheets()
                    worksheet_names = [ws.title for ws in worksheets]
                    logger.info(f"Знайдено аркуші: {', '.join(worksheet_names)}")
                    
                    # Підраховуємо кількість рядків в усіх аркушах
                    for ws in worksheets:
                        if ws.title in ["товары", "traking", "трекинг", "history"]:
                            continue  # Пропускаємо службові аркуші
                        
                        # Підраховуємо кількість рядків у кожному аркуші
                        rows_count = len(ws.get_all_values())
                        if rows_count > 0:
                            total_rows_count += rows_count
                            total_sheets_count += 1
                    
                except Exception as e:
                    error_msg = f"Помилка при отриманні списку аркушів з {sheet_url}: {str(e)}"
                    logger.error(error_msg)
                    all_parsing_errors.append({
                        'row_num': 'N/A',
                        'sheet_name': 'N/A',
                        'client': 'N/A',
                        'issue': error_msg
                    })
                    continue
                    
            except Exception as e:
                error_msg = f"Помилка при підрахунку рядків в {sheet_url}: {str(e)}"
                logger.error(error_msg)
        
        # Оновлюємо глобальний статус парсингу
        update_parsing_status("total_sheets", total_sheets_count)
        update_parsing_status("total_rows", total_rows_count)
        logger.info(f"Загальна кількість аркушів для обробки: {total_sheets_count}, загальна кількість рядків: {total_rows_count}")
        
    except Exception as count_error:
        logger.error(f"Помилка при підрахунку загальної кількості рядків: {count_error}")
    
    # Обробка файлів за допомогою короткострокових транзакцій
    for sheet_url in sheet_links:
        try:
            logger.info(f"Обробка файлу: {sheet_url}")
            
            # Отримуємо дані з Google Sheets
            gc = get_google_sheet_client()
            if not gc:
                error_msg = f"Не вдалося отримати клієнт Google Sheets для {sheet_url}"
                logger.error(error_msg)
                all_parsing_errors.append({
                    'row_num': 'N/A',
                    'sheet_name': 'N/A',
                    'client': 'N/A',
                    'issue': error_msg
                })
                continue
                
            try:
                sheet = gc.open_by_url(sheet_url)
                logger.info(f"Відкрито файл: {sheet.title}")
            except Exception as e:
                error_msg = f"Помилка при відкритті файлу {sheet_url}: {str(e)}"
                logger.error(error_msg)
                all_parsing_errors.append({
                    'row_num': 'N/A',
                    'sheet_name': 'N/A',
                    'client': 'N/A',
                    'issue': error_msg
                })
                continue
                
            # Отримуємо список аркушів
            try:
                # Сортуємо аркуші за датою
                worksheets = sort_worksheets_by_date(sheet.worksheets())
                worksheet_names = [ws.title for ws in worksheets]
                logger.info(f"Знайдено аркуші: {', '.join(worksheet_names)}")
            except Exception as e:
                error_msg = f"Помилка при отриманні списку аркушів з {sheet_url}: {str(e)}"
                logger.error(error_msg)
                all_parsing_errors.append({
                    'row_num': 'N/A',
                    'sheet_name': 'N/A',
                    'client': 'N/A',
                    'issue': error_msg
                })
                continue
            
            # Обробляємо дані з аркушів
            for worksheet in worksheets:
                worksheet_name = worksheet.title
                if worksheet_name in ["товары", "traking", "трекинг", "history"]:
                    continue  # Пропускаємо службові аркуші
                    
                try:
                    logger.info(f"Обробка аркуша: {worksheet_name}")
                    
                    # Отримуємо дані аркуша
                    try:
                        # Використовуємо get_all_values замість get_all_records для кращої сумісності
                        all_values = worksheet.get_all_values()
                        logger.info(f"Отримано {len(all_values)} рядків з аркуша {worksheet_name}")
                        
                        if len(all_values) <= 1:  # Тільки заголовок або порожній аркуш
                            logger.warning(f"Аркуш {worksheet_name} порожній або містить лише заголовки")
                            continue
                    except Exception as e:
                        error_msg = f"Помилка при отриманні даних з аркуша {worksheet_name}: {str(e)}"
                        logger.error(error_msg)
                        all_parsing_errors.append({
                            'row_num': 'N/A',
                            'sheet_name': worksheet_name,
                            'client': 'N/A',
                            'issue': error_msg
                        })
                        continue
                    
                    # Обробляємо дані аркуша
                    sheet_errors = process_orders_sheet_data(all_values[1:], worksheet_name, force_process)
                    
                    # Додаємо помилки з цього аркуша до загального списку
                    if sheet_errors:
                        all_parsing_errors.extend(sheet_errors)
                        
                except Exception as e:
                    error_msg = f"Помилка при обробці аркуша {worksheet_name}: {str(e)}"
                    logger.error(error_msg)
                    import traceback
                    logger.error(traceback.format_exc())
                    all_parsing_errors.append({
                        'row_num': 'N/A',
                        'sheet_name': worksheet_name,
                        'client': 'N/A',
                        'issue': error_msg
                    })
            
            # Якщо у файлі є аркуш трекінгу, обробляємо його
            tracking_worksheet_names = [ws.title for ws in worksheets if ws.title in ["traking", "трекинг"]]
            if tracking_worksheet_names:
                tracking_worksheet_name = tracking_worksheet_names[0]
                try:
                    tracking_worksheet = sheet.worksheet(tracking_worksheet_name)
                    tracking_rows = tracking_worksheet.get_all_values()
                    logger.info(f"Отримано {len(tracking_rows)} рядків з аркуша трекінгу {tracking_worksheet_name}")
                    
                    # Тут можна додати функцію для обробки даних трекінгу
                    # tracking_added += process_tracking_data(tracking_rows, tracking_worksheet_name)
                except Exception as e:
                    error_msg = f"Помилка при обробці аркуша трекінгу {tracking_worksheet_name}: {str(e)}"
                    logger.error(error_msg)
                    all_parsing_errors.append({
                        'row_num': 'N/A',
                        'sheet_name': tracking_worksheet_name,
                        'client': 'N/A',
                        'issue': error_msg
                    })
                
        except Exception as e:
            error_msg = f"Помилка при обробці файлу {sheet_url}: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            all_parsing_errors.append({
                'row_num': 'N/A',
                'sheet_name': 'N/A',
                'client': 'N/A',
                'issue': error_msg
            })
    
    # Логуємо всі зібрані помилки в кінці імпорту
    if all_parsing_errors:
        log_sheets_issues(all_parsing_errors)
    
    # Видаляємо дублікати замовлень, якщо необхідно
    try:
        # Відключаємо блокуюче видалення дублікатів, якщо процес запущено асинхронно
        if not parsing_status["is_running"] or len(sheet_links) == 1:
            remove_redundant_order_duplicates()
    except Exception as e:
        logger.error(f"Помилка при видаленні дублікатів замовлень: {str(e)}")
    
    # Статистика та тривалість імпорту
    elapsed_time = datetime.now() - start_time
    logger.info(f"Імпорт завершено. Тривалість: {elapsed_time}")
    logger.info(f"Статистика: оброблено {orders_processed} замовлень, пропущено {orders_skipped}, " +
               f"оновлено {orders_updated}, додано {products_added} продуктів, {tracking_added} трекінгів")
    logger.info(f"Зафіксовано {len(all_parsing_errors)} помилок парсингу, деталі у файлі: {SHEETS_ISSUES_LOG_FILE}")
    
    return orders_processed, orders_skipped, orders_updated, products_added, tracking_added

# -------------------------------------------------------
#   Точка входу
# -------------------------------------------------------
if __name__ == "__main__":
    # Додаємо парсинг аргументів командного рядка
    parser = argparse.ArgumentParser(description='Обробка замовлень з Google Sheets.')
    parser.add_argument('--force', action='store_true', help='Форсований режим - обробляти всі рядки незалежно від хешу')
    parser.add_argument('--list-errors', action='store_true', help='Вивести список рядків з помилками')
    parser.add_argument('--retry-errors', action='store_true', help='Повторно обробити рядки з помилками')
    parser.add_argument('--sheet', type=str, help='Ім\'я аркуша для обробки помилок', default=None)
    parser.add_argument('--async', action='store_true', help='Запустити парсинг асинхронно у фоновому режимі')
    parser.add_argument('--status', action='store_true', help='Показати поточний статус асинхронного парсингу')
    
    args = parser.parse_args()
    
    if args.list_errors:
        list_failed_rows()
    elif args.retry_errors:
        retry_failed_rows(args.sheet)
    elif args.status:
        status = get_parsing_status()
        print(json.dumps(status, default=str, indent=2))
    elif getattr(args, 'async'):
        # Запускаємо парсинг асинхронно
        sheet_urls = [SPREADSHEET_NAME]  # Треба замінити на ввід від користувача або значення за замовчуванням
        success = start_async_parsing(sheet_urls, args.force)
        if success:
            print("Асинхронний парсинг запущено у фоновому режимі")
        else:
            print("Не вдалося запустити асинхронний парсинг - можливо, процес вже виконується")
    else:
        # Запускаємо парсинг синхронно (стандартна поведінка)
        sheet_urls = [SPREADSHEET_NAME]  # Треба замінити на ввід від користувача або значення за замовчуванням
        import_data(sheet_urls, args.force)

# Додаємо нову функцію для отримання всіх помилок парсингу
def get_parsing_errors():
    """Повертає список всіх помилок, що виникли під час парсингу"""
    global parsing_errors
    return parsing_errors

# -------------------------------------------------------
#   Видалення неправильно об'єднаних замовлень для "Невідомих" клієнтів
# -------------------------------------------------------
def split_unknown_client_orders():
    """
    Знаходить і розділяє всі замовлення "Невідомих" клієнтів, 
    які містять більше 1 товару (були неправильно об'єднані).
    
    Для кожного такого замовлення:
    1. Знаходимо всі товари
    2. Створюємо нове замовлення для кожного товару, якщо такого ще не існує
    3. Видаляємо оригінальне "змішане" замовлення
    
    Повертає кількість розділених замовлень та створених нових.
    """
    conn = connect_to_db()
    if not conn:
        logger.error("Не вдалося підключитися до БД для розділення замовлень.")
        return 0, 0
    
    cur = conn.cursor()
    try:
        # Знаходимо ID клієнта "Невідомий"
        cur.execute("""
            SELECT id FROM clients
            WHERE first_name = 'Невідомий' AND last_name IS NULL
            LIMIT 1
        """)
        
        unknown_client_row = cur.fetchone()
        if not unknown_client_row:
            logger.info("Не знайдено клієнта 'Невідомий'.")
            return 0, 0
        
        unknown_client_id = unknown_client_row[0]
        
        # Знаходимо всі замовлення "Невідомого" клієнта з більш ніж одним продуктом
        cur.execute("""
            SELECT o.id, COUNT(od.id) AS product_count
            FROM orders o
            JOIN order_details od ON o.id = od.order_id
            WHERE o.client_id = %s
            GROUP BY o.id
            HAVING COUNT(od.id) > 1
        """, (unknown_client_id,))
        
        mixed_orders = cur.fetchall()
        if not mixed_orders:
            logger.info("Не знайдено змішаних замовлень для розділення.")
            return 0, 0
        
        logger.info(f"Знайдено {len(mixed_orders)} змішаних замовлень для розділення.")
        
        # Отримуємо список всіх окремих замовлень "Невідомого" клієнта з одним товаром
        cur.execute("""
            WITH single_product_orders AS (
                SELECT o.id AS order_id, p.id AS product_id, p.productnumber,
                       o.order_date, o.payment_status, o.total_amount
                FROM orders o
                JOIN order_details od ON o.id = od.order_id
                JOIN products p ON p.id = od.product_id
                WHERE o.client_id = %s
                GROUP BY o.id, p.id
                HAVING COUNT(od.id) = 1
            )
            SELECT order_id, product_id, productnumber, order_date, payment_status, total_amount
            FROM single_product_orders
        """, (unknown_client_id,))
        
        existing_single_orders = cur.fetchall()
        existing_orders_by_product = {}
        
        for order_id, product_id, productnumber, order_date, payment_status, total_amount in existing_single_orders:
            existing_orders_by_product[product_id] = {
                'order_id': order_id,
                'productnumber': productnumber,
                'order_date': order_date,
                'payment_status': payment_status,
                'total_amount': total_amount
            }
        
        logger.info(f"Знайдено {len(existing_single_orders)} існуючих окремих замовлень для 'Невідомого' клієнта")
        
        # Для кожного змішаного замовлення створюємо окремі замовлення для кожного товару
        split_count = 0
        new_orders_count = 0
        
        for order_id, product_count in mixed_orders:
            # Отримуємо деталі замовлення
            cur.execute("""
                SELECT order_date, order_status_id, payment_status_id, payment_status,
                       delivery_method_id, delivery_status_id, tracking_number,
                       deferred_until, priority, notes, total_amount
                FROM orders
                WHERE id = %s
            """, (order_id,))
            
            order_details_row = cur.fetchone()
            if not order_details_row:
                logger.warning(f"Не вдалося отримати деталі замовлення ID={order_id}.")
                continue
            
            (order_date, order_status_id, payment_status_id, payment_status,
             delivery_method_id, delivery_status_id, tracking_number,
             deferred_until, priority, notes, total_amount) = order_details_row
            
            # Отримуємо всі продукти замовлення
            cur.execute("""
                SELECT od.product_id, od.price, od.discount_type, od.discount_value,
                       od.additional_operation, od.additional_operation_value,
                       p.productnumber
                FROM order_details od
                JOIN products p ON p.id = od.product_id
                WHERE od.order_id = %s
            """, (order_id,))
            
            product_details = cur.fetchall()
            if not product_details:
                logger.warning(f"Не знайдено продуктів у замовленні ID={order_id}.")
                continue
            
            logger.info(f"Розділення замовлення ID={order_id} з {len(product_details)} продуктами.")
            products_processed = 0
            
            # Для кожного продукту створюємо нове замовлення, якщо такого ще не існує
            for (product_id, price, discount_type, discount_value,
                 additional_operation, additional_operation_value, 
                 productnumber) in product_details:
                
                # Перевіряємо, чи вже існує окреме замовлення з цим продуктом
                if product_id in existing_orders_by_product:
                    existing_info = existing_orders_by_product[product_id]
                    logger.info(f"Знайдено існуюче замовлення ID={existing_info['order_id']} для продукту '{productnumber}' - пропускаємо")
                    products_processed += 1
                    continue
                    
                # Для розділених товарів можемо розрахувати частку від загальної суми
                item_price = price or 0
                item_total = item_price
                if discount_type == 'Відсоток' and discount_value:
                    item_total = item_price * (1 - discount_value/100)
                elif discount_type == 'Фіксована' and discount_value:
                    item_total = item_price - discount_value
                
                if additional_operation_value:
                    item_total += additional_operation_value
                
                # Створюємо нове замовлення
                try:
                    cur.execute("""
                        INSERT INTO orders (
                            client_id, order_date, order_status_id, payment_status_id,
                            payment_status, delivery_method_id, delivery_status_id,
                            tracking_number, deferred_until, priority, notes,
                            total_amount, created_at, updated_at
                        )
                        VALUES (
                            %s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, now(), now()
                        )
                        RETURNING id
                    """, (
                        unknown_client_id, order_date, order_status_id, payment_status_id,
                        payment_status, delivery_method_id, delivery_status_id,
                        tracking_number, deferred_until, priority, notes,
                        item_total
                    ))
                    
                    new_order_id = cur.fetchone()[0]
                    
                    # Додаємо деталь замовлення
                    cur.execute("""
                        INSERT INTO order_details (
                            order_id, product_id, quantity, price,
                            discount_type, discount_value,
                            additional_operation, additional_operation_value,
                            created_at, updated_at
                        )
                        VALUES (
                            %s, %s, 1, %s,
                            %s, %s,
                            %s, %s,
                            now(), now()
                        )
                    """, (
                        new_order_id, product_id, price,
                        discount_type, discount_value,
                        additional_operation, additional_operation_value
                    ))
                    
                    # Додаємо інформацію до існуючих замовлень, щоб уникнути дублювання в поточному процесі
                    existing_orders_by_product[product_id] = {
                        'order_id': new_order_id,
                        'productnumber': productnumber,
                        'order_date': order_date,
                        'payment_status': payment_status,
                        'total_amount': item_total
                    }
                    
                    logger.info(f"Створено нове замовлення ID={new_order_id} для продукту '{productnumber}'.")
                    new_orders_count += 1
                    products_processed += 1
                    
                except Exception as e:
                    logger.error(f"Помилка при створенні нового замовлення для продукту '{productnumber}': {e}")
                    conn.rollback()
                    # Переходимо до наступного продукту
                    continue
            
            # Видаляємо оригінальне "змішане" замовлення та його деталі, якщо всі продукти оброблені
            if products_processed == len(product_details):
                try:
                    # Спочатку видаляємо деталі замовлення
                    cur.execute("DELETE FROM order_details WHERE order_id = %s", (order_id,))
                    
                    # Потім видаляємо саме замовлення
                    cur.execute("DELETE FROM orders WHERE id = %s", (order_id,))
                    
                    logger.info(f"Видалено оригінальне змішане замовлення ID={order_id}.")
                    split_count += 1
                    
                    # Підтверджуємо транзакцію після успішного розділення замовлення
                    conn.commit()
                    
                except Exception as e:
                    logger.error(f"Помилка при видаленні оригінального замовлення ID={order_id}: {e}")
                    conn.rollback()
                    # Переходимо до наступного змішаного замовлення
                    continue
            else:
                logger.warning(f"Не всі продукти замовлення ID={order_id} оброблені: {products_processed}/{len(product_details)}")
                conn.rollback()
        
        logger.info(f"Розділено {split_count} змішаних замовлень, створено {new_orders_count} нових окремих замовлень.")
        return split_count, new_orders_count
        
    except Exception as e:
        logger.error(f"Помилка при розділенні змішаних замовлень: {e}")
        conn.rollback()
        return 0, 0
    finally:
        cur.close()
        conn.close()

# -------------------------------------------------------
#   Функції небезпечного читання даних без блокування
# -------------------------------------------------------
def read_orders_without_lock(limit=100, offset=0, client_id=None, order_status_id=None, filter_text=None):
    """
    Читає дані замовлень без блокування транзакцій - для використання в UI
    під час імпорту даних.
    
    Використовує окреме з'єднання з рівнем ізоляції READ COMMITTED,
    а також WITH (NOLOCK) підказки для забезпечення доступу до даних під час оновлення.
    
    Args:
        limit: максимальна кількість замовлень
        offset: зміщення для пагінації
        client_id: ID клієнта для фільтрації
        order_status_id: ID статусу замовлення для фільтрації
        filter_text: текст для пошуку
        
    Returns:
        Список замовлень або порожній список у разі помилки
    """
    try:
        conn = get_read_only_connection()
        if not conn:
            logger.error("Не вдалося створити підключення лише для читання")
            return []
            
        cursor = conn.cursor()
        
        # Будуємо SQL-запит з параметрами фільтрації
        sql_query = """
            SELECT o.id, o.client_id, c.first_name, c.last_name, o.order_date, 
                   o.total_amount, o.order_status_id, os.status_name, 
                   o.payment_status, o.delivery_method_id, dm.method_name,
                   o.tracking_number, o.notes, o.created_at
            FROM orders o
            JOIN clients c ON o.client_id = c.id
            LEFT JOIN order_statuses os ON o.order_status_id = os.id
            LEFT JOIN delivery_methods dm ON o.delivery_method_id = dm.id
            WHERE 1=1
        """
        
        query_params = []
        
        if client_id:
            sql_query += " AND o.client_id = %s"
            query_params.append(client_id)
            
        if order_status_id:
            sql_query += " AND o.order_status_id = %s"
            query_params.append(order_status_id)
            
        if filter_text:
            sql_query += """ AND (
                c.first_name ILIKE %s OR 
                c.last_name ILIKE %s OR 
                COALESCE(o.notes, '') ILIKE %s OR
                COALESCE(o.tracking_number, '') ILIKE %s
            )"""
            like_pattern = f"%{filter_text}%"
            query_params.extend([like_pattern, like_pattern, like_pattern, like_pattern])
        
        # Додаємо сортування та ліміти
        sql_query += " ORDER BY o.created_at DESC LIMIT %s OFFSET %s"
        query_params.extend([limit, offset])
        
        # Виконуємо запит
        cursor.execute(sql_query, tuple(query_params))
        orders = cursor.fetchall()
        
        # Для кожного замовлення отримуємо інформацію про товари
        result = []
        for order in orders:
            order_id = order[0]
            
            # Окремий запит для отримання товарів замовлення
            cursor.execute("""
                SELECT od.id, p.productnumber, p.id as product_id, od.price
                FROM order_details od
                JOIN products p ON od.product_id = p.id
                WHERE od.order_id = %s
                ORDER BY od.id
            """, (order_id,))
            
            order_details = cursor.fetchall()
            
            result.append({
                "order": order,
                "details": order_details
            })
        
        cursor.close()
        conn.close()
        return result
        
    except Exception as e:
        logger.error(f"Помилка при читанні замовлень без блокування: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []

def read_products_without_lock(limit=100, offset=0, filter_text=None, only_available=False):
    """
    Читає дані продуктів без блокування транзакцій - для використання в UI
    під час імпорту даних.
    
    Args:
        limit: максимальна кількість продуктів
        offset: зміщення для пагінації
        filter_text: текст для пошуку
        only_available: якщо True, повертає лише доступні (не продані) продукти
        
    Returns:
        Список продуктів або порожній список у разі помилки
    """
    try:
        conn = get_read_only_connection()
        if not conn:
            logger.error("Не вдалося створити підключення лише для читання")
            return []
            
        cursor = conn.cursor()
        
        # Будуємо SQL-запит з параметрами фільтрації
        sql_query = """
            SELECT p.id, p.productnumber, p.clonednumbers, p.price, p.oldprice,
                   p.statusid, s.status_name, p.created_at, p.updated_at
            FROM products p
            LEFT JOIN statuses s ON p.statusid = s.id
            WHERE 1=1
        """
        
        query_params = []
        
        if only_available:
            sql_query += " AND p.statusid = %s"  # 2 = Непродано
            query_params.append(PRODUCT_STATUS_NOT_SOLD)
            
        if filter_text:
            sql_query += """ AND (
                p.productnumber ILIKE %s OR 
                COALESCE(p.clonednumbers, '') ILIKE %s
            )"""
            like_pattern = f"%{filter_text}%"
            query_params.extend([like_pattern, like_pattern])
        
        # Додаємо сортування та ліміти
        sql_query += " ORDER BY p.created_at DESC LIMIT %s OFFSET %s"
        query_params.extend([limit, offset])
        
        # Виконуємо запит
        cursor.execute(sql_query, tuple(query_params))
        products = cursor.fetchall()
        
        cursor.close()
        conn.close()
        return products
        
    except Exception as e:
        logger.error(f"Помилка при читанні продуктів без блокування: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []

def get_parsing_progress_html():
    """
    Генерує HTML-код для відображення статусу парсингу в UI
    
    Returns:
        HTML-код зі статусом парсингу або порожній рядок, якщо парсинг не виконується
    """
    status = get_parsing_status()
    
    if not status["is_running"]:
        if status["end_time"]:
            # Парсинг завершено
            elapsed_time = status["end_time"] - status["start_time"] if status["start_time"] else datetime.now() - datetime.now()
            elapsed_seconds = elapsed_time.total_seconds()
            
            minutes = int(elapsed_seconds // 60)
            seconds = int(elapsed_seconds % 60)
            
            return f"""
            <div class="alert alert-success">
                <h4>Парсинг завершено</h4>
                <p>Оброблено {status["processed_sheets"]}/{status["total_sheets"]} аркушів, {status["processed_rows"]}/{status["total_rows"]} рядків.</p>
                <p>Додано/оновлено {status["orders_processed"] + status["orders_updated"]} замовлень.</p>
                <p>Тривалість: {minutes} хв {seconds} сек</p>
                <p>Помилок: {status["errors"]}</p>
            </div>
            """
        else:
            # Парсинг не виконується
            return ""
    
    # Парсинг виконується
    elapsed_time = datetime.now() - status["start_time"] if status["start_time"] else datetime.now() - datetime.now()
    elapsed_seconds = elapsed_time.total_seconds()
    
    minutes = int(elapsed_seconds // 60)
    seconds = int(elapsed_seconds % 60)
    
    # Розрахунок часу до завершення
    estimated_time_remaining = status.get("estimated_time_remaining", "Невідомо")
    if isinstance(estimated_time_remaining, (int, float)):
        remaining_minutes = int(estimated_time_remaining // 60)
        remaining_seconds = int(estimated_time_remaining % 60)
        remaining_str = f"{remaining_minutes} хв {remaining_seconds} сек"
    else:
        remaining_str = estimated_time_remaining
    
    return f"""
    <div class="alert alert-info">
        <h4>Парсинг виконується...</h4>
        <p>Оброблено {status["processed_sheets"]}/{status["total_sheets"]} аркушів, {status["processed_rows"]}/{status["total_rows"]} рядків.</p>
        <div class="progress">
            <div class="progress-bar progress-bar-striped progress-bar-animated" 
                 role="progressbar" 
                 style="width: {status['progress_percent']}%;" 
                 aria-valuenow="{status['progress_percent']}" 
                 aria-valuemin="0" 
                 aria-valuemax="100">
                {status['progress_percent']}%
            </div>
        </div>
        <p>Поточний аркуш: {status["current_sheet"]}</p>
        <p>Додано/оновлено замовлень: {status["orders_processed"] + status["orders_updated"]}</p>
        <p>Тривалість: {minutes} хв {seconds} сек</p>
        <p>Приблизний час до завершення: {remaining_str}</p>
        <p>Помилок: {status["errors"]}</p>
    </div>
    """

# -------------------------------------------------------
#   Функції управління асинхронним парсингом
# -------------------------------------------------------
def get_parsing_status():
    """Повертає поточний статус процесу парсингу у форматі JSON"""
    global parsing_status
    
    # Розрахунок загального прогресу
    if parsing_status["total_rows"] > 0:
        parsing_status["progress_percent"] = round((parsing_status["processed_rows"] / parsing_status["total_rows"]) * 100, 2)
    else:
        parsing_status["progress_percent"] = 0
        
    # Розрахунок приблизного часу до завершення
    if parsing_status["is_running"] and parsing_status["start_time"]:
        elapsed_time = datetime.now() - parsing_status["start_time"]
        elapsed_seconds = elapsed_time.total_seconds()
        
        if parsing_status["progress_percent"] > 0:
            estimated_total_seconds = elapsed_seconds * 100 / parsing_status["progress_percent"]
            remaining_seconds = estimated_total_seconds - elapsed_seconds
            
            parsing_status["estimated_time_remaining"] = round(remaining_seconds, 0)
        else:
            parsing_status["estimated_time_remaining"] = "Невідомо"
    
    return parsing_status

def update_parsing_status(key, value):
    """Оновлює статус парсингу за вказаним ключем"""
    global parsing_status
    parsing_status[key] = value

def reset_parsing_status():
    """Скидає статус парсингу до початкових значень"""
    global parsing_status
    parsing_status = {
        "is_running": False,
        "total_sheets": 0,
        "processed_sheets": 0,
        "total_rows": 0,
        "processed_rows": 0,
        "current_sheet": "",
        "start_time": None,
        "end_time": None,
        "errors": 0,
        "orders_processed": 0,
        "orders_updated": 0,
        "memory_usage": 0,
        "progress_percent": 0
    }

def start_async_parsing(sheet_links, force_process=False):
    """
    Запускає асинхронний процес парсингу в окремому потоці
    :param sheet_links: список посилань на Google Sheets
    :param force_process: якщо True, примусово обробляємо всі замовлення
    :return: True якщо парсинг запущено успішно, False якщо вже виконується
    """
    global parsing_status
    
    # Перевіряємо, чи не запущений уже процес парсингу
    if parsing_status["is_running"]:
        logger.warning("Спроба запустити парсинг, але процес уже виконується")
        return False
    
    # Створюємо і запускаємо потік для парсингу
    parsing_thread = threading.Thread(
        target=import_data_async,
        args=(sheet_links, force_process),
        daemon=True  # Потік буде автоматично завершено при закритті програми
    )
    
    # Скидаємо статус парсингу
    reset_parsing_status()
    
    # Оновлюємо статус
    parsing_status["is_running"] = True
    parsing_status["start_time"] = datetime.now()
    parsing_status["total_sheets"] = len(sheet_links)
    
    # Запускаємо потік
    parsing_thread.start()
    logger.info(f"Запущено асинхронний парсинг {len(sheet_links)} файлів")
    
    return True

def import_data_async(sheet_links, force_process=False):
    """
    Асинхронна версія функції import_data для роботи у фоновому потоці
    :param sheet_links: список посилань на Google Sheets
    :param force_process: якщо True, примусово обробляємо всі замовлення
    """
    try:
        orders_processed, orders_skipped, orders_updated, products_added, tracking_added = import_data(sheet_links, force_process)
        
        # Оновлюємо фінальний статус
        update_parsing_status("is_running", False)
        update_parsing_status("end_time", datetime.now())
        update_parsing_status("orders_processed", orders_processed)
        update_parsing_status("orders_updated", orders_updated)
        
        logger.info(f"Фоновий парсинг завершено. Оброблено {orders_processed} замовлень, оновлено {orders_updated}")
    except Exception as e:
        logger.error(f"Помилка у фоновому потоці парсингу: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Оновлюємо статус про помилку
        update_parsing_status("is_running", False)
        update_parsing_status("end_time", datetime.now())
        update_parsing_status("last_error", str(e))

# -------------------------------------------------------
#   Підключення до PostgreSQL з рівнем ізоляції
# -------------------------------------------------------
def connect_to_db_with_isolation(isolation_level):
   try:
       connection = psycopg2.connect(
           host=DB_HOST,
           port=DB_PORT,
           database=DB_NAME,
           user=DB_USER,
           password=DB_PASSWORD
       )
       connection.set_isolation_level(isolation_level)
       return connection
   except psycopg2.Error as e:
       logger.error(f"Помилка підключення до бази даних з ізоляцією {isolation_level}: {e}")
       return None