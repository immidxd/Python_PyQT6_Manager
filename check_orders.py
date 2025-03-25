#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Дані для підключення до БД
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")

def connect_to_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print(f"Підключено до PostgreSQL: {DB_HOST}:{DB_PORT}/{DB_NAME}")
        return conn
    except psycopg2.Error as e:
        print(f"Помилка підключення до бази даних: {e}")
        return None

def check_orders():
    conn = connect_to_db()
    if not conn:
        return
    
    cur = conn.cursor()
    
    # Перевіряємо загальну кількість замовлень
    cur.execute("SELECT COUNT(*) FROM orders")
    total_orders = cur.fetchone()[0]
    print(f'Всього замовлень у базі даних: {total_orders}')
    
    # Перевіряємо статуси оплати
    cur.execute("SELECT payment_status, COUNT(*) FROM orders GROUP BY payment_status")
    payment_statuses = cur.fetchall()
    print('Статуси оплати:')
    for status, count in payment_statuses:
        print(f'- {status}: {count}')
    
    # Перевіряємо замовлення за статусом оплати
    cur.execute("SELECT id FROM payment_statuses WHERE status_name = 'оплачено'")
    paid_status_row = cur.fetchone()
    if paid_status_row:
        paid_status_id = paid_status_row[0]
        cur.execute(f"SELECT COUNT(*) FROM orders WHERE payment_status_id = {paid_status_id}")
        paid_count = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM orders WHERE payment_status_id != {paid_status_id} OR payment_status_id IS NULL")
        unpaid_count = cur.fetchone()[0]
        print(f'Оплачених замовлень (payment_status_id={paid_status_id}): {paid_count}')
        print(f'Неоплачених замовлень (payment_status_id!={paid_status_id}): {unpaid_count}')
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_orders() 