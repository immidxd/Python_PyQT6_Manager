#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
from datetime import datetime

from .orders_pars import (
    start_async_parsing, 
    get_parsing_status, 
    read_orders_without_lock, 
    read_products_without_lock,
    get_parsing_progress_html
)

logger = logging.getLogger(__name__)

class AsyncParsingAPI:
    """
    API для керування асинхронним парсингом замовлень з Google Sheets
    і отримання даних без блокування під час парсингу
    """
    
    @staticmethod
    def start_parsing(sheets_urls, force_process=False):
        """
        Запускає асинхронний процес парсингу у фоновому режимі
        
        Args:
            sheets_urls: список URL-адрес Google Sheets для парсингу
            force_process: якщо True, форсує оновлення всіх рядків
            
        Returns:
            dict: результат операції (success, message)
        """
        try:
            # Перевірка вхідних параметрів
            if not sheets_urls or not isinstance(sheets_urls, list):
                return {"success": False, "message": "Не вказані URL-адреси таблиць Google Sheets"}
                
            # Запускаємо асинхронний парсинг
            result = start_async_parsing(sheets_urls, force_process)
            
            if result:
                return {
                    "success": True, 
                    "message": f"Асинхронний парсинг {len(sheets_urls)} таблиць запущено успішно"
                }
            else:
                return {
                    "success": False, 
                    "message": "Не вдалося запустити парсинг - можливо, процес вже виконується"
                }
                
        except Exception as e:
            logger.error(f"Помилка при запуску асинхронного парсингу: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"success": False, "message": f"Помилка: {str(e)}"}
    
    @staticmethod
    def get_status():
        """
        Повертає поточний статус парсингу
        
        Returns:
            dict: статус парсингу
        """
        try:
            status = get_parsing_status()
            
            # Перетворюємо datetime об'єкти в рядки для JSON
            if status.get("start_time"):
                status["start_time_str"] = status["start_time"].strftime("%Y-%m-%d %H:%M:%S")
                
            if status.get("end_time"):
                status["end_time_str"] = status["end_time"].strftime("%Y-%m-%d %H:%M:%S")
                
            # Додаємо додаткову інформацію
            if status.get("is_running") and status.get("start_time"):
                elapsed_time = datetime.now() - status["start_time"]
                status["elapsed_seconds"] = elapsed_time.total_seconds()
                status["elapsed_str"] = f"{int(elapsed_time.total_seconds() // 60)} хв {int(elapsed_time.total_seconds() % 60)} сек"
            
            return status
            
        except Exception as e:
            logger.error(f"Помилка при отриманні статусу парсингу: {e}")
            return {"success": False, "message": f"Помилка: {str(e)}"}
    
    @staticmethod
    def get_status_html():
        """
        Повертає HTML-код для відображення статусу парсингу
        
        Returns:
            str: HTML-код
        """
        try:
            return get_parsing_progress_html()
        except Exception as e:
            logger.error(f"Помилка при генерації HTML статусу парсингу: {e}")
            return f'<div class="alert alert-danger">Помилка отримання статусу: {str(e)}</div>'
    
    @staticmethod
    def get_orders(limit=100, offset=0, client_id=None, order_status_id=None, filter_text=None):
        """
        Отримує список замовлень без блокування (для використання під час парсингу)
        
        Args:
            limit: максимальна кількість замовлень
            offset: зміщення для пагінації
            client_id: ID клієнта для фільтрації
            order_status_id: ID статусу замовлення для фільтрації
            filter_text: текст для пошуку
            
        Returns:
            list: список замовлень
        """
        try:
            orders = read_orders_without_lock(
                limit=limit, 
                offset=offset, 
                client_id=client_id, 
                order_status_id=order_status_id,
                filter_text=filter_text
            )
            
            # Перетворюємо результати у словники для зручності використання
            result = []
            for order_data in orders:
                order_dict = {
                    "id": order_data["order"][0],
                    "client_id": order_data["order"][1],
                    "client_name": f"{order_data['order'][2] or ''} {order_data['order'][3] or ''}".strip(),
                    "order_date": order_data["order"][4].strftime("%Y-%m-%d") if order_data["order"][4] else None,
                    "total_amount": float(order_data["order"][5]) if order_data["order"][5] else 0,
                    "status_id": order_data["order"][6],
                    "status_name": order_data["order"][7],
                    "payment_status": order_data["order"][8],
                    "delivery_method_id": order_data["order"][9],
                    "delivery_method": order_data["order"][10],
                    "tracking_number": order_data["order"][11],
                    "notes": order_data["order"][12],
                    "created_at": order_data["order"][13].strftime("%Y-%m-%d %H:%M:%S") if order_data["order"][13] else None,
                    "details": []
                }
                
                # Додаємо деталі замовлення
                for detail in order_data["details"]:
                    order_dict["details"].append({
                        "id": detail[0],
                        "product_number": detail[1],
                        "product_id": detail[2],
                        "price": float(detail[3]) if detail[3] else 0
                    })
                
                result.append(order_dict)
            
            return result
            
        except Exception as e:
            logger.error(f"Помилка при отриманні замовлень: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    @staticmethod
    def get_products(limit=100, offset=0, filter_text=None, only_available=False):
        """
        Отримує список продуктів без блокування (для використання під час парсингу)
        
        Args:
            limit: максимальна кількість продуктів
            offset: зміщення для пагінації
            filter_text: текст для пошуку
            only_available: якщо True, повертає лише доступні (не продані) продукти
            
        Returns:
            list: список продуктів
        """
        try:
            products = read_products_without_lock(
                limit=limit, 
                offset=offset, 
                filter_text=filter_text,
                only_available=only_available
            )
            
            # Перетворюємо результати у словники для зручності використання
            result = []
            for product in products:
                result.append({
                    "id": product[0],
                    "product_number": product[1],
                    "clones": product[2],
                    "price": float(product[3]) if product[3] else 0,
                    "old_price": float(product[4]) if product[4] else 0,
                    "status_id": product[5],
                    "status_name": product[6],
                    "created_at": product[7].strftime("%Y-%m-%d %H:%M:%S") if product[7] else None,
                    "updated_at": product[8].strftime("%Y-%m-%d %H:%M:%S") if product[8] else None,
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Помилка при отриманні продуктів: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []

# Створюємо екземпляр API для використання
parsing_api = AsyncParsingAPI() 