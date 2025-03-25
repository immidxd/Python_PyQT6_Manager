import os
import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import time

# Налаштування логування
logger = logging.getLogger('google_sheets_service')

class GoogleSheetsService:
    """
    Сервіс для роботи з Google Sheets API.
    
    Відповідає за:
    - Підключення до Google Sheets API
    - Отримання даних з таблиць замовлень і товарів
    - Управління кешем підключення для зменшення використання API
    """
    
    def __init__(self):
        """Ініціалізує сервіс Google Sheets."""
        self.client = None
        self.last_auth_time = 0
        self.auth_expiry = 3600  # Термін дії авторизації (1 година)
        self.credentials = None
        self.orders_spreadsheet = None
        self.products_spreadsheet = None
        
        # Завантажуємо змінні середовища
        load_dotenv()
        
        # Отримуємо шлях до файлу облікових даних
        self.script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.json_key_file = os.path.join(
            self.script_dir, 
            "views/scripts", 
            os.getenv("GOOGLE_SHEETS_JSON_KEY", "newproject2024-419923-8aec36a3b0ce.json")
        )
        
        # Назви документів
        self.orders_document_name = os.getenv("GOOGLE_SHEETS_DOCUMENT_NAME_ORDERS", "Замовлення")
        self.products_document_name = os.getenv("GOOGLE_SHEETS_DOCUMENT_NAME_PRODUCTS", "Товари")
        
        logger.info(f"GoogleSheetsService ініціалізовано. JSON-ключ: {self.json_key_file}")
    
    def _authenticate(self):
        """
        Автентифікується в Google Sheets API.
        
        Returns:
            bool: True, якщо автентифікація успішна, False - інакше.
        """
        current_time = time.time()
        
        # Перевіряємо, чи потрібно оновлювати автентифікацію
        if (self.client is not None and 
            current_time - self.last_auth_time < self.auth_expiry):
            logger.debug("Використовуємо існуючу автентифікацію")
            return True
        
        try:
            logger.info("Виконуємо нову автентифікацію в Google Sheets API")
            
            # Перевіряємо існування файлу облікових даних
            if not os.path.exists(self.json_key_file):
                logger.error(f"Файл облікових даних не знайдено: {self.json_key_file}")
                return False
            
            # Створюємо облікові дані
            self.credentials = ServiceAccountCredentials.from_json_keyfile_name(
                self.json_key_file,
                ["https://www.googleapis.com/auth/spreadsheets",
                 "https://www.googleapis.com/auth/drive"]
            )
            
            # Авторизуємось
            self.client = gspread.authorize(self.credentials)
            self.last_auth_time = current_time
            
            # Скидаємо кеші документів
            self.orders_spreadsheet = None
            self.products_spreadsheet = None
            
            logger.info("Автентифікація в Google Sheets API успішна")
            return True
            
        except Exception as e:
            logger.error(f"Помилка автентифікації в Google Sheets API: {str(e)}")
            self.client = None
            return False
    
    def is_ready(self):
        """
        Перевіряє доступність Google Sheets API.
        
        Returns:
            bool: True, якщо API доступне, False - інакше.
        """
        # Перевіряємо автентифікацію
        if not self._authenticate():
            return False
        
        try:
            # Пробуємо отримати список доступних документів
            _ = self.client.list_spreadsheet_files()
            return True
        except Exception as e:
            logger.error(f"Google Sheets API не доступне: {str(e)}")
            return False
    
    def get_orders_worksheets(self):
        """
        Отримує список аркушів з документа замовлень.
        
        Returns:
            list: Список об'єктів аркушів або None у випадку помилки.
        """
        # Перевіряємо автентифікацію
        if not self._authenticate():
            logger.error("Не вдалося автентифікуватися для отримання аркушів замовлень")
            return None
        
        # Параметри для повторних спроб при перевищенні квоти
        retry_count = 0
        max_retries = 5
        backoff_time = 1  # Початкова затримка в секундах
        max_backoff_time = 60  # Максимальна затримка в секундах
        
        while retry_count <= max_retries:
            try:
                # Відкриваємо документ з замовленнями (або використовуємо кеш)
                if self.orders_spreadsheet is None:
                    logger.info(f"Відкриваємо документ замовлень: {self.orders_document_name}")
                    self.orders_spreadsheet = self.client.open(self.orders_document_name)
                
                # Отримуємо всі аркуші та відфільтровуємо службові
                all_sheets = self.orders_spreadsheet.worksheets()
                worksheets = [ws for ws in all_sheets if ws.title.strip() != "Клієнти"]
                
                # Сортуємо аркуші за датою (якщо можливо)
                try:
                    from views.scripts.orders_pars import sort_worksheets_by_date
                    worksheets = sort_worksheets_by_date(worksheets)
                except (ImportError, AttributeError) as e:
                    logger.warning(f"Не вдалося імпортувати функцію сортування: {e}")
                    # Сортуємо за назвою аркуша як запасний варіант
                    worksheets.sort(key=lambda ws: ws.title)
                
                # Ігнорувати певні аркуші
                ignore_sheets = ["New", "Temporary"]
                filtered_worksheets = [ws for ws in worksheets if ws.title.strip() not in ignore_sheets]
                
                logger.info(f"Отримано {len(filtered_worksheets)} аркушів замовлень з {len(all_sheets)} загальних")
                return filtered_worksheets
                
            except gspread.exceptions.APIError as api_error:
                error_str = str(api_error)
                retry_count += 1
                
                # Перевірка на перевищення квоти
                if "Quota exceeded" in error_str or "Rate Limit Exceeded" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                    wait_time = min(backoff_time * 2 ** (retry_count - 1), max_backoff_time)
                    import random
                    jitter = random.uniform(0.8, 1.2)  # Додаємо невелику рандомізацію
                    wait_time = wait_time * jitter
                    
                    logger.warning(f"Перевищено квоту API при отриманні аркушів замовлень. Спроба {retry_count}/{max_retries}. Очікування {wait_time:.1f} секунд")
                    
                    # Чекаємо перед наступною спробою
                    time.sleep(wait_time)
                    
                    # Скидаємо кеш документа для наступної спроби
                    self.orders_spreadsheet = None
                    
                    # Якщо це остання спроба, повертаємо помилку
                    if retry_count >= max_retries:
                        logger.error(f"Вичерпано всі спроби отримання аркушів замовлень через перевищення квоти")
                        return None
                else:
                    # Інші помилки API
                    logger.error(f"Помилка API при отриманні аркушів замовлень: {api_error}")
                    self.orders_spreadsheet = None  # Скидаємо кеш
                    return None
            except Exception as e:
                logger.error(f"Помилка при отриманні аркушів замовлень: {str(e)}")
                self.orders_spreadsheet = None  # Скидаємо кеш
                return None
    
    def get_products_worksheets(self):
        """
        Отримує список аркушів з документа товарів.
        
        Returns:
            list: Список об'єктів аркушів або None у випадку помилки.
        """
        # Перевіряємо автентифікацію
        if not self._authenticate():
            logger.error("Не вдалося автентифікуватися для отримання аркушів товарів")
            return None
        
        # Параметри для повторних спроб при перевищенні квоти
        retry_count = 0
        max_retries = 5
        backoff_time = 1  # Початкова затримка в секундах
        max_backoff_time = 60  # Максимальна затримка в секундах
        
        while retry_count <= max_retries:
            try:
                # Відкриваємо документ з товарами (або використовуємо кеш)
                if self.products_spreadsheet is None:
                    logger.info(f"Відкриваємо документ товарів: {self.products_document_name}")
                    self.products_spreadsheet = self.client.open(self.products_document_name)
                
                # Отримуємо всі аркуші
                worksheets = self.products_spreadsheet.worksheets()
                
                logger.info(f"Отримано {len(worksheets)} аркушів товарів")
                return worksheets
                
            except gspread.exceptions.APIError as api_error:
                error_str = str(api_error)
                retry_count += 1
                
                # Перевірка на перевищення квоти
                if "Quota exceeded" in error_str or "Rate Limit Exceeded" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                    wait_time = min(backoff_time * 2 ** (retry_count - 1), max_backoff_time)
                    import random
                    jitter = random.uniform(0.8, 1.2)  # Додаємо невелику рандомізацію
                    wait_time = wait_time * jitter
                    
                    logger.warning(f"Перевищено квоту API при отриманні аркушів товарів. Спроба {retry_count}/{max_retries}. Очікування {wait_time:.1f} секунд")
                    
                    # Чекаємо перед наступною спробою
                    time.sleep(wait_time)
                    
                    # Скидаємо кеш документа для наступної спроби
                    self.products_spreadsheet = None
                    
                    # Якщо це остання спроба, повертаємо помилку
                    if retry_count >= max_retries:
                        logger.error(f"Вичерпано всі спроби отримання аркушів товарів через перевищення квоти")
                        return None
                else:
                    # Інші помилки API
                    logger.error(f"Помилка API при отриманні аркушів товарів: {api_error}")
                    self.products_spreadsheet = None  # Скидаємо кеш
                    return None
            except Exception as e:
                logger.error(f"Помилка при отриманні аркушів товарів: {str(e)}")
                self.products_spreadsheet = None  # Скидаємо кеш
                return None

# Створюємо глобальний екземпляр сервісу
google_sheets_service = GoogleSheetsService() 