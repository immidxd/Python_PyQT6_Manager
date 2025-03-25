# db.py
import os
import logging
import json
from sqlalchemy import create_engine, event, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from dotenv import load_dotenv

# Настраиваем логирование
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Загружаем переменные окружения из .env
load_dotenv()

# Создаем экземпляр Base для объявления моделей
Base = declarative_base()

# Получаем параметры подключения из переменных окружения
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "bsstorage")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

# Формируем строку подключения к PostgreSQL
POSTGRES_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# SQLite в файле для локальной разработки
SQLITE_URL = "sqlite:///local_database.db"

# Сначала создаем движок
engine = None
try:
    # Пытаемся подключиться к PostgreSQL
    logger.info(f"Пытаемся подключиться к PostgreSQL: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    engine = create_engine(POSTGRES_URL, echo=False)
    # Проверяем подключение
    engine.connect()
    logger.info("Подключение к PostgreSQL успешно")
except Exception as e:
    logger.error(f"Database connection error: {e}")
    logger.info(f"Using in-memory SQLite database as fallback")
    # Используем SQLite в качестве запасного варианта
    engine = create_engine(SQLITE_URL, echo=False)
    
    # Включаем поддержку внешних ключей в SQLite
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

# Создаем фабрику сессий
Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
session = Session()

def init_db():
    """
    Инициализирует базу данных, создавая таблицы и заполняя начальными данными.
    Этот метод должен вызываться после импорта всех моделей.
    """
    # Создаем таблицы
    try:
        # Получаем метаданные для создания таблиц
        metadata = Base.metadata
        
        # Проверяем, какие таблицы уже существуют
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        
        # Создаем таблицы по одной для более точной обработки ошибок
        for table in metadata.sorted_tables:
            if table.name not in existing_tables:
                try:
                    table.create(engine)
                    logger.info(f"Создана таблица: {table.name}")
                except Exception as e:
                    logger.error(f"Ошибка при создании таблицы {table.name}: {e}")
        
        # Если используем SQLite, добавляем базовые данные
        if 'sqlite' in str(engine.url):
            create_initial_data()
            
    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {e}")
        raise

def create_initial_data():
    """
    Создает начальные данные для базы данных SQLite.
    Функция импортирует модели непосредственно здесь, чтобы избежать циклических импортов.
    """
    try:
        # Импортируем модели здесь, чтобы избежать циклических импортов
        from models import (
            Gender, Status, Condition, Brand, Type, Subtype, Color, Country, 
            OrderStatus, PaymentStatus, DeliveryMethod, DeliveryStatus, PaymentMethod
        )
        
        # Проверяем, есть ли уже записи в таблице Gender
        if session.query(Gender).count() == 0:
            session.add_all([
                Gender(id=1, gendername="чоловічий"),
                Gender(id=2, gendername="жіночий"),
                Gender(id=3, gendername="унісекс")
            ])
        
        # Status
        if session.query(Status).count() == 0:
            session.add_all([
                Status(id=1, statusname="Продано", statusdescription="Товар продан"),
                Status(id=2, statusname="Непродано", statusdescription="Товар не продан"),
                Status(id=7, statusname="Видалено", statusdescription="Товар видалено з бази")
            ])
        
        # Condition
        if session.query(Condition).count() == 0:
            session.add_all([
                Condition(id=1, conditionname="Новий", conditiondescription="Новий товар"),
                Condition(id=2, conditionname="Хороший", conditiondescription="Товар у хорошому стані"),
                Condition(id=3, conditionname="Вживаний", conditiondescription="Товар був у використанні"),
                Condition(id=4, conditionname="Пошкоджений", conditiondescription="Товар має пошкодження")
            ])
        
        # Brand
        if session.query(Brand).count() == 0:
            session.add_all([
                Brand(id=1, brandname="Nike"),
                Brand(id=2, brandname="Adidas"),
                Brand(id=3, brandname="Puma"),
                Brand(id=4, brandname="Reebok")
            ])
        
        # Type
        if session.query(Type).count() == 0:
            session.add_all([
                Type(id=1, typename="Взуття"),
                Type(id=2, typename="Одяг"),
                Type(id=3, typename="Аксесуари")
            ])
        
        # Subtype
        if session.query(Subtype).count() == 0:
            session.add_all([
                Subtype(id=1, typeid=1, subtypename="Кросівки"),
                Subtype(id=2, typeid=1, subtypename="Черевики"),
                Subtype(id=3, typeid=2, subtypename="Футболки"),
                Subtype(id=4, typeid=2, subtypename="Штани")
            ])
        
        # Color
        if session.query(Color).count() == 0:
            session.add_all([
                Color(id=1, colorname="Чорний"),
                Color(id=2, colorname="Білий"),
                Color(id=3, colorname="Червоний"),
                Color(id=4, colorname="Синій")
            ])
        
        # Country
        if session.query(Country).count() == 0:
            session.add_all([
                Country(id=1, countryname="Україна", countrycode="UA"),
                Country(id=2, countryname="США", countrycode="US"),
                Country(id=3, countryname="Китай", countrycode="CN"),
                Country(id=4, countryname="Unknown", countrycode="ZZ")
            ])
        
        # OrderStatus
        if session.query(OrderStatus).count() == 0:
            session.add_all([
                OrderStatus(id=1, status_name="підтверджено"),
                OrderStatus(id=2, status_name="очікується"),
                OrderStatus(id=3, status_name="уточнити"),
                OrderStatus(id=4, status_name="фото"),
                OrderStatus(id=5, status_name="відміна"),
                OrderStatus(id=6, status_name="ігнорування"),
                OrderStatus(id=7, status_name="подарунок"),
                OrderStatus(id=8, status_name="в черзі"),
                OrderStatus(id=9, status_name="повернення"),
                OrderStatus(id=10, status_name="обмін"),
                OrderStatus(id=11, status_name="передати")
            ])
        
        # PaymentStatus
        if session.query(PaymentStatus).count() == 0:
            session.add_all([
                PaymentStatus(id=1, status_name="оплачено"),
                PaymentStatus(id=2, status_name="доплатити"),
                PaymentStatus(id=3, status_name="відкладено"),
                PaymentStatus(id=4, status_name="не оплачено")
            ])
        
        # DeliveryMethod
        if session.query(DeliveryMethod).count() == 0:
            session.add_all([
                DeliveryMethod(id=1, method_name="нп"),
                DeliveryMethod(id=2, method_name="уп"),
                DeliveryMethod(id=3, method_name="міст"),
                DeliveryMethod(id=4, method_name="самовивіз"),
                DeliveryMethod(id=5, method_name="місцевий"),
                DeliveryMethod(id=6, method_name="відкладено"),
                DeliveryMethod(id=7, method_name="магазин")
            ])
        
        # DeliveryStatus
        if session.query(DeliveryStatus).count() == 0:
            session.add_all([
                DeliveryStatus(id=1, status_name="створено"),
                DeliveryStatus(id=2, status_name="відправлено"),
                DeliveryStatus(id=3, status_name="в дорозі"),
                DeliveryStatus(id=4, status_name="доставлено"),
                DeliveryStatus(id=5, status_name="повернуто")
            ])
        
        # PaymentMethod
        if session.query(PaymentMethod).count() == 0:
            session.add_all([
                PaymentMethod(id=1, method_name="Картка"),
                PaymentMethod(id=2, method_name="Готівка"),
                PaymentMethod(id=3, method_name="Переказ")
            ])
        
        session.commit()
        logger.info("SQLite: базові дані успішно додані")
    except Exception as e:
        session.rollback()
        logger.error(f"Не вдалося створити початкові дані: {e}")

# Функция для получения сессии (чтобы избежать глобальных переменных)
def get_session():
    return session