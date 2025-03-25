#!/usr/bin/env python3
# -*- coding: utf-8 -*-




import sys
import asyncio
import logging
import traceback
from PyQt6.QtWidgets import QApplication, QStyleFactory
import qasync
import qtawesome as qta
import os
from dotenv import load_dotenv




# Ініціалізуємо базу даних
from db import init_db, session




# Імпортуємо головне вікно з нової структури
from views.main_window import MainWindow




# Налаштування логування
logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)




def setup_environment():
    # Завантаження змінних середовища з .env файлу
    load_dotenv()




def main():
    """
    Точка входу у програму. Створює QApplication, головне вікно
    та налаштовує асинхронний цикл подій.
    """
    setup_environment()

    # Отримуємо шлях до додатку та створюємо QtApp
    app = QApplication(sys.argv)
    
    # Додаємо глобальний стиль для всіх таблиць, щоб текст був чорним
    app.setStyleSheet("""
    QTableWidget {
        color: #000000;
    }
    QTableWidgetItem {
        color: #000000;
    }
    """)
    
    # Вибираємо єдиний стиль для всіх ОС
    app.setStyle(QStyleFactory.create("Fusion"))
    
    # Ініціалізуємо базу даних з детальним логуванням
    logging.info("Починаємо ініціалізацію бази даних...")
    try:
        # Переконуємося, що попередній файл видалений, якщо він пошкоджений
        if os.path.exists('local_database.db') and os.path.getsize('local_database.db') == 0:
            logging.warning("Знайдено порожній файл бази даних. Видаляємо його для перестворення.")
            os.remove('local_database.db')
            
        # Ініціалізуємо базу даних
        init_db()
        
        # Перевіряємо, чи успішно створена база даних
        from models import Gender, OrderStatus
        gender_count = session.query(Gender).count()
        order_status_count = session.query(OrderStatus).count()
        logging.info(f"Успішно ініціалізовано базу даних: {gender_count} записів Gender, {order_status_count} записів OrderStatus")
    except Exception as e:
        logging.error(f"Помилка при ініціалізації бази даних: {e}")
        logging.error(traceback.format_exc())
        # Не завершуємо програму, продовжуємо для інтерфейсу
    
    print("Запуск QApplication")
    print("QApplication створено")




    loop = qasync.QEventLoop(app)
    asyncio.set_event_loop(loop)
    print("qasync loop створено")




    window = MainWindow()
    print("MainWindow створено")
    window.show()
    print("window.show() викликано")




    search_icon = qta.icon('fa5s.search', color='#888888')




    with loop:
        print("Запускаємо event loop")
        loop.run_forever()




if __name__ == "__main__":
    main()


