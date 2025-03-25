#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from PyQt6.QtWidgets import (
    QWidget, QLabel, QVBoxLayout, QHBoxLayout,
    QPushButton, QGraphicsOpacityEffect, QProgressBar
)
from PyQt6.QtCore import (
    Qt, QTimer, QPropertyAnimation,
    QEasingCurve, QRect, QRectF, QPoint, QSize,
    QObject, pyqtSignal
)
from PyQt6.QtGui import QColor, QPainter, QPainterPath, QFont, QPen
import logging
import time
import weakref

class NotificationWidget(QWidget):
    """
    Мінімалістичний віджет сповіщення з високою продуктивністю
    """
    closed = pyqtSignal(object)  # Сигнал закриття для менеджера
    hide_all = pyqtSignal()      # Сигнал приховування всіх сповіщень

    def __init__(self, parent=None, message="", error=False, timeout=4000):
        super().__init__(parent)
        
        # Налаштування віджета
        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint | 
            Qt.WindowType.Tool | 
            Qt.WindowType.WindowStaysOnTopHint |
            Qt.WindowType.BypassWindowManagerHint |
            Qt.WindowType.X11BypassWindowManagerHint
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setAttribute(Qt.WidgetAttribute.WA_ShowWithoutActivating)
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        
        # Налаштування розмірів і стилів
        self.setFixedWidth(380)  # Збільшуємо ширину для кращого вигляду
        self.setMinimumHeight(60)
        self.setMaximumHeight(120)  # Збільшуємо максимальну висоту для розміщення більшого тексту

        # Ідентифікатор і дані
        self.message = message
        self.error = error
        
        # Для всіх повідомлень про завершення процесів встановлюємо таймаут 10000 мс (10 секунд)
        process_completion_phrases = [
            "Імпорт успішно завершено",
            "Імпорт завершено з",
            "Товари оновлено",
            "Замовлення оновлено", 
            "Оновлення бази завершено",
            "Оновлення успішно завершено",
            "Базу даних успішно оновлено",
            "Базу даних оновлено",
            "успішно завершено",
            "завершено з"
        ]
        
        for phrase in process_completion_phrases:
            if phrase in message:
                timeout = 10000  # 10 секунд для повідомлень про завершення процесів
                break
        
        self.timeout = timeout
        self.creation_time = time.time()
        self.is_closing = False
        self.auto_close = True if timeout > 0 else False
        
        # Налаштування кольорів - використовуємо колір прогрес-бара для помилок також
        self.bg_color = QColor(35, 35, 35)  # Темно-сірий фон
        self.border_color = QColor(120, 81, 169)  # #7851A9 - фіолетовий колір прогрес-бара
        self.text_color = QColor(255, 255, 255)  # Білий текст
        
        # Для підтримки свайпу праворуч
        self.drag_enabled = True
        self.is_dragging = False
        self.drag_start_position = None
        self.drag_current_position = None
        self.drag_threshold = 5  # мінімальна відстань для початку перетягування
        self.swipe_threshold = 100  # мінімальна відстань для спрацювання свайпу
        
        # Ініціалізація UI
        self._init_ui()
        
        # Налаштування таймера автозакриття
        if self.auto_close:
            self.close_timer = QTimer(self)
            self.close_timer.timeout.connect(self.start_closing)
            self.close_timer.setSingleShot(True)
            
            # Створення таймера прогресу
            self.progress_timer = QTimer(self)
            self.progress_timer.timeout.connect(self._update_progress)
            self.progress_value = 0
            self.progress_step = 1
            self.progress_interval = 30  # мс
            self.progress_max = self.timeout / self.progress_interval
            
        logging.debug(f"Створено віджет сповіщення: {message[:30]}...")

    def _init_ui(self):
        """Ініціалізація UI компонентів"""
        # Основний макет
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(12, 10, 12, 10)
        self.layout.setSpacing(5)
        
        # Верхній ряд: заголовок і кнопка закриття
        header_layout = QHBoxLayout()
        header_layout.setSpacing(4)
        
        # Заголовок без заливки
        title = "Помилка" if self.error else "Увага"
        self.title_label = QLabel(title)
        self.title_label.setStyleSheet(f"""
            color: {self.border_color.name()};
            font-weight: bold;
            font-size: 13px;
            background-color: transparent; /* Прозорий фон для заголовка */
            padding: 0px;
        """)
        
        # Спільний стиль для кнопок
        button_style = f"""
            QPushButton {{
                background-color: transparent;
                color: #cccccc;
                font-weight: bold;
                font-size: 14px;
                padding: 0px;
                border: none;
            }}
            QPushButton:hover {{
                color: {self.border_color.name()};
            }}
        """
        
        # Кнопка приховування всіх сповіщень
        self.hide_all_button = QPushButton("_")
        self.hide_all_button.setFixedSize(16, 16)
        self.hide_all_button.setStyleSheet(button_style)
        self.hide_all_button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.hide_all_button.setToolTip("Приховати всі сповіщення")
        self.hide_all_button.clicked.connect(self._hide_all_clicked)
        
        # Кнопка закриття
        self.close_button = QPushButton("×")
        self.close_button.setFixedSize(16, 16)
        self.close_button.setStyleSheet(button_style)
        self.close_button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.close_button.setToolTip("Закрити це сповіщення")
        self.close_button.clicked.connect(self.start_closing)
        
        header_layout.addWidget(self.title_label)
        header_layout.addStretch()
        header_layout.addWidget(self.hide_all_button)
        header_layout.addWidget(self.close_button)
        
        # Текст повідомлення з кращою читабельністю
        processed_message = self._format_message(self.message)
        self.message_label = QLabel(processed_message)
        self.message_label.setWordWrap(True)
        self.message_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.message_label.setStyleSheet(f"""
            color: #ffffff;
            font-size: 12px;
            background-color: transparent;
            padding: 2px;
            margin: 0px;
        """)
        
        # Збільшуємо максимальну висоту мітки тексту
        self.message_label.setMinimumHeight(40)
        
        # Прогрес-бар для таймауту
        if self.auto_close:
            self.progress_bar = QProgressBar(self)
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(0)
            self.progress_bar.setTextVisible(False)
            self.progress_bar.setFixedHeight(2)
            self.progress_bar.setStyleSheet(f"""
                QProgressBar {{
                    background-color: rgba(80, 80, 80, 30);
                    border: none;
                    border-radius: 0px;
                }}
                QProgressBar::chunk {{
                    background-color: {self.border_color.name()};
                    border-radius: 0px;
                }}
            """)
            
            # Додаємо всі елементи до макету
            self.layout.addLayout(header_layout)
            self.layout.addWidget(self.message_label)
            self.layout.addWidget(self.progress_bar)
        else:
            # Додаємо елементи без прогрес-бару
            self.layout.addLayout(header_layout)
            self.layout.addWidget(self.message_label)
        
        # Підлаштовуємо розмір
        self.adjustSize()

    def _format_message(self, message):
        """Форматує повідомлення для кращого відображення"""
        # Скорочуємо занадто довгі повідомлення
        if len(message) > 200:
            message = message[:197] + "..."
            
        # Обробляємо SQL помилки та інші технічні повідомлення
        if "SQL" in message:
            # Для SQL помилок особливе форматування
            if ":" in message:
                parts = message.split(':', 1)
                return parts[0] + ":\n" + parts[1].strip()
            
        # Для помилки "column does not exist" - спеціальне форматування
        if "column" in message and "does not exist" in message:
            lines = message.split('\n')
            if len(lines) > 1:
                return lines[0] + "\n" + lines[1].strip()
        
        if "argument of" in message:
            # Розбиваємо повідомлення на рядки за словами
            words = message.split()
            result = []
            line = ""
            
            for word in words:
                if len(line) + len(word) + 1 > 50:  # Якщо рядок стає занадто довгим
                    result.append(line)
                    line = word
                else:
                    if line:
                        line += " " + word
                    else:
                        line = word
            
            if line:
                result.append(line)
                
            return "\n".join(result)
        
        # Для інших повідомлень стандартна обробка
        if len(message) > 50:
            # Шукаємо ключові поділювачі
            break_points = [': ', '. ', ', ', ') ', '? ']
            for bp in break_points:
                if bp in message:
                    parts = message.split(bp, 1)
                    if len(parts[0]) > 10:
                        return parts[0] + bp + "\n" + parts[1]
            
            # Якщо не знайдено природного поділу - розбиваємо по довжині
            if len(message) > 50:
                # Спробуємо знайти хороше місце для розриву
                words = message.split()
                if len(words) > 4:
                    mid_point = len(words) // 2
                    first_part = ' '.join(words[:mid_point])
                    second_part = ' '.join(words[mid_point:])
                    return first_part + "\n" + second_part
                
                # Якщо не вдалося розбити за словами
                return message[:50] + "\n" + message[50:]
                
        return message

    def show(self):
        """Показує сповіщення з анімацією"""
        logging.debug(f"Показую сповіщення: {self.message[:30]}...")
        
        # Підіймаємо вікно над іншими
        self.raise_()
        
        super().show()
        self.setWindowOpacity(0.0)
        
        # Анімація появи
        self.show_animation = QPropertyAnimation(self, b"windowOpacity")
        self.show_animation.setDuration(150)
        self.show_animation.setStartValue(0.0)
        self.show_animation.setEndValue(1.0)
        self.show_animation.setEasingCurve(QEasingCurve.Type.OutQuad)
        self.show_animation.start()
        
        # Запускаємо таймери для автозакриття
        if self.auto_close:
            self.close_timer.start(self.timeout)
            self.progress_timer.start(self.progress_interval)
    
    def start_closing(self):
        """Починає процес закриття"""
        if self.is_closing:
            return
            
        self.is_closing = True
        
        if hasattr(self, 'close_timer') and self.close_timer.isActive():
            self.close_timer.stop()
            
        if hasattr(self, 'progress_timer') and self.progress_timer.isActive():
            self.progress_timer.stop()
        
        # Анімація зникнення
        self.close_animation = QPropertyAnimation(self, b"windowOpacity")
        self.close_animation.setDuration(150)
        self.close_animation.setStartValue(self.windowOpacity())
        self.close_animation.setEndValue(0.0)
        self.close_animation.setEasingCurve(QEasingCurve.Type.InQuad)
        self.close_animation.finished.connect(self._on_close_animation_finished)
        self.close_animation.start()
    
    def _on_close_animation_finished(self):
        """Обробляє закінчення анімації закриття"""
        self.closed.emit(self)  # Сигналізуємо менеджеру про закриття
        self.close()  # Закриваємо віджет
    
    def _update_progress(self):
        """Оновлює значення прогрес-бару"""
        if hasattr(self, 'progress_bar'):
            self.progress_value += self.progress_step
            progress_percent = min(100, int(self.progress_value * 100 / self.progress_max))
            self.progress_bar.setValue(progress_percent)
    
    def paintEvent(self, event):
        """Малює фон та рамку сповіщення"""
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        # Створюємо шлях для основного прямокутника
        rect = QRectF(0, 0, self.width() - 1, self.height() - 1)
        radius = 8
        path = QPainterPath()
        path.addRoundedRect(rect, radius, radius)
        
        # Малюємо фон
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(self.bg_color)
        painter.drawPath(path)
        
        # Малюємо тонку рамку
        painter.setPen(QPen(QColor(30, 30, 30), 1))
        painter.setBrush(Qt.BrushStyle.NoBrush)
        painter.drawPath(path)

    def _hide_all_clicked(self):
        """Обробник натискання кнопки приховування всіх сповіщень"""
        self.hide_all.emit()  # Випромінюємо сигнал для менеджера

    def mousePressEvent(self, event):
        """Обробка натискання миші для підтримки свайпу"""
        if self.drag_enabled and event.button() == Qt.MouseButton.LeftButton:
            self.drag_start_position = event.position()
            self.drag_current_position = event.position()
            self.is_dragging = False
        
        # Передаємо подію далі, щоб кнопки працювали
        super().mousePressEvent(event)
    
    def mouseMoveEvent(self, event):
        """Обробка руху миші для підтримки свайпу"""
        if not self.drag_enabled or not self.drag_start_position:
            return super().mouseMoveEvent(event)
            
        # Перевіряємо, чи достатня відстань для початку перетягування
        if not self.is_dragging:
            distance = (event.position() - self.drag_start_position).manhattanLength()
            if distance < self.drag_threshold:
                return super().mouseMoveEvent(event)
            
            # Починаємо перетягування, якщо рух переважно горизонтальний
            delta_x = abs(event.position().x() - self.drag_start_position.x())
            delta_y = abs(event.position().y() - self.drag_start_position.y())
            
            if delta_x > delta_y:
                self.is_dragging = True
            else:
                return super().mouseMoveEvent(event)
        
        # Оновлюємо поточну позицію і переміщуємо віджет
        self.drag_current_position = event.position()
        delta_x = int(self.drag_current_position.x() - self.drag_start_position.x())
        
        # Рухаємо тільки праворуч
        if delta_x > 0:
            # Створюємо ефект "пружини" - чим далі тягнемо, тим важче
            resistance_factor = 0.8
            movement = int(delta_x * resistance_factor)
            
            # Переміщуємо віджет
            self.move(self.x() + movement, self.y())
            # Оновлюємо початкову позицію для наступного переміщення
            self.drag_start_position = event.position()
            
            # Змінюємо прозорість залежно від відстані свайпу
            if delta_x > 0:
                fade_factor = max(0.3, 1.0 - delta_x / (self.width() * 1.5))
                self.setWindowOpacity(fade_factor)
    
    def mouseReleaseEvent(self, event):
        """Обробка відпускання кнопки миші"""
        if self.is_dragging and self.drag_start_position and self.drag_current_position:
            # Визначаємо загальну відстань свайпу
            total_delta_x = self.x() - self._original_position().x()
            
            # Якщо свайп достатньо довгий, закриваємо сповіщення
            if total_delta_x > self.swipe_threshold:
                self._complete_swipe_animation()
            else:
                # Інакше повертаємо на місце
                self._cancel_swipe_animation()
            
            self.is_dragging = False
            self.drag_start_position = None
            self.drag_current_position = None
            
            # Обробляємо подію
            event.accept()
            return
            
        # Передаємо подію далі, якщо це не було перетягуванням
        super().mouseReleaseEvent(event)
    
    def _original_position(self):
        """Повертає оригінальну позицію сповіщення"""
        if hasattr(self, 'original_pos'):
            return self.original_pos
            
        # Якщо оригінальна позиція не збережена, беремо батьківську точку
        parent = self.parent()
        if parent:
            screen_width = parent.screen().geometry().width()
            margin = 20
            x = screen_width - self.width() - margin
            y = self.y()
            self.original_pos = QPoint(x, y)
            return self.original_pos
        
        # За замовчуванням
        return QPoint(self.x(), self.y())
    
    def _complete_swipe_animation(self):
        """Завершує свайп анімацією"""
        # Анімація переміщення за межі екрану
        self.swipe_animation = QPropertyAnimation(self, b"pos")
        self.swipe_animation.setDuration(200)
        self.swipe_animation.setStartValue(self.pos())
        
        # Визначаємо кінцеву позицію за межами екрану праворуч
        screen_width = self.screen().geometry().width()
        end_pos = QPoint(screen_width + 50, self.y())
        self.swipe_animation.setEndValue(end_pos)
        
        self.swipe_animation.setEasingCurve(QEasingCurve.Type.OutQuad)
        self.swipe_animation.finished.connect(self._on_close_animation_finished)
        self.swipe_animation.start()
    
    def _cancel_swipe_animation(self):
        """Скасовує свайп, повертаючи сповіщення на місце"""
        # Анімація повернення на місце
        self.return_animation = QPropertyAnimation(self, b"pos")
        self.return_animation.setDuration(150)
        self.return_animation.setStartValue(self.pos())
        self.return_animation.setEndValue(self._original_position())
        self.return_animation.setEasingCurve(QEasingCurve.Type.OutBounce)
        
        # Паралельна анімація відновлення прозорості
        self.opacity_animation = QPropertyAnimation(self, b"windowOpacity")
        self.opacity_animation.setDuration(150)
        self.opacity_animation.setStartValue(self.windowOpacity())
        self.opacity_animation.setEndValue(1.0)
        
        # Запускаємо обидві анімації
        self.return_animation.start()
        self.opacity_animation.start()


class NotificationQueue(QObject):
    """
    Черга сповіщень для послідовного відображення
    """
    def __init__(self):
        super().__init__()
        self.queue = []  # черга повідомлень
        self.current_notification = None  # поточне активне сповіщення
        self.is_processing = False  # флаг обробки черги
    
    def add(self, message, error=False, timeout=4000):
        """Додає повідомлення в чергу"""
        self.queue.append((message, error, timeout))
        if not self.is_processing:
            self._process_next()
    
    def _process_next(self):
        """Обробляє наступне повідомлення в черзі"""
        if not self.queue or self.current_notification is not None:
            self.is_processing = False
            return
            
        self.is_processing = True
        message, error, timeout = self.queue.pop(0)
        
        # Створюємо нове сповіщення через менеджера
        self.current_notification = NotificationManager.instance.show_notification(
            message, error, timeout
        )
        
        # Підключаємо сигнал закриття
        if self.current_notification:
            self.current_notification.closed.connect(self._on_notification_closed)
    
    def _on_notification_closed(self, notification):
        """Обробляє закриття поточного сповіщення"""
        self.current_notification = None
        # Затримка перед показом наступного сповіщення
        QTimer.singleShot(100, self._process_next)
    
    def clear(self):
        """Очищає чергу повідомлень"""
        self.queue.clear()
        if self.current_notification and not self.current_notification.is_closing:
            self.current_notification.start_closing()


class NotificationIndicator(QWidget):
    """
    Мінімалістичний індикатор, який з'являється при приховуванні сповіщень
    і дозволяє розгорнути їх знову
    """
    clicked = pyqtSignal()  # Сигнал кліку
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Налаштування віджета - додаємо додаткові прапори для гарантованого показу над усіма вікнами
        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint | 
            Qt.WindowType.Tool | 
            Qt.WindowType.WindowStaysOnTopHint |
            Qt.WindowType.BypassWindowManagerHint
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setAttribute(Qt.WidgetAttribute.WA_ShowWithoutActivating)
        
        # Додаємо запис у журнал для відстеження
        logging.debug("Індикатор прихованих сповіщень створено")
        
        # Фіксовані розміри індикатора
        self.setFixedSize(24, 24)
        
        # Налаштування кольорів
        self.bg_color = QColor(60, 60, 60)
        self.accent_color = QColor(120, 81, 169)  # #7851A9 - фіолетовий
        
        # Кількість прихованих сповіщень
        self.hidden_count = 0
        
        # Таймер для анімації
        self.pulse_timer = QTimer(self)
        self.pulse_timer.timeout.connect(self._pulse_animation)
        self.pulse_timer.start(1000)  # Пульсація кожну секунду
        self.pulse_state = False
        
        # Підказка
        self.setToolTip("Показати приховані сповіщення")
        self.setCursor(Qt.CursorShape.PointingHandCursor)
    
    def set_count(self, count):
        """Встановлює кількість прихованих сповіщень"""
        self.hidden_count = count
        self.update()  # Оновлюємо відображення
        
        # Якщо сповіщень немає, ховаємо індикатор
        if count == 0:
            self.hide()
            logging.debug("Індикатор приховано (кількість = 0)")
        else:
            # Забезпечуємо видимість та підняття над іншими вікнами
            self.show()
            self.raise_()
            self.setWindowState((self.windowState() & ~Qt.WindowState.WindowMinimized) | Qt.WindowState.WindowActive)
            logging.debug(f"Індикатор показано (кількість = {count})")
            
            # Для гарантованого відображення використовуємо активацію
            self.activateWindow()
    
    def paintEvent(self, event):
        """Малює індикатор у вигляді кола з цифрою"""
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        # Малюємо коло
        rect = QRectF(1, 1, self.width() - 2, self.height() - 2)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(self.bg_color)
        painter.drawEllipse(rect)
        
        # Малюємо обрамлення
        border_color = self.accent_color
        if self.pulse_state:
            border_color = QColor(150, 100, 210)  # Світліший колір для пульсації
            
        painter.setPen(QPen(border_color, 2))
        painter.setBrush(Qt.BrushStyle.NoBrush)
        painter.drawEllipse(rect)
        
        # Малюємо тексто
        painter.setPen(QColor(255, 255, 255))
        font = QFont("Arial", 9, QFont.Weight.Bold)
        painter.setFont(font)
        
        # Відображаємо кількість або іконку
        text = str(self.hidden_count) if self.hidden_count <= 9 else "9+"
        painter.drawText(rect, Qt.AlignmentFlag.AlignCenter, text)
    
    def _pulse_animation(self):
        """Пульсація для привертання уваги"""
        self.pulse_state = not self.pulse_state
        self.update()
    
    def mousePressEvent(self, event):
        """Обробка кліку для розгортання сповіщень"""
        self.clicked.emit()
        event.accept()


class NotificationManager(QObject):
    """
    Одиночний менеджер для керування сповіщеннями
    """
    instance = None  # Єдиний екземпляр (singleton)
    
    def __new__(cls, parent=None):
        if cls.instance is None:
            logging.debug("Створюю новий екземпляр NotificationManager")
            cls.instance = super(NotificationManager, cls).__new__(cls)
            cls.instance.initialized = False
        return cls.instance
    
    def __init__(self, parent=None):
        # Спочатку викликаємо super().__init__ незалежно від того, 
        # чи ініціалізовано вже об'єкт
        if hasattr(self, 'initialized') and self.initialized:
            # Якщо вже ініціалізовано, просто оновлюємо батьківський об'єкт
            self.parent = parent
            logging.debug(f"NotificationManager вже ініціалізовано, оновлюю батьківський об'єкт: {parent}")
            return
            
        super().__init__(parent)
        self.initialized = True
        self.parent = parent
        self.notifications = []  # список активних сповіщень
        self.notification_queue = NotificationQueue()  # черга сповіщень
        
        # Прапорець для відстеження приховування сповіщень
        self.are_notifications_hidden = False
        self.hidden_notifications = []  # Сховані сповіщення
        
        # Індикатор прихованих сповіщень
        self.indicator = None
        
        # Словник для групування однотипних помилок
        self.error_groups = {}
        self.last_error_time = {}
        
        # Таймер для дроселювання помилок (обмеження частоти)
        self.throttle_timer = QTimer(self)
        self.throttle_timer.timeout.connect(self._process_throttled_errors)
        self.throttle_timer.setSingleShot(True)
        self.is_throttling = False
        self.throttled_errors = {}
        
        logging.debug(f"Ініціалізую NotificationManager з батьківським об'єктом: {parent}")
        
        # Отримуємо розмір екрану
        if parent:
            screen_geometry = parent.screen().geometry()
            self.screen_width = screen_geometry.width()
            self.screen_height = screen_geometry.height()
            logging.debug(f"Розмір екрану: {self.screen_width}x{self.screen_height}")
            
        # Налаштування таймера для очищення старих сповіщень
        self.cleanup_timer = QTimer(self)
        self.cleanup_timer.timeout.connect(self.cleanup_notifications)
        self.cleanup_timer.start(5000)  # Кожні 5 секунд
    
    def _extract_error_key(self, message):
        """Витягує ключ для групування однотипних помилок"""
        # Для SQL помилок використовуємо першу частину до ':'
        if "SQL" in message and ":" in message:
            return message.split(":", 1)[0]
        
        # Для парсингу аркушів беремо ім'я аркуша
        if "аркуші" in message and ":" in message:
            return message.split(":", 1)[0]
            
        # Шукаємо ключові шаблони в повідомленні
        key_patterns = [
            "відсутні номери продуктів",
            "argument of AND",
            "не знайдено продукт",
            "помилка парсингу"
        ]
        
        for pattern in key_patterns:
            if pattern in message.lower():
                return pattern
                
        # Використовуємо перші 30 символів як ключ
        return message[:min(30, len(message))]
    
    def _process_throttled_errors(self):
        """Обробляє накопичені помилки, групуючи їх разом"""
        if not self.throttled_errors:
            self.is_throttling = False
            return
            
        # Створюємо копію словника, щоб уникнути помилки зміни розміру під час ітерації
        throttled_errors_copy = self.throttled_errors.copy()
        error_groups_copy = self.error_groups.copy()
            
        # Якщо є згруповані помилки, показуємо їх загальну кількість
        for error_key, count in throttled_errors_copy.items():
            try:
                if count > 1:
                    # Формуємо узагальнене повідомлення
                    base_message = error_key
                    if len(base_message) > 40:
                        base_message = base_message[:37] + "..."
                        
                    summary_message = f"{base_message}\nВсього: {count} аналогічних помилок"
                    self.show_notification(summary_message, True, 5000, False)
                else:
                    # Якщо тільки одна помилка цього типу, показуємо її як є
                    original_message = error_groups_copy.get(error_key, error_key)
                    self.show_notification(original_message, True, 5000, False)
            except Exception as e:
                logging.error(f"Помилка при обробці групованої помилки '{error_key}': {e}", exc_info=True)
        
        # Очищуємо накопичені помилки
        self.throttled_errors.clear()
        self.error_groups.clear()
        self.is_throttling = False
    
    def show_notification(self, message, error=False, timeout=4000, use_queue=True):
        """Показує нове сповіщення"""
        # Базові перевірки
        if not self.parent or not message or message.strip() == "":
            logging.warning(f"Не вдалося показати сповіщення, батьківський об'єкт: {self.parent}, повідомлення: {message}")
            return None
        
        # Якщо сповіщення приховані, то не показуємо нові
        if self.are_notifications_hidden:
            logging.debug(f"Сповіщення приховані, додаємо в чергу прихованих: {message[:30]}...")
            self.hidden_notifications.append((message, error, timeout))
            return None
        
        # Для помилок використовуємо спеціальний механізм групування і дроселювання
        if error:
            current_time = time.time()
            error_key = self._extract_error_key(message)
            
            # Якщо вже є аналогічні помилки в очікуванні, збільшуємо лічильник
            if self.is_throttling:
                self.throttled_errors[error_key] = self.throttled_errors.get(error_key, 0) + 1
                self.error_groups[error_key] = message  # Зберігаємо оригінальне повідомлення
                logging.debug(f"Додано до групи помилок: {error_key}, всього: {self.throttled_errors[error_key]}")
                return None
                
            # Перевіряємо, чи нещодавно вже показувалась ця помилка
            if error_key in self.last_error_time:
                last_time = self.last_error_time[error_key]
                if current_time - last_time < 5.0:  # Не показуємо ту саму помилку частіше ніж раз на 5 секунд
                    # Починаємо групувати помилки
                    self.is_throttling = True
                    self.throttled_errors[error_key] = 1
                    self.error_groups[error_key] = message
                    
                    # Через 1 секунду покажемо всі накопичені помилки
                    self.throttle_timer.start(1000)
                    logging.debug(f"Запущено дроселювання помилок: {error_key}")
                    return None
            
            # Оновлюємо час останнього показу цієї помилки
            self.last_error_time[error_key] = current_time
            
        # Якщо потрібно використовувати чергу, додаємо в чергу
        if use_queue and self.notification_queue:
            logging.debug(f"Додаю повідомлення в чергу: {message[:30]}...")
            self.notification_queue.add(message, error, timeout)
            return None
            
        try:
            logging.debug(f"Показую нове сповіщення: {message[:30]}...")
            
            # Закриваємо всі активні сповіщення
            self.close_all()
            
            # Створюємо нове сповіщення
            notification = NotificationWidget(self.parent, message, error, timeout)
            notification.closed.connect(lambda n: self._remove_notification(n))
            notification.hide_all.connect(self.hide_all_notifications)
            
            # Позиціонуємо і показуємо
            position = self._calculate_position()
            logging.debug(f"Позиція сповіщення: {position.x()}, {position.y()}")
            notification.move(position)
            self.notifications.append(notification)
            notification.show()
            
            return notification
        except Exception as e:
            logging.error(f"Помилка при створенні сповіщення: {e}", exc_info=True)
            return None
    
    def _calculate_position(self):
        """Розраховує позицію для нового сповіщення"""
        # Забезпечуємо коректну позицію навіть якщо розмір екрану не визначено
        screen_width = getattr(self, 'screen_width', 1000)
        margin = 20
        x = screen_width - 360 - margin
        y = 70  # Відступ від верху
        return QPoint(x, y)
    
    def _remove_notification(self, notification):
        """Видаляє сповіщення зі списку активних"""
        if notification in self.notifications:
            logging.debug(f"Видаляю сповіщення зі списку")
            self.notifications.remove(notification)
    
    def cleanup_notifications(self):
        """Очищає неактивні сповіщення"""
        active_notifications = []
        for notification in self.notifications:
            if notification.isVisible() and not notification.is_closing:
                active_notifications.append(notification)
        
        if len(self.notifications) != len(active_notifications):
            logging.debug(f"Очищено {len(self.notifications) - len(active_notifications)} неактивних сповіщень")
            
        self.notifications = active_notifications
    
    def close_all(self):
        """Закриває всі активні сповіщення"""
        closed_count = 0
        for notification in list(self.notifications):
            if notification.isVisible() and not notification.is_closing:
                notification.start_closing()
                closed_count += 1
        
        if closed_count > 0:
            logging.debug(f"Закрито {closed_count} активних сповіщень")
        
        # Очищаємо чергу сповіщень
        if hasattr(self, 'notification_queue'):
            self.notification_queue.clear()
    
    def showNotification(self, message, error=False, timeout=4000):
        """Сумісність зі старим API"""
        logging.debug(f"Виклик starого API showNotification: {message[:30]}...")
        
        # Викликаємо новий метод з прямим виведенням (без черги)
        return self.show_notification(message, error, timeout, use_queue=False)

    def hide_all_notifications(self):
        """Приховує всі сповіщення"""
        logging.debug("Приховую всі сповіщення")
        self.are_notifications_hidden = True
        
        # Закриваємо всі активні сповіщення і зберігаємо їх у списку прихованих
        for notification in list(self.notifications):
            if notification.isVisible() and not notification.is_closing:
                # Зберігаємо повідомлення в список прихованих перед закриттям
                self.hidden_notifications.append((notification.message, notification.error, notification.timeout))
                notification.start_closing()
                
        # Очищаємо чергу і зберігаємо її елементи в список прихованих
        if hasattr(self, 'notification_queue'):
            # Зберігаємо всі повідомлення з черги в список прихованих
            for message, error, timeout in self.notification_queue.queue:
                self.hidden_notifications.append((message, error, timeout))
            self.notification_queue.clear()
            
        # Для тестування додаємо одне тестове сповіщення, якщо список порожній
        if not self.hidden_notifications:
            logging.debug("Додаю тестове приховане сповіщення для перевірки індикатора")
            self.hidden_notifications.append(("Тестове приховане сповіщення", False, 4000))
        
        # Показуємо індикатор прихованих сповіщень
        self._update_indicator()
        logging.debug(f"Після приховування є {len(self.hidden_notifications)} прихованих сповіщень")
        
    def show_hidden_notifications(self):
        """Відновлює відображення прихованих сповіщень"""
        if not self.are_notifications_hidden:
            return
            
        logging.debug("Відновлюю відображення прихованих сповіщень")
        self.are_notifications_hidden = False
        
        # Приховуємо індикатор
        if self.indicator and self.indicator.isVisible():
            self.indicator.hide()
        
        # Показуємо приховані сповіщення
        hidden_copy = self.hidden_notifications.copy()
        self.hidden_notifications.clear()
        
        # Показуємо їх у зворотному порядку, щоб більш нові були видні
        for message, error, timeout in reversed(hidden_copy):
            self.show_notification(message, error, timeout, use_queue=False)
    
    def _update_indicator(self):
        """Оновлює індикатор прихованих сповіщень"""
        if not self.parent:
            logging.warning("Не вдалося оновити індикатор: немає батьківського об'єкта")
            return
            
        hidden_count = len(self.hidden_notifications)
        logging.debug(f"Оновлення індикатора прихованих сповіщень: {hidden_count} сповіщень")
        
        # Якщо індикатор ще не створений, створюємо його
        if not self.indicator:
            logging.debug("Створюю новий індикатор прихованих сповіщень")
            self.indicator = NotificationIndicator(self.parent)
            self.indicator.clicked.connect(self.show_hidden_notifications)
        
        # Оновлюємо кількість і позицію
        self.indicator.set_count(hidden_count)
        
        # Позиціонуємо його у правому верхньому куті
        position = self._calculate_indicator_position()
        self.indicator.move(position)
        
        # Показуємо індикатор, якщо є що показувати
        if hidden_count > 0:
            # Гарантуємо, що індикатор буде видно
            QTimer.singleShot(100, lambda: self._ensure_indicator_visible())
        else:
            self.indicator.hide()
            logging.debug("Індикатор приховано, оскільки немає прихованих сповіщень")
    
    def _calculate_indicator_position(self):
        """Розраховує позицію для індикатора"""
        screen_width = getattr(self, 'screen_width', 1000)
        x = screen_width - 30
        y = 30  # Відступ від верху
        return QPoint(x, y)

    def _ensure_indicator_visible(self):
        """Гарантує, що індикатор видно"""
        if self.indicator and len(self.hidden_notifications) > 0:
            self.indicator.show()
            self.indicator.raise_()
            logging.debug("Забезпечено видимість індикатора прихованих сповіщень")

    def toggle_notifications_visibility(self):
        """Перемикає видимість сповіщень"""
        logging.debug(f"Перемикаю видимість сповіщень. Поточний стан: {'приховані' if self.are_notifications_hidden else 'видимі'}")
        if self.are_notifications_hidden:
            self.show_hidden_notifications()
        else:
            self.hide_all_notifications() 