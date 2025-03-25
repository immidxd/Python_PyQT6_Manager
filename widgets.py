from PyQt6 import QtCore
from PyQt6.QtWidgets import (
 QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QCheckBox, QLabel,
 QSlider, QScrollArea, QPushButton, QSizePolicy, QComboBox, QGridLayout,
 QGraphicsDropShadowEffect, QStyleOptionSlider, QStyle, QGraphicsOpacityEffect
)
from PyQt6.QtGui import QPainter, QPen, QColor, QFont
from PyQt6.QtCore import Qt, pyqtSignal, QPropertyAnimation, QEasingCurve, QPoint
import qtawesome as qta








class FocusableSearchLineEdit(QLineEdit):
 """
 Спеціальний QLineEdit, який зберігає тінь при фокусі
 та не збиває візуальних стилів при втраті фокуса.
 """
 def __init__(self, parent=None):
     super().__init__(parent)
     self.setFixedHeight(35)
     self._current_style = ""  # Для збереження поточного стилю








 def focusInEvent(self, event):
     super().focusInEvent(event)
     shadow = QGraphicsDropShadowEffect()
     shadow.setBlurRadius(10)
     shadow.setColor(QColor(0, 0, 0, 80))
     shadow.setOffset(0, 0)
     self.setGraphicsEffect(shadow)
     self._current_style = self.styleSheet()  # Зберігаємо поточний стиль








 def focusOutEvent(self, event):
     super().focusOutEvent(event)
     self.setGraphicsEffect(None)








 def keyPressEvent(self, event):
     """Перевизначаємо обробку натискання клавіш, щоб зберігати стиль при Enter"""
     super().keyPressEvent(event)
     
     # Якщо натиснуто Enter, забезпечуємо збереження поточного стилю
     if event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
         if self._current_style:
             self.setStyleSheet(self._current_style)
             # Додаємо таймер, щоб відновити стиль після того, як сигнал returnPressed буде оброблений
             QtCore.QTimer.singleShot(10, lambda: self.setStyleSheet(self._current_style))








class RangeSlider(QSlider):
 """
 Слайдер з двома ручками для вибору діапазону (low/high).
 Покращений і оптимізований для компактного відображення
 з плавною анімацією при взаємодії.
 """
 valueChanged = pyqtSignal(int)








 def __init__(self, *args, **kwargs):
     super().__init__(*args, **kwargs)
     self.setOrientation(Qt.Orientation.Horizontal)
     self.low = self.minimum()
     self.high = self.maximum()
     self.setTickPosition(QSlider.TickPosition.NoTicks)
     self.setTickInterval(1)
     self.pressed_control = None
     self.hover_control = None
     self.setMouseTracking(True)








     # Зменшуємо розміри для компактності
     self.handle_width = 20  # Було 18, збільшуємо до 20
     self.groove_height = 4  # Зберігаємо попереднє значення
   
     # Ці відступи можна коригувати ззовні
     self.left_margin = 15  # Збільшено для надійності, щоб кружок не вилазив зліва
     self.right_margin = 15  # Також збільшено для симетрії








     # Розміри для анімації при взаємодії
     self.handle_normal_width = 20  # Було 18, збільшуємо до 20
     self.handle_hover_width = 26  # Було 24, збільшуємо до 26
     self.handle_pressed_width = 28  # Було 26, збільшуємо до 28
     
     # Відслідковування стану кружечків для анімації
     self._min_handle_width = self.handle_normal_width
     self._max_handle_width = self.handle_normal_width
     
     # Анімації для плавної зміни розмірів кружечків
     self.min_animation = QPropertyAnimation(self, b"min_handle_width")
     self.min_animation.setDuration(150)
     self.min_animation.setEasingCurve(QEasingCurve.Type.OutCubic)
     
     self.max_animation = QPropertyAnimation(self, b"max_handle_width")
     self.max_animation.setDuration(150)
     self.max_animation.setEasingCurve(QEasingCurve.Type.OutCubic)








     # Забезпечуємо достатньо місця для слайдера
     self.setMinimumHeight(30)  # Зменшено з 36
     self.setMinimumWidth(200)  # Зменшено з 300








 def paintEvent(self, event):
     painter = QPainter(self)
     style = self.style()
     option = self.styleOption()








     # Отримуємо groove_rect (смугу) й коригуємо його
     groove_rect = style.subControlRect(
         QStyle.ComplexControl.CC_Slider,
         option,
         QStyle.SubControl.SC_SliderGroove,
         self
     )
     groove_rect.setHeight(self.groove_height)
     
     # Робимо groove коротшим з обох боків, щоб він не випирав з-під кружечків
     groove_rect.setLeft(self.left_margin + 10) # Відступ зліва, щоб вісь не випирала з-під кружечка
     groove_rect.setRight(self.width() - self.right_margin - 10) # Відступ справа
     
     groove_rect.moveCenter(QtCore.QPoint(groove_rect.center().x(), self.rect().center().y()))

     # Малюємо підкладку "groove"
     painter.setPen(QPen(QColor('#f0f0f0'), 0))
     painter.setBrush(QColor('#f0f0f0'))
     painter.drawRect(groove_rect)

     # Обчислюємо позиції ручок
     # Коригуємо доступну ширину з врахуванням зміненого groove_rect
     available_width = groove_rect.width()
     style_min_pos = style.sliderPositionFromValue(
         self.minimum(), self.maximum(), self.low, available_width
     )
     style_max_pos = style.sliderPositionFromValue(
         self.minimum(), self.maximum(), self.high, available_width
     )
     
     # Центруємо ручки відносно нових меж groove
     min_pos = groove_rect.left() + style_min_pos
     max_pos = groove_rect.left() + style_max_pos








     # Виділений "фіолетовий" діапазон
     selected_range_rect = QtCore.QRect(
         min(min_pos, max_pos),
         groove_rect.y(),
         abs(max_pos - min_pos),
         groove_rect.height()
     )
     painter.setPen(QPen(QColor('#7851A9'), 0))
     painter.setBrush(QColor('#7851A9'))
     painter.drawRect(selected_range_rect)








     # Малюємо ручки (кружечки) з анімованим розміром
     # Ліва ручка
     min_handle_rect = QtCore.QRect(0, 0, int(self._min_handle_width), int(self._min_handle_width))
     min_handle_pos = QtCore.QPoint(min_pos, self.rect().center().y())
     
     # Гарантуємо, що кружечок не виходить за ліву межу
     if min_handle_pos.x() - int(self._min_handle_width) // 2 < 0:
         min_handle_pos.setX(int(self._min_handle_width) // 2)
     
     min_handle_rect.moveCenter(min_handle_pos)
     painter.setPen(QPen(QColor('#7851A9')))
     painter.setBrush(QColor('#ffffff'))
     painter.drawEllipse(min_handle_rect)








     # Права ручка
     max_handle_rect = QtCore.QRect(0, 0, int(self._max_handle_width), int(self._max_handle_width))
     max_handle_pos = QtCore.QPoint(max_pos, self.rect().center().y())
     
     # Гарантуємо, що кружечок не виходить за праву межу
     if max_handle_pos.x() + int(self._max_handle_width) // 2 > self.width() - self.right_margin // 2:
         max_handle_pos.setX(self.width() - self.right_margin // 2 - int(self._max_handle_width) // 2)
     
     max_handle_rect.moveCenter(max_handle_pos)
     painter.drawEllipse(max_handle_rect)








 def styleOption(self):
     option = QStyleOptionSlider()
     self.initStyleOption(option)
     return option








 def mousePressEvent(self, event):
     style = self.style()
     option = self.styleOption()








     groove_rect = style.subControlRect(
         QStyle.ComplexControl.CC_Slider,
         option,
         QStyle.SubControl.SC_SliderGroove,
         self
     )
     groove_rect.setHeight(self.groove_height)
     groove_rect.setLeft(self.left_margin + 10)
     groove_rect.setRight(self.width() - self.right_margin - 10)








     available_width = groove_rect.width()
     style_min_pos = style.sliderPositionFromValue(
         self.minimum(), self.maximum(), self.low, available_width
     )
     style_max_pos = style.sliderPositionFromValue(
         self.minimum(), self.maximum(), self.high, available_width
     )
     min_pos = groove_rect.left() + style_min_pos
     max_pos = groove_rect.left() + style_max_pos








     # Прямокутники handle для визначення, який кружечок натиснуто
     min_handle_rect = QtCore.QRect(0, 0, int(self._min_handle_width), int(self._min_handle_width))
     min_handle_rect.moveCenter(QtCore.QPoint(min_pos, self.rect().center().y()))
     max_handle_rect = QtCore.QRect(0, 0, int(self._max_handle_width), int(self._max_handle_width))
     max_handle_rect.moveCenter(QtCore.QPoint(max_pos, self.rect().center().y()))








     if min_handle_rect.contains(event.pos()):
         self.pressed_control = 'min'
         self.setCursor(Qt.CursorShape.SplitHCursor)
         
         # Анімація збільшення лівого кружечка
         self.min_animation.stop()
         self.min_animation.setStartValue(self._min_handle_width)
         self.min_animation.setEndValue(self.handle_pressed_width)
         self.min_animation.start()
         
     elif max_handle_rect.contains(event.pos()):
         self.pressed_control = 'max'
         self.setCursor(Qt.CursorShape.SplitHCursor)
         
         # Анімація збільшення правого кружечка
         self.max_animation.stop()
         self.max_animation.setStartValue(self._max_handle_width)
         self.max_animation.setEndValue(self.handle_pressed_width)
         self.max_animation.start()
     else:
         self.pressed_control = None
     self.update()








 def mouseReleaseEvent(self, event):
     previous_control = self.pressed_control
     self.pressed_control = None
     self.unsetCursor()
     
     # Анімація зменшення кружечка після відпускання
     if previous_control == 'min':
         self.min_animation.stop()
         self.min_animation.setStartValue(self._min_handle_width)
         self.min_animation.setEndValue(self.handle_normal_width)
         self.min_animation.start()
     elif previous_control == 'max':
         self.max_animation.stop()
         self.max_animation.setStartValue(self._max_handle_width)
         self.max_animation.setEndValue(self.handle_normal_width)
         self.max_animation.start()
     
     self.update()








 def mouseMoveEvent(self, event):
     if self.pressed_control is not None:
         style = self.style()
         option = self.styleOption()

         groove_rect = style.subControlRect(
             QStyle.ComplexControl.CC_Slider,
             option,
             QStyle.SubControl.SC_SliderGroove,
             self
         )
         groove_rect.setHeight(self.groove_height)
         groove_rect.setLeft(self.left_margin + 10)
         groove_rect.setRight(self.width() - self.right_margin - 10)

         available_width = groove_rect.width()
         new_value = style.sliderValueFromPosition(
             self.minimum(),
             self.maximum(),
             event.pos().x() - groove_rect.left(),
             available_width
         )

         if self.pressed_control == 'min':
             self.low = max(min(new_value, self.high), self.minimum())
         elif self.pressed_control == 'max':
             self.high = min(max(new_value, self.low), self.maximum())

         self.valueChanged.emit(self.low)
         self.valueChanged.emit(self.high)
         self.update()
     else:
         # Обробка наведення миші (hover) на кружечки
         style = self.style()
         option = self.styleOption()
         
         groove_rect = style.subControlRect(
             QStyle.ComplexControl.CC_Slider,
             option,
             QStyle.SubControl.SC_SliderGroove,
             self
         )
         groove_rect.setHeight(self.groove_height)
         groove_rect.setLeft(self.left_margin + 10)
         groove_rect.setRight(self.width() - self.right_margin - 10)
         
         available_width = groove_rect.width()
         style_min_pos = style.sliderPositionFromValue(
             self.minimum(), self.maximum(), self.low, available_width
         )
         style_max_pos = style.sliderPositionFromValue(
             self.minimum(), self.maximum(), self.high, available_width
         )
         
         min_pos = groove_rect.left() + style_min_pos
         max_pos = groove_rect.left() + style_max_pos
         
         # Прямокутники для перевірки наведення - збільшуємо область для визначення наведення
         hover_area_size = int(self.handle_hover_width * 1.5)  # Збільшена область виявлення
         min_handle_rect = QtCore.QRect(0, 0, hover_area_size, hover_area_size)
         min_handle_rect.moveCenter(QtCore.QPoint(min_pos, self.rect().center().y()))
         max_handle_rect = QtCore.QRect(0, 0, hover_area_size, hover_area_size)
         max_handle_rect.moveCenter(QtCore.QPoint(max_pos, self.rect().center().y()))
         
         old_hover_control = self.hover_control
         
         # Визначаємо, чи знаходиться курсор на кружечку
         if min_handle_rect.contains(event.pos()):
             self.hover_control = 'min'
             self.setCursor(Qt.CursorShape.SplitHCursor)
             if old_hover_control != 'min':
                 # Анімація збільшення при наведенні на лівий кружечок
                 self.min_animation.stop()
                 self.min_animation.setStartValue(self._min_handle_width)
                 self.min_animation.setEndValue(self.handle_hover_width)
                 self.min_animation.start()
         elif max_handle_rect.contains(event.pos()):
             self.hover_control = 'max'
             self.setCursor(Qt.CursorShape.SplitHCursor)
             if old_hover_control != 'max':
                 # Анімація збільшення при наведенні на правий кружечок
                 self.max_animation.stop()
                 self.max_animation.setStartValue(self._max_handle_width)
                 self.max_animation.setEndValue(self.handle_hover_width)
                 self.max_animation.start()
         else:
             # Якщо курсор не на жодному з кружечків - забезпечуємо зменшення
             if old_hover_control == 'min' or self.hover_control == 'min':
                 # Анімація зменшення лівого кружечка при виході з нього
                 self.min_animation.stop()
                 self.min_animation.setStartValue(self._min_handle_width)
                 self.min_animation.setEndValue(self.handle_normal_width)
                 self.min_animation.start()
             elif old_hover_control == 'max' or self.hover_control == 'max':
                 # Анімація зменшення правого кружечка при виході з нього
                 self.max_animation.stop()
                 self.max_animation.setStartValue(self._max_handle_width)
                 self.max_animation.setEndValue(self.handle_normal_width)
                 self.max_animation.start()
                 
             self.hover_control = None
             self.unsetCursor()
             
         if old_hover_control != self.hover_control:
             self.update()








 def leaveEvent(self, event):
     """Обробка події виходу курсору за межі слайдера"""
     # Анімація зменшення обох кружечків при виході курсору з області слайдера
     if self.hover_control == 'min' or self._min_handle_width > self.handle_normal_width:
         self.min_animation.stop()
         self.min_animation.setStartValue(self._min_handle_width)
         self.min_animation.setEndValue(self.handle_normal_width)
         self.min_animation.start()
     
     if self.hover_control == 'max' or self._max_handle_width > self.handle_normal_width:
         self.max_animation.stop()
         self.max_animation.setStartValue(self._max_handle_width)
         self.max_animation.setEndValue(self.handle_normal_width)
         self.max_animation.start()
     
     self.hover_control = None
     self.unsetCursor()
     self.update()
     
     super().leaveEvent(event)








 def setLow(self, value):
     self.low = value
     self.update()








 def setHigh(self, value):
     self.high = value
     self.update()








 # Qt property для анімації мінімального кружечка
 def get_min_handle_width(self):
     return self._min_handle_width
     
 def set_min_handle_width(self, width):
     self._min_handle_width = width
     self.update()
     
 min_handle_width = QtCore.pyqtProperty(float, get_min_handle_width, set_min_handle_width)
 
 # Qt property для анімації максимального кружечка
 def get_max_handle_width(self):
     return self._max_handle_width
     
 def set_max_handle_width(self, width):
     self._max_handle_width = width
     self.update()
     
 max_handle_width = QtCore.pyqtProperty(float, get_max_handle_width, set_max_handle_width)








class CollapsibleSection(QWidget):
 """
 Секція з можливістю згортання/розгортання (анімація).
 Виправлено "дьоргання":
   - Задали тривалість (600ms) і плавну криву (InOutQuint).
   - on_animation_finished -> setVisible(False) якщо згорнули.
 """
 toggle_animation_finished = pyqtSignal()








 def __init__(self, title="", parent=None):
     super().__init__(parent)
     self.title = title








     # Кнопка, що розгортає/згортає контент
     self.toggle_button = QPushButton(title)
     self.toggle_button.setCheckable(True)
     self.toggle_button.setChecked(False)








     self.toggle_button.setStyleSheet("""
       QPushButton {
           text-align: left;
           padding: 6px;
           background-color: transparent;
           border: none;
           font-weight: bold;
           font-size:14pt;
           color: black;
       }
       QPushButton:hover {
           background-color: rgba(0,0,0,0.03);
           border-radius:3px;
       }
     """)
     self.toggle_button.clicked.connect(self.on_toggle)








     self.content_area = QWidget()
     self.content_area.setVisible(False)
     self.content_area.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)








     self.content_layout = QVBoxLayout()
     self.content_area.setLayout(self.content_layout)








     self.main_layout = QVBoxLayout(self)
     self.main_layout.addWidget(self.toggle_button)
     self.main_layout.addWidget(self.content_area)
     self.main_layout.setContentsMargins(0, 0, 0, 0)
     self.main_layout.setSpacing(0)
     self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Minimum)








     # Іконки зі стрілочками
     self.arrow_down_icon = qta.icon('fa5s.angle-down', color='black')
     self.arrow_up_icon = qta.icon('fa5s.angle-up', color='black')
     self.update_arrow_icon(False)








     # Анімація
     self.animation = QPropertyAnimation(self.content_area, b"maximumHeight")
     self.animation.setDuration(500)  # Трохи швидша анімація
     self.animation.setEasingCurve(QEasingCurve.Type.InOutQuint)
     self.animation.setStartValue(0)
     self.animation.setEndValue(0)
     self.animation.finished.connect(self.on_animation_finished)








     # Для зручності
     self.toggle_animation = self.animation








 def on_toggle(self):
     checked = self.toggle_button.isChecked()
     self.animation.stop()








     start_val = self.content_area.height()
     if checked:
         self.content_area.setVisible(True)
         self.animation.setStartValue(start_val)
         end_val = self.content_area.sizeHint().height()
         self.animation.setEndValue(end_val)
         self.animation.start()
     else:
         self.animation.setStartValue(start_val)
         self.animation.setEndValue(0)
         self.animation.start()








     self.update_arrow_icon(checked)








 def on_animation_finished(self):
     if not self.toggle_button.isChecked():
         self.content_area.setVisible(False)
     else:
         # Дозволяємо контенту "розтягуватися" вгору
         self.content_area.setMaximumHeight(16777215)
     self.toggle_animation_finished.emit()








 def hide_content(self):
     if not self.toggle_button.isChecked():
         self.content_area.setVisible(False)








 def update_arrow_icon(self, expanded):
     if expanded:
         self.toggle_button.setIcon(self.arrow_up_icon)
     else:
         self.toggle_button.setIcon(self.arrow_down_icon)
     self.toggle_button.setIconSize(QtCore.QSize(16, 16))








 def setContentLayout(self, layout):
     while self.content_layout.count():
         item = self.content_layout.takeAt(0)
         w = item.widget()
         if w is not None:
             w.setParent(None)
     self.content_layout.addLayout(layout)








class CollapsibleWidget(CollapsibleSection):
 """
 Те саме, але оформлення трохи інше (відступи, паддінги),
 зручно для "великих" секцій наче "Фільтри Пошуку".
 """
 def __init__(self, title="", parent=None):
     super().__init__(title, parent)
     self.toggle_button.setStyleSheet("""
       QPushButton {
           text-align: left;
           padding:8px;
           padding-left:25px;
           background-color: transparent;
           border: none;
           font-weight: bold;
           font-size:14pt;
           color:black;
       }
       QPushButton:hover {
           background-color: rgba(0,0,0,0.03);
           border-radius:3px;
       }
     """)








class FilterSection(CollapsibleSection):
 """
 Секція з фільтрами (чекбоксами) і пошуковим рядком вгорі.
 Анімація плавна (dynamic_animation) під час зміни кількості віджетів,
 щоб уникнути "дьоргання".
 """
 checkbox_state_changed = pyqtSignal()








 def __init__(
     self,
     title="",
     items=None,
     parent=None,
     columns=4,
     minHeight=80,  # Зменшено з 100
     maxHeight=500  # Зменшено з 600
 ):
     super().__init__(title, parent)








     self.items = items or []
     self.columns = columns
     self.minHeight = minHeight
     self.maxHeight = maxHeight








     self._defaultHeight = None








     self.all_checkboxes = []








     self.main_layout = QVBoxLayout()
     self.main_layout.setSpacing(5)
     self.main_layout.setContentsMargins(10, 10, 10, 10)








     self.search_bar = QLineEdit()
     self.search_bar.setPlaceholderText("Пошук...")
     self.search_bar.setFont(QFont("Arial", 12))
     self.search_bar.setFixedHeight(35)
     # Прибираємо встановлення жорсткого стилю тут, він буде застосовуватись через функцію update_text_colors
     # для правильної адаптації до світлої/темної теми
     self.search_bar.textChanged.connect(self.on_search_text_changed)
     
     top_layout = QHBoxLayout()
     top_layout.addWidget(self.search_bar)
     self.main_layout.addLayout(top_layout)








     self.grid_layout = QGridLayout()
     self.grid_layout.setSpacing(10)  # Зменшено з 15
     self.grid_layout.setContentsMargins(15, 10, 15, 10)  # Зменшено з (15, 15, 15, 15)
     self.grid_layout.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)








     for c in range(self.columns):
         self.grid_layout.setColumnStretch(c, 1)








     self.checkboxes_widget = QWidget()
     self.checkboxes_widget.setLayout(self.grid_layout)








     self.scroll_area = QScrollArea()
     self.scroll_area.setWidgetResizable(True)
     self.scroll_area.setWidget(self.checkboxes_widget)
    
     # Новий стиль скроллбара
     self.scroll_area.setStyleSheet("""
         QScrollArea {
             border: none;
             background-color: #ffffff;
         }
         QScrollBar:vertical {
             width: 0px;
             background: transparent;
             margin: 0px;
             border: none;
         }
         QScrollBar:vertical:hover {
             width: 8px;
             background: transparent;
             margin: 0px;
             border-radius: 4px;
         }
         QScrollBar::handle:vertical {
             background: rgba(120, 120, 120, 0.4);
             min-height: 20px;
             border-radius: 4px;
         }
         QScrollBar::handle:vertical:hover {
             background: rgba(80, 80, 80, 0.7);
         }
         QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
             height: 0px;
             background: none;
             border: none;
         }
         QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
             background: none;
             border: none;
         }
        
         QScrollBar:horizontal {
             height: 0px;
             background: transparent;
             margin: 0px;
             border: none;
         }
         QScrollBar:horizontal:hover {
             height: 8px;
             background: transparent;
             margin: 0px;
             border-radius: 4px;
         }
         QScrollBar::handle:horizontal {
             background: rgba(120, 120, 120, 0.4);
             min-width: 20px;
             border-radius: 4px;
         }
         QScrollBar::handle:horizontal:hover {
             background: rgba(80, 80, 80, 0.7);
         }
         QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
             width: 0px;
             background: none;
             border: none;
         }
         QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal {
             background: none;
             border: none;
         }
     """)


     self.main_layout.addWidget(self.scroll_area)
     self.setContentLayout(self.main_layout)








     self.populate_checkboxes()
     self.layout_checkboxes(self.all_checkboxes)








     self.update_arrow_icon(False)
     self.toggle_button.setChecked(False)
     self.on_toggle()








     # Анімація для зміни висоти "контенту" (динамічна)
     self.dynamic_animation = QPropertyAnimation(self.content_area, b"maximumHeight")
     self.dynamic_animation.setDuration(400)  # Зменшено з 600
     self.dynamic_animation.setEasingCurve(QEasingCurve.Type.InOutQuint)








 def on_toggle(self):
     super().on_toggle()
     self.update_arrow_icon(self.toggle_button.isChecked())








 def populate_checkboxes(self):
     for text_item in self.items:
         cb = QCheckBox(text_item)
         cb.setFont(QFont("Arial", 12))
         cb.setStyleSheet("""
           QCheckBox::indicator {
               width: 15px;
               height: 15px;
               margin-right: 8px;
           }
           QCheckBox {
               margin: 2px;
               padding: 0px;
           }
         """)
         cb.stateChanged.connect(self.on_checkbox_state_changed_internal)
         self.all_checkboxes.append(cb)








 def on_checkbox_state_changed_internal(self, state):
     self.checkbox_state_changed.emit()








 def on_search_text_changed(self, text):
     self.filter_checkboxes()








 def filter_checkboxes(self):
     t = self.search_bar.text().strip().lower()
     if not t:
         visible = self.all_checkboxes
     else:
         visible = [cb for cb in self.all_checkboxes if t in cb.text().lower()]








     self.layout_checkboxes(visible)








 def layout_checkboxes(self, visible_cbs):
     """
     Показуємо лише чекбокси "visible_cbs".
     Активні (isChecked) йдуть першими.
     """
     visible_checked = [cb for cb in visible_cbs if cb.isChecked()]
     visible_unchecked = [cb for cb in visible_cbs if not cb.isChecked()]
     final_list = visible_checked + visible_unchecked








     # Прибираємо всі старі віджети з grid_layout
     for i in reversed(range(self.grid_layout.count())):
         item = self.grid_layout.takeAt(i)
         w = item.widget()
         if w:
             w.setParent(None)








     # Викладаємо чекбокси по сітці
     row = 0
     col = 0
     for cb in final_list:
         self.grid_layout.addWidget(cb, row, col)
         cb.show()
         col += 1
         if col >= self.columns:
             col = 0
             row += 1








     for cb in self.all_checkboxes:
         if cb not in final_list:
             cb.hide()








     self.update_panel_height()








 def update_panel_height(self):
     if not self.toggle_button.isChecked():
         return








     self.checkboxes_widget.adjustSize()
     extra_space_from_last_item = 70  # Зменшено з 90
     needed = self.checkboxes_widget.sizeHint().height() + extra_space_from_last_item








     if self._defaultHeight is None or needed > self._defaultHeight:
         self._defaultHeight = needed








     text = self.search_bar.text().strip()
     if not text:
         desired = min(self._defaultHeight, self.maxHeight)
         desired = max(desired, self.minHeight)
     else:
         desired = max(needed, self.minHeight)
         desired = min(desired, self._defaultHeight, self.maxHeight)








     self.animate_height(desired)








 def animate_height(self, target_height):
     self.dynamic_animation.stop()








     current_val = self.content_area.maximumHeight()
     if current_val < 0 or current_val >= 16777215:
         current_val = self.content_area.height()








     self.dynamic_animation.setStartValue(current_val)
     self.dynamic_animation.setEndValue(target_height)
     self.dynamic_animation.start()




