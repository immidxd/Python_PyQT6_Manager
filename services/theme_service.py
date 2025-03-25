import os
import qtawesome as qta
from PyQt6.QtCore import QSize
from widgets import FilterSection  # Додаємо імпорт для FilterSection








def apply_theme(main_window, is_dark_theme):
 """
 Завантажуємо відповідний QSS-файл (dark чи light), застосовуємо до main_window.
 Потім оновлюємо кольори тексту та стиль autocomplete, щоб усе виглядало гармонійно.
 """
 qss_file = "styles_dark.qss" if is_dark_theme else "styles_light.qss"
 project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
 qss_path = os.path.join(project_root, "style", "css", qss_file)




 if not os.path.exists(qss_path):
     raise FileNotFoundError(f"Не знайдено файл стилів: {qss_path}")




 with open(qss_path, "r", encoding="utf-8") as f:
     qss = f.read()




 main_window.setStyleSheet(qss)




 # Після застосування QSS — оновлюємо текстові кольори, чекбокси тощо.
 from services.filter_service import update_filter_counts
 update_text_colors(main_window, is_dark_theme)




 # Оновимо лічильники фільтрів на вкладках (якщо є)
 if hasattr(main_window, 'products_tab'):
     update_filter_counts(main_window.products_tab)
     update_section_headers(main_window.products_tab, is_dark_theme)
 if hasattr(main_window, 'orders_tab'):
     update_filter_counts(main_window.orders_tab)
     update_section_headers(main_window.orders_tab, is_dark_theme)




 # Стиль autocomplete (QListWidget), щоб він гарно вписувався в dark/light




 update_completer_style(main_window, is_dark_theme)




def update_theme_icon_for_button(button, is_dark_theme):
 """
 Залежно від теми (світла/темна) виставляємо іконку "лампочки" (lightbulb).
 """
 if is_dark_theme:
     bulb_icon = qta.icon('fa5s.lightbulb', color='#ffffff')
 else:
     bulb_icon = qta.icon('fa5s.lightbulb', color='#000000')
 button.setIcon(bulb_icon)
 button.setIconSize(QSize(22, 22))  # Трохи збільшимо розмір іконки для кращої видимості








def update_text_colors(obj, is_dark_theme):
 """
 Оновлює стилі та кольори тексту, чекбоксів, розгорнутих/згорнутих панелей і т.д.
 """
 text_color = "#ffffff" if is_dark_theme else "#000000"
 bg_color = "#3c3f41" if is_dark_theme else "#ffffff"
 bg_color_input = "#3c3f41" if is_dark_theme else "#f0f0f0"
 placeholder_color = "#bbbbbb" if is_dark_theme else "#888888"




 # Стиль заголовків CollapsibleSection/Widget
 section_title_style = f"""
 QPushButton {{
     text-align: left;
     padding: 8px;
     padding-left: 25px;
     background-color: transparent;
     border: none;
     font-weight: bold;
     font-size: 14pt;
     color: {text_color};
 }}
 QPushButton:hover {{
     background-color: rgba({255 if is_dark_theme else 0}, {255 if is_dark_theme else 0}, {255 if is_dark_theme else 0}, 0.1);
     border-radius: 3px;
 }}
 QPushButton:focus {{
     border: 2px solid #7851A9;
     border-radius: 5px;
 }}
 """




 if hasattr(obj, 'filters_panel') and hasattr(obj.filters_panel, 'toggle_button'):
     obj.filters_panel.toggle_button.setStyleSheet(section_title_style)




 if hasattr(obj, 'displayed_section') and hasattr(obj.displayed_section, 'toggle_button'):
     obj.displayed_section.toggle_button.setStyleSheet(section_title_style)
  # Оновлюємо стилі для orders_displayed_section, якщо є
 if hasattr(obj, 'orders_displayed_section') and hasattr(obj.orders_displayed_section, 'toggle_button'):
     obj.orders_displayed_section.toggle_button.setStyleSheet(section_title_style)




 # Перевірка внутрішніх секцій
 possible_sections = [
     'brand_section', 'gender_section', 'type_section', 'color_section', 'country_section',
     'answer_status_section', 'payment_status_section', 'delivery_section'
 ]
 for sec_attr in possible_sections:
     if hasattr(obj, sec_attr):
         sec = getattr(obj, sec_attr, None)
         if sec and hasattr(sec, 'toggle_button'):
             sec.toggle_button.setStyleSheet(section_title_style)
             # Оновлюємо також іконки для секцій
             if hasattr(sec, 'arrow_down_icon') and hasattr(sec, 'arrow_up_icon'):
                 sec.arrow_down_icon = qta.icon('fa5s.angle-down', color=text_color)
                 sec.arrow_up_icon = qta.icon('fa5s.angle-up', color=text_color)
                 if sec.toggle_button.isChecked():
                     sec.toggle_button.setIcon(sec.arrow_up_icon)
                 else:
                     sec.toggle_button.setIcon(sec.arrow_down_icon)




 # Оновлюємо чекбокси
 def set_checkbox_style(check_list, color):
     for cb in check_list:
         cb.setStyleSheet(f"""
             QCheckBox::indicator {{
                 width: 15px;
                 height: 15px;
                 margin-right: 5px;
             }}
             QCheckBox {{
                 margin: 2px;
                 padding: 0px;
                 color: {color};
             }}
         """)




 # Перевіряємо всі FilterSection та оновлюємо всі чекбокси в них
 for attr_name in dir(obj):
     attr = getattr(obj, attr_name)
     if isinstance(attr, FilterSection):
         set_checkbox_style(attr.all_checkboxes, text_color)
     elif attr_name.endswith('_checkboxes') and isinstance(attr, list):
         set_checkbox_style(attr, text_color)




 # Оновлюємо стилі чекбоксів "Тільки Непродані", "Тільки неоплачені" і "Тільки оплачені"
 if hasattr(obj, 'unsold_checkbox'):
     obj.unsold_checkbox.setStyleSheet(f"""
         QCheckBox::indicator {{
             width: 15px;
             height: 15px;
             margin-right: 5px;
         }}
         QCheckBox {{
             margin: 2px;
             padding: 0px;
             color: {text_color};
         }}
     """)
 if hasattr(obj, 'unpaid_checkbox'):
     obj.unpaid_checkbox.setStyleSheet(f"""
         QCheckBox::indicator {{
             width: 15px;
             height: 15px;
             margin-right: 5px;
         }}
         QCheckBox {{
             margin: 2px;
             padding: 0px;
             color: {text_color};
         }}
     """)
 if hasattr(obj, 'paid_checkbox'):
     obj.paid_checkbox.setStyleSheet(f"""
         QCheckBox::indicator {{
             width: 15px;
             height: 15px;
             margin-right: 5px;
         }}
         QCheckBox {{
             margin: 2px;
             padding: 0px;
             color: {text_color};
         }}
     """)




 # Оновимо стилі QComboBox
 combobox_style = generate_combobox_style(is_dark_theme)
 possible_comboboxes = [
     'condition_combobox', 'supplier_combobox', 'sort_combobox',
     'orders_sort_combobox', 'priority_combobox', 'calendar_button'
 ]
 for combo_attr in possible_comboboxes:
     if hasattr(obj, combo_attr):
         combo = getattr(obj, combo_attr, None)
         if combo is not None:
             combo.setStyleSheet(combobox_style)




 # Оновлюємо стилі полів пошуку в секціях фільтрів
 search_style = f"""
 QLineEdit {{
     border: 1px solid {'#666666' if is_dark_theme else '#cccccc'};
     background-color: {bg_color_input};
     padding: 5px;
     color: {text_color};
     border-radius: 5px;
 }}
 QLineEdit::placeholder {{
     color: {placeholder_color};
     font-style: italic;
 }}
 QLineEdit:focus {{
     background-color: {bg_color_input};
     border: 2px solid #7851A9;
     border-radius: 5px;
 }}
 """




 # Перевіряємо всі FilterSection та оновлюємо поля пошуку в них
 for attr_name in dir(obj):
     attr = getattr(obj, attr_name)
     if isinstance(attr, FilterSection) and hasattr(attr, 'search_bar'):
         attr.search_bar.setStyleSheet(search_style)




 # Додаємо глобальний стиль до RangeSlider
 add_slider_global_style(obj, is_dark_theme)








def update_section_headers(obj, is_dark_theme):
 """
 Оновлює заголовки секцій фільтрів для правильного відображення в темній/світлій темі.
 Проходиться по всіх підлеглих елементах і оновлює їх стилі та іконки.
 """
 text_color = "#ffffff" if is_dark_theme else "#000000"
 # Оновлюємо стиль для всіх CollapsibleSection/Widget в об'єкті
 for child in obj.findChildren(object):
     # Перевірка для CollapsibleSection та CollapsibleWidget
     if hasattr(child, 'toggle_button') and hasattr(child, 'toggle_animation'):
         # Для секцій-заголовків
         if hasattr(child, 'title') and child.title:
             header_style = f"""
             QPushButton {{
                 text-align: left;
                 padding: 8px;
                 padding-left: 25px;
                 background-color: transparent;
                 border: none;
                 font-weight: bold;
                 font-size: 14pt;
                 color: {text_color};
             }}
             QPushButton:hover {{
                 background-color: rgba({255 if is_dark_theme else 0}, {255 if is_dark_theme else 0}, {255 if is_dark_theme else 0}, 0.1);
                 border-radius: 3px;
             }}
             """
             child.toggle_button.setStyleSheet(header_style)
           
             # Оновимо також іконку стрілки
             if hasattr(child, 'arrow_down_icon') and hasattr(child, 'arrow_up_icon'):
                 child.arrow_down_icon = qta.icon('fa5s.angle-down', color=text_color)
                 child.arrow_up_icon = qta.icon('fa5s.angle-up', color=text_color)
               
                 # Оновлюємо іконку відповідно до поточного стану
                 if child.toggle_button.isChecked():
                     child.toggle_button.setIcon(child.arrow_up_icon)
                 else:
                     child.toggle_button.setIcon(child.arrow_down_icon)








def generate_combobox_style(is_dark_theme):
 """
 Генеруємо стиль QComboBox, змінюючи колір і тло залежно від теми.
 Покращені стилі для комбобоксів з кращим відображенням контурів.
 """
 if is_dark_theme:
     return """
     QComboBox {
         border: 1px solid #666666;
         border-radius: 5px;
         padding: 2px 40px 2px 10px;
         background-color: #3c3f41;
         color: #ffffff;
         font-size: 13pt;
         min-height: 35px;
     }
     QComboBox:focus {
         border: 2px solid #7851A9;
     }
     QComboBox::drop-down {
         border: none;
         background: transparent;
         width: 30px;
         subcontrol-position: top right;
         subcontrol-origin: padding;
     }
     QComboBox::down-arrow {
         image: url(style/images/icons/down_arrow_flat_white.png);
         width: 12px; height: 12px;
     }
     QComboBox QAbstractItemView {
         background: #3c3f41;
         color: #ffffff;
         selection-background-color: #4b4f51;
         border: 1px solid #666666;
         border-radius: 0 0 5px 5px;
     }
     QComboBox QAbstractItemView::item {
         padding: 5px;
         min-height: 25px;
     }
     """
 else:
     return """
     QComboBox {
         border: 1px solid #cccccc;
         border-radius: 5px;
         padding: 2px 40px 2px 10px;
         background-color: #ffffff;
         color: #000000;
         font-size: 13pt;
         min-height: 35px;
     }
     QComboBox:focus {
         border: 2px solid #7851A9;
     }
     QComboBox::drop-down {
         border: none;
         background: transparent;
         width: 30px;
         subcontrol-position: top right;
         subcontrol-origin: padding;
     }
     QComboBox::down-arrow {
         image: url(style/images/icons/down_arrow_flat.png);
         width: 12px; height: 12px;
     }
     QComboBox QAbstractItemView {
         background: #ffffff;
         color: #000000;
         selection-background-color: #d5d5d5;
         border: 1px solid #cccccc;
         border-radius: 0 0 5px 5px;
     }
     QComboBox QAbstractItemView::item {
         padding: 5px;
         min-height: 25px;
     }
     """








def add_slider_global_style(obj, is_dark_theme):
 """
 Накладаємо невеликий стиль на QSlider/RangeSlider, щоб виглядало охайно.
 Зменшено розміри слайдерів для більш компактного відображення.
 """
 slider_color = "#7851A9"  # Фіксований колір для обох тем
 background_color = "#3c3f41" if is_dark_theme else "#ffffff"
 handle_color = "#ffffff" if is_dark_theme else "#ffffff"
 current_style = obj.styleSheet()
 slider_style = f"""
 QSlider::groove:horizontal {{
     background: transparent;
     height: 4px;
     margin: 0px;
     margin-left: 10px;
     margin-right: 10px;
 }}
 QSlider::handle:horizontal {{
     background: {handle_color};
     border: 1px solid {slider_color};
     width: 20px;
     height: 20px;
     margin: -8px 0px;
     border-radius: 10px;
 }}
 RangeSlider {{
     background: {background_color};
 }}
 """
 obj.setStyleSheet(current_style + slider_style)








def update_completer_style(main_window, is_dark_theme):
 """
 Налаштовуємо стиль autocomplete (QListWidget) для ProductsTab i OrdersTab (якщо є).
 """
 if hasattr(main_window, 'products_tab') and hasattr(main_window.products_tab, 'completer_list'):
     if is_dark_theme:
         style_products = """
             QListWidget {
                 background-color: #3c3f41;
                 color: #ffffff;
                 font-size: 12pt;
                 border: 1px solid #888888;
                 border-radius: 0 0 5px 5px;
             }
             QListWidget::item {
                 padding: 6px 8px;
             }
             QListWidget::item:hover {
                 background-color: #4b4f51;
                 color: #ffffff;
             }
             QListWidget::item:selected {
                 background-color: #5c5c5c;
                 color: #ffffff;
             }
         """
     else:
         style_products = """
             QListWidget {
                 background-color: #ffffff;
                 color: #000000;
                 font-size: 12pt;
                 border: 1px solid #bcbcbc;
                 border-radius: 0 0 5px 5px;
             }
             QListWidget::item {
                 padding: 6px 8px;
             }
             QListWidget::item:hover {
                 background-color: #d0d0d0;
                 color: #000000;
             }
             QListWidget::item:selected {
                 background-color: #bcbcbc;
                 color: #000000;
             }
         """
     main_window.products_tab.completer_list.setStyleSheet(style_products)




 if hasattr(main_window, 'orders_tab') and hasattr(main_window.orders_tab, 'orders_completer_list'):
     if is_dark_theme:
         style_orders = """
             QListWidget {
                 background-color: #3c3f41;
                 color: #ffffff;
                 font-size: 12pt;
                 border: 1px solid #888888;
                 border-radius: 0 0 5px 5px;
             }
             QListWidget::item {
                 padding: 6px 8px;
             }
             QListWidget::item:hover {
                 background-color: #4b4f51;
                 color: #ffffff;
             }
             QListWidget::item:selected {
                 background-color: #5c5c5c;
                 color: #ffffff;
             }
         """
     else:
         style_orders = """
             QListWidget {
                 background-color: #ffffff;
                 color: #000000;
                 font-size: 12pt;
                 border: 1px solid #bcbcbc;
                 border-radius: 0 0 5px 5px;
             }
             QListWidget::item {
                 padding: 6px 8px;
             }
             QListWidget::item:hover {
                 background-color: #d0d0d0;
                 color: #000000;
             }
             QListWidget::item:selected {
                 background-color: #bcbcbc;
                 color: #000000;
             }
         """
     main_window.orders_tab.orders_completer_list.setStyleSheet(style_orders)


