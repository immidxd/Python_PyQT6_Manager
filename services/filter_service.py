import re
import logging
from datetime import datetime
from sqlalchemy import func, or_
from sqlalchemy.orm import aliased
from models import (
   Product, Brand, Gender, Type, Subtype, Color, Country, Status,
   Condition, Import, DeliveryMethod, PaymentStatus, OrderStatus
)
from db import session


# Потрібно встановити rapidfuzz (pip install rapidfuzz)
from rapidfuzz import fuzz, process


# Налаштування нечіткого пошуку
FUZZY_SEARCH_THRESHOLD = 65  # мінімальний поріг схожості (0-100)
FUZZY_MAX_MATCHES = 100  # максимальна кількість результатів для fuzzy-пошуку


# ----------------------------------------------
#         autocomplete / suggestions
# ----------------------------------------------


# Раніше було recent_queries як dict, але зручно й list або dict.
# Якщо хочемо підраховувати частоту — лишаємо dict
recent_queries = {}


# Статичний словник expansions
expansions = {
   "кросівки": [
       "кросівки трекінгові", "кросівки на зиму", "кросівки дитячі",
       "кросівки для бігу", "кросівки чоловічі", "кросівки жіночі"
   ],
   "туфлі": [
       "туфлі вечірні", "туфлі ділові", "туфлі шкіряні",
       "туфлі жіночі", "туфлі чоловічі"
   ],
   "куртка": [
       "куртка пухова", "куртка зимова", "куртка демісезонна"
   ],
   "ботинки": [
       "ботінки", "черевики", "черевички", "чоботи"
   ]
}


# Динамічні розширення (synonyms), які будуємо за описами, кольорами
dynamic_expansions = {}


# Основний словник синонімів
synonym_dict = {
   "взуття": ["туфлі", "кросівки", "ботинки", "кеди", "сандалі"],
   "куртка": ["пуховик", "бомбер", "піджак", "парка", "куртка"],
   "кросівки": ["кеди", "снікери", "кроси", "кросівки"],
   "туфлі": ["черевики", "взуття", "туфлі"],
   "клогі": ["крокс", "crocs"],
   "тапки": [
       "капці", "клогі", "тапочки", "тапулі", "клог", "крокси", "шльопанці",
       "шльопки", "сандалі", "в'єтнамки", "вєтнамки"
   ],
   "ботинки": ["черевики", "боти", "ботинки"]
}




def remember_query(query_text):
   """
   Зберігає рядок запиту (autocomplete) в recent_queries,
   щоб показувати в "Останні Пошуки".
   """
   if not query_text or not query_text.strip():
       return


   q = query_text.strip().lower()
   if q:
       recent_queries[q] = recent_queries.get(q, 0) + 1
      
   try:
       from db import session, Recent_Query
       # Перевіряємо, чи є вже такий запит
       existing = session.query(Recent_Query).filter_by(query_text=query_text).first()
       if existing:
           # Якщо є, оновлюємо timestamp:
           existing.created_at = datetime.now()
           session.commit()
       else:
           # Якщо немає, створюємо новий:
           new_query = Recent_Query(query_text=query_text)
           session.add(new_query)
           # Видаляємо старі запити, якщо їх > 20:
           count = session.query(func.count(Recent_Query.id)).scalar()
           if count > 20:
               oldest = session.query(Recent_Query).order_by(Recent_Query.created_at).first()
               if oldest:
                   session.delete(oldest)
           session.commit()
   except Exception as e:
       logging.error(f"Помилка при запам'ятовуванні запиту: {e}")
       session.rollback()




def get_suggestions(query, db_session=None):
   """
   Повертає підказки для автодоповнення:
   1. Останні пошуки
   2. Основні рекомендації (базовані на типах продуктів, статі, тощо)
   3. Бренди, що містять запит
   4. Моделі, що містять запит
   5. Частини описів, що містять запит
   """
   suggestions = {
       "Останні Пошуки": [],
       "Рекомендації": [],
       "Бренд": [],
       "Модель": [],
       "Опис": []
   }
   
   query = query.lower().strip()
   if not query or not db_session:
       return suggestions
   
   # Останні пошуки
   # Беремо з recent_queries замість бази даних для швидкості
   if recent_queries:
       # Сортуємо за частотою (від більшої до меншої)
       sorted_recent = sorted(recent_queries.items(), key=lambda x: x[1], reverse=True)
       matching_recent = []
       
       # Спочатку точні співпадіння
       for q, _ in sorted_recent:
           if q.lower() == query:
               matching_recent.append(q)
               
       # Потім часткові співпадіння
       if len(matching_recent) < 5:
           for q, _ in sorted_recent:
               if query in q.lower() and q not in matching_recent:
                   matching_recent.append(q)
                   if len(matching_recent) >= 5:
                       break
       
       # Далі нечіткі співпадіння
       if len(matching_recent) < 5:
           # Використовуємо нечіткий пошук для останніх запитів
           fuzzy_matches = fuzzy_search(
               query, 
               [q for q, _ in sorted_recent if q not in matching_recent], 
               threshold=60,
               limit=5
           )
           
           for match, score, _ in fuzzy_matches:
               if match not in matching_recent:
                   matching_recent.append(match)
                   if len(matching_recent) >= 5:
                       break
                   
       suggestions["Останні Пошуки"] = matching_recent[:5]
   
   # Рекомендації: якщо запит довгий, робимо пропозиції на основі synonyms/expansions
   if len(query) >= 2:
       for key, values in EXTENDED_SYNONYMS.items():
           if query in key:
               # Рекомендуємо ключове слово, якщо запит є його частиною
               if key not in suggestions["Рекомендації"]:
                   suggestions["Рекомендації"].append(key)
           elif any(query in val for val in values):
               # Рекомендуємо ключове слово, якщо запит є частиною його значень
               if key not in suggestions["Рекомендації"]:
                   suggestions["Рекомендації"].append(key)
       
       # Шукаємо також за допомогою нечіткого співпадіння
       all_keys = list(EXTENDED_SYNONYMS.keys())
       all_values = [item for sublist in EXTENDED_SYNONYMS.values() for item in sublist]
       
       fuzzy_key_matches = fuzzy_search(query, all_keys, threshold=70, limit=3)
       for match, score, _ in fuzzy_key_matches:
           if match not in suggestions["Рекомендації"]:
               suggestions["Рекомендації"].append(match)
       
       fuzzy_val_matches = fuzzy_search(query, all_values, threshold=70, limit=3)
       for match, score, _ in fuzzy_val_matches:
           # Знаходимо ключі для цього значення
           for key, values in EXTENDED_SYNONYMS.items():
               if match in values and key not in suggestions["Рекомендації"]:
                   suggestions["Рекомендації"].append(key)
   
   # Обмежуємо кількість рекомендацій
   suggestions["Рекомендації"] = suggestions["Рекомендації"][:5]
   
   # Для решти - використовуємо базу
   try:
       # Бренди
       brands = db_session.query(Brand.brandname).filter(
           Brand.brandname.ilike(f"%{query}%")
       ).limit(5).all()
       suggestions["Бренд"] = [b[0] for b in brands]
       
       # Моделі - використовуємо нечіткий пошук
       all_models = db_session.query(Product.model).distinct().all()
       model_values = [m[0] for m in all_models if m[0]]
       fuzzy_model_matches = fuzzy_search(query, model_values, threshold=70, limit=5)
       suggestions["Модель"] = [match for match, score, _ in fuzzy_model_matches]
       
       # Опис - використовуємо нечіткий пошук для коротких фраз
       all_descriptions = db_session.query(Product.description).distinct().all()
       # Виберемо тільки короткі описи для підказок (менше 40 символів)
       short_descriptions = [d[0] for d in all_descriptions if d[0] and len(d[0]) < 40]
       fuzzy_desc_matches = fuzzy_search(query, short_descriptions, threshold=65, limit=5)
       suggestions["Опис"] = [match for match, score, _ in fuzzy_desc_matches]
   
   except Exception as e:
       logging.error(f"Помилка при отриманні підказок: {e}")
   
   return suggestions




def get_synonyms(word: str):
   """
   Повертає масив можливих синонімів, включно з вихідним словом,
   автотранслітераціями (UA->EN, EN->UA), dynamic_expansions тощо.
   """
   base = word.lower().strip()
   synonyms_set = set()


   # 1) статичний словник
   if base in synonym_dict:
       synonyms_set.update(synonym_dict[base])


   # 2) саме слово
   if base:
       synonyms_set.add(base)


   # 3) dynamic_expansions
   if base in dynamic_expansions:
       synonyms_set.update(dynamic_expansions[base])


   # 4) UA->EN
   lat_variant = transliterate_ua_to_lat(base)
   if lat_variant:
       synonyms_set.add(lat_variant)


   # 5) EN->UA
   if base != lat_variant:
       back_ua = transliterate_lat_to_ua(base)
       if back_ua and back_ua != base:
           synonyms_set.add(back_ua)


   return list(synonyms_set)




def transliterate_ua_to_lat(text: str) -> str:
   """
   Спрощена реалізація (UA -> EN).
   """
   mapping = {
       'а': 'a', 'б': 'b', 'в': 'v', 'г': 'h', 'ґ': 'g',
       'д': 'd', 'е': 'e', 'є': 'ie', 'ж': 'zh', 'з': 'z',
       'и': 'y', 'і': 'i', 'ї': 'i', 'й': 'i', 'к': 'k',
       'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o', 'п': 'p',
       'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f',
       'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'shch',
       'ь': '', 'ю': 'iu', 'я': 'ia'
   }
   result = []
   for char in text:
       lower_c = char.lower()
       result.append(mapping.get(lower_c, char))
   return "".join(result)




def transliterate_lat_to_ua(text: str) -> str:
   """
   Спрощена реалізація (EN -> UA).
   """
   reversed_map = {
       'shch': 'щ', 'kh': 'х', 'ch': 'ч', 'sh': 'ш', 'zh': 'ж',
       'ts': 'ц', 'iu': 'ю', 'ia': 'я', 'ie': 'є',
       'a': 'а', 'b': 'б', 'v': 'в', 'h': 'г', 'g': 'ґ',
       'd': 'д', 'e': 'е', 'z': 'з', 'y': 'и', 'i': 'і',
       'k': 'к', 'l': 'л', 'm': 'м', 'n': 'н', 'o': 'о',
       'p': 'п', 'r': 'р', 's': 'с', 't': 'т', 'u': 'у',
       'f': 'ф'
   }
   # Спочатку довші збіги (shch, kh...) -> щоб не перебити 'shch' на 'sh' + 'ch'
   sorted_keys = sorted(reversed_map.keys(), key=len, reverse=True)
   result = text
   for k in sorted_keys:
       if k in result:
           result = result.replace(k, reversed_map[k])
   return result




def build_dynamic_expansions():
   """
   Якщо потрібно, можна викликати цю функцію один раз при старті,
   щоб автоматично наповнити dynamic_expansions з описів товарів.
   """
   products = session.query(Product).join(Color, Product.colorid == Color.id, isouter=True).all()
   for p in products:
       desc = (p.description or "").lower()
       color = (p.color.colorname if p.color else "") or ""
       color = color.lower()


       desc_parts = re.split(r'[\/,;]+', desc)
       color_parts = re.split(r'[\/,;]+', color)
       desc_parts = [x.strip() for x in desc_parts if x.strip()]
       color_parts = [x.strip() for x in color_parts if x.strip()]


       for i in range(len(desc_parts)):
           for j in range(i + 1, len(desc_parts)):
               add_to_dynamic_expansions(desc_parts[i], desc_parts[j])
               add_to_dynamic_expansions(desc_parts[j], desc_parts[i])


       for dp in desc_parts:
           for cp in color_parts:
               add_to_dynamic_expansions(dp, cp)
               add_to_dynamic_expansions(cp, dp)




def add_to_dynamic_expansions(base_word: str, expansion_word: str):
   """
   Додає expansion_word до dynamic_expansions[base_word].
   """
   base = base_word.strip().lower()
   exp = expansion_word.strip().lower()
   if not base or not exp:
       return
   if base not in dynamic_expansions:
       dynamic_expansions[base] = []
   if exp not in dynamic_expansions[base]:
       dynamic_expansions[base].append(exp)




# -------------------------------------------------
#    Методи отримання даних із БД (OrdersTab)
# -------------------------------------------------


def get_order_statuses(db_session):
   """
   Повертає список статусів (order_statuses.status_name) з бази.
   """
   results = db_session.query(OrderStatus.status_name).order_by(OrderStatus.status_name).all()
   final = []
   for r in results:
       if r[0]:
           # можна першу букву робити великою
           txt = r[0][0].upper() + r[0][1:]
           final.append(txt)
   return final




def get_genders_db(db_session):
   """
   Повертає список статей (genders.gendername) з бази.
   """
   rows = db_session.query(Gender.gendername).order_by(Gender.gendername).all()
   result = []
   for r in rows:
       if r[0]:
           # перша велика, решта малі
           result.append(r[0].capitalize())
   return result




def get_payment_statuses_db(db_session):
   """
   Повертає список статусів оплати (payment_statuses.status_name),
   обробляючи регістр, щоб було красиво.
   """
   raw = db_session.query(PaymentStatus.status_name).order_by(PaymentStatus.status_name).all()
   final = []
   for row in raw:
       if row[0]:
           txt = row[0].strip().lower()
           # приклад: якщо починається з "оплач"
           if txt.startswith('оплач'):
               final.append('Оплачено')
           else:
               final.append(txt.capitalize())
       else:
           final.append("")
   return final




def get_delivery_methods_db(db_session):
   """
   Список доставок (delivery_methods.method_name)
   'УП' -> 'Укрпошта', 'НП' -> 'Нова пошта', 'Міст' -> 'Meest Express';
   інакше .capitalize().
   """
   raw = db_session.query(DeliveryMethod.method_name).order_by(DeliveryMethod.method_name).all()
   final = []
   for row in raw:
       name = (row[0] or "").strip().lower()
       if name == 'уп':
           final.append('Укрпошта')
       elif name == 'нп':
           final.append('Нова пошта')
       elif name == 'міст':
           final.append('Meest Express')
       else:
           final.append(name.capitalize())
   return final




def get_countries_db(db_session):
   """
   Список країн (countries.countryname) (з великої літери).
   """
   results = db_session.query(Country.countryname).order_by(Country.countryname).all()
   final = []
   for r in results:
       if r[0]:
           final.append(r[0].capitalize())
   return final




def get_suppliers(db_session):
   """
   Список постачальників (Import.importname).
   """
   results = db_session.query(Import.importname).order_by(Import.importname).all()
   final = []
   for r in results:
       if r[0] and r[0].strip():
           final.append(r[0])
   return final




# -------------------------------------------------
#          build_query_params  (ProductsTab)
# -------------------------------------------------


def build_query_params(products_tab):
   """
   Збираємо фільтри з вкладки "Товари".
   """
   c_text = products_tab.condition_combobox.currentText().strip() if hasattr(products_tab, 'condition_combobox') else None
   s_text = products_tab.supplier_combobox.currentText().strip() if hasattr(products_tab, 'supplier_combobox') else None
   sort_txt = products_tab.sort_combobox.currentText().strip() if hasattr(products_tab, 'sort_combobox') else None


   if c_text in ["Стан", "Всі", None, ""]:
       c_text = None
   if s_text in ["Постачальник", "Всі", None, ""]:
       s_text = None
   if sort_txt in ["Сортування", None, ""]:
       sort_txt = None


   params = {
       'unsold_only': products_tab.unsold_checkbox.isChecked() if hasattr(products_tab, 'unsold_checkbox') else False,
       'search_text': products_tab.search_bar.text().strip() if hasattr(products_tab, 'search_bar') else None,


       'selected_brands': [cb.text() for cb in products_tab.brand_checkboxes if cb.isChecked()] if hasattr(products_tab, 'brand_checkboxes') else None,
       'selected_genders': [cb.text() for cb in products_tab.gender_checkboxes if cb.isChecked()] if hasattr(products_tab, 'gender_checkboxes') else None,
       'selected_types': [cb.text() for cb in products_tab.type_checkboxes if cb.isChecked()] if hasattr(products_tab, 'type_checkboxes') else None,
       'selected_colors': [cb.text() for cb in products_tab.color_checkboxes if cb.isChecked()] if hasattr(products_tab, 'color_checkboxes') else None,
       'selected_countries': [cb.text() for cb in products_tab.country_checkboxes if cb.isChecked()] if hasattr(products_tab, 'country_checkboxes') else None,


       'price_min': products_tab.price_min.value() if hasattr(products_tab, 'price_min') else 0,
       'price_max': products_tab.price_max.value() if hasattr(products_tab, 'price_max') else 9999,
       'size_min': products_tab.size_min.value() if hasattr(products_tab, 'size_min') else 14,
       'size_max': products_tab.size_max.value() if hasattr(products_tab, 'size_max') else 60,
       'dim_min': products_tab.dimensions_min.value() if hasattr(products_tab, 'dimensions_min') else 5,
       'dim_max': products_tab.dimensions_max.value() if hasattr(products_tab, 'dimensions_max') else 40,


       'selected_condition': c_text,
       'selected_supplier': s_text,
       'sort_option': sort_txt
   }
  
   # Прибираємо None значення для чистоти
   params = {k: v for k, v in params.items() if v is not None}
   return params




# -------------------------------------------------
#        build_orders_query_params (OrdersTab)
# -------------------------------------------------


def build_orders_query_params(orders_tab):
   """
   Збираємо фільтри з вкладки "Замовлення".
   Можна додати більше полів, якщо потрібно.
   """
   s_txt = orders_tab.orders_sort_combobox.currentText().strip() if hasattr(orders_tab, 'orders_sort_combobox') else None
   if s_txt in ["Сортування", None, ""]:
       s_txt = None
  
   p_txt = orders_tab.priority_combobox.currentText().strip() if hasattr(orders_tab, 'priority_combobox') else None
   if p_txt in ["Пріоритет", "Будь-який", None, ""]:
       p_txt = None


   # Додаємо параметри для слайдерів місяців і років
   month_min = orders_tab.month_min.value() if hasattr(orders_tab, 'month_min') else 1
   month_max = orders_tab.month_max.value() if hasattr(orders_tab, 'month_max') else 12
   year_min = orders_tab.year_min.value() if hasattr(orders_tab, 'year_min') else 2020
   year_max = orders_tab.year_max.value() if hasattr(orders_tab, 'year_max') else 2030
  
   # Параметри з фільтра пошуку
   params = {
       'search_text': orders_tab.orders_search_bar.text().strip() if hasattr(orders_tab, 'orders_search_bar') else None,
       'answer_statuses': [
           cb.text() for cb in getattr(orders_tab, 'answer_status_checkboxes', [])
           if cb.isChecked()
       ] or None,
       'payment_statuses': [
           cb.text() for cb in getattr(orders_tab, 'payment_status_checkboxes', [])
           if cb.isChecked()
       ] or None,
       'delivery_methods': [
           cb.text() for cb in getattr(orders_tab, 'delivery_checkboxes', [])
           if cb.isChecked()
       ] or None,
       'sort_option': s_txt,
       'priority': p_txt,
       'month_min': month_min,
       'month_max': month_max,
       'year_min': year_min,
       'year_max': year_max,
       'unpaid_only': orders_tab.unpaid_checkbox.isChecked() if hasattr(orders_tab, 'unpaid_checkbox') else False,
       'paid_only': orders_tab.paid_checkbox.isChecked() if hasattr(orders_tab, 'paid_checkbox') else False,
       'selected_date': orders_tab.selected_filter_date if hasattr(orders_tab, 'selected_filter_date') else None
   }
  
   # Прибираємо None значення для чистоти
   params = {k: v for k, v in params.items() if v is not None}
   return params




# -------------------------------------------------
#      update_filter_counts  (ProductsTab / OrdersTab)
# -------------------------------------------------


def update_filter_counts(tab_obj):
   """
   Перерахунок кількості вибраних чекбоксів, слайдерів і т.д.
   Показуємо у заголовку "Фільтри Пошуку (N)".
   """
   text_color = "#ffffff" if hasattr(tab_obj, 'is_dark_theme') and tab_obj.is_dark_theme else "#000000"
  
   # Перелік усіх фільтрів секцій
   all_sections = ['filters_panel', 'displayed_section', 'brand_section', 'gender_section',
                   'type_section', 'color_section', 'country_section',
                   'answer_status_section', 'payment_status_section', 'delivery_section']
  
   # Оновлення заголовків і підрахунок вибраних елементів для всіх секцій
   for section_name in all_sections:
       if hasattr(tab_obj, section_name):
           section = getattr(tab_obj, section_name)
          
           # Підрахунок вибраних елементів
           selected_count = 0
           checkboxes_attr = None
          
           # Визначаємо атрибут з чекбоксами для цієї секції
           if section_name == 'brand_section':
               checkboxes_attr = 'brand_checkboxes'
           elif section_name == 'gender_section':
               checkboxes_attr = 'gender_checkboxes'
           elif section_name == 'type_section':
               checkboxes_attr = 'type_checkboxes'
           elif section_name == 'color_section':
               checkboxes_attr = 'color_checkboxes'
           elif section_name == 'country_section':
               checkboxes_attr = 'country_checkboxes'
           elif section_name == 'answer_status_section':
               checkboxes_attr = 'answer_status_checkboxes'
           elif section_name == 'payment_status_section':
               checkboxes_attr = 'payment_status_checkboxes'
           elif section_name == 'delivery_section':
               checkboxes_attr = 'delivery_checkboxes'
           elif section_name == 'displayed_section':
               checkboxes_attr = 'displayed_checkboxes'
          
           # Підрахунок вибраних чекбоксів
           if checkboxes_attr and hasattr(tab_obj, checkboxes_attr):
               checkboxes = getattr(tab_obj, checkboxes_attr)
               if checkboxes:
                   selected_count = sum(1 for cb in checkboxes if cb.isChecked())
          
           # Особливий випадок для filters_panel - різні підрахунки для різних вкладок
           if section_name == 'filters_panel':
               if hasattr(tab_obj, 'brand_checkboxes'):
                   # Вкладка Товари
                   selected_count = (
                       sum(1 for cb in tab_obj.brand_checkboxes if cb.isChecked()) +
                       sum(1 for cb in tab_obj.gender_checkboxes if cb.isChecked()) +
                       sum(1 for cb in tab_obj.type_checkboxes if cb.isChecked()) +
                       sum(1 for cb in tab_obj.color_checkboxes if cb.isChecked()) +
                       sum(1 for cb in tab_obj.country_checkboxes if cb.isChecked())
                   )
                   # Додаткові критерії для фільтрів цін, розмірів та слайдерів
                   if hasattr(tab_obj, 'price_min') and hasattr(tab_obj, 'price_max'):
                       if tab_obj.price_min.value() > tab_obj.price_min.minimum() or tab_obj.price_max.value() < tab_obj.price_max.maximum():
                           selected_count += 1
                  
                   if hasattr(tab_obj, 'size_min') and hasattr(tab_obj, 'size_max'):
                       if tab_obj.size_min.value() > tab_obj.size_min.minimum() or tab_obj.size_max.value() < tab_obj.size_max.maximum():
                           selected_count += 1
                  
                   if hasattr(tab_obj, 'dimensions_min') and hasattr(tab_obj, 'dimensions_max'):
                       if tab_obj.dimensions_min.value() > tab_obj.dimensions_min.minimum() or tab_obj.dimensions_max.value() < tab_obj.dimensions_max.maximum():
                           selected_count += 1
                  
                   # Перевірка comboboxes
                   if hasattr(tab_obj, 'condition_combobox') and tab_obj.condition_combobox.currentText() not in ["Стан", "Всі"]:
                       selected_count += 1
                  
                   if hasattr(tab_obj, 'supplier_combobox') and tab_obj.supplier_combobox.currentText() not in ["Постачальник", "Всі"]:
                       selected_count += 1
                  
                   if hasattr(tab_obj, 'sort_combobox') and tab_obj.sort_combobox.currentIndex() > 0:
                       selected_count += 1
              
               elif hasattr(tab_obj, 'answer_status_checkboxes'):
                   # Вкладка Замовлення
                   selected_count = (
                       sum(1 for cb in tab_obj.answer_status_checkboxes if cb.isChecked()) +
                       sum(1 for cb in tab_obj.payment_status_checkboxes if cb.isChecked()) +
                       sum(1 for cb in tab_obj.delivery_checkboxes if cb.isChecked())
                   )
                  
                   # Додаткові критерії для слайдерів місяців і років
                   if hasattr(tab_obj, 'month_min') and hasattr(tab_obj, 'month_max'):
                       if tab_obj.month_min.value() > tab_obj.month_min.minimum() or tab_obj.month_max.value() < tab_obj.month_max.maximum():
                           selected_count += 1
                  
                   if hasattr(tab_obj, 'year_min') and hasattr(tab_obj, 'year_max'):
                       if tab_obj.year_min.value() > tab_obj.year_min.minimum() or tab_obj.year_max.value() < tab_obj.year_max.maximum():
                           selected_count += 1
                  
                   # Перевірка comboboxes та інших фільтрів
                   if hasattr(tab_obj, 'orders_sort_combobox') and tab_obj.orders_sort_combobox.currentIndex() > 0:
                       selected_count += 1
                  
                   if hasattr(tab_obj, 'priority_combobox') and tab_obj.priority_combobox.currentText() not in ["Пріоритет", "Будь-який"]:
                       selected_count += 1
                       
                   # Враховуємо фільтр дати
                   if hasattr(tab_obj, 'selected_filter_date') and tab_obj.selected_filter_date is not None:
                       selected_count += 1
                       
                   # Враховуємо чекбокси "Тільки оплачені" і "Тільки неоплачені"
                   if hasattr(tab_obj, 'unpaid_checkbox') and tab_obj.unpaid_checkbox.isChecked():
                       selected_count += 1
                   
                   if hasattr(tab_obj, 'paid_checkbox') and tab_obj.paid_checkbox.isChecked():
                       selected_count += 1
          
           # Оновлення тексту заголовка з лічильником вибраних елементів
           if hasattr(section, 'toggle_button'):
               original_title = section.title if hasattr(section, 'title') else section.toggle_button.text()
               # Видаляємо старий лічильник, якщо він є
               title_base = re.sub(r'\s*\(\d+\)$', '', original_title)
              
               if selected_count > 0:
                   new_title = f"{title_base} ({selected_count})"
               else:
                   new_title = title_base
              
               # Зберігаємо базовий заголовок та встановлюємо новий з лічильником
               if hasattr(section, 'title'):
                   section.title = title_base
               section.toggle_button.setText(new_title)
              
               # Оновлюємо стиль заголовка з урахуванням кольору теми
               section.toggle_button.setStyleSheet(f"""
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
                       background-color: rgba({255 if tab_obj.is_dark_theme else 0}, {255 if tab_obj.is_dark_theme else 0}, {255 if tab_obj.is_dark_theme else 0}, 0.1);
                       border-radius: 3px;
                   }}
               """)




def rename_section_with_count(section, count):
   """
   Допоміжна функція: "Назва (N)" або просто "Назва".
   Припускаємо, що section.title зберігає початкову назву ("Бренд", "Стать").
   Якщо ні, можна використати section.toggle_button.text() і відчистити існуючі (N).
   """
   base_title = section.title
   if count > 0:
       section.toggle_button.setText(f"{base_title} ({count})")
   else:
       section.toggle_button.setText(base_title)




def standardize_filter_layouts(left_layout, right_layout, obj_type="products"):
   """
   Стандартизує макети фільтрів для обох вкладок.
  
   Args:
       left_layout: Layout для лівої частини фільтрів
       right_layout: Layout для правої частини фільтрів
       obj_type: "products" або "orders" - тип вкладки
   """
   # Загальні налаштування для обох вкладок
   left_layout.setContentsMargins(10, 5, 10, 10)
   left_layout.setSpacing(15)
  
   right_layout.setContentsMargins(10, 10, 10, 10)
   right_layout.setSpacing(15)
   
   # Стандартизовані відступи для кращого вигляду у фільтрах
   # Це дозволить уникнути "з'їдання" нижнього контуру елементів
   if obj_type == "orders":
       # Додаткові відступи для елементів у вкладці "Замовлення"
       right_layout.setContentsMargins(10, 10, 10, 20)  # Збільшено нижній відступ


def fuzzy_search(query, field_values, threshold=FUZZY_SEARCH_THRESHOLD, limit=FUZZY_MAX_MATCHES):
    """
    Виконує нечіткий пошук по заданому списку значень.
    
    Args:
        query: Пошуковий запит
        field_values: Список значень для пошуку
        threshold: Поріг схожості (0-100)
        limit: Максимальна кількість результатів
        
    Returns:
        Список кортежів (значення, оцінка схожості)
    """
    if not query or not field_values:
        return []
        
    # Видаляємо None значення
    field_values = [str(x) for x in field_values if x is not None]
    
    # Виконуємо нечіткий пошук
    results = process.extract(
        query, 
        field_values, 
        scorer=fuzz.WRatio,  # Використовуємо зважений аналіз схожості
        score_cutoff=threshold,
        limit=limit
    )
    
    return results


# Словник додаткових синонімів для пошуку
EXTENDED_SYNONYMS = {
    # Додаємо популярні види одягу
    "взуття": ["туфлі", "кросівки", "ботинки", "кеди", "сандалі", "мокасини", "лофери"],
    "верхній одяг": ["куртка", "пальто", "плащ", "шуба", "жилет", "пуховик", "бомбер"],
    "сукня": ["плаття", "сарафан", "комбінезон", "спідниця"],
    "джинси": ["штани", "брюки", "шорти", "легінси"],
    
    # Розширюємо існуючі синоніми
    "куртка": ["пуховик", "бомбер", "піджак", "парка", "куртка", "жакет", "вітровка"],
    "кросівки": ["кеди", "снікери", "кроси", "кросівки", "сникеры", "теніски", "бігові"],
    "туфлі": ["черевики", "взуття", "туфлі", "лофери", "оксфорди", "мешти"],
    "клогі": ["крокс", "crocs", "кроксы", "сабо"],
    "тапки": [
        "капці", "клогі", "тапочки", "тапулі", "клог", "крокси", "шльопанці",
        "шльопки", "сандалі", "в'єтнамки", "вєтнамки", "слайды", "шлепанцы"
    ],
    "ботинки": ["черевики", "боти", "ботинки", "чоботи", "сапоги"]
}


def expanded_search_query(search_text, db_session=None):
    """
    Розширений пошуковий запит, який використовує синоніми та нечіткий пошук.
    
    Args:
        search_text: Текст пошуку
        db_session: Сесія бази даних
        
    Returns:
        Список SQL-умов для пошуку за різними полями
    """
    search_terms = []
    
    # Додаємо оригінальний пошуковий запит
    search_terms.append(search_text.strip())
    
    # Додаємо синоніми з EXTENDED_SYNONYMS
    for key, values in EXTENDED_SYNONYMS.items():
        if search_text.lower() in key.lower() or key.lower() in search_text.lower():
            # Додаємо ключові слова
            search_terms.append(key)
            # Додаємо значення
            search_terms.extend(values)
        else:
            # Перевіряємо, чи search_text міститься в значеннях
            for val in values:
                if search_text.lower() in val.lower() or val.lower() in search_text.lower():
                    search_terms.append(key)
                    search_terms.extend(values)
                    break
    
    # Додаємо нечіткі співпадіння, якщо є сесія бази даних
    if db_session:
        # Отримуємо унікальні значення з полів бази даних для нечіткого пошуку
        try:
            # Бренди
            brands = db_session.query(Brand.brandname).distinct().all()
            brand_names = [b[0] for b in brands if b[0]]
            
            # Моделі
            models = db_session.query(Product.model).distinct().all()
            model_names = [m[0] for m in models if m[0]]
            
            # Поєднуємо все для пошуку
            all_searchable_values = brand_names + model_names
            
            # Виконуємо нечіткий пошук
            fuzzy_matches = fuzzy_search(
                search_text, 
                all_searchable_values, 
                threshold=75,  # Високий поріг для запобігання помилкових результатів
                limit=5
            )
            
            # Додаємо нечіткі співпадіння
            for match, score, _ in fuzzy_matches:
                search_terms.append(match)
                
        except Exception as e:
            logging.error(f"Помилка при виконанні розширеного пошуку: {e}")
    
    # Видаляємо дублікати і порожні значення
    search_terms = list(set([term for term in search_terms if term]))
    
    # Формуємо SQL-умови для LIKE-запитів
    search_conditions = []
    for term in search_terms:
        term_like = f"%{term}%"
        search_conditions.append(term_like)
    
    return search_conditions