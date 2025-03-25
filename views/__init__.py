# views/__init__.py
"""
Пакет views містить всі представлення (вікна, вкладки) програми.
"""
from .main_window import MainWindow
from .products_tab import ProductsTab, fix_sold_filter
from .orders_tab import OrdersTab, fix_unpaid_filter

__all__ = ['MainWindow', 'ProductsTab', 'OrdersTab', 'fix_sold_filter', 'fix_unpaid_filter']