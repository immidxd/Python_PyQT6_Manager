import datetime
import json
from sqlalchemy import (
 Column,
 Integer,
 String,
 ForeignKey,
 Numeric,
 Text,
 DateTime,
 Date,
 Boolean,
 Interval,
 func
)
from sqlalchemy.types import TypeDecorator
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import relationship, backref

# Створюємо тип, сумісний як з PostgreSQL, так і з SQLite
class JSONBType(TypeDecorator):
    impl = Text
    
    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)
        return value
    
    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value

# Імпортуємо JSONB для PostgreSQL
from sqlalchemy.dialects.postgresql import JSONB as PgJSONB

# Використовуємо JSONB для PostgreSQL або наш власний тип для SQLite
def get_jsonb_type():
    from db import engine
    if engine.dialect.name == 'postgresql':
        return PgJSONB
    else:
        return JSONBType

# Отримуємо відповідний тип JSONB на основі діалекту
JSONB = get_jsonb_type()

from db import Base  # <-- беремо Base з db.py (а не session!)


###############################################################################
#                           Основні моделі для товарів
###############################################################################
class Product(Base):
 __tablename__ = 'products'


 id = Column(Integer, primary_key=True)
 productnumber = Column(String(50), nullable=False, unique=True)
 clonednumbers = Column(Text)
 model = Column(String(500))
 marking = Column(String(500))
 year = Column(Integer)
 description = Column(Text)
 extranote = Column(Text)
 price = Column(Numeric(10, 2))
 oldprice = Column(Numeric(10, 2))
 dateadded = Column(DateTime, default=func.now())
 sizeeu = Column(String(10))
 sizeua = Column(String(10))
 sizeusa = Column(String(10))
 sizeuk = Column(String(10))
 sizejp = Column(String(10))
 sizecn = Column(String(10))
 measurementscm = Column(String(50))
 quantity = Column(Integer, default=1)
 typeid = Column(Integer, ForeignKey('types.id'))
 subtypeid = Column(Integer, ForeignKey('subtypes.id'))
 brandid = Column(Integer, ForeignKey('brands.id'))
 genderid = Column(Integer, ForeignKey('genders.id'))
 colorid = Column(Integer, ForeignKey('colors.id'))
 ownercountryid = Column(Integer, ForeignKey('countries.id'))
 manufacturercountryid = Column(Integer, ForeignKey('countries.id'))
 statusid = Column(Integer, ForeignKey('statuses.id'))
 conditionid = Column(Integer, ForeignKey('conditions.id'))
 importid = Column(Integer, ForeignKey('imports.id'))
 deliveryid = Column(Integer, ForeignKey('deliveries.id'))
 mainimage = Column(String(255))
 created_at = Column(DateTime(timezone=True), default=func.now())
 updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())


 # Relationships
 type = relationship('Type', back_populates='products', foreign_keys=[typeid])
 subtype = relationship('Subtype', back_populates='products', foreign_keys=[subtypeid])
 brand = relationship('Brand', back_populates='products', foreign_keys=[brandid])
 gender = relationship('Gender', back_populates='products', foreign_keys=[genderid])
 color = relationship('Color', back_populates='products', foreign_keys=[colorid])
 status = relationship('Status', back_populates='products', foreign_keys=[statusid])
 condition = relationship('Condition', back_populates='products', foreign_keys=[conditionid])
 import_info = relationship('Import', back_populates='products', foreign_keys=[importid])
 delivery = relationship('Delivery', back_populates='products', foreign_keys=[deliveryid])




class Type(Base):
 __tablename__ = 'types'


 id = Column(Integer, primary_key=True)
 typename = Column(String(100), nullable=False, unique=True)


 products = relationship('Product', back_populates='type')




class Subtype(Base):
 __tablename__ = 'subtypes'


 id = Column(Integer, primary_key=True)
 typeid = Column(Integer, ForeignKey('types.id'))
 subtypename = Column(String(100), nullable=False, unique=True)


 products = relationship('Product', back_populates='subtype')
 type = relationship('Type', backref='subtypes')




class Brand(Base):
 __tablename__ = 'brands'


 id = Column(Integer, primary_key=True)
 brandname = Column(String(100), nullable=False, unique=True)


 products = relationship('Product', back_populates='brand')




class Gender(Base):
 __tablename__ = 'genders'


 id = Column(Integer, primary_key=True)
 gendername = Column(String(50), nullable=False, unique=True)


 products = relationship('Product', back_populates='gender')




class Color(Base):
 __tablename__ = 'colors'


 id = Column(Integer, primary_key=True)
 colorname = Column(String(50), nullable=False, unique=True)


 products = relationship('Product', back_populates='color')




class Country(Base):
 __tablename__ = 'countries'


 id = Column(Integer, primary_key=True)
 countryname = Column(String(100), nullable=False, unique=True)
 countrycode = Column(String(2), nullable=False, unique=True)




class Status(Base):
 __tablename__ = 'statuses'


 id = Column(Integer, primary_key=True)
 statusname = Column(String(100), nullable=False, unique=True)
 statusdescription = Column(Text)


 products = relationship('Product', back_populates='status')




class Condition(Base):
 __tablename__ = 'conditions'


 id = Column(Integer, primary_key=True)
 conditionname = Column(String(100), nullable=False, unique=True)
 conditiondescription = Column(Text)


 products = relationship('Product', back_populates='condition')




class Import(Base):
 __tablename__ = 'imports'


 id = Column(Integer, primary_key=True)
 importname = Column(String(100))
 description = Column(Text)
 created_at = Column(Date)
 importdate = Column(Date)


 products = relationship('Product', back_populates='import_info')




class Delivery(Base):
 __tablename__ = 'deliveries'


 id = Column(Integer, primary_key=True)
 deliveryname = Column(String(100), nullable=False)
 description = Column(Text)
 created_at = Column(Date, default=func.now())
 deliverydate = Column(Date)
 supplier_id = Column(Integer, ForeignKey('suppliers.id'))


 products = relationship('Product', back_populates='delivery')


###############################################################################
#                           Модель для OrderDetails
###############################################################################
class OrderDetails(Base):
 __tablename__ = 'order_details'


 id = Column(Integer, primary_key=True)
 order_id = Column(Integer, ForeignKey('orders.id'), nullable=False)
 product_id = Column(Integer, ForeignKey('products.id'), nullable=False)
 quantity = Column(Integer, nullable=False)
 price = Column(Numeric(12, 2))
 discount_type = Column(String(50))
 discount_value = Column(Numeric(12, 2))
 additional_operation = Column(String(100))
 additional_operation_value = Column(Numeric(12, 2))
 total = Column(Numeric(12, 2), default=None)  # actual default via expression
 notes = Column(Text)
 created_at = Column(DateTime(timezone=True), default=func.now())
 updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())


 # Зв'язки (relationship)
 order = relationship(
     'Order',
     backref=backref('order_details', cascade='all, delete-orphan'),
     foreign_keys=[order_id]
 )
 product = relationship(
     'Product',
     backref=backref('order_detail_items', lazy='joined'),
     foreign_keys=[product_id]
 )


###############################################################################
#               Моделі для Замовлень (orders), Статусів, Оплат
###############################################################################
class OrderStatus(Base):
 __tablename__ = 'order_statuses'


 id = Column(Integer, primary_key=True)
 status_name = Column(String(100), nullable=False, unique=True)
 status_description = Column(Text)
 created_at = Column(DateTime(timezone=True), default=func.now())
 updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())


 orders = relationship('Order', back_populates='order_status')




class PaymentStatus(Base):
 __tablename__ = 'payment_statuses'


 id = Column(Integer, primary_key=True)
 status_name = Column(String(100))
 status_description = Column(Text)
 created_at = Column(DateTime(timezone=True), default=func.now())
 updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())


 orders = relationship('Order', back_populates='payment_status')




class DeliveryMethod(Base):
 __tablename__ = 'delivery_methods'


 id = Column(Integer, primary_key=True)
 method_name = Column(String(100), nullable=False, unique=True)
 description = Column(Text)
 created_at = Column(DateTime(timezone=True), default=func.now())
 updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())


 orders = relationship('Order', back_populates='delivery_method')




###############################################################################
#                               КЛАС Order
###############################################################################
class Order(Base):
 __tablename__ = 'orders'


 id = Column(Integer, primary_key=True)
 client_id = Column(Integer, ForeignKey('clients.id'), nullable=False)
 order_date = Column(Date, server_default=func.current_date())
 order_status_id = Column(Integer, ForeignKey('order_statuses.id'))
 total_amount = Column(Numeric(12, 2))
 # старе поле payment_status, тепер називаємо payment_status_text
 payment_method_id = Column(Integer, ForeignKey('payment_methods.id'))
 payment_status_text = Column("payment_status", String(50))
 payment_date = Column(Date)


 # Нове поле - посилання на табл. payment_statuses
 payment_status_id = Column(Integer, ForeignKey('payment_statuses.id'))


 delivery_method_id = Column(Integer, ForeignKey('delivery_methods.id'))
 delivery_address_id = Column(Integer, ForeignKey('addresses.id'))
 recipient_name = Column(String(255))
 tracking_number = Column(String(100))
 delivery_status_id = Column(Integer, ForeignKey('delivery_statuses.id'))
 notes = Column(Text)
 promo_code = Column(String(50))
 details = Column(JSONB)
 deferred_until = Column(Date)
 priority = Column(Integer, default=0)
 alternative_order_number = Column(String(100))
 updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
 created_at = Column(DateTime(timezone=True), default=func.now())
 broadcast_id = Column(Integer, ForeignKey('broadcasts.id'))
 number_of_lots = Column(Integer, default=1)


 # relationship на PaymentStatus
 payment_status = relationship("PaymentStatus", back_populates="orders", foreign_keys=[payment_status_id])


 # relationship на OrderStatus
 order_status = relationship("OrderStatus", back_populates="orders", foreign_keys=[order_status_id])


 # relationship на DeliveryMethod
 delivery_method = relationship("DeliveryMethod", back_populates="orders", foreign_keys=[delivery_method_id])


 # relationship на PaymentMethod
 payment_method = relationship(
     "PaymentMethod",
     back_populates="orders",
     foreign_keys=[payment_method_id]
 )


 # relationship на Client (щоб уникнути KeyError "no attribute 'client'")
 client = relationship("Client", backref="orders", foreign_keys=[client_id])




###############################################################################
#          Допоміжні моделі: PaymentMethod, DeliveryStatus, ...
###############################################################################
class PaymentMethod(Base):
 __tablename__ = 'payment_methods'


 id = Column(Integer, primary_key=True)
 method_name = Column(String(100), nullable=False)
 description = Column(Text)
 created_at = Column(DateTime(timezone=True), default=func.now())
 updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())


 # Зворотний зв'язок з Order, щоб Order.payment_method існував
 orders = relationship("Order", back_populates="payment_method", foreign_keys="Order.payment_method_id")




class DeliveryStatus(Base):
 __tablename__ = 'delivery_statuses'


 id = Column(Integer, primary_key=True)
 status_name = Column(String(100), nullable=False, unique=True)
 status_description = Column(Text)
 created_at = Column(DateTime(timezone=True), default=func.now())
 updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())


 # За потреби:
 # orders = relationship('Order', backref='delivery_status', foreign_keys=[Order.delivery_status_id])


###############################################################################
#    Приклади: clients, addresses, broadcasts, ...
###############################################################################
class Client(Base):
 __tablename__ = 'clients'


 id = Column(Integer, primary_key=True)
 last_name = Column(String(100))
 first_name = Column(String(100))
 middle_name = Column(String(100))
 gender_id = Column(Integer, ForeignKey('genders.id'))
 date_of_birth = Column(Date)
 phone_number = Column(String(20))
 email = Column(String(255))
 registration_date = Column(DateTime(timezone=True), default=func.now())
 facebook = Column(String(255))
 instagram = Column(String(255))
 tiktok = Column(String(255))
 telegram = Column(String(255))
 viber = Column(String(255))
 messenger = Column(String(255))
 olx = Column(String(255))
 first_order_date = Column(Date)
 last_order_date = Column(Date)
 last_order_address_id = Column(Integer, ForeignKey('addresses.id'))
 order_count = Column(Integer, default=0)
 average_order_value = Column(Numeric(10, 2))
 total_order_amount = Column(Numeric(10, 2))
 largest_purchase = Column(Numeric(10, 2))
 client_discount = Column(Numeric)
 bonus_account = Column(Numeric(10, 2))
 city_of_residence = Column(String(100))
 country_of_residence = Column(Integer, ForeignKey('countries.id'))
 preferred_delivery_method_id = Column(Integer, ForeignKey('delivery_methods.id'))
 preferred_payment_method_id = Column(Integer, ForeignKey('payment_methods.id'))
 address_id = Column(Integer, ForeignKey('addresses.id'))
 client_type_id = Column(Integer, ForeignKey('client_types.id'))
 rating = Column(Numeric(5, 2))
 notes = Column(Text)
 status_id = Column(Integer, ForeignKey('client_statuses.id'))
 priority = Column(Integer, default=0)
 number_of_purchased_lots = Column(Integer, default=0)
 created_at = Column(DateTime(timezone=True), default=func.now())
 updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())




class Address(Base):
 __tablename__ = 'addresses'


 id = Column(Integer, primary_key=True)
 address_line1 = Column(String(255))
 address_line2 = Column(String(255))
 city = Column(String(100))
 state = Column(String(100))
 postal_code = Column(String(20))
 country_id = Column(Integer, ForeignKey('countries.id'))
 recipient_name = Column(String(255))
 created_at = Column(DateTime(timezone=True), server_default=func.now())
 updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())




class Broadcast(Base):
 __tablename__ = 'broadcasts'


 id = Column(Integer, primary_key=True)
 broadcast_date = Column(Date)
 platform_id = Column(Integer, ForeignKey('platforms.id'))
 broadcast_topic = Column(String(255))
 description = Column(Text)
 notes = Column(Text)
 revenue = Column(Numeric(12,2), default=0)
 duration = Column(Interval)
 has_giveaway = Column(Boolean, default=False)
 has_gifts = Column(Boolean, default=False)
 created_at = Column(DateTime(timezone=True), default=func.now())
 updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())




class Platform(Base):
 __tablename__ = 'platforms'


 id = Column(Integer, primary_key=True)
 platform_name = Column(String(100), nullable=False)
 created_at = Column(DateTime(timezone=True), default=func.now())
 updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())




###############################################################################
#     Приклади та інші допоміжні (client_statuses, client_types, suppliers)
###############################################################################
class ClientStatus(Base):
 __tablename__ = 'client_statuses'


 id = Column(Integer, primary_key=True)
 status_name = Column(String(50), nullable=False, unique=True)
 status_description = Column(Text)
 created_at = Column(DateTime(timezone=True), default=func.now())
 updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())




class ClientType(Base):
 __tablename__ = 'client_types'


 id = Column(Integer, primary_key=True)
 type_name = Column(String(50), nullable=False, unique=True)
 type_description = Column(Text)
 created_at = Column(DateTime(timezone=True), default=func.now())
 updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())




class Supplier(Base):
 __tablename__ = 'suppliers'


 id = Column(Integer, primary_key=True)
 company_name = Column(String(200))
 contact_person = Column(String(200))
 synonyms_json = Column(JSONB)
 country_location_id = Column(Integer, ForeignKey('countries.id'))
 country_dispatch_id = Column(Integer, ForeignKey('countries.id'))
 city_location = Column(String(200))
 address_location = Column(Text)
 address_dispatch = Column(Text)
 supply_volume = Column(String(100))
 payment_requisites = Column(Text)
 description = Column(Text)
 status = Column(String(50), default='Активний')
 priority = Column(Integer, default=0)




###############################################################################
#   Приклади зв'язків між клієнтами: (relationship_types, connections)
###############################################################################
class RelationshipType(Base):
 __tablename__ = 'relationship_types'


 id = Column(Integer, primary_key=True)
 relationship_name = Column(String(100), nullable=False, unique=True)
 relationship_description = Column(Text)
 created_at = Column(DateTime(timezone=True), default=func.now())
 updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())




class Connection(Base):
 __tablename__ = 'connections'


 id = Column(Integer, primary_key=True)
 client_id = Column(Integer, ForeignKey('clients.id'), nullable=False)
 related_client_id = Column(Integer, ForeignKey('clients.id'), nullable=False)
 relationship_type_id = Column(Integer, ForeignKey('relationship_types.id'))
 created_at = Column(DateTime(timezone=True), default=func.now())
 updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())




# Кінець файлу models.py
