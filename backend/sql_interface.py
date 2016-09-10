# -*- coding: utf-8 -*-
import logging
from enum import Enum
# from datetime import datetime
import datetime
from sqlalchemy import create_engine, or_, and_, select
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import Column, Integer, BigInteger, String, ForeignKey, Date, DECIMAL, Text,\
    exists, UniqueConstraint, Boolean, func
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.types import Enum as SqlEnum
from sqlalchemy.inspection import inspect
from sqlalchemy.ext.compiler import compiles

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Base = declarative_base()

class MyBigInteger(BigInteger):
    pass

@compiles(MyBigInteger, 'sqlite')
def bi_c(element, compiler, **kw):
    return "INTEGER"

@compiles(MyBigInteger)
def bi_c(element, compiler, **kw):
    return compiler.visit_BIGINT(element, **kw)

# TODO: work around for sqlite primary key autoincrmenet issue

dbs = {
    'sqlite_development': 'sqlite:///C:/Users/eli/python projects/shopping/backend/shopping.db',
    'sqlite_testing': 'sqlite:///C:/Users/eli/python projects/shopping/backend/test.db',
    'postgres_development': 'postgresql+psycopg2://test:123@localhost:5432/shop',
    'postgres_parallel': 'postgresql+psycopg2://test:123@localhost:5432/shop_parallel'
}
db = dbs['postgres_development']
# db = dbs['postgres_parallel']

if 'sqlite' in db:
    BigInteger = MyBigInteger

class StoreType(Enum):
    unknown = 0
    physical = 1
    web = 2
    both = 3

class Unit(Enum):
    """
    Unit type enum
    """
    unknown = 0
    kg = 1
    gr = 2
    liter = 3
    ml = 4
    unit = 5
    m = 6  # Meter

    @staticmethod
    def to_unit(unit_str):
        """
        convert string to Unit enum value
        Args:
            unit_str:

        Returns:

        """
        str_dict = {
            Unit.kg: ['קג', 'קילוגרם', 'קילוגרמים', 'ק"ג'],
            Unit.gr: ['גר', 'גרמים', "גר'"],
            Unit.liter: ['ליטר', 'ליטרים', "ל'"],
            Unit.ml: ['מ"ל', 'מיליליטרים', 'מיליליטר', 'מל'],
            Unit.unit: ['יחידה'],
            Unit.m: ['מטר', 'מטרים', 'מ', "מ'"]
        }
        try:
            unit_str = unit_str.strip()
        except AttributeError:
            unit_str = ''
        for unit_type, unit_type_strings in str_dict.items():
            if any(s == unit_str for s in unit_type_strings):
                return Unit(unit_type.value)
        return Unit.unknown


class Chain(Base):
    __tablename__ = 'chains'

    id = Column(Integer, primary_key=True) #, autoincrement=True)
    full_id = Column(BigInteger, nullable=False)
    subchain_id = Column(Integer, default=None)
    name = Column(String)

    UniqueConstraint(full_id, subchain_id)

    stores = relationship("Store", backref='chain', lazy='subquery')  # one to many
    web_access = relationship('ChainWebAccess', backref='chain', uselist=False, lazy='subquery')  # one to one

    def __str__(self):
        return self.name


class ChainWebAccess(Base):
    __tablename__ = 'web_access'

    chain_id = Column(Integer, ForeignKey(Chain.id), primary_key=True)
    url = Column(String)
    username = Column(String, default='')
    password = Column(String, default='')


class Store(Base):
    __tablename__ = 'stores'

    id = Column(Integer, primary_key=True)# , autoincrement=True)
    store_id = Column(Integer)
    chain_id = Column(Integer, ForeignKey(Chain.id))
    name = Column(String)
    city = Column(String)
    address = Column(String, default='')
    type = Column(SqlEnum(StoreType))
    UniqueConstraint(store_id, chain_id)

    def __repr__(self):
        return 'id: {}. {}-{}'.format(self.id, self.name, self.address)

    # def __str__(self):
    #     return '{}-{}:{}'.format(self.chain.name, self.name, self.address)

    def __eq__(self, other):
        # must be different for different  chain-store combination because of the UniqueConstraint
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


class Item(Base):
    __tablename__ = 'items'

    id = Column(BigInteger, primary_key=True, index=True)
    code = Column(BigInteger, index=True)
    quantity = Column(DECIMAL(precision=10, scale=2))
    unit = Column(SqlEnum(Unit))
    # TODO: not as discussed, but I think it makes sense to have single unified name the same as for unit and quantity
    name = Column(Text, index=True)

    store_products = relationship('StoreProduct', backref='item', lazy='joined')

    def __repr__(self):
        return '{}: {}'.format(self.name, self.code)

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id

    @staticmethod
    def from_store_product(product):
        return Item(code=product.code, quantity=product.quantity, unit=Unit.to_unit(product.unit), name=product.name)


class StoreProduct(Base):
    __tablename__ = 'store_products'

    id = Column(BigInteger, primary_key=True)
    item_id = Column(BigInteger, ForeignKey(Item.id), nullable=True, index=True)
    store_id = Column(BigInteger, ForeignKey(Store.id), index=True)
    code = Column(BigInteger)
    external = Column(Boolean)
    name = Column(Text)
    # saving the quantity/unit_qty for cases that auto parsing don't work, to allow manual parsing
    quantity = Column(Text)
    unit = Column(Text)

    UniqueConstraint(store_id, code)

    current_prices = relationship("CurrentPrice", backref='store_product', uselist=False)#, lazy='joined')
    prices_history = relationship("PriceHistory", backref='store_product', uselist=False)#, lazy='joined')

    def is_external(self):
        return self.external

    def __repr__(self):
        return '{} - {}{}'.format(self.name, self.quantity, self.unit)

    def __str__(self):
        return self.name

    # __eq__ and __hash__ are defined for easier set/dict usage
    def __hash__(self):
        return hash((self.store_id, self.code))

    def __eq__(self, other):
        return self.store_id == other.store_id and self.code == other.code

    def __ne__(self, other):
        return not self.__eq__(other)


class PriceHistory(Base):
    __tablename__ = 'price_history'

    id = Column(BigInteger, primary_key=True)
    store_product_id = Column(BigInteger, ForeignKey(StoreProduct.id), index=True)
    start_date = Column(Date, default=datetime.date.today, index=True)
    end_date = Column(Date, default=None, index=True) # None means current
    price = Column(DECIMAL(precision=10, scale=2))

    UniqueConstraint(start_date, store_product_id)

    def __repr__(self):
        return '{}: {}<->{} = {}'.format(self.store_product.name, self.start_date, self.end_date if self.end_date is not None
                                         else datetime.date.today(), self.price)


class CurrentPrice(Base):
    __tablename__ = 'current_price'

    store_product_id = Column(BigInteger, ForeignKey(StoreProduct.id), primary_key=True)
    price = Column(DECIMAL(precision=10, scale=2))

    UniqueConstraint(store_product_id)

    def __hash__(self):
        return hash(self.store_product_id)

    # __eq__ and __hash__ are defined for easier set/dict usage
    def __eq__(self, other):
        return self.store_product_id == other.store_product_id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return '{}: {}'.format(self.store_product.name, self.price)


# """
class Promotion(Base):
    __tablename__ = 'promotions'

    id = Column(BigInteger, primary_key=True) #, autoincrement=True)
    store_id = Column(BigInteger, ForeignKey(Store.id), index=True)
    internal_promotion_code = Column(BigInteger)
    description = Column(Text)
    start_date = Column(Date, default=datetime.date.today)
    end_date = Column(Date, default=datetime.date.today)
    UniqueConstraint(store_id, internal_promotion_code)

    items = relationship('PromotionProducts', backref='promotion', lazy='joined')
    restrictions = relationship('Restrictions', backref='promotion', lazy='joined')
    price_func = relationship('PriceFunction', backref='promotion')

    def __repr__(self):
        return '{}: {}'.format(self.internal_promotion_code, self.description)


class PromotionProducts(Base):
    __tablename__ = 'promotion_prodcuts'

    promotion_id = Column(BigInteger, ForeignKey(Promotion.id), primary_key=True)
    item_id = Column(BigInteger, ForeignKey(StoreProduct.id), primary_key=True) # TODO should be store_product_id?

    def __repr__(self):
        return '{}'.format(self.item.name)

class RestrictionType(Enum):
    min_qty = 1
    max_qty = 2
    basket_price = 3
    club_ids = 4
    specific_item = 5


class Restrictions(Base):
    __tablename__  = 'restrictions'

    id = Column(BigInteger, primary_key=True) #, autoincrement=True)
    promotion_id = Column(BigInteger, ForeignKey(Promotion.id), index=True)
    restriction_type = Column(SqlEnum(RestrictionType))
    amount = Column(Integer, default=None)
    store_product_id = Column(BigInteger, ForeignKey(StoreProduct.id), nullable=True)

    def __repr__(self):
        return '{}'.format(self.restriction_type)


class PriceFunctionType(Enum):
    percentage = 0
    total_price = 1

    def __repr__(self):
        return '%' if self.vale == PriceFunctionType.percentage else '₪'

class PriceFunction(Base):
    __tablename__ = 'price_functions'

    promotion_id = Column(BigInteger, ForeignKey(Promotion.id), primary_key=True)
    function_type = Column(SqlEnum(PriceFunctionType))
    value = Column(DECIMAL(precision=10, scale=2))

    def __repr__(self):
        return '{}{}'.format(self.value, '%' if self.function_type == PriceFunctionType.percentage else '₪')
# """





class SessionController(object):
    """
    This is the DB access interface
    """
    def __init__(self, db_path=db, db_logging=False):
        logger.info('connecting to DB: {}'.format(db_path))
        self.engine = create_engine(db_path, echo=db_logging)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        Base.metadata.create_all(self.engine)
        logger.info('DB connected')

    def get_session(self):
        return self.session

    def query(self, *args):
        """
        Get a
        Args:
            models:

        Returns:

        """
        return self.session.query(*args)

    def exists(self, obj_field, value):
        (ret, ), = self.session.query(exists().where(obj_field == value))
        return ret

    def exists_where_list(self, obj_fields, values):
        return self.session.query(exists().where(and_(*[field == value for field, value in zip(obj_fields, values)]))).scalar()

    def key(self, model):
        return inspect(model).primary_key

    def bulk_insert(self, objects):
        self.session.bulk_save_objects(objects)

    def bulk_update(self, mapper, mappings):
        self.session.bulk_update_mappings(mapper, mappings)

    def flush(self):
        self.session.flush()

    def commit(self):
        """
        Commit changes to the DB
        Returns:

        """
        logger.info('Committing to db')
        try:
            self.session.commit()
        except Exception:
            logger.exception('Commit to DB failed')
            self.session.rollback()
            return False
        logger.info('Commit ended successfully')
        return True

    def update(self, model, update_dict):
        """
        session.query(Stuff).update({Stuff.foo: Stuff.foo + 1})
        :param model:
        :param update_dict:
        :return:
        """
        self.session.query(model).update(update_dict)

    def add(self, model):
        return self.session.add(model)

    def delete(self, row):
        self.session.delete(row)

    def get(self, model, **kwargs):
        instance = self.query(model).filter_by(**kwargs).first()
        if instance:
            return instance

    def get_or_create(self, model, **kwargs):
        instance = self.query(model).filter_by(**kwargs).first()
        if instance:
            return instance
        else:
            instance = model(**kwargs)
            self.add(instance)
            self.commit()
            return instance

    def instance_key(self, cls, instance):
        return [getattr(instance, key.name) for key in self.key(cls)]

    def exists_in_db(self, cls, instance):
        """
        check if the instance with same key(s) exists in DB
        Args:
            cls:
            instance:

        Returns:

        """
        exist_dict = {}
        for key in self.key(cls):
            exist_dict[key.name] = getattr(instance, key.name)

        q = self.query(cls)
        for field, value in exist_dict.items():
            q = q.filter(getattr(cls, field).like(value))
        return q.all()

    def _drop_table(self, model):
        logger.info('Dropping table {}'.format(model.__table__))
        model.__table__.drop(self.engine)

    def filter_or(self, query, conditions):
        return query.filter(or_(*conditions))

    def filter_and(self, query, conditions):
        return query.filter(and_(*conditions))

    def filter_in(self, query, column, lst):
        """

        Args:
            query:
            column:
            lst:

        Returns:

        """
        return query.filter(column.in_(lst))

    def filter_condition(self, model, cond):
        return self.query(model).filter(cond)

    def query_sum(self, query, column):
        return query.with_entities(func.sum(column)).scalar()

def main():
    engine = create_engine('sqlite:///sql_interface_test.db', echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

            # print(session.query(Chain).get(Chains_ids[Chain]))
    # for item in items.values():
    #     session.add(Item(item.code, item.Chain, item))
    session.commit()


if __name__ == '__main__':
    main()
