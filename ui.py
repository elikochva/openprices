# -*- coding: utf-8 -*-
from datetime import date
from sql_interface import Chain, Store, Item, CurrentPrice, PriceHistory, SessionController, StoreProduct
import xml_parser
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    import matplotlib
    from matplotlib import pyplot
except ImportError:
    logger.warn("Couldn't import matplotlib, some of UI functionality may not work")



class ItemList(object):
    def __init__(self):
        self.items = {}

    def clear(self):
        logger.info('Clearing item list')
        self.items.clear()

    def add_item(self, item):
        try:
            self.items[item] += 1
        except KeyError:
            self.items[item] = 1

    def item_num(self, item):
        try:
            return self.items[item]
        except KeyError:
            return 0

    def remove_item(self, item):
        try:
            self.items.pop(item)
        except KeyError:
            logger.error('Item {} not in list'.format(item))

    def price(self):
        # TODO need to take promotions into account (2 for 1 etc.)
        return sum([self.item_num(item) * item.price for item in self.items])

    def __str__(self):
        for item in self.items:
            print('{}: {}'.format(item, self.items[item]))


class Basket(ItemList):
    pass




class ShopPlanner(object):
    def __init__(self, city, db=None, logger=None):
        logging.basicConfig(level=logging.INFO)
        logger = logger or logging.getLogger(__name__)
        self.db = db or SessionController()
        self.parser = xml_parser.ChainXmlParser(db)
        self.city = city

        logger.info('getting city stores')
        self.stores = self.get_city_stores()
        logger.info(self.stores)
        self.basket = Basket()

        self.stores_items = {}
        for store in self.stores:
            logger.info('getting store {} items'.format(store))
            items = self.get_store_items(store)
            if not items:
                self.parser.parse_store_prices(store.chain, store)
                items = self.get_store_items(store)

            for item in items:
                try:
                    self.stores_items[item].append(store)
                except KeyError:
                    self.stores_items[item] = [store]
            # break

    def get_city_stores(self):
        """
        get all stores from the city (either in city name or in store name)

        Returns:

        """
        q = self.db.query(Store)
        cond = [Store.city == self.city, Store.name.contains(self.city)]
        return self.db.filter_or(q, cond).all()

    def get_store_items(self, store):
        """
        get all items of given store
        Args:
            store:

        Returns:

        """
        return self.db.filter_condition(CurrentPrice, CurrentPrice.store_id == store.id).all()

    def find_item_in_db(self, partial_name):
        """
        search the DB for an item that matches partial name
        Args:
            partial_name:

        Returns:

        """
        page_num = 1000
        q = self.db.query(CurrentPrice)
        q = self.db.filter_in(q, CurrentPrice.store_id, [store.id for store in self.stores])
        res = self.db.filter_and(q, [CurrentPrice.item.has(Item.name.contains(partial_name))]).\
            order_by(CurrentPrice.price).yield_per(page_num)
        if res:
            return res
        return

    def find_item(self, partial_name):
        return [item for item in self.stores_items if partial_name in item.item.name]

    def get_lowest_price_item(self, items):
        res = sorted(items, key=lambda x: x.price) #/x.item.quantity)
        if res:
            return res[0]


class UI(object):
    def __init__(self, db=None):
        self.db = db or SessionController()
        self.page_size = 10000

    def get_cities(self):
        return [c for c, in self.db.query(Store.city).distinct().order_by(Store.city).all()]

    def get_chains(self):
        return self.db.query(Chain).order_by(Chain.name).all()

    def get_chain_stores(self, chain):
        return [store for store in self.db.query(Store).join(Chain).filter(Store.chain_id == chain.id).all()]

    def get_city_stores(self, city):
        """
        get list of all stores in given city
        Args:
            city(str): city name

        Returns:

        """
        q = self.db.query(Store)
        return self.db.filter_or(q, [Store.city == city, Store.name == city]).all()

    def get_current_products(self, store):
        """
        get list of all items in store
        Args:
            store:

        Returns:

        """
        return self.db.query(CurrentPrice).join(StoreProduct).filter(StoreProduct.store_id == store.id)

    def get_product_history(self, product):
        return self.db.query(PriceHistory).filter(PriceHistory.store_product_id == product.store_product_id).\
            order_by(PriceHistory.start_date.asc()).yield_per(self.page_size)

    def find_product_in_other_stores(self, store_product, *stores):  # TODO, *stores or stores
        """
        find the same product in other stores

        Args:
            store_product:
            *stores: stores to search in (default is all stores)

        Returns:

        """
        if store_product.item_id is None:
            logger.warn("no ")
        return self.db.query(StoreProduct).join(Item).filter(Item.id == store_product.item_id)

    def get_items_with_partial_name_match(self, partial_name):
        return self.db.query(Item).filter(Item.name.like('{}%'.format(partial_name))).all()#.yield_per(self.page_size)
        # TODO: is Item.name.contains has better performance?

    def get_item_from_code(self, item_code):
        return self.db.query(Item).filter(Item.code == item_code).one()

    def get_item_products(self, item):
        """
        get all store products that are linked to given item

        Args:
            item:

        Returns:

        """
        return self.db.query(StoreProduct).filter(StoreProduct.item_id == item.id).yield_per(self.page_size)


def find_products_with_history(db):
    return db.query(PriceHistory).filter(PriceHistory.end_date != None).limit(2).all()

def print_list(lst):
    # return
    for i in lst:
        print(i)
    print('')


if __name__ == '__main__':
    ui = UI()
    """
    chains = ui.get_chains()
    print_list(chains)

    cities = ui.get_cities()
    print_list(cities)

    chain_stores = ui.get_chain_stores(chains[0])
    print_list(chain_stores)

    stores = ui.get_city_stores('לוד')
    print_list(stores)

    # for store in stores:
    #     items = ui.get_current_products(store)
    #     for p in items[:10]:
    #         print(p)
    """
    # sugar = ui.get_items_with_partial_name_match('סוכר')
    # print_list(sugar)

    # sugar_item = ui.get_item_from_code(7290011474898) # coconut sugar
    # print(sugar_item)
    # stores_sugar = ui.get_item_products(sugar_item)
    # print_list(stores_sugar)
    #
    # history = ui.get_product_history(stores_sugar[0])
    # print_list(history)

    products_with_history = find_products_with_history(ui.db)
    print_list(products_with_history )
    # products_with_history.store_products
    for p in products_with_history:
        h = ui.get_product_history(p)
        print(h.all())
        ui.find_product_in_other_stores(p)
