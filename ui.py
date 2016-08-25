# -*- coding: utf-8 -*-
from datetime import date
# import matplotlib
# from matplotlib import pyplot
import logging
from sql_interface import Chain, Store, Item, CurrentPrice, PriceHistory, SessionController, StoreProduct
import xml_parser


class ItemList(object):
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.items = {}

    def clear(self):
        self.logger.info('Clearing item list')
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
            self.logger.error('Item {} not in list'.format(item))

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
        self.logger = logger or logging.getLogger(__name__)
        self.db = db or SessionController()
        self.parser = xml_parser.ChainXmlParser(db)
        self.city = city

        self.logger.info('getting city stores')
        self.stores = self.get_city_stores()
        self.logger.info(self.stores)
        self.basket = Basket()

        self.stores_items = {}
        for store in self.stores:
            self.logger.info('getting store {} items'.format(store))
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
        self.db = db or SessionController('sqlite:///linux_test.db')

    def get_city_stores(self, city):
        """
        get list of all stores in given city
        Args:
            city(str): city name

        Returns:

        """
        q = self.db.query(Store)
        return self.db.filter_or(q, [Store.city == city, Store.name == city]).all()

    def get_store_items(self, store):
        """
        get list of all items in store
        Args:
            store:

        Returns:

        """

        store_products = self.db.query(StoreProduct).filter(StoreProduct.store_id == store.id).all()
        ids = [p.id for p in store_products]
        q = self.db.query(PriceHistory).join(StoreProduct).filter(PriceHistory.end_date == None)
        prices = []
        for i in range(0, len(ids), 100):
            prices.extend(self.db.filter_in(q, PriceHistory.store_product_id, ids[i:i+99]).all())
        return prices


if __name__ == '__main__':
    ui = UI()
    stores = ui.get_city_stores('לוד')
    # return
    for store in stores:
        if store.id == 423:
            prices_history = ui.get_store_items(store)
            for p in prices_history[:10]:
                print(p)
