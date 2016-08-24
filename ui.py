# -*- coding: utf-8 -*-
from datetime import date
import matplotlib
from matplotlib import pyplot
import logging
from sql_interface import Chain, Store, Item, CurrentPrice, PriceHistory, SessionController, sqlite
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


class UI(object):

    def __init__(self):
        self.db = SessionController()#db_path=sqlite)

    def get_item_history(self, item):
        """
        get all the history prices values of given item
        Args:
            item_id:
            store:

        Returns:
            query of PriceHistory with the given conditions
        """

        q = self.db.query(PriceHistory)
        cond = [
            PriceHistory.item_id == item.item_id,
            # PriceHistory.chain_id == chain.id,
            PriceHistory.store_id == item.store_id
        ]
        res = self.db.filter_and(q, cond).all()
        return res

    def find_items_with_history(self):
        """
        Helper function for testing purposes.
        returns query of all items that have history data
        """
        q = self.db.query(PriceHistory)
        cond = [
            PriceHistory.end_date != None,
            PriceHistory.store_id == 1,
        ]
        return self.db.filter_and(q, cond).yield_per(10000)

    def main(self):
        matplotlib.rc('font', family='Arial')
        items = self.find_items_with_history()
        # import random
        # item_i = random.randint(0, len(items))

        for item in items:
            h = self.get_item_history(item)
            dates = [(i.start_date, i.end_date if i.end_date else date.today()) for i in h]
            prices = [(i.price, i.price) for i in h]
            dates = [item for sublist in dates for item in sublist]
            prices = [item for sublist in prices for item in sublist]
            pyplot.plot(dates, prices, label=h[0].item.name)
            break
        pyplot.legend()
        pyplot.show()


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

if __name__ == '__main__':
    ui = UI()
    ui.main()
