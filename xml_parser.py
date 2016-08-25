# -*- coding: utf-8 -*-
import re
import os
import zipfile
import gzip
import logging
from datetime import datetime, timedelta
try:
    import lxml.etree as ET
except ImportError:
    import xml.etree.cElementTree as ET

from sql_interface import Chain, Item, Store, CurrentPrice, PriceHistory, Unit, SessionController, \
    StoreType, StoreProduct

import web_scraper
float_re = re.compile(r'\d+\.*\d*')

shufersal_full_id = 7290027600007 # needed for folder name workaround
mega_full_id = 7290055700007
zol_full_id = 7290058140886

class ChainXmlParser(object):
    def __init__(self, db_chain, db=None, logger=None):
        logging.basicConfig(level=logging.INFO)
        self.logger = logger or logging.getLogger(__name__)
        self.db = db or SessionController()
        self.page_size = 100000
        self.chain = db_chain

    @staticmethod
    def elm2str(element, tag):
        """
        Convert xml element tag data to string

        Args:
            element: ET.element
            tag: internal tag in the element

        Returns:
            str
        """
        text = element.find(tag).text
        try:
            return text.strip()
        except AttributeError: # no text for the tag
            return ''

    @staticmethod
    def elm2int(element, tag):
        """
        Convert xml element tag data to int

        Args:
            element: ET.element
            tag: internal tag in the element

        Returns:
            int
        """
        text = element.find(tag).text
        try:
            return int(''.join([s for s in text if s.isdigit()]))
        except AttributeError: # no text for the tag
            return 0

    @staticmethod
    def elm2float(element, tag):
        """
        Convert xml element tag data to float

        Args:
            element: ET.element
            tag: internal tag in the element

        Returns:
            float
        """
        text = element.find(tag).text
        try:
            return float(float_re.match(text).group(0))
        except AttributeError: # no text for the tag
            return 0
        except TypeError:
            return 0

    @staticmethod
    def elm2bool(element, tag):
        """
        Convert xml element tag data to bool

        Args:
            element: ET.element
            tag: internal tag in the element

        Returns:
            bool
        """
        try:
            val = ChainXmlParser.elm2int(element, tag)
            # assert val == 0 or val == 1 # TODO assuming 1 is the only TRUE value. when try to assume 0 is FALSE, getting some strange
            return True if val == 1 else False
        except AttributeError as e:  # no text for the tag
            raise e  # TODO better handling

    @staticmethod
    def get_subchains_ids(xml):
        """
        return set of all subchain ids appearing in the file
        Args:
            xml: parsed xml object

        Returns:
            set(int):
        """
        return set([int(sub.text) for sub in xml.iter('subchainid')])

    def parse_stores(self):
        """
        parse the stores file of the chain and add them to DB
        Args:
            chain: DB Chain
        """
        chain = self.chain
        self.logger.info('Parsing {} stores'.format(chain))
        stores_file = self.get_stores_file()
        xml = self.get_parsed_file(stores_file)

        # code for handling stupid naming convention. TODO: ask government to enforce fixed field names
        name = chain.name
        if name in ('מחסני להב', 'מחסני השוק', 'ויקטורי'):
            store_elm_text = 'branch'
        else:
            store_elm_text = 'store'

        chain_id = chain.id
        subchains = self.get_subchains_ids(xml)
        stores = []
        for store_elm in xml.iter(store_elm_text):
            store_id = self.elm2int(store_elm, 'storeid')
            name = self.elm2str(store_elm, 'storename')
            city = self.elm2str(store_elm, 'city')
            address = self.elm2str(store_elm, 'address')
            store_type = StoreType(self.elm2int(store_elm, 'storetype'))

            if len(subchains) > 1:   # handling chains with multiple subchains in same file
                subchain_id = self.elm2int(store_elm, 'subchainid')
                if subchain_id != chain.subchain_id: continue
                subchain_name = self.elm2str(store_elm, 'subchainname')
                chain.name = subchain_name
            store = Store(store_id=store_id, chain_id=chain_id, name=name, city=city, address=address, type=store_type)
            stores.append(store)

        # filter out existing stores # TODO update instead of filtering?
        existing_stores_ids = set(store.store_id for store in self.db.query(Store).filter(Store.chain_id == chain.id).all())
        new_stores = [store for store in stores if store.store_id not in existing_stores_ids]
        if new_stores:
            self.logger.info('adding {} new stores to chain {}\n'.format(len(new_stores), chain))
            self.db.bulk_insert(new_stores)
            self.db.commit()

    def get_items_prices(self, store, prices_xml):
        """
        Parse prices xml and return all items in it in a dictionary of Item: price
        Args:
            store: db store
            prices_xml:

        Returns:

        """
        item_elm_name = 'item'
        if not [tag for tag in prices_xml.iter(item_elm_name)]: # TODO handling sudden change of file format!
            item_elm_name = 'product'

        products_prices = {}
        for item_elm in prices_xml.iter(item_elm_name):
            code = self.elm2int(item_elm, 'itemcode')
            # TODO zolBagadol has no item code for internal items
            # if self.chain.name == 'זול ובגדול':
            #     is_internal = not self.elm2bool(item_elm, 'innerbarcode')  # TODO !!!
            # else:
            is_external = self.elm2bool(item_elm, 'itemtype') # 1 is global, 0 is internal
            is_external &= len(str(code)) >= 13  # TODO: double check for internal item value & code because of stupid chains (zol)
            name = self.elm2str(item_elm, 'itemname')
            quantity = self.elm2float(item_elm, 'quantity')
            unit = self.elm2str(item_elm, 'unitqty')
            # TODO add itemstatus?
            item = StoreProduct(
                code=code, store_id=store.id, external=is_external, name=name, quantity=quantity,
                unit=unit
            )
            price = self.elm2float(item_elm, 'itemprice')
            products_prices[item] = price

        self.logger.info('Parsed items: {}'.format(len(products_prices)))
        return products_prices

    @staticmethod
    def set_products_item_id(db):
        """
        connect store products with items table. for global items only
        need to be called after db was committed
        """
        # TODO logging
        # TODO this unction assumes all global items are correctly marked as such in the files
        # and that no internal item is marked as global (which is far fetched assumption... :(
        page_size = 100000
        product_groups = db.query(StoreProduct).filter(StoreProduct.external == True).\
            filter(StoreProduct.item_id == None).group_by(StoreProduct.code).yield_per(page_size)

        item_codes_ids = dict(db.query(Item.code, Item.id).yield_per(page_size))
        for product_group in product_groups:
            if isinstance(product_group, list):
                print(list)
                code = product_group[0].code
                length = len(product_group)
                item_id = item_codes_ids[code]
                db.bulk_update(StoreProduct,
                               dict(
                                   zip(product_group, [item_id] * length)
                                   )
                               )
            else:
                code = product_group.code
                item_id = item_codes_ids[code]
                product_group.item_id = item_id

        db.commit()

    def parse_store_prices(self, store, date=None):
        """
        for a given Chain and Store, parse prices file from (date) and add the items to DB.

        Important:
            this method assumes the calls for parsing files from different dates are ordered.
            That is, calling for parsing file from older date, after a newer file was already parsed, may cause the DB
            to brake!!! (still working on it though)
        Args:
            store: DB Store
        """
        date = date or datetime.today()
        self.logger.info('Parsing store: {} prices ({})'.format(store, date))
        try:
            prices_xml = self.get_prices_file(store, date)
        except BaseException:
            self.logger.exception("something want bad when trying to get prices for {}".format(store))
            return
        # TODO change handling on all lower levels to raise exceptions
        if prices_xml is None:
            self.logger.warn("Missing prices xml for {}!".format(store))
            return

        chain_id = self.chain.id
        store_id = store.id

        parsed_products_prices = self.get_items_prices(store, prices_xml)

        # 1) add new items to main items table
        existing_items_codes = set(code for code, # this ',' is there for unpacking the results
                                   in self.db.query(Item.code).yield_per(self.page_size))

        # need to separate internal items from regular items

        # TODO internal products not used right now since some manual manipulation needed here
        global_products = set(product for product in parsed_products_prices if product.is_external())

        new_global_items = set(item for item in global_products if item.code not in existing_items_codes)
        if new_global_items:
            new_items = [Item.from_store_product(product) for product in new_global_items] # generate new Items
            self.logger.info('adding new global items to items table ({})'.format(len(new_items)))
            self.db.bulk_insert(new_items)

        # 2) add new products to store_products table
        exiting_store_products_codes = set(code for code,   # this ',' is there for unpacking the results
            in self.db.query(StoreProduct.code).filter(StoreProduct.store_id == store_id).yield_per(self.page_size))

        new_store_products = set(product for product in parsed_products_prices if product.code not in exiting_store_products_codes)
        if new_store_products:
            self.logger.info('adding new store products to store products table ({})'.format(len(new_store_products)))
            # for i in new_store_products:
                # self.db.add(i)
            self.db.bulk_insert(new_store_products)
        self.db.commit()


        # 4) update price history table
        db_products = set(self.db.query(StoreProduct).filter(StoreProduct.store_id == store_id).all())
        # since __hash__ and __eq__ for StoreProduct are defined by (store_id, code) we can do this trick
        parsed_products_prices = dict([(product, parsed_products_prices[product]) for product in db_products if product in parsed_products_prices])
        parsed_ids = set(p.id for p in parsed_products_prices)

        all_products = set(self.db.query(PriceHistory).join(StoreProduct)\
            .filter(StoreProduct.store_id == store_id) \
            .filter(PriceHistory.end_date == None).yield_per(self.page_size))
        # is None test won't work here^
        all_products_ids = set(p.store_product_id for p in all_products)

        # we have 3 different categories here
        # 1) items that don't have current price (new items, or that were out of store)
        new_products = [
            PriceHistory(store_product_id=product.id, price=parsed_products_prices[product])
            for product in parsed_products_prices if product.id not in all_products_ids
            ]
        if new_products:
            self.logger.info('Adding {} items with no current price'.format(len(new_products)))
            self.db.bulk_insert(new_products)

        # 2) items that were removed from store today - need to update end_date
        # (appear in db as having current price (have end_date == None), but not appearing in today file)
        removed_from_store = [product for product in all_products if product.store_product_id not in parsed_ids]  # TODO change to set methods
        if removed_from_store:
            self.logger.info('Updating end_date to yesterday for all items that are out of store ({})'.
                             format(len(removed_from_store)))
            for item in removed_from_store:
                # update end_date to yesterday # TODO (or for today?)
                item.end_date = date - timedelta(days=1)

        # 3) items that need to update their current price:
        # all items with end_date == None and also appear in db_products_prices  are possible candidates for this.
        #   a) need to find all items that have new prices
        update_candidate_ids = set(p.store_product_id for p in all_products if p.store_product_id in parsed_ids)
        ids_prices = dict((p.store_product_id, p.price) for p in all_products if p.store_product_id in update_candidate_ids)
        updated_parsed_products = [product for product in parsed_products_prices if
                                   product.id in update_candidate_ids and
                                   abs(float(ids_prices[product.id]) - parsed_products_prices[product]) > 0.01 # TODO better type safety for this check
                                   ]

        updated_products_ids = set(p.id for p in updated_parsed_products)

        new_prices = [PriceHistory(store_product_id=product.id, price=parsed_products_prices[product])
                      for product in updated_parsed_products]

        if new_prices:
            self.logger.info('Inserting new entries for all items with new price ({})'.format(len(new_prices)))
            self.db.bulk_insert(new_prices)

        #   b) need to update old entry to have yesterday(?) [assuming code is running every day] as last date for previous price
        history_updated_products = [product for product in all_products if product.store_product_id in updated_products_ids]
        if history_updated_products:
            self.logger.info('Updating end_date to yesterday for all items that have new price ({})'.
                             format(len(history_updated_products)))
            for item in history_updated_products:
                item.end_date = date - timedelta(days=1)
        self.db.commit()

        # TODO: need to update the current prices table (db or python???)
        # need to:
        # 1) update existing items with new prices if price had changed
        # 2) add new items if don't exist
        # 3) remove items that were dropped from store

        # short way (for now at least)
        # remove all current items

        # get all items for this store
        self.logger.warn('updating current price table is working currently :(((((((')
        return
        current_products = self.db.query(CurrentPrice).join(StoreProduct).filter(StoreProduct.store_id == store_id).yield_per(self.page_size)
        ids = set(p.id for p in current_products)
        q = self.db.query(CurrentPrice).order_by()
        current_products = self.db.filter_in(q, CurrentPrice.id, ids).yield_per(self.page_size)
        self.logger.info('Deleting all items for {} from current items table'.format(store))
        current_products.delete(synchronize_session=False).limit(1000)

        # Must have commit before next step
        self.db.commit()

        current_products = [
            CurrentPrice(store_product_id=product.id, price=parsed_products_prices [product])
            for product in parsed_products_prices]

        self.logger.info('Inserting all current items({})'.format(len(current_products)))
        self.db.bulk_insert(current_products)
        self.db.commit()

    @staticmethod
    def get_parsed_file(file_path):
        """
        get a parsed xml object from a given file path
        the file can be either compressed gz file or not

        Args:
            file_path: path to file

        Returns:

        """
        if ChainXmlParser.is_gz(file_path):
            xml = ChainXmlParser.get_xml_from_gz(file_path)
        elif ChainXmlParser.is_zip(file_path):
            f = zipfile.ZipFile(file_path, 'r')
            for name in f.namelist():
                if web_scraper.file_pattern.match(name):
                    xml = f.open(name).read()
        elif ChainXmlParser.is_xml(file_path):
            try:
                with open(file_path, encoding="utf16") as f:
                    xml = f.read()
            except UnicodeDecodeError:
                with open(file_path, encoding="utf8") as f:
                    xml = f.read()
        return ChainXmlParser.parse_xml_object(xml)

    def get_folder(self):     # TODO: remove workaround by fixing web scraper folder names to be taken from DB
        folder = self.chain.name
        if self.chain.full_id == shufersal_full_id:
            folder = 'שופרסל'
        if self.chain.full_id == mega_full_id:
            folder = 'מגה'
        return folder

    def get_stores_file(self, date=None):
        """
        Get the stores.xml file of the given chain
        Args:
            chain: chain from DB
            date: not really needed unless you know the stores had changed
        Returns:
            str: path to xml file
        """
        stores_file = self.get_file_path(parent_folder=self.get_folder(), pattern=web_scraper.ChainScraper.get_stores_pattern(date))
        if stores_file is None:
            self.logger.info("couldn't find Stores file for chain: {}, date {}".format(self.chain, date))
            self.logger.info("Trying to download it...")
            chain_scraper = web_scraper.db_chain_factory(self.chain)
            stores_file = chain_scraper.get_stores_xml(date)
        return stores_file

    def get_prices_file(self, store, date=None):
        pattern = web_scraper.ChainScraper.get_prices_pattern(store.store_id, date)
        prices_file = self.get_file_path(self.get_folder(), pattern)
        if prices_file is None:
            self.logger.info("couldn't find Prices file for store: {}".format(store))
            self.logger.info("Trying to download it...")
            chain_scraper = web_scraper.db_chain_factory(self.chain)
            prices_file = chain_scraper.get_prices_xml(store.store_id, pattern, date)
        return self.get_parsed_file(prices_file)

    @staticmethod
    def get_file_path(parent_folder, pattern):
        """
        find file path
        Args:
            parent_folder:
            pattern (re.pattern):

        Returns:

        """
        for dirpath, dirnames, filenames in os.walk(parent_folder):
            for f in filenames:
                if pattern.match(f):
                    return os.path.join(dirpath, f)
    @staticmethod
    def is_gz(file_path):
        return ChainXmlParser.is_file_type(file_path, 'gz')

    @staticmethod
    def is_zip(file_path):
        return ChainXmlParser.is_file_type(file_path, 'zip')

    @staticmethod
    def is_xml(file_path):
        return ChainXmlParser.is_file_type(file_path, 'xml')

    @staticmethod
    def is_file_type(file_path, ext):
        """
        Check if given file has the given file type extension

        Args:
            file_path: path to file
            ext: type extension

        Returns:
            bool:
        """
        return file_path.lower().split('.')[-1] == ext

    @staticmethod
    def get_xml_from_gz(file_path):
        """
        Get xml file (as str) from gz file

        Args:
            file_path: path to the xml.gz file

        Returns:
            str: the xml file
        """
        decompressed = gzip.GzipFile(file_path, mode='r')
        decompressed.seek(0)
        xml_file = decompressed.read()
        return xml_file

    @staticmethod
    def parse_xml_object(xmlobj):
        """
        Parse given xml file str and return the xml's ElementTree

        Args:
            xmlobj: xml file str [result of open(xml).read()]

        Returns:
            ElementTree: parsed xml object
        """
        tree = ET.fromstring(xmlobj)
        # we can convert the xml to lower case, since the intresting data is in hebrew anyway
        t = ET.tostring(tree)
        t = t.lower()
        return ET.fromstring(t)


def main():
    db = SessionController(db_logging=False)

    parser = ChainXmlParser(db)
    for chain in db.query(Chain):
        if 'יינות' not in chain.name: continue
        parser.parse_stores(chain)
        stores = db.query(Store).filter(Store.chain_id == chain.id).all()
        for store in stores:
            parser.parse_store_prices(chain, store)
            break

if __name__ == '__main__':
    main()
