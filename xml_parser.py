import re
import os
import zipfile
import gzip
import logging
from datetime import datetime, timedelta
import lxml.etree as ET
from sql_interface import Chain, Item, Store, CurrentPrice, PriceHistory, Unit, SessionController

import web_scraper
float_re = re.compile(r'\d+\.*\d*')

class ChainXmlParser(object):
    def __init__(self, db_chain, db=None, logger=None):
        logging.basicConfig(level=logging.INFO)
        self.logger = logger or logging.getLogger(__name__)
        self.db = db or SessionController()
        self.page_size = 100000
        self.chain = db_chain

    def elm2str(self, element, tag):
        text = element.find(tag).text
        try:
            return text.strip()
        except AttributeError: # no text for the tag
            return ''

    def elm2int(self, element, tag):
        text = element.find(tag).text
        try:
            return int(''.join([s for s in text if s.isdigit()]))
        except AttributeError: # no text for the tag
            return 0

    def elm2float(self, element, tag):
        text = element.find(tag).text
        try:
            return float(float_re.match(text).group(0))
        except AttributeError: # no text for the tag
            return 0

    def elm2bool(self, element, tag):
        text = element.find(tag).text
        try:
            return bool(text.strip())
        except AttributeError:  # no text for the tag
            return False

    def parse_stores(self):
        """
        parse the stores file of the chain and add them to DB
        Args:
            chain: DB Chain
        """
        chain = self.chain
        self.logger.info('Parsing {} stores'.format(chain))
        stores_file = self.get_stores_file(chain)
        xml = self.get_parsed_file(stores_file)

        # code for handling stupid naming convention. TODO: ask government to enforce fixed field names
        name = chain.name
        if name in ('מחסני להב', 'מחסני השוק', 'ויקטורי'):
            store_elm_text = 'branch'
        else:
            store_elm_text = 'store'

        stores = []
        for store_elm in xml.iter(store_elm_text):
            try:
                store_id = self.elm2int(store_elm, 'storeid')  # TODO have different
                name = self.elm2str(store_elm, 'storename')
                city = self.elm2str(store_elm, 'city')
                address = self.elm2str(store_elm, 'address')
                store = Store(store_id=store_id, chain_id=chain.id, name=name, city=city, address=address)
                stores.append(store)
            except Exception:
                self.logger.exception()

        existing_stores_ids = set(store.store_id for store in self.db.query(Store).filter(Store.chain_id == chain.id).all())
        new_stores = [store for store in stores if store.store_id not in existing_stores_ids]
        if new_stores:
            self.logger.info('adding {} new stores to chain {}\n'.format(len(new_stores), chain))
            self.db.bulk_insert(new_stores)
            self.db.commit()

    def get_items_prices(self, prices_xml, gen_internal_item_code):
        """
        Parse prices xml and return all items in it in a dictionary of Item: price
        Args:
            prices_xml:
            gen_internal_item_code: function to generate item code for internal items

        Returns:

        """
        item_elm_name = 'item'
        if not [tag for tag in prices_xml.iter(item_elm_name)]: # TODO handling sudden change of file format!
            item_elm_name = 'product'

        items_prices = {}
        for item_elm in prices_xml.iter(item_elm_name):
            item_id = self.elm2int(item_elm, 'itemcode') # TODO zolBagadol has no item code for internal items
            is_intrenal =
            name = self.elm2str(item_elm, 'itemname')
            quantity = float(self.elm2str(item_elm, 'quantity') or 0)  # TODO - all function: need to handle all input cases
            unit_type = Unit.to_unit(self.elm2str(item_elm, 'unitqty'))
            item = Item(id=item_id, name=name, quantity=quantity, unit_type=unit_type)
            price = float(self.elm2str(item_elm, 'itemprice'))
            # update_date = self.str2datetime(self.elm_text(item_elm, 'priceupdatedate'))  # TODO not a must use field
            items_prices[item] = price  # now we have a list of all items and their current prices
        return items_prices

    def parse_store_prices(self, chain, store, date=None):
        """
        for a given Chain and Store, parse prices file from (date) and add the items to DB.

        Important:
            this method assumes the calls for parsing files from different dates are ordered.
            That is, calling for parsing file from older date, after a newer file was already parsed, may cause the DB
            to brake!!! (still working on it though)
        Args:
            chain: DB Chain
            store: DB Store
        """
        date = date or datetime.today()
        self.logger.info('Parsing store: {} prices ({})'.format(store, date))
        try:
            prices_xml = self.get_prices_file(chain, store, date)
        except BaseException:
            self.logger.exception("something want bad when trying to get prices for {}".format(store))
            return
        # TODO change handling on all lower levels to raise exceptions
        if prices_xml is None:
            self.logger.warn("Missing prices xml for {}!".format(store))
            return

        chain_id = chain.id
        store_id = store.id
        # TODO better cahin_item creation
        chain_store_item_id = lambda x: int('{:03}{:013}'.format(chain_id, x)) # probably chain_id is enough...

        items_prices = self.get_items_prices(prices_xml, chain_store_item_id)
        self.logger.info('Parsed items: {}'.format(len(items_prices)))
        # add new items to main items table
        existing_items_ids = set(item.id for item in self.db.query(Item).yield_per(self.page_size))
        parsed_items_ids = set(item.id for item in items_prices)
        new_items = [item for item in items_prices if item.id not in existing_items_ids]
        if new_items:
            self.logger.info('adding new items to items table ({})'.format(len(new_items)))
            self.db.bulk_insert(new_items)

        # update price history table
        q = self.db.query(PriceHistory)
        current_prices_conditions = [  # selecting all items from history table that are
            # PriceHistory.chain_id == chain_id,  # this chain - redundent
            PriceHistory.store_id == store_id,  # this store
            # using the "==" check because the "is None" check evaluates to False every time
            PriceHistory.end_date == None,       # have current value (no end_date)
        ]
        store_history_items = self.db.filter_and(q, current_prices_conditions).yield_per(self.page_size)

        # all the item ids in history data
        store_history_ids = set(item.item_id for item in store_history_items)

        # we have 3 different categories here
        # 1) items that don't have current price (new items, or that were out of store)
        new_price_history_items = [
            PriceHistory(item_id=item.id, chain_id=chain_id, store_id=store_id, price=items_prices[item])
            for item in items_prices if item.id not in store_history_ids
            ]
        if new_price_history_items:
            self.logger.info('Adding {} items with no current price'.format(len(new_price_history_items)))
            self.db.bulk_insert(new_price_history_items)

        # 2) items that were removed from store today - need to update end_date
        # (appear in db as having current price (have end_date == None), but not appearing in today file)
        removed_from_store = [item for item in store_history_items if item.item_id not in parsed_items_ids]
        # update end_date to yesterday # TODO (or for today?)
        if removed_from_store:
            self.logger.info('Updating end_date to yesterday for all items that are out of store ({})'.
                             format(len(removed_from_store)))
            for item in removed_from_store:
                item.end_date = date - timedelta(days=1)

        # 3) items that need to update their current price:
        # all items with end_date == None, and also appear in parsed_items are possible candidates for this.
        #   a) need to find all items that have new prices
        history_ids_prices = set((item.item_id, item.price) for item in store_history_items) # set() for faster check - using set TODO: benchmark
        updated_parsed_items = [item for item in items_prices if (item.id, items_prices[item]) not in history_ids_prices] # this is the *new* items (of type Item)

        #   b) need to update old entry to have yesterday(?) [assuming code is running every day] as last date to price
        updated_items_ids = set(item.id for item in updated_parsed_items) # this is the *history* items (type HistoryPrice)
        history_updated_items = [item for item in store_history_items if item.item_id in updated_items_ids]
        if history_updated_items:
            self.logger.info('Updating end_date to yesterday for all items that have new price ({})'.
                             format(len(updated_parsed_items)))
            for item in history_updated_items:
                item.end_date = date - timedelta(days=1)

        new_prices = [PriceHistory(item_id=item.id, chain_id=chain_id, store_id=store_id, price=items_prices[item])
                      for item in updated_parsed_items]
        if new_prices:
            self.logger.info('Inserting new entries for all items with new price ({})'.format(len(new_prices)))
            self.db.bulk_insert(new_prices)

        # TODO: need to update the current prices table (db or python???)
        # need to:
        # 1) update existing items with new prices if price had changed
        # 2) add new items if don't exist
        # 3) remove items that were dropped from store

        # short way (for now at least)
        # remove all current items

        # get all items for this store
        q = self.db.query(CurrentPrice)
        current_prices_conditions = [  # selecting all items from current table that are
            CurrentPrice.chain_id == chain_id,  # this chain
            CurrentPrice.store_id == store_id,  # this store
        ]
        current_items = self.db.filter_and(q, current_prices_conditions).yield_per(self.page_size)
        self.logger.info('Deleting all items for {}: {} from current items table'.format(chain, store))
        current_items.delete()

        # Must have commit before next step
        self.db.commit()
        # get all updated items from history table and insert them
        q = self.db.query(PriceHistory)
        current_prices_conditions = [  # selecting all items from history table that are
            PriceHistory.chain_id == chain_id,  # this chain
            PriceHistory.store_id == store_id,  # this store
            PriceHistory.end_date == None  # have current value (no end_date)
        ]
        current_items = self.db.filter_and(q, current_prices_conditions).yield_per(self.page_size)
        current_items = [
            CurrentPrice(item_id=item.item_id, chain_id=chain_id, store_id=store_id, price=item.price)
            for item in current_items
            ]
        self.logger.info('Inserting all current items({})'.format(len(current_items)))
        self.db.bulk_insert(current_items)
        self.db.commit()

    def get_parsed_file(self, file_path):
        """
        get a parsed xml object from a given file path
        the file can be either compressed gz file or not

        Args:
            file_path: path to file

        Returns:

        """
        if self.is_gz(file_path):
            xml = self.get_xml_from_gz(file_path)
        elif self.is_zip(file_path):
            f = zipfile.ZipFile(file_path, 'r')
            for name in f.namelist():
                if web_scraper.file_pattern.match(name):
                    xml = f.open(name).read()
        elif self.is_xml(file_path):
            with open(file_path, encoding="utf16") as f:
                xml = f.read()
        else:
            self.logger.error('unrecognized file type: {}'.format(file_path))
            return
        return self.parse_xml_object(xml)

    def get_stores_file(self, chain, date=None):
        """
        Get the stores.xml file of the given chain
        Args:
            chain: chain from DB
            date: not really needed unless you know the stores had changed
        Returns:
            str: path to xml file
        """
        stores_file = self.get_file_path(parent_folder=chain.name, pattern=web_scraper.ChainScraper.get_stores_pattern(date))
        if stores_file is None:
            self.logger.info("couldn't find Stores file for chain: {}, date {}".format(chain, date))
            self.logger.info("Trying to download it...")
            chain_scraper = web_scraper.web_scraper_factory(chain.name, chain.url, chain.username, chain.password)
            stores_file = chain_scraper.get_stores_xml(date)
        return stores_file

    def get_prices_file(self, chain, store, date=None):
        pattern = web_scraper.ChainScraper.get_prices_pattern(store.store_id, date) # TODO
        prices_file = self.get_file_path(parent_folder=chain.name, pattern=pattern)
        if prices_file is None:
            self.logger.info("couldn't find Prices file for chain: {}".format(chain))
            self.logger.info("Trying to download it...")
            chain_scraper = web_scraper.web_scraper_factory(chain.name, chain.url, chain.username, chain.password)
            prices_file = chain_scraper.get_prices_xml(store.store_id, pattern, date)
        return self.get_parsed_file(prices_file)

    def get_file_path(self, parent_folder, pattern):
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

    def is_gz(self, file_path):
        return self.is_file_type(file_path, 'gz')

    def is_zip(self, file_path):
        return self.is_file_type(file_path, 'zip')

    def is_xml(self, file_path):
        return self.is_file_type(file_path, 'xml')

    def is_file_type(self, file_path, ext):
        """
        Check if given file has the given file type extension

        Args:
            file_path: path to file
            ext: type extension

        Returns:
            bool:
        """
        return file_path.lower().split('.')[-1] == ext

    def get_xml_from_gz(self, file_path):
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

    def parse_xml_object(self, xmlobj):
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
