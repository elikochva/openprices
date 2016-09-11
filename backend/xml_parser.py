# -*- coding: utf-8 -*-
import re
import os
import zipfile
import gzip
import logging
from datetime import timedelta, date
try:
    import lxml.etree as ET
except ImportError:
    import xml.etree.cElementTree as ET

import web_scraper
from sql_interface import Chain, Item, Store, CurrentPrice, PriceHistory, Unit, SessionController, \
    StoreType, StoreProduct, PriceFunction, PromotionProducts, RestrictionType, Promotion, PriceFunctionType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

float_re = re.compile(r'\d+\.*\d*')

shufersal_full_id = 7290027600007  # needed for folder name workaround
mega_full_id = 7290055700007
zol_full_id = 7290058140886


class ChainXmlParser(object):
    def __init__(self, db_chain, db=None):
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
        except AttributeError:  # no text for the tag
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
        try:
            return int(ChainXmlParser.elm2float(element, tag))
        except BaseException:  # TODO better handling
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
        except AttributeError:  # no text for the tag
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
        logger.info('Parsing {} stores'.format(chain))
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

            if len(subchains) > 1:  # handling chains with multiple subchains in same file
                subchain_id = self.elm2int(store_elm, 'subchainid')
                if subchain_id != chain.subchain_id: continue
                subchain_name = self.elm2str(store_elm, 'subchainname')
                chain.name = subchain_name
            store = Store(store_id=store_id, chain_id=chain_id, name=name, city=city, address=address, type=store_type)
            stores.append(store)

        # filter out existing stores # TODO update instead of filtering?
        existing_stores_ids = set(
            store.store_id for store in self.db.query(Store).filter(Store.chain_id == chain.id).all())
        new_stores = [store for store in stores if store.store_id not in existing_stores_ids]
        if new_stores:
            logger.info('adding {} new stores to chain {}\n'.format(len(new_stores), chain))
            self.db.bulk_insert(new_stores)
            self.db.commit()

    def get_products_prices(self, store, prices_xml):
        """
        Parse prices xml and return all items in it in a dictionary of Item: price
        Args:
            store: db store
            prices_xml:

        Returns:

        """
        item_elm_name = 'item'
        if not [tag for tag in prices_xml.iter(item_elm_name)]:  # TODO handling sudden change of file format!
            item_elm_name = 'product'

        products_prices = {}
        for item_elm in prices_xml.iter(item_elm_name):
            code = self.elm2int(item_elm, 'itemcode')
            # TODO zolBagadol has no item code for internal items
            # if self.chain.name == 'זול ובגדול':
            #     is_internal = not self.elm2bool(item_elm, 'innerbarcode')  # TODO !!!
            # else:
            is_external = self.elm2bool(item_elm, 'itemtype')  # 1 is global, 0 is internal
            is_external &= len(
                str(code)) >= 13  # TODO: double check for internal item value & code because of stupid chains (zol)
            name = self.elm2str(item_elm, 'itemname')
            quantity = self.elm2float(item_elm, 'quantity')
            if quantity > 10 ** 3:  # TODO some sotres use wrong numbers for some of the products here
                quantity = 0
            unit = self.elm2str(item_elm, 'unitqty')
            # TODO add itemstatus?
            item = StoreProduct(
                code=code, store_id=store.id, external=is_external, name=name, quantity=quantity,
                unit=unit
            )
            price = self.elm2float(item_elm, 'itemprice')
            products_prices[item] = price

        logger.info('Parsed items: {}'.format(len(products_prices)))
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
        products = db.query(StoreProduct).filter(StoreProduct.external == True). \
            filter(StoreProduct.item_id == None).yield_per(page_size)

        item_codes_ids = dict(db.query(Item.code, Item.id).yield_per(page_size))
        i = 0
        for product in products:
            code = product.code
            item_id = item_codes_ids[code]
            product.item_id = item_id
            i += 1
            if not i % 10000:
                print('flushing ', i)
                db.session.flush()

        db.commit()

    @staticmethod
    def set_internal_items_ids(db):
        """
        need an easy interface for finding all internal items, across all stores, that are actually the same item.
         then we can manually assign them to same Item.

         flow should be something like this:
             get part of the item name
             find all store products that *may* match
                possible options:
                a. only products without item id
                b. also products with item id (probably less useful)

             manually choose all that are the same
             verify selection
             - now we have a group of same product from different stores
             look if there is possible existing Item that match this group.
                if there is - assign the group to it
                if not - create one, and assign the group to it

        Args:
            db:

        Returns:

        """
        pass

    def parse_store_prices(self, store, file_date=None):
        """
        for a given Chain and Store, parse prices file from (date) and add the items to DB.

        Important:
            this method assumes the calls for parsing files from different dates are ordered.
            That is, calling for parsing file from older date, after a newer file was already parsed, may cause the DB
            to brake!!! (still working on it though)
        Args:
            store: DB Store
        """
        # TODO clean up file getting part
        file_date = file_date or date.today()
        logger.info('Parsing store: {} prices ({})'.format(store, file_date))
        try:
            prices_xml = self.get_prices_file(store, file_date)
        except BaseException:
            logger.exception("something went wrong while trying to get prices for {}".format(store))
            return
        if prices_xml is None:
            logger.warn("Missing prices xml for {}!".format(store))
            return
        # get all products from the file
        products_prices = self.get_products_prices(store, prices_xml)

        # 1) add new items to main items table
        self.add_new_items(products_prices)

        # 2) add new products to store_products table
        self.add_new_store_products(store, products_prices)

        self.db.flush()  # need to commit in order to assign ids to Items and StoreProducts #TODO maybe session.flush?

        # 3) update price history table
        self.update_history_table(store, products_prices, file_date)

        self.db.flush()  # commit needed for next step

        # update current prices table
        if file_date == date.today():
            self.update_current_prices(store)
        self.db.commit()  # finally - commit everything ot DB

    def add_new_items(self, products_prices):
        """
        Add new items (if found such) to items table

        Args:
            products_prices:

        Returns:

        """
        existing_items_codes = set(code for code,  # this ',' is there for unpacking the results
                                   in self.db.query(Item.code).yield_per(self.page_size))

        global_products = set(product for product in products_prices if product.is_external())
        new_global_items = set(item for item in global_products if item.code not in existing_items_codes)
        if new_global_items:
            new_items = [Item.from_store_product(product) for product in new_global_items]  # generate new Items
            logger.info('adding new global items to items table ({})'.format(len(new_items)))
            self.db.bulk_insert(new_items)

    def add_new_store_products(self, store, products_prices):
        """
        Add new products to store (if any new products exist)

        Args:
            store:
            products_prices:

        Returns:

        """
        exiting_codes = set(code for code,  # this ',' is there for unpacking the results
                            in self.db.query(StoreProduct.code).filter(
            StoreProduct.store_id == store.id).yield_per(self.page_size))

        new_products = set(product for product in products_prices if product.code not in exiting_codes)
        if new_products:
            logger.info('adding new store products to store products table ({})'.format(len(new_products)))
            self.db.bulk_insert(new_products)

    def update_history_table(self, store, products_prices, file_date):
        """
        Update price history table to include the changes that new parsing had found.
        assuming files with dates are parsed in order of dates
        see comments in code for full flow description.

        Args:
            store:
            products_prices:

        """
        store_id = store.id
        # there is a need to have StoreProducts with actual IDs (the ones created in the xml parsing stage has their
        # id set to None until added to DB.
        # since __hash__ and __eq__ for StoreProduct are defined by (store_id, code), we can do this trick
        # that will switch the unassigned products with existing ones (that have id)
        db_products = set(self.db.query(StoreProduct).filter(StoreProduct.store_id == store_id).all())
        products_prices = dict(
            [(product, products_prices[product]) for product in db_products if product in products_prices])

        parsed_ids = set(p.id for p in products_prices)

        all_products = set(self.db.query(PriceHistory).join(StoreProduct) \
                           .filter(StoreProduct.store_id == store_id) \
                           .filter(PriceHistory.end_date == None).yield_per(self.page_size))
        # is None test won't work here^

        all_products_ids = set(p.store_product_id for p in all_products)

        # we have 3 different stages here

        # 1) add items that don't have current price (new items, or that were out of store)
        new_products = [PriceHistory(store_product_id=product.id, price=products_prices[product], start_date=file_date)
                        for product in products_prices if product.id not in all_products_ids]
        if new_products:
            logger.info('Adding {} new items (no current price) to history table'.format(len(new_products)))
            self.db.bulk_insert(new_products)

        # 2) update end_date for items that were removed from store today
        # (appear in db as having current price (have end_date == None), and not appearing in today file)
        removed_from_store = [p for p in all_products if
                              p.store_product_id not in parsed_ids]  # TODO change to set methods
        if removed_from_store:
            logger.info('Updating end_date to yesterday for all items that are out of store ({})'.
                        format(len(removed_from_store)))
            for item in removed_from_store:  # TODO bulk_update
                # update end_date to yesterday # TODO (or for today?)
                item.end_date = file_date - timedelta(days=1)

        # 3) items that need to update their current price:
        # all items with end_date == None and also appear in db_products_prices are possible candidates for this.

        #   a) need to find all items that have new prices
        update_candidate_ids = set(p.store_product_id for p in all_products if p.store_product_id in parsed_ids)
        ids_prices = dict(
            (p.store_product_id, p.price) for p in all_products if p.store_product_id in update_candidate_ids
        )
        updated_parsed_products = [product for product in products_prices if
                                   product.id in update_candidate_ids and
                                   abs(float(ids_prices[product.id]) - products_prices[product]) > 0.01
                                   # TODO better type safety for this check
                                   ]

        updated_products_ids = set(p.id for p in updated_parsed_products)

        new_prices = [PriceHistory(store_product_id=product.id, price=products_prices[product], start_date=file_date)
                      for product in updated_parsed_products]

        if new_prices:
            logger.info('Inserting new entries for all items with new price ({})'.format(len(new_prices)))
            self.db.bulk_insert(new_prices)

        # b) need to update old entry to have yesterday(?) as last date for previous price
        # [assuming code is running every day]
        history_updated_products = [product for product in all_products if
                                    product.store_product_id in updated_products_ids]
        if history_updated_products:
            logger.info('Updating end_date to yesterday for all items that have new price ({})'.
                        format(len(history_updated_products)))
            for item in history_updated_products:  # TODO bulk update
                item.end_date = file_date - timedelta(days=1)

    def update_current_prices(self, store):
        """
        Update current prices table for this store.
        Assuming price history table is already updated (and committed)

        Args:
            store:
        """
        # this is brute force solution, but probably the fastest...
        old_current_prices = self.db.query(CurrentPrice).join(StoreProduct) \
            .filter(StoreProduct.store_id == store.id).yield_per(self.page_size)
        for item in old_current_prices:
            self.db.delete(item)

        self.db.flush()
        new_current_prices = self.db.query(PriceHistory).join(StoreProduct) \
            .filter(StoreProduct.store_id == store.id) \
            .filter(PriceHistory.end_date == None).yield_per(self.page_size)

        self.db.bulk_insert([
                                CurrentPrice(store_product_id=p.store_product_id, price=p.price) for p in
                                new_current_prices
                                ])

    def parse_store_promos(self, store, file_date=None):
        file_date = file_date or date.today()
        logger.info('Parsing promos for store: {}  ({})'.format(store, file_date))
        try:
            promos_xml = self.get_promos_file(store, file_date)
        except BaseException:
            logger.exception("something went wrong while trying to get promos for {}".format(store))
            return
        # TODO change handling on all lower levels to raise exceptions
        if promos_xml is None:
            logger.warn("Missing promos xml for {}!".format(store))
            return

        promos = self.get_promos_from_file(store, promos_xml)
        # basic same flow as prices:
        # 1) find all old promotions that still continue and update their end date to today
        # 2) add all new promotions
        # 3) check if promotions is renewed after some time????
        for p in promos:
            print(p)

    def get_promos_from_file(self, store, promos_xml):

        p_elm_name = 'promotion'

        store_id = store.id
        promotions = []
        for p_elm in promos_xml.iter(p_elm_name):
            internal_promotion_code = self.elm2int(p_elm, 'promotionid')
            description = self.elm2str(p_elm, 'promotiondescription')

            products = self.get_promotion_products(p_elm, store)
            restrictions = self.get_promotion_restrictions(p_elm, store)

            price_func = self.get_promotion_price_function(p_elm)
            promo = Promotion(internal_promotion_code=internal_promotion_code, store_id=store_id,
                              description=description)
            promotions.append({
                'promotion': promo,
                'restrictions': restrictions,
                'products': products,
                'price_func': price_func
            })

        return promotions

    def get_promotion_products(self, p_elm, store):
        item_codes = []
        products = []
        for item in p_elm.iter('promotionitems'):
            item_code = self.elm2int(item, 'itemcode')
            item_codes.append(item_code)

        for item_code in item_codes:
            try:
                products.append(self.db.query(StoreProduct).filter(StoreProduct.store_id == store.id) \
                                .filter(StoreProduct.code == item_code).one())
            except BaseException:
                pass  # TODO not adding unrecognized item

        return products

    def get_promotion_restrictions(self, p_elm, store):
        restrictions = {}

        restrictions['min_quantity'] = self.elm2int(p_elm, 'minqty')
        restrictions['max_quantity'] = self.elm2int(p_elm, 'maxqty')

        club_ids = []
        for club in p_elm.iter('clubs'):
            club_ids.append(self.elm2int(club, 'clubid'))
        restrictions['club_ids'] = club_ids

        return dict((k, v) for k, v in restrictions.items() if v)

    def get_promotion_price_function(self, p_elm):
        func_type = PriceFunctionType(self.elm2int(p_elm, 'discounttype'))
        if func_type == PriceFunctionType.percentage:
            print(1)
            amount = self.elm2float(p_elm, 'discountrate')
            if amount > 100:  # TODO normalize rate for chains that show it in
                amount /= 100.0
        elif func_type == PriceFunctionType.total_price:
            amount = self.elm2float(p_elm, 'discountedprice')
        return PriceFunction(function_type=func_type, value=amount)

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

    def get_folder(self):  # TODO: remove workaround by fixing web scraper folder names to be taken from DB
        folder = self.chain.name
        if self.chain.full_id == shufersal_full_id:
            folder = 'שופרסל'
        if self.chain.full_id == mega_full_id:
            folder = 'מגה'
        return folder

    def get_stores_file(self, file_date=None):
        """
        Get the stores.xml file of the given chain
        Args:
            chain: chain from DB
            file_date: not really needed unless you know the stores had changed
        Returns:
            str: path to xml file
        """
        stores_file = self.get_file_path(parent_folder=self.get_folder(),
                                         pattern=web_scraper.ChainScraper.get_stores_pattern(file_date))
        if stores_file is None:
            logger.info("couldn't find Stores file for chain: {}, date {}".format(self.chain, file_date))
            logger.info("Trying to download it...")
            chain_scraper = web_scraper.db_chain_factory(self.chain)
            stores_file = chain_scraper.get_stores_xml(file_date)
        return stores_file

    def get_prices_file(self, store, file_date=None):
        pattern = web_scraper.ChainScraper.get_prices_pattern(store.store_id, file_date)
        prices_file = self.get_file_path(self.get_folder(), pattern)
        if prices_file is None:
            logger.info("couldn't find Prices file for store: {}".format(store))
            logger.info("Trying to download it...")
            chain_scraper = web_scraper.db_chain_factory(self.chain)
            prices_file = chain_scraper.get_prices_xml(store.store_id, pattern, file_date)
        return self.get_parsed_file(prices_file)

    def get_promos_file(self, store, file_date=None):
        promos_file = self.get_file_path(parent_folder=self.get_folder(),
                                         pattern=web_scraper.ChainScraper.get_promos_pattern(store.store_id, file_date))
        if promos_file is None:
            logger.info("couldn't find Promos file for chain: {}, date {}".format(self.chain, file_date))
            logger.info("Trying to download it...")
            chain_scraper = web_scraper.db_chain_factory(self.chain)
            promos_file = chain_scraper.get_promos_xml(store.store_id)
        return self.get_parsed_file(promos_file)

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
    db = SessionController(db_path='sqlite:///C:/Users/eli/python projects/shopping/backend/test.db', db_logging=False)  #
    #
    # for chain in db.query(Chain):
    #     if chain.name != 'סופר דוש': continue
    #     parser = ChainXmlParser(chain, db)
    #     for store in db.query(Store).filter(Store.chain_id == chain.id):
    #         parser.parse_store_promos(store)
    #         break
    #     # break


    store = db.query(Store).filter(Store.name.contains('מב. גני אביב לוד')).one()
    chain = db.query(Chain).filter(Chain.id == store.chain_id).one()
    parser = ChainXmlParser(chain, db)
    # f = parser.get_prices_file(store, d)
    for i in reversed(range(200)):
        d = date.today() - timedelta(days=i)
        parser.parse_store_prices(store, d)
    # prices = parser.get_products_prices(store, f)
    # print(prices)
    # parser.get_products_prices()

if __name__ == '__main__':
    main()
