# -*- coding: utf-8 -*-
import web_scraper
from sql_interface import SessionController, Chain, Store
from xml_parser import ChainXmlParser


def main():
    db = SessionController(db_path=None, db_logging=False)  # 'sqlite:///linux_test.db'  'sqlite:///shopping.db'
    gov = web_scraper.GovDataScraper(db)
    gov.parse_chains_to_db()


    chain_counter = 0
    for chain in db.query(Chain):
        store_counter = 0
        if chain_counter == 2: break
        parser = ChainXmlParser(chain, db)
        parser.parse_stores()
        if chain.name == 'זול ובגדול': continue  # TODO has very bad xml formats... need to find how to parse them correctly
        for store in db.query(Store).filter(Store.chain_id == chain.id):
            # if store.city != 'לוד': continue
            if store_counter == 2: break
            parser.parse_store_prices(store)
            store_counter += 1
        chain_counter += 1

    ChainXmlParser.set_products_item_id(db)

if __name__ == '__main__':
    main()
