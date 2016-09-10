# -*- coding: utf-8 -*-
from multiprocessing import Process, Pool
from itertools import repeat
import argparse
import web_scraper
from sql_interface import SessionController, Chain, Store, dbs
from xml_parser import ChainXmlParser

import time


def download_chain_data(chain):
    try:
        scraper = web_scraper.db_chain_factory(chain)
        scraper.download_all_data()
        print('finished downloading data: {}'.format(chain.name))
    except BaseException as e:
        print('{} data download failed'.format(chain.name))
        print(e)


def parse_chain_stores(chain):
    try:
        parser = ChainXmlParser(chain)
        parser.parse_stores()
        print('parsed stores for')
    except BaseException as e:
        print(e)


def parse_chain_prices(chain, store):
    try:
        parser = ChainXmlParser(chain)
        parser.parse_store_prices(store)
        print('parsed prices for', parser.chain.name, store)
    except BaseException as e:
        print(e)


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--processes', '-p', help='run data scraping and parsing in X parallel processes', default=1, type=int)
    arg_parser.add_argument('--no-download', '-nd', help="don't download data at start (assumes data already downloaded)", default=False,
                            action='store_true')
    arg_parser.add_argument('--parse-chains', '-c', help="parse chains login data from the government webpage",
                            default=False, action='store_true')

    args = arg_parser.parse_args()

    start = time.time()
    p = Pool(processes=args.processes)
    db = SessionController()

    # 1) get all chains (and subchains)
    if args.parse_chains:
        gov = web_scraper.GovDataScraper(db)
        gov.parse_chains_to_db()

    chains = [chain for chain in db.query(Chain)]

    # 2) download all data before starting
    if not args.no_download:
        s = time.time()
        print('Downloading all chains data')
        res = p.map(download_chain_data, chains)
        print('data download: {}'.format(time.time() - s))

    # 3) parse all chain stores
    s = time.time()
    print('parsing all chains stores')
    res = p.map(parse_chain_stores, chains)
    print('stores parsing: {}'.format(time.time() - s))

    # 4) parse stores daily prices and promos
    for chain in chains:
        s = time.time()
        print('parsing prices for chain {}'.format(chain.name))
        stores = [store for store in db.query(Store).filter(Store.chain_id == chain.id)]
        p.starmap(parse_chain_prices, zip(repeat(chain), stores))
        print('chain parsing ended: {}'.format(time.time() - s))

    # ChainXmlParser.set_products_item_id(db)
    print('total time: {}'.format(time.time() - start))

if __name__ == '__main__':
    main()

