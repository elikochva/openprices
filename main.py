import web_scraper
from sql_interface import SessionController, Chain, Store
from xml_parser import ChainXmlParser


def main():
    db = SessionController()

    # gov = web_scraper.GovDataScraper()
    # gov.parse_chains_to_db()

    for chain in db.query(Chain):
        print(chain)

if __name__ == '__main__':
    main()
