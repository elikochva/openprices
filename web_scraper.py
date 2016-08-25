# -*- coding: utf-8 -*-
import os
import re
import logging
from datetime import datetime
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import unicodedata
from enum import Enum
from bs4 import BeautifulSoup
from sql_interface import Chain, ChainWebAccess, Store, Item, SessionController
import xml_parser

# remove annoying logger prints from requests
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
logging.getLogger("requests").setLevel(logging.WARNING)

file_pattern = re.compile(
    r'.*(?P<type>Stores|Promo|Price(s)?)'
    r'(?P<full>Full)?'  # TODO only "Full" files?
    r'(-|_)?'
    r'(?P<id>\d{13})'
    r'((-|_)'
    r'(?P<store>\d{2,3}))?' # 2 for Coopshop store id numbers
    r'(?P<bikoret>\d?)'    # Mega has 4 digits store id part, when last digit is bikoret number
    r'(-|_)'
    r'(?P<full_date>'
        r'(?P<date>'
            r'(?P<year>\d{4})'
            r'(?P<month>\d{2})'
            r'(?P<day>\d{2})'
        r')'
        r'(?P<hour>\d{2})'
        r'(?P<min>\d{2})'
    r').*'
)

# TODO removing
stores_file_pattern = re.compile(re.sub(re.escape('(?P<type>Stores|Promo|Price(s)?)'), r'(?P<type>Stores)', file_pattern.pattern))
full_file_pattern = re.compile(re.sub(re.escape('Full)?'), r'Full)', file_pattern.pattern))
price_file_pattern = re.compile(re.sub(re.escape('(?P<type>Stores|Promo|Price(s)?)'), r'(?P<type>Price(s)?)', full_file_pattern.pattern))
promo_file_pattern = re.compile(re.sub(re.escape('(?P<type>Stores|Promo|Price(s)?)'), r'(?P<type>Promo)', full_file_pattern.pattern))

# this magic code is for handling the additional Unicode characters in some of the chain names (in MOE webpage)
def filter_non_printable(s):
    """
    filter all non printable chars from a string
    Args:
        s: string to filter

    Returns:
        str: filtered string
    """
    PRINTABLE = set(('Lu', 'Ll', 'Nd', 'Zs', 'Mn', 'Lo', 'Po'))
    result = []
    for c in s:
        c = unicodedata.category(c) in PRINTABLE and c or u'#'
        result.append(c)
    return u''.join(result).replace(u'#', u' ')


def bs_parse_url(url):
    """
    get a BeautifulSoup parsed webpage from url
    Args:
        url:

    Returns:

    """
    try:
        html = requests.get(url)
    except requests.exceptions.SSLError:
        html = requests.get(url, verify=False)  # TODO: fix SSL certification
    return BeautifulSoup(html.text, 'html.parser')


def bs_parse_page(text):
    return BeautifulSoup(text, 'html.parser')

def db_chain_factory(chain):
    web_access = chain.web_access
    return web_scraper_factory(chain.name, web_access.url, web_access.username, web_access.password)

def web_scraper_factory(name, url, username, password):
    """
    create ChainScraper of the appropriate type according to given parameters
    Args:
        name:
        url:
        username:
        password:

    Returns:
        ChainScraper
    """
    if 'publishedprices' in url:
        url = url[:url.index('.co.il') + len('.co.il')]
        return PublishedpricesDatabase(url=url, chain_name=name, username=username, password=password)
    elif 'shufersal' in url:
        return Shufersal()
    elif 'matrixcatalog.co.il' in url:
        return Nibit(chain_name=name)
    elif 'mega' in url:
        return Mega()
    elif 'zolvebegadol' in url:
        return ZolVebegadol()
    elif 'bitan' in url:
        return Bitan()
    # TODO add all other options
    else:
        return None

class MissingFileException(Exception):
    pass

class GovDataScraper(object):
    """
    This class gets the different chains websites and login details from the ministry of economy webpage
    """

    def __init__(self, db=None):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)  # logger or
        self.db = db or SessionController()
        self.chain_table_url = "http://www.economy.gov.il/Trade/ConsumerProtection/Pages/PriceTransparencyRegulations.aspx"

    def parse_chains_to_db(self):
        """
        Parse the information in the ministry of economy web page, and populate the DB with the results.

        Will create a Chain and ChainWebAccess for each chain in the web page.
         Additional subchain parsing should be done in different step (after parsing each chain stores file)
        """
        page = bs_parse_url(self.chain_table_url)
        tag = page.find('th')  # find first table header, and then search backwards to get full table
        while not tag('table'):
            tag = tag.parent
        table = tag

        full_subchains_ids = self.db.query(Chain.full_id, Chain.subchain_id).all()
        for row in table.find('tbody').find_all('tr'):
            cells = row.find_all('td')
            if cells:
                name = re.sub(' +', ' ', filter_non_printable(cells[0].text).strip())
                # print(name)
                url = cells[1].find('a')['href']
                username, password = self.parse_login_data(cells[2])
                chain_scraper = web_scraper_factory(name, url, username, password)
                if chain_scraper is None:
                    self.logger.warn("No scarper defined for {} ({})".format(name, url))
                    continue

                full_id = chain_scraper.get_chain_full_id()
                if full_id is None:
                    self.logger.warn("Couldn't find full id for {}.\n skipping".format(name))
                    continue

                subchains_ids = chain_scraper.get_subchains_ids()
                for subchain in subchains_ids:
                    if (int(full_id), subchain) in full_subchains_ids:
                        self.logger.info("chain {} already in DB ".format(name))
                        continue
                    self.logger.info('Adding chain {}'.format(name))
                    chain = Chain(full_id=full_id, name=name, subchain_id=subchain)
                    self.db.add(chain)
                    self.db.commit()  # need to commit for getting the chain.id set by the DB

                    chain_web_access = ChainWebAccess(chain_id=chain.id, url=url, username=username, password=password)
                    self.db.add(chain_web_access)
                    self.db.commit()

    def parse_login_data(self, login_data_cell):
        """
        parse login data from table cell containing it (as text)
        :param login_data_cell: table cell with the login information
        :return: (username, password)
        """
        user = password = ''
        for br in login_data_cell.find_all('br'):
            br.replace_with('\n')
        text = login_data_cell.text
        lines = text.split('\n')
        data_match = lambda x: re.search('[a-zA-Z0-9_]+', x)  # find username/password strings
        for line in lines:
            data = data_match(line)
            if data:
                if u'שם משתמש' in line:
                    user = data.group()
                if u'סיסמא' in line or u'סיסמה' in line:
                    password = data.group()
        return user, password

    @staticmethod
    def get_chain_id(name, url, username, password):
        site = web_scraper_factory(name, url, username, password)
        if site:
            return site.get_chain_full_id()

    @staticmethod
    def get_subchain_id(name):  # TODO make general case!!!
        if name == 'זול ובגדול':
            return 1


class ChainScraper(object):
    """

    """

    def __init__(self, url, chain_name=None, username='', password=''):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__) # logger or
        self.name = chain_name
        self.url = url
        # self.base_page = None
        self.session = None
        self.stores_items = {}
        self.stores = None
        self.session = self.login(url, username, password)
        self.id = self.get_chain_full_id()

    def login(self, url, user, password):
        """

        :param url: url to login page
        :param user: username
        :param password: password
        :return: A BeautifulSoup parsed page of the database
        """
        s = requests.Session()
        return s

    def get_chain_full_id(self):
        raise NotImplementedError

    def get_subchains_ids(self):
        xml = xml_parser.ChainXmlParser.get_parsed_file(self.get_stores_xml())
        return xml_parser.ChainXmlParser.get_subchains_ids(xml)

    def get_chain_folder(self):
        """
        get the (relative) path to the chain folder
        :return:
        """
        if not os.path.exists(self.name):
            os.makedirs(self.name)
        return self.name

    def download_all_data(self, date=None):
        """
        will download all relevant files from the chain web page, and return list of paths to the files
        Args:
            date: date of files to download

        Returns:
            list(str): list of paths to downloaded files
        """
        pattern = self.set_pattern_date(full_file_pattern, date)
        return self.download_files_by_pattern(pattern, date)

    def download_files_by_pattern(self, pattern=file_pattern, date=None):
        """
        download all files that match the given pattern
        Args:
            pattern (re.pattern):

        Returns:
            list(str): list of paths to downloaded files
        """
        raise NotImplementedError

    def download_url_to_path(self, url, file_path, session=None):
        """
        download given url into given file path
        Args:
            url:
            file_path:
            session:

        Returns:

        """
        session = session or self.session
        with open(file_path, 'wb') as f:
            res = session.get(url, stream=True, verify=False)
            if not res.ok:
                return  # error
            for block in res.iter_content(1024*1000):
                f.write(block)
        return file_path

    @staticmethod
    def set_pattern_date(pattern, date):
        """
        set the date for a given (file) pattern
        Args:
            pattern:
            date:

        Returns:
            re.pattern: compiled pattern with the date groups set to match given date
        """
        return re.compile(re.sub(
            re.escape(r'(?P<date>(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2}))'),
            r'(?P<date>(?P<year>{:04})(?P<month>{:02})(?P<day>{:02}))'.format(date.year, date.month, date.day),
            pattern.pattern))

    @staticmethod
    def set_pattern_store(store_id, pattern):
        return re.compile(re.sub(re.escape('(?P<store>\d{2,3})'), r'(?P<store>{:03})'.format(store_id), pattern.pattern))

    @staticmethod
    def get_prices_pattern(store_id, date=None):
        pattern = ChainScraper.set_pattern_date(price_file_pattern, date)
        return ChainScraper.set_pattern_store(store_id, pattern)

    @staticmethod
    def get_stores_pattern(date=None):
        if date:
            return ChainScraper.set_pattern_date(stores_file_pattern, date)
        else:
            return stores_file_pattern

    def get_stores_xml(self, date=None):
        """
        get stores xml and return path to the downloaded file

        Returns:
            str: path to downloaded file
        """
        pattern = stores_file_pattern
        if date is not None:
            pattern = self.set_pattern_date(pattern, date=date)

        paths = self.download_files_by_pattern(pattern, date)
        return paths[0]

    def get_today_timestamp(self):
        return self.get_date_timestamp(datetime.today())

    def get_date_timestamp(self, date=None):
        date = date or datetime.today()
        return '{:04}{:02}{:02}'.format(date.year, date.month, date.day)

    def get_prices_xml(self, store_id, pattern=price_file_pattern, date=None):
        """
        download specific prices xml(.gz)
        Args:
            store_id:

        Returns:

        """
        pattern = self.set_pattern_store(store_id, pattern)
        try:
            return self.download_files_by_pattern(pattern, date)[0]
        except IndexError:
            self.logger.warn("Can't find xml for store {}".format(store_id))
            # raise MissingFileException("Can't find xml for store {}".format(store_id))

    def get_promos_xml(self, store_id):
        """
        download specific store promotion file
        Args:
            store_id:

        Returns:

        """
        return self._get_single_xml(store_id, promo_file_pattern)


class Shufersal(ChainScraper):
    """
    Shufersal chain page scraping
    """

    class Categories(Enum):
        all = 0
        prices = 1
        prices_full = 2
        promos = 3
        promos_full = 4
        stores = 5

    def __init__(self):
        super().__init__(url='http://prices.shufersal.co.il/', chain_name='שופרסל')

    def login(self, url, user, password):
        s = requests.Session()
        s.get(url)
        return s

    def get_chain_full_id(self):
        page = bs_parse_url(self.url)
        some_file = page.find('td', text=re.compile('Price')).string
        return some_file.split('-')[0][re.search('\d', some_file).start():]

    def get_stores_xml(self, date=None):
        page = bs_parse_url(self.url)
        last_page_url = page.find('a', text='>>')['href']
        last_page = bs_parse_url(self.url + last_page_url)
        url = last_page.find('a', {'href': re.compile('stores', flags=re.IGNORECASE)})['href']
        file_path = url.split('/')[-1]  # drop all portal info
        file_path = file_path[:file_path.index('?')]  # drop additional query info
        file_path = os.path.join(self.get_chain_folder(), file_path)
        return self.download_url_to_path(url, file_path)

    def download_all_data(self, date=None):
        # TODO: not implemented correctly!!!
        self.download_files_by_pattern(pattern=full_file_pattern)

    def get_prices_xml(self, store_id, pattern=price_file_pattern, date=None):
        matches = list(filter(None.__ne__, [pattern.match(f) for f in os.listdir(self.get_chain_folder())]))
        files = [m.string for m in matches if
                 int(m.group('store')) == int(store_id) and
                 m.group('date') == self.get_today_timestamp()]
        if any(files):
            return os.path.join(self.get_chain_folder(), files[0])

        url = "http://prices.shufersal.co.il/FileObject/UpdateCategory?catID={}&storeId={}".format(
            Shufersal.Categories.prices_full.value, store_id)
        page = bs_parse_url(url)
        url = [a['href'] for a in page.find_all('a') if pattern.match(a['href'])][0]
        file_path = os.path.join(self.get_chain_folder(), url.split('?')[0].split('/')[-1])   # TODO add today dir also to path?
        if not os.path.exists(file_path):
            return self.download_url_to_path(url, file_path)
        return file_path

    def download_files_by_pattern(self, pattern=file_pattern, date=None):
        # import multiprocessing
        file_paths = []
        page = bs_parse_url(self.url)
        while page.find('a', text='>'):
            refs = [a['href'] for a in page.find_all('a')]
            # p = multiprocessing.Pool(10)
            # p.map(f, refs)
            for url in refs:
                if pattern.match(url):
                    file_path = os.path.join(self.get_chain_folder(), url.split('?')[0].split('/')[-1])
                    self.download_url_to_path(url, file_path)
            # p.close()
            # p.join()
            next_page_url = page.find('a', text='>')['href']
            page = bs_parse_url(self.url + next_page_url)
        return file_paths


# TODO part of playing with multiprocessing the file
def download_url_to_file(url, file_path, session=None):
    session = session
    with open(file_path, 'wb') as f:
        res = session.get(url, stream=True)
        if not res.ok:
            return  # error
        for block in res.iter_content(1024):
            f.write(block)
    return file_path

def f(a, pattern=file_pattern):
    # for a in self.base_page.find_all('a'):
    if pattern.match(a):
        session = requests.session()
        file_path = os.path.join(a.split('?')[0].split('/')[-1])
        download_url_to_file(a, file_path, session)


class PublishedpricesDatabase(ChainScraper):
    def __init__(self, url='https://url.publishedprices.co.il', chain_name=None, username='', password=''):
        self.base_url = url
        super().__init__(url=url, chain_name=chain_name, username=username, password=password)

    def login(self, url, user, password):
        s = requests.Session()
        login_url = url + '/login'
        res = s.get(login_url, verify=False)

        soup = bs_parse_page(res.content)
        token = soup.find('input', {'name': 'csrftoken'})['value']
        payload = {
            'url': login_url,
            'username': user,
            'password': password,
            'csrftoken': token
        }
        res = s.post(login_url + '/user', data=payload)
        return s

    def get_chain_full_id(self):
        body = 'iDisplayLength=1'  # this number will define the number of file results that we will get
        res = self.session.post(self.base_url + '/file/ajax_dir', data=body, verify=False)
        for s in res.content.decode('utf8').split('"'):
            m = file_pattern.match(s)  # part of file name
            if m:
                return m.group('id')

    def download_all_data(self, date=None):
        return self.download_files_by_pattern()

    def download_files_by_pattern(self, pattern=full_file_pattern, date=None):
        folder = self.get_chain_folder()
        body = 'iDisplayLength=10000'  # this number will define the number of file results that we will get
        res = self.session.post(self.base_url + '/file/ajax_dir', data=body)
        pattern = pattern or re.compile(self.id)
        files = [file_name for file_name in res.content.decode('utf8').split('"') if pattern.match(file_name)]
        file_paths = []
        for file_name in files:
            file_path = os.path.join(folder, file_name)
            file_paths.append(file_path)
            if not os.path.exists(file_path):
                url = self.base_url + '/file/d/' + file_name
                self.download_url_to_path(url, file_path)
        return file_paths


class Nibit(ChainScraper):
    # TODO use the info in http://matrixcatalog.co.il/Content/instructions.txt
    # for better scraping
    def __init__(self, chain_name):
        super().__init__(url='http://matrixcatalog.co.il/NBCompetitionRegulations.aspx', chain_name=chain_name)

    def get_chain_full_id(self):
        page = bs_parse_url(self.url)
        for tr in page.find('table').find_all('tr'):
            cells = tr.find_all('td')
            if cells and cells[1].text == self.name:  # column [1] is the chain name
                return file_pattern.match(cells[0].text).group('id')

    def download_all_data(self, date=None):
        self.download_files_by_pattern(file_pattern, date) # TODO is date supported for matrixcatalog?

    def download_files_by_pattern(self, pattern=file_pattern, date=None):
        page = bs_parse_url(self.url)
        file_paths = []
        for tr in page.find('table').find_all('tr'):
            cells = tr.find_all('td')
            if cells and cells[1].text == self.name:
                url = 'http://matrixcatalog.co.il/' + cells[7].find('a')['href'].replace('\\', '/')
                file_name = url.split('/')[-1]
                if pattern.match(file_name):
                    file_path = os.path.join(self.get_chain_folder(), file_name)
                    file_paths.append(file_path)
                    if not os.path.exists(file_path):
                        self.download_url_to_path(url, file_path)

        return file_paths


class Mega(ChainScraper):
    def __init__(self):
        super().__init__(url='http://publishprice.mega.co.il/', chain_name='מגה')

    def get_chain_full_id(self):
        today_dir = self.get_today_timestamp()
        soup = bs_parse_url(self.url + today_dir)
        for a in soup.find_all('a'):
            if file_pattern.match(a.text):
                return file_pattern.match(a.text).group('id')

    def download_all_data(self, date=None):
        self.download_files_by_pattern(date=date)

    def download_files_by_pattern(self, pattern=full_file_pattern, date=None):
        url = self.url + self.get_date_timestamp(date)
        soup = bs_parse_url(url)
        folder_path = os.path.join(self.get_chain_folder(), self.get_date_timestamp(date))
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        file_paths = []
        for a in soup.find_all('a'):
            if pattern.match(a.text):
                file_path = os.path.join(folder_path, a.text)
                file_paths.append(file_path)
                if not os.path.exists(file_path):
                    self.download_url_to_path(url + '/' + a['href'], file_path)

        return file_paths


class ZolVebegadol(ChainScraper):
    def __init__(self):
        super().__init__(url='http://zolvebegadol.com/', chain_name='זול ובגדול')

    def get_subchains_ids(self):
        return [0]

    def get_chain_full_id(self):
        today_dir = self.get_today_timestamp()
        soup = bs_parse_url(self.url + today_dir + '/gz/')
        for a in soup.find_all('a'):
            if file_pattern.match(a.text):
                return file_pattern.match(a.text).group('id')

    def download_all_data(self, date=None):
        return self.download_files_by_pattern()

    def download_files_by_pattern(self, pattern=full_file_pattern, date=None):
        url = self.url + self.get_date_timestamp(date) + '/gz/'
        soup = bs_parse_url(url)
        folder_path = os.path.join(self.get_chain_folder(), self.get_date_timestamp(date))
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        file_paths = []
        for a in soup.find_all('a'):
            if pattern.match(a.text):
                file_path = os.path.join(folder_path, a.text)
                file_paths.append(file_path)
                if not os.path.exists(file_path):
                    self.download_url_to_path(url + '/' + a['href'], file_path)

        return file_paths

class Bitan(ChainScraper):
    def __init__(self):
        super().__init__(url='http://www.ybitan.co.il/pirce_update', chain_name='יינות ביתן')
        # TODO note misspelling in the website name - probably should update it dynamiccaly?

    def get_chain_full_id(self):
        page = bs_parse_url(self.url)
        for a in page.find_all('a'):
            m = file_pattern.match(a.text)
            if m:
                return m.group('id')

    def download_files_by_pattern(self, pattern=full_file_pattern, date=None):
        folder = self.get_chain_folder()
        file_paths = []
        page = bs_parse_url(self.url)
        for a in page.find_all('a'):
            if pattern.match(a.text):
                file_path = os.path.join(folder, a.text)
                file_paths.append(file_path)
                if not os.path.exists(file_path):
                    self.download_url_to_path(self.url.rstrip('pirce_update') + a['href'], file_path)
        return file_paths


class Coop(ChainScraper):
    def __init__(self):
        super().__init__(url='http://www.coopisrael.coop/home/', chain_name='קואופ')

        self.prices_url = 'get'
        self.promos_url = 'promos'

    def get_chain_full_id(self):
        name = self.get_stores_xml()
        return file_pattern.match(name).group('id')

    def get_stores_xml(self, date=None):
        if date is not None: #or date != datetime.date
            self.logger.warn("Coop doesn't support older dates!")
        res = self.session.post(self.url + 'branches_to_xml')
        if not res.ok:
            return  # error
        return self.save_res_to_file(res)

    def get_prices_xml(self, store_id, pattern=price_file_pattern, date=None):
        params = {
            'product': '0',
            'branch': str(store_id),
            'type': 'gzip',
            'agree': 1,
        }
        res = self.session.post(self.url + 'get_prices', data=params)
        return self.save_res_to_file(res)

    def get_promos_xml(self, store_id):
        params = {
            'branch': str(store_id),
            'type': 'gzip',
            'agree': 1,
        }
        res = self.session.post(self.url + 'get_promo', data=params)
        return self.save_res_to_file(res)

    def save_res_to_file(self, res):
        file_path = str(res.headers._store['content-disposition']).split('=')[1].rstrip(r"\\')")
        file_path = os.path.join(self.get_chain_folder(), file_path)
        with open(file_path, 'wb') as f:
            for block in res.iter_content(1024*1000):
                f.write(block)
        return file_path

def main():
    try:
        db = SessionController()
        for chain in db.query(Chain):
            scraper = db_chain_factory(chain)
            print(chain.name)
            # print(scraper.get_chain_full_id())
            print(scraper.get_stores_xml())
            print(scraper.get_prices_xml(1))
            print(scraper.get_promos_xml(1))
    except BaseException as e:
        raise e


if __name__ == '__main__':
    main()
