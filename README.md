TODO
====
0) all TODOs in code...

1) data issues:
    a. RamiLevi and ZolVbegadol are the same chain (same full_id), and also have *the same subchain id!!!*
       but have 2 different websites - how to represent in the DB?
       solution for now - give ZolVabagdol different (hardcoded subchain id) 2

    b. need to create some kind of admin interface for manually defining Item(s) from StoreProducts (for internal items, items
       with ambiguous names/unit/quantity etc)
       e.g. for 'מלפפון': we want to be able to define same Item for all internal item codes

    c. TivTaam have multiple items with ItemType=0 (internal item) although these are external items


2) general:
    * logger for each module
    * fix/add/update docstrings

3) performance:
    - seems like the list comprehensions checks are taking too much time
    - maybe can use more memory in some way?
    - also - reduce db committing (think hard on what commits can be omitted from the flow (maybe need only the last one)
    - multi threaded solution for web access (download and parsing)

4) UI:
    a. define and implement DB interface for basic data retrieval (web page interface - compare current basket prices)
       need to have:
        ItemsBasket (add, remove, get_total_price [*including promotions]...)
        store_compare_selection (add, remove, etc...)
        easy to use item search utility (by name
    b. do the same for advanced data retrieval (history data) - see trends analyzer  [may have thee same interface]
    c. implement web UI (start simple, than go crazy...)
    d. trends analyzer:
       will give trends of prices for:
        - item over time (days, weeks, etc), including details of price change over days of weeks
        - some defined basket for different networks/stores
        - similar items
        - etc


5) web scraping:
    a. clean up the basic interface:
       need to have:
        - get_prices, get_stores, get_promos: which will get the full files. each with option for specific date stamp (supported only
          in some chains webpages)
        - get daily updates (non full files) for prices/promos. maybe not worth the effort. need to determine!
        - get_all_data that will get all the full files (and daily updates?) in one operation
        - clean_up function to clean all data (with date parameter?). can be used by the ChainXmlParser to clean files that
          were committed to db

    b. missing chain implementation:
        * freshmarket: wrong cerdentials at gov webpage. and empty webpage with correct login?
        * http://operations.edenteva.co.il/Prices/index  # unavailable right now


Code dependencies:
==================
* python 3 - probably will run on 2.7 also assuming all other dependencies are met...
* BeautifulSoup (bs4) (webpage parsing)
* SqlAlchemy (1.1.0b3 - beta version, since native Enum support isn't working in earlier versions)
* lxml (for fast xml parsing - can be replaced by native xml of python in imports)
* requests (online resource fetching)


online resources:
===================
1) http://www.economy.gov.il/Trade/ConsumerProtection/Instructions/DocLib/O2015004355.pdf
2) http://www.justice.gov.il/SitePages/OpenFile.aspx?d=6e912Mdhgu5lbUUjdGs76H4rsi6rJKBB7ODCgudMdlQ%3d
