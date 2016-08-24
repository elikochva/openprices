* logger
* handle different time/date tags in xmls (not sure really needed. probably date timestamp is enough)
* how to incorporate promotions into table?
    * different table?

* items:
    - how to define items with only internal itemCode?
    - how to find same internal item on different chains?
    - use ItemStatus
* this ^ is more general issue - need to have the search functionality smart enough to decide what is the important
    part of the item name one someone is searching for it
    - for each item, define the name as only the words that appear at each store itemName
    - use some kind of smart learning to create 2-3 words that describe each item

* dependencies:
    BeautifulSoup (bs4) (webpage parsing)

* performance:
    - see csv log files of cProfile
    - seems like the list comprehensions checks are taking too much time
    - maybe can use more memory in some way?
    - also - reduce db committing (think hard on what commits can be omitted from the flow (mayb need only the last one)
    - multi threaded solution (download and parsing)

* online resources:
1) http://www.economy.gov.il/Trade/ConsumerProtection/Instructions/DocLib/O2015004355.pdf
2) http://www.justice.gov.il/SitePages/OpenFile.aspx?d=6e912Mdhgu5lbUUjdGs76H4rsi6rJKBB7ODCgudMdlQ%3d

web_scraper missing networks implementations:
    * freshmarket: wrong cerdentials at gov webpage. and empty webpage with correct login?
    * http://operations.edenteva.co.il/Prices/index  # unavailable right now

Design:


3(4) headed monster:
1) DB (sql) of:
   a) Networks (Id, name, url, login details) - can be created once and updated only if new network is added to the
      government data base page (see GovDataParser).
   b) Stores (Id, Network, name, address) - also can be created once and updated only if needed (new one added or
      removed, address change, etc)
   c) Items (Id, Network, Store, Date, price, etc) - each time the user runs online version of the code, need to check
      if data for today/store exists (create it if not). should allow for easy comparisons between networks/stores

   d) how to account for promotions?

   Important considerations:
   * need interface for accessing the DB that will allow for changing the base DB method without breaking anything

2) Web scraper:
   simple solution for getting all the stores and items data from the web.

   a) scrape all the networks urls and login data from the government website
      (http://www.economy.gov.il/Trade/ConsumerProtection/Pages/PriceTransparencyRegulations.aspx)
   b) for each network, grab all the data.
      some networks use same basic web page for storing the data (good :))
      some has their own specific website and/or data format (booooo!!!)
      - what to do with promotions?
    c) after (or during?) getting the data, update the DB with the new data


3) User interface:
   best part! (just kidding...)

   need to have:
   a) creating basket (with option to save basket) from list of items (easy)
      - should include search option by part of the item name (not so easy - need to be fast!)
   b) seeing basket price and availability in different stores
      - allow for easy comparisons between few (many?) stores
      - offer substitutions/similar items (how? need learning solution here...)
   c) include promotions data somehow
   d) easy to add more functionality

   need to have the "under the hood" implementation in some generic interface, and build some GUI and CLI for it
   (may help at some point: http://stackoverflow.com/questions/3523174/raw-input-in-python-without-pressing-enter)



4) trends analyzer:
    will give trends of prices for:
        - item over time (days, weeks, etc), including details of price change over days of weeks
        - some defined basket for different networks/stores
        - similar items
        - etc


