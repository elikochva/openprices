import time
from datetime import date
import sys, os

sys.path.append(os.path.join(os.path.dirname(os.path.relpath(__file__)), '../backend'))
from backend.ui import UI, SessionController
from backend.ui import ShopPlanner
from flask import Flask, request, session, render_template, jsonify


def timing(func):
    def timed(*args, **kwargs):
        s = time.time()
        res = func(*args, **kwargs)
        print('{}: {:.2f} ]{}, {})'.format(func.__name__, time.time() - s, args or '', kwargs or ''))
        print(res)
        return res

    return timed


# TODO move all "json" methods from UI to here - makes more sense this way
app = Flask(__name__)
app.config.from_object(__name__)
# app.debug = True

# db = UI(SessionController('sqlite:///C:/Users/eli/python projects/shopping/backend/test.db'))
db = UI()
shop_planner = None


@timing
def get_city_stores(city):
    stores = db.get_city_stores(city)
    return [(store.id, store.chain.name, store.name) for store in stores]


@timing
def get_stores_items_by_name(ids, name):
    products = db.get_stores_current_items_by_name(name, ids)
    return [{'label': p.name, 'value': p.id} for p in products]


@timing
def item2stores_products(item_id, stores_ids):
    # TODO: too much db queries need to clean it up somehow
    item = db.get_item_by_id(item_id)
    stores = db.get_stores_by_ids(stores_ids)
    products = db.item2current_products(item, stores)
    if products:
        return [{  # TODO very slow because of joins, need to be done at the query level
                    'id': item_id,
                    'name': item.name,
                    'price': float(p.price),
                    'store_id': p.store_product.store_id,
                } for p in products]


@timing
def get_products_history(item_id, stores_ids):
    item = db.get_item_by_id(item_id)
    stores = db.get_stores_by_ids(stores_ids)
    products = db.item2history_products(item, stores)
    store_product_history = {}
    for p in products:
        try:
            store_product_history[p.store_product].append(p)
        except KeyError:
            store_product_history[p.store_product] = [p]

    return [{
                'name': item.name,
                'price_history': get_product_price_history(store_product_history[store_history]),
                'store_id': store_product_history[store_history][0].store_product.store_id
            } for store_history in store_product_history]


def get_product_price_history(price_history_list):
    data = [(p.start_date, float(p.price)) for p in price_history_list]  # TODO ,p.end_date
    data.extend((p.end_date or date.today(), float(p.price)) for p in price_history_list)
    data.sort(key=lambda x: x[0])
    data = [(time.mktime(d[0].timetuple())*1000, d[1]) for d in data]
    return data


@app.route('/compare')
def index():
    return render_template('index.html', cities=db.get_cities())



@timing
@app.route('/_show_stores', methods=['GET'])
def store_select():
    city = request.args.get('city', 0, type=str)
    stores = get_city_stores(city)
    return jsonify(stores)


@timing
@app.route('/_search', methods=['GET'])
def search_items():
    name = request.args.get('search')
    stores_ids = request.args.getlist('stores_ids[]')
    items = get_stores_items_by_name(stores_ids, name)
    return jsonify(items=items)


@timing
@app.route('/_add_item')
def add_item_to_basket():
    item_id = request.args.get('item_id')
    stores_ids = request.args.getlist('stores_ids[]')
    products = item2stores_products(item_id, stores_ids)
    return jsonify(products=products)


@timing
@app.route('/history')
def price_history():
    return render_template('history.html', cities=db.get_cities())


@timing
@app.route('/_get_item_history')
def item_history():
    item_id = request.args.get('item_id')
    stores_ids = request.args.getlist('stores_ids[]')
    products_history = get_products_history(item_id, stores_ids)
    return jsonify(products_history=products_history)


if __name__ == '__main__':
    app.run(threaded=False)
