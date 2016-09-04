import time
import sys, os
sys.path.append(os.path.join(os.path.dirname(os.path.relpath(__file__)), '../backend'))
from backend.ui import UI
from backend.ui import ShopPlanner
from flask import Flask, request, session, render_template, jsonify

app = Flask(__name__)
app.config.from_object(__name__)
db = UI()
shop_planner = None

@app.route('/')
def index():
    return render_template('index.html', cities=db.get_cities())


@app.route('/_show_stores', methods=['GET'])
def store_select():
    city = request.args.get('city', 0, type=str)
    stores = db.get_city_stores_json(city)
    return jsonify(stores)


@app.route('/_search', methods=['GET'])
def search_items():
    s = time.time()
    name = request.args.get('search')
    stores_ids = request.args.getlist('store_ids[]')
    items = []
    for store_id in stores_ids:
        items.extend(db.get_store_items_by_name_json(name, store_id))
    print(time.time() - s)
    return jsonify(items=items)


@app.route('/_total_price')
def get_total_price():
    product_ids = request.args.get('product_ids[]')
    totals = db.get_totals_json(product_ids)
    return jsonify(totals=totals)

if __name__ == '__main__':
    app.run()
