import os

from flask import Flask, request, jsonify

from application.get_offerer_enriched_data_status import get_offerer_enriched_data_status
from application.get_stock_enriched_data_status import get_stock_enriched_data_status
from application.get_user_enriched_data_status import get_user_enriched_data_status
from models.db import DATABASE_URL, db
from models.create_enriched_data_views import create_enriched_data_views
from repository.health_check_queries import does_enriched_offerer_data_exists, does_enriched_user_data_exists, \
    does_enriched_offerer_contains_data, does_enriched_users_contains_data, does_enriched_stocks_contains_data, \
    does_enriched_stock_data_exists

app = Flask(__name__, static_url_path='/static')
app.secret_key = os.environ.get('FLASK_SECRET', '+%+3Q23!zbc+!Dd@')
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_POOL_SIZE'] = int(os.environ.get('DATABASE_POOL_SIZE', 20))
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)


@app.route('/')
def ping():
    return '', 200


@app.route('/health/offerer')
def health_check_offerer_status():
    table_status = get_offerer_enriched_data_status(
        is_enriched_offerer_data_exists=does_enriched_offerer_data_exists,
        is_enriched_offerer_contains_data=does_enriched_offerer_contains_data
    )

    return jsonify(table_status), 200

@app.route('/health/user')
def health_check_user_status():
    table_status = get_user_enriched_data_status(
        is_enriched_user_data_exists=does_enriched_user_data_exists,
        is_enriched_users_contains_data=does_enriched_users_contains_data,
    )

    return jsonify(table_status), 200


@app.route('/health/stock')
def health_check_stock_status():
    table_status = get_stock_enriched_data_status(
        is_enriched_stock_data_exists=does_enriched_stock_data_exists,
        is_enriched_stocks_contains_data=does_enriched_stocks_contains_data,
    )

    return jsonify(table_status), 200


@app.route('/', methods=['POST'])
def write_enriched_data():
    token = request.args.get('token')
    bastion_token = os.environ.get('BASTION_TOKEN')
    if token == bastion_token:
        create_enriched_data_views()
        return '', 200
    return '', 401


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, use_reloader=True)
