import os
from functools import partial

from db import DATABASE_URL, db
from flask import Flask, jsonify
from read.postgresql_database.health_check_queries import (
    is_enriched_materialized_view_queryable,
    does_enriched_offerer_contain_data,
    does_enriched_users_contains_data,
    does_enriched_stock_contain_data,
    does_enriched_offer_contain_data,
    is_enriched_view_queryable,
)
from utils.health_check.get_offer_enriched_data_status import (
    get_offer_enriched_data_status,
)
from utils.health_check.get_offerer_enriched_data_status import (
    get_offerer_enriched_data_status,
)
from utils.health_check.get_stock_enriched_data_status import (
    get_stock_enriched_data_status,
)
from utils.health_check.get_user_enriched_data_status import (
    get_user_enriched_data_status,
)

app = Flask(__name__, static_url_path="/static")
app.secret_key = os.environ.get("FLASK_SECRET", "+%+3Q23!zbc+!Dd@")
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_POOL_SIZE"] = int(os.environ.get("DATABASE_POOL_SIZE", 20))
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(app)


@app.route("/")
def ping():
    return "", 200


@app.route("/health/offerer")
def health_check_offerer_status():
    does_enriched_offerer_data_exist = partial(
        is_enriched_materialized_view_queryable,
        materialized_view_name="enriched_offerer_data",
    )
    table_status = get_offerer_enriched_data_status(
        is_enriched_offerer_data_exists=does_enriched_offerer_data_exist,
        is_enriched_offerer_contains_data=does_enriched_offerer_contain_data,
    )

    return jsonify(table_status), 200


@app.route("/health/user")
def health_check_user_status():
    does_enriched_user_data_exist = partial(
        is_enriched_materialized_view_queryable,
        materialized_view_name="enriched_user_data",
    )
    table_status = get_user_enriched_data_status(
        is_enriched_user_data_exists=does_enriched_user_data_exist,
        is_enriched_users_contains_data=does_enriched_users_contains_data,
    )

    return jsonify(table_status), 200


@app.route("/health/stock")
def health_check_stock_status():
    does_enriched_stock_data_exist = partial(
        is_enriched_view_queryable, view_name="enriched_stock_data"
    )
    table_status = get_stock_enriched_data_status(
        is_enriched_stock_data_exists=does_enriched_stock_data_exist,
        is_enriched_stocks_contains_data=does_enriched_stock_contain_data,
    )

    return jsonify(table_status), 200


@app.route("/health/offer")
def health_check_offer_status():
    does_enriched_offer_data_exist = partial(
        is_enriched_view_queryable, view_name="enriched_offer_data"
    )
    table_status = get_offer_enriched_data_status(
        is_enriched_offer_present=does_enriched_offer_data_exist,
        is_enriched_offers_contains_data=does_enriched_offer_contain_data,
    )

    return jsonify(table_status), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, use_reloader=True)
