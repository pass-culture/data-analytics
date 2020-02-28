import os
from pprint import pprint

from flask import Flask, request

from db import DATABASE_URL, db
from create_enriched_data_views import create_enriched_data_views
from repository.health_check_repository import is_enriched_offerer_data_exists

app = Flask(__name__, static_url_path='/static')
app.secret_key = os.environ.get('FLASK_SECRET', '+%+3Q23!zbc+!Dd@')
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_POOL_SIZE'] = int(os.environ.get('DATABASE_POOL_SIZE', 20))
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False


db.init_app(app)


@app.route('/')
def ping():
    return '', 200


@app.route('/health')
def health_check():
    pprint('HEALTH')
    is_enriched_offerer_data_exists()
    return '', 200


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
