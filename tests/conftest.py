import os

import pytest
from flask import Flask

from models.db import db
from models.db import DATABASE_URL


@pytest.fixture(scope='session')
def app():
    app = Flask(__name__, static_url_path='/static')

    app.secret_key = os.environ.get('FLASK_SECRET', '+%+3Q23!zbc+!Dd@')
    app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
    app.config['SQLALCHEMY_POOL_SIZE'] = int(os.environ.get('DATABASE_POOL_SIZE', 20))
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['TESTING'] = True

    db.init_app(app)
    app.app_context().push()

    return app
