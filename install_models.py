from models.install import install_materialized_views
from install_database_extensions import install_database_extensions

from models.db import db
from flask import Flask
from utils.logger import logger
import os

from sqlalchemy import orm

app = Flask(__name__, static_url_path='/static')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

with app.app_context():
    install_database_extensions(app)

    orm.configure_mappers()
    for db_model in models.models:
         model_to_create = db_model.__table__
         model_to_create.create(bind=db.engine, checkfirst=True)
    db.session.commit()

    install_materialized_views()

logger.info('[INSTALL MODELS] Installation completed')
