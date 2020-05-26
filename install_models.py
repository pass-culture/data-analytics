from models.install import install_models, install_database_extensions, install_materialized_views

from models.db import db
from flask import Flask
from utils.logger import logger
import os


app = Flask(__name__, static_url_path='/static')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

with app.app_context():
    install_database_extensions()
    install_models()
    install_materialized_views()

logger.info('[INSTALL MODELS] Installation completed')
