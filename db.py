import os

from sqlalchemy import create_engine

# By default DATABASE_URL is set to localhost in order to execute tests in local
# For others environments DATABASE_URL should be set as env variable

DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://pass_culture:passq@localhost:5435/pass_culture')

ENGINE = create_engine(DATABASE_URL)
CONNECTION = ENGINE.connect()
