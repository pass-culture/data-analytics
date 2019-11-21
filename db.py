import os

from sqlalchemy import create_engine

DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://pass_culture:passq@postgres-product:5432/pass_culture')

ENGINE = create_engine(DATABASE_URL)
CONNECTION = ENGINE.connect()
