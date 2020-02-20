import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.orm import sessionmaker

LOCAL_DATABASE_URL = 'postgresql://pass_culture:passq@localhost:5435/pass_culture'
DATABASE_URL = os.environ.get('DATABASE_URL', LOCAL_DATABASE_URL)

ENGINE = create_engine(DATABASE_URL)
CONNECTION = ENGINE.connect()
Session = sessionmaker(bind=ENGINE)
SESSION = Session()


def get_connection(engine: Engine) -> Connection:
    return engine.connect()
