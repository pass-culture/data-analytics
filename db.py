import os
from functools import wraps
from typing import Callable

import psycopg2
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine

from utils.logger import logger

LOCAL_DATABASE_URL = ['postgresql://pass_culture:passq@localhost:5435/pass_culture', 'postgresql://pass_culture:passq@datasource-postgres-blue:5432/pass_culture']
DATABASE_URL = os.environ.get('DATABASE_URL', LOCAL_DATABASE_URL[0])
ENGINE = create_engine(DATABASE_URL)
CONNECTION = ENGINE.connect()

def connect_to_database(f: Callable) -> Callable:
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            connection = psycopg2.connect(DATABASE_URL)
            output = f(connection)
        except (Exception, psycopg2.Error) as error:
            logger.error("[PostgreSQL ERROR]", error)
            raise error

        finally:
            if connection:
                connection.close()

        return output

    return decorated_function

db = SQLAlchemy()
