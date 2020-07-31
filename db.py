import os

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine

LOCAL_DATABASE_URL = ['postgresql://pass_culture:passq@localhost:5435/pass_culture',
                      'postgresql://pass_culture:passq@datasource-postgres-blue:5432/pass_culture']
DATABASE_URL = os.environ.get('DATABASE_URL', LOCAL_DATABASE_URL[0])
ENGINE = create_engine(DATABASE_URL)

db = SQLAlchemy()


class AppNameNotFound(Exception):
    pass
