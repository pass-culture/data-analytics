from db import db


def is_enriched_offerer_data_exists():
    query = '''select * from information_schema.tables WHERE table_name = 'enriched_offerer_data';'''
    results = db.session.execute(query).fetchall()

    return len(results) == 1


def is_enriched_user_data_exists():
    query = '''select * from information_schema.tables WHERE table_name = 'enriched_user_data';'''
    results = db.session.execute(query).fetchall()

    return len(results) == 1


def is_enriched_stock_data_exists():
    query = '''select * from information_schema.tables WHERE table_name = 'enriched_stock_data';'''
    results = db.session.execute(query).fetchall()

    return len(results) == 1

