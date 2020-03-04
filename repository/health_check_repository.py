from db import db


def is_enriched_offerer_data_exists():
    query = '''select * from information_schema.tables WHERE table_name = 'enriched_offerer_data';'''
    results = db.session.execute(query).fetchall()

    return len(results) == 1


def is_enriched_offerer_contains_data():

    if is_enriched_offerer_data_exists():
        query = 'SELECT COUNT(*) FROM enriched_offerer_data'
        results = db.session.execute(query)
        return results.rowcount > 0

    return False


def is_enriched_user_data_exists():
    query = '''select * from information_schema.tables WHERE table_name = 'enriched_user_data';'''
    results = db.session.execute(query).fetchall()

    return len(results) == 1


def is_enriched_users_contains_data():

    if is_enriched_user_data_exists():
        query = 'SELECT COUNT(*) FROM enriched_user_data'
        results = db.session.execute(query)
        return results.rowcount > 0

    return False


def is_enriched_stock_data_exists():
    query = '''select * from information_schema.tables WHERE table_name = 'enriched_stock_data';'''
    results = db.session.execute(query).fetchall()

    return len(results) == 1


def is_enriched_stocks_contains_data():

    if is_enriched_stock_data_exists():
        query = 'SELECT * FROM enriched_stock_data'
        results = db.session.execute(query)
        return results.rowcount > 0

    return False
