from db import db


def does_enriched_offerer_data_exists():
    query = '''SELECT * FROM pg_matviews WHERE matviewname = 'enriched_offerer_data';'''
    results = db.session.execute(query).fetchall()
    return len(results) == 1


def does_enriched_offerer_contains_data():
    if does_enriched_offerer_data_exists():
        query = '''SELECT * FROM enriched_offerer_data limit 1;'''
        results = db.session.execute(query)
        return results.rowcount > 0

    return False


def does_enriched_user_data_exists():
    query = '''SELECT * FROM pg_matviews WHERE matviewname = 'enriched_user_data';'''
    results = db.session.execute(query).fetchall()

    return len(results) == 1


def does_enriched_users_contains_data():
    if does_enriched_user_data_exists():
        query = '''SELECT * FROM enriched_user_data limit 1;'''
        results = db.session.execute(query)
        return results.rowcount > 0

    return False


def does_enriched_stock_data_exists():
    query = '''SELECT * FROM information_schema.tables WHERE table_name = 'enriched_stock_data';'''
    results = db.session.execute(query).fetchall()

    return len(results) == 1


def does_enriched_stocks_contains_data():
    if does_enriched_stock_data_exists():
        query = '''SELECT * FROM enriched_stock_data limit 1;'''
        results = db.session.execute(query)
        return results.rowcount > 0

    return False


def does_enriched_offer_data_exists():
    query = '''SELECT * FROM information_schema.tables WHERE table_name = 'enriched_offer_data';'''
    results = db.session.execute(query).fetchall()

    return len(results) == 1


def does_enriched_offer_contains_data():
    if does_enriched_offer_data_exists():
        query = '''SELECT * FROM enriched_offer_data limit 1;'''
        results = db.session.execute(query)
        return results.rowcount > 0

    return False
