from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db import DATABASE_URL

health_check_engine = create_engine(DATABASE_URL, connect_args={"options": "-c statement_timeout=1000"})
HealthCheckSession = sessionmaker(bind=health_check_engine)
health_check_session = HealthCheckSession()


def does_enriched_offerer_data_exists():
    query = '''SELECT * FROM pg_matviews WHERE matviewname = 'enriched_offerer_data';'''
    results = health_check_session.execute(query).fetchall()
    health_check_session.commit()
    return len(results) == 1


def does_enriched_offerer_contains_data():
    if does_enriched_offerer_data_exists():
        query = '''SELECT * FROM enriched_offerer_data limit 1;'''
        results = health_check_session.execute(query)
        health_check_session.commit()
        return results.rowcount > 0

    return False


def does_enriched_user_data_exists():
    query = '''SELECT * FROM pg_matviews WHERE matviewname = 'enriched_user_data';'''
    results = health_check_session.execute(query).fetchall()
    health_check_session.commit()

    return len(results) == 1


def does_enriched_users_contains_data():
    if does_enriched_user_data_exists():
        query = '''SELECT * FROM enriched_user_data limit 1;'''
        results = health_check_session.execute(query)
        health_check_session.commit()

        return results.rowcount > 0

    return False


def does_enriched_stock_data_exists():
    query = '''SELECT * FROM information_schema.tables WHERE table_name = 'enriched_stock_data';'''
    results = health_check_session.execute(query).fetchall()
    health_check_session.commit()

    return len(results) == 1


def does_enriched_stocks_contains_data():
    if does_enriched_stock_data_exists():
        query = '''SELECT * FROM enriched_stock_data limit 1;'''
        results = health_check_session.execute(query)
        health_check_session.commit()
        return results.rowcount > 0

    return False


def does_enriched_offer_data_exists():
    query = '''SELECT * FROM information_schema.tables WHERE table_name = 'enriched_offer_data';'''
    results = health_check_session.execute(query).fetchall()
    health_check_session.commit()

    return len(results) == 1


def does_enriched_offer_contains_data():
    if does_enriched_offer_data_exists():
        query = '''SELECT * FROM enriched_offer_data limit 1;'''
        results = health_check_session.execute(query)
        health_check_session.commit()
        return results.rowcount > 0

    return False
