from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, session

from db import DATABASE_URL
from utils.logger import logger

health_check_engine = create_engine(DATABASE_URL, connect_args={"options": "-c statement_timeout=30000"})
HealthCheckSession = sessionmaker(bind=health_check_engine)


def is_enriched_materialized_view_queryable(Session: sessionmaker, materialized_view_name: str):
    is_enriched_offerer_data_present = False
    health_check_session = Session()
    try:
        is_enriched_offerer_data_present = does_materialize_view_exist(health_check_session, materialized_view_name)
        health_check_session.commit()
    except SQLAlchemyError as error:
        logger.error(f"[HEALTH CHECK] There was an error while handling the query : {str(error)}")
    except Exception as error:
        raise error
    health_check_session.close()
    return is_enriched_offerer_data_present


def is_enriched_view_queryable(Session: sessionmaker, view_name: str):
    is_enriched_offerer_data_present = False
    health_check_session = Session()
    try:
        is_enriched_offerer_data_present = does_materialize_view_exist(health_check_session, view_name)
        health_check_session.commit()
    except SQLAlchemyError as error:
        logger.error(f"[HEALTH CHECK] There was an error while handling the query : {str(error)}")
    except Exception as error:
        raise error
    health_check_session.close()
    return is_enriched_offerer_data_present


def does_materialize_view_exist(health_check_session: session.Session, materialized_view_name: str) -> bool:
    query = f'''SELECT EXISTS(SELECT FROM pg_matviews WHERE matviewname = '{materialized_view_name}');'''
    is_enriched_materialized_view_present = health_check_session.execute(query).scalar()
    return is_enriched_materialized_view_present


def does_view_exist(health_check_session: session.Session, view_name: str) -> bool:
    query = f'''SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name = '{view_name}');'''
    is_enriched_view_present = health_check_session.execute(query).scalar()
    return is_enriched_view_present


def does_view_have_data(health_check_session: session.Session, view_name: str) -> bool:
    query = f'''SELECT EXISTS(SELECT * FROM {view_name} limit 1);'''
    is_enriched_view_present = health_check_session.execute(query).scalar()
    return is_enriched_view_present


def does_view_contain_data():
    pass


def does_enriched_offerer_contain_data(Session: sessionmaker) -> bool:
    is_enriched_offerer_with_data = False
    health_check_session = Session()
    if is_enriched_materialized_view_queryable(Session, 'enriched_offerer_data'):
        try:
            is_enriched_offerer_with_data = does_view_have_data(health_check_session, 'enriched_offerer_data')
            health_check_session.commit()
        except SQLAlchemyError as error:
            logger.error(f"[HEALTH CHECK] Query ended unexpectedly : {str(error)}")
        except Exception:
            raise Exception
        health_check_session.close()
        return is_enriched_offerer_with_data

    return False


def does_enriched_users_contains_data(Session: sessionmaker) -> bool:
    is_enriched_user_with_data = False
    health_check_session = Session()
    if is_enriched_materialized_view_queryable(Session, 'enriched_user_data'):
        try:
            is_enriched_user_with_data = does_view_have_data(health_check_session, 'enriched_user_data')
            health_check_session.commit()
        except SQLAlchemyError as error:
            logger.error(f"[HEALTH CHECK] Query ended unexpectedly : {str(error)}")
        except Exception:
            raise Exception
        health_check_session.close()
        return is_enriched_user_with_data

    return False


def does_enriched_stock_contain_data(Session: sessionmaker) -> bool:
    is_enriched_user_with_data = False
    health_check_session = Session()
    if is_enriched_view_queryable(Session, 'enriched_stock_data'):
        try:
            is_enriched_user_with_data = does_view_have_data(health_check_session, 'enriched_stock_data')
            health_check_session.commit()
        except SQLAlchemyError as error:
            logger.error(f"[HEALTH CHECK] Query ended unexpectedly : {str(error)}")
        except Exception:
            raise Exception
        health_check_session.close()
        return is_enriched_user_with_data

    return False


def does_enriched_offer_contain_data(Session: sessionmaker) -> bool:
    is_enriched_user_with_data = False
    health_check_session = Session()
    if is_enriched_view_queryable(Session, 'enriched_offer_data'):
        try:
            is_enriched_user_with_data = does_view_have_data(health_check_session, 'enriched_offer_data')
            health_check_session.commit()
        except SQLAlchemyError as error:
            logger.error(f"[HEALTH CHECK] Query ended unexpectedly : {str(error)}")
        except Exception:
            raise Exception
        health_check_session.close()
        return is_enriched_user_with_data

    return False
