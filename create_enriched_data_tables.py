from flask import current_app as app

from db import CONNECTION
from logger import logger
from query_enriched_data_tables import create_enriched_offerer_data, create_enriched_user_data, \
    create_enriched_stock_data


def create_enriched_data_tables():
    logger.info('[ENRICHED DATA] Start enriched data creation')

    create_enriched_offerer_data(CONNECTION)
    logger.info('[ENRICHED DATA] Created enriched offerer data')
    with app.app_context():
        create_enriched_user_data()
    logger.info('[ENRICHED DATA] Created enriched beneficiary user data')
    with app.app_context():
        create_enriched_stock_data()
    logger.info('[ENRICHED DATA] Created enriched stock data')

    logger.info('[ENRICHED DATA] End enriched data creation')
