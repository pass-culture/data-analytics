from flask import current_app as app
from utils.logger import logger

from write.create_views import create_enriched_offerer_data, create_enriched_user_data, \
    create_enriched_stock_data

def create_enriched_data_views():
    logger.info('[ENRICHED DATA] Start enriched data creation')

    with app.app_context():
        create_enriched_offerer_data()
        logger.info('[ENRICHED DATA] Created enriched offerer data')
        create_enriched_user_data()
        logger.info('[ENRICHED DATA] Created enriched beneficiary user data')
        create_enriched_stock_data()
        logger.info('[ENRICHED DATA] Created enriched stock data')

    logger.info('[ENRICHED DATA] End enriched data creation')
