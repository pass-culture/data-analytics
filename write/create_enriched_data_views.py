from sqlalchemy import create_engine

from utils.logger import logger

from write.create_views import (
    create_enriched_offerer_data,
    create_enriched_user_data,
    create_enriched_stock_data,
    create_enriched_offer_data,
    create_enriched_venue_data,
)


def create_enriched_data_views(db_url):
    engine = create_engine(db_url)
    logger.info("[ENRICHED DATA] Start enriched data creation")

    create_enriched_offerer_data(engine)
    logger.info("[ENRICHED DATA] Created enriched offerer data")
    create_enriched_user_data(engine)
    logger.info("[ENRICHED DATA] Created enriched beneficiary user data")
    create_enriched_stock_data(engine)
    logger.info("[ENRICHED DATA] Created enriched stock data")
    create_enriched_offer_data(engine)
    logger.info("[ENRICHED DATA] Created enriched offer data")
    create_enriched_venue_data(engine)
    logger.info("[ENRICHED DATA] Created enriched venue data")

    logger.info("[ENRICHED DATA] End enriched data creation")
