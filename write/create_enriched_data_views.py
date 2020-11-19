import argparse
from sqlalchemy import create_engine
from utils.logger import logger

from write.create_views import (
    create_table_regions_departments,
    create_enriched_offerer_data,
    create_enriched_user_data,
    create_enriched_stock_data,
    create_enriched_offer_data,
    create_enriched_venue_data,
)


def create_enriched_data_views(db_url):
    engine = create_engine(db_url)
    logger.info("[ENRICHED DATA] Start enriched data creation")

    create_table_regions_departments(engine)
    logger.info("[ENRICHED DATA] Imported regions departments table")
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create enriched views for analytics")
    parser.add_argument("db_url", help="The database url")
    args = parser.parse_args()

    create_enriched_data_views(args.db_url)
