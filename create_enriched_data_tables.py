from db import CONNECTION
from logger import logger

from query_enriched_data_tables import create_enriched_offerer_data, create_enriched_user_data

connection = CONNECTION

if __name__ == '__main__':
    logger.info('[ENRICHED DATA] Start enriched data creation')

    create_enriched_offerer_data(connection)
    logger.info('[ENRICHED DATA] Created enriched offerer data')
    create_enriched_user_data(connection)
    logger.info('[ENRICHED DATA] Created enriched beneficiary user data')

    logger.info('[ENRICHED DATA] End enriched data creation')

