from utils.database_cleaners import clean_database, clean_views
from db import DATABASE_URL, LOCAL_DATABASE_URL

from utils.logger import logger


def clean_database_if_local():
    if DATABASE_URL in LOCAL_DATABASE_URL:
        clean_database()
        clean_views()
        logger.info("[CLEAN DATABASE AND VIEW] Database cleaned")
        return
    logger.info("[CLEAN DATABASE AND VIEW] Cannot clean production database")


if __name__ == "__main__":
    clean_database_if_local()
