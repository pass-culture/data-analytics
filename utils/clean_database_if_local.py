from utils.database_cleaners import (
    clean_database,
    clean_views,
    drop_offerer_cultural_activity_table,
    drop_regions_departments_table,
    drop_offerer_humanized_id_table,
)
from db import DATABASE_URL, LOCAL_DATABASE_URL

from utils.logger import logger


def clean_database_if_local():
    if DATABASE_URL in LOCAL_DATABASE_URL:
        clean_database()
        clean_views()
        drop_offerer_cultural_activity_table()
        drop_regions_departments_table()
        drop_offerer_humanized_id_table()
        logger.info("[CLEAN DATABASE AND VIEW] Database cleaned")
        return
    logger.info("[CLEAN DATABASE AND VIEW] Cannot clean production database")


if __name__ == "__main__":
    clean_database_if_local()
