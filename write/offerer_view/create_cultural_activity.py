import pandas
import sqlalchemy

from read.postgresql_database.offerer_queries import get_siren_dataframe
from transform.compute_offerer_cultural_activity import get_offerer_cultural_activity_dataframe


def create_offerer_cultural_activity_data(ENGINE) -> None:
    siren_dataframe = get_siren_dataframe(ENGINE)
    offerer_cultural_activity_dataframe = get_offerer_cultural_activity_dataframe(siren_dataframe)
    _create_table_offerer_cultural_activity(offerer_cultural_activity_dataframe, ENGINE)


def _create_table_offerer_cultural_activity(offerer_cultural_activity_dataframe: pandas.DataFrame, ENGINE) -> None:
    with ENGINE.connect() as connection:
        offerer_cultural_activity_dataframe.to_sql(
            name='offerer_cultural_activity',
            con=connection,
            if_exists='replace',
            dtype={
                'id': sqlalchemy.types.BIGINT(),
                'APE_label': sqlalchemy.types.VARCHAR(length=250)
            }
        )
