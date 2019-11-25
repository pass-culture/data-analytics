from sqlalchemy.engine import Connection

from offerer_queries import get_offerers_details
from user_queries import get_beneficiary_users_details


def create_enriched_offerer_data(connection: Connection):
    enriched_offerer_data = get_offerers_details(connection)
    enriched_offerer_data.to_sql(name='enriched_offerer_data',
                                 con=connection,
                                 if_exists='replace')


def create_enriched_user_data(connection: Connection):
    get_beneficiary_users_details(connection)\
        .sample(frac=1)\
        .reset_index(drop=True)\
        .to_sql(name='enriched_user_data', con=connection, if_exists='replace')
