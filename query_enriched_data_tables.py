from sqlalchemy.engine import Connection

from offerer_queries import get_offerers_details
from stock_queries import create_stock_view, create_stocks_offer_view, create_stock_venue_view, \
    create_stocks_booking_view
from user_queries import get_beneficiary_users_details
from view_queries import create_enriched_stock_view


def create_enriched_offerer_data(connection: Connection):
    enriched_offerer_data = get_offerers_details(connection)
    enriched_offerer_data.to_sql(name='enriched_offerer_data',
                                 con=connection,
                                 if_exists='replace')


def create_enriched_user_data(connection: Connection):
    get_beneficiary_users_details(connection) \
        .sample(frac=1) \
        .reset_index(drop=True) \
        .to_sql(name='enriched_user_data',
                con=connection,
                if_exists='replace')


def create_enriched_stock_data():
    create_stock_view()
    create_stocks_offer_view()
    create_stock_venue_view()
    create_stocks_booking_view()
    create_enriched_stock_view()
