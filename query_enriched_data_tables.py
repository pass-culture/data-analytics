from sqlalchemy.engine import Connection

from offerer_queries import get_offerers_details
from stock_queries import create_stocks_booking_view
from user_queries import create_experimentation_sessions_view, \
    create_activation_dates_view, create_first_connection_dates_view, \
    create_date_of_first_bookings_view, create_date_of_second_bookings_view, \
    create_date_of_bookings_on_third_product_view, create_last_recommendation_dates_view, \
    create_number_of_bookings_view, create_number_of_non_cancelled_bookings_view, create_actual_amount_spent_view, \
    create_theoric_amount_spent_view, create_theoric_amount_spent_in_digital_goods_view, \
    create_theoric_amount_spent_in_physical_goods_view, create_users_seniority_view
from view_queries import create_enriched_stock_view, create_enriched_user_view


def create_enriched_offerer_data(connection: Connection):
    enriched_offerer_data = get_offerers_details(connection)
    enriched_offerer_data.to_sql(name='enriched_offerer_data',
                                 con=connection,
                                 if_exists='replace')


def create_enriched_user_data():
    create_experimentation_sessions_view()
    create_activation_dates_view()
    create_first_connection_dates_view()
    create_date_of_first_bookings_view()
    create_date_of_second_bookings_view()
    create_date_of_bookings_on_third_product_view()
    create_last_recommendation_dates_view()
    create_number_of_bookings_view()
    create_number_of_non_cancelled_bookings_view()
    create_users_seniority_view()
    create_actual_amount_spent_view()
    create_theoric_amount_spent_view()
    create_theoric_amount_spent_in_digital_goods_view()
    create_theoric_amount_spent_in_physical_goods_view()
    create_enriched_user_view()


def create_enriched_stock_data():
    create_stocks_booking_view()
    create_enriched_stock_view()
