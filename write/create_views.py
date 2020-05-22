from write.create_intermediate_views_for_stock import create_stocks_booking_view, create_available_stocks_view, \
    create_enriched_stock_view
from write.create_intermediate_views_for_user import create_experimentation_sessions_view, create_activation_dates_view, \
    create_first_connection_dates_view, create_date_of_first_bookings_view, create_date_of_second_bookings_view, \
    create_date_of_bookings_on_third_product_view, create_last_recommendation_dates_view, \
    create_number_of_bookings_view, create_number_of_non_cancelled_bookings_view, create_users_seniority_view, \
    create_actual_amount_spent_view, create_theoric_amount_spent_view, \
    create_theoric_amount_spent_in_digital_goods_view, create_theoric_amount_spent_in_physical_goods_view, \
    create_materialized_enriched_user_view
from write.create_intermediate_views_for_offerer import create_offerer_cultural_activity_data, \
    create_first_stock_creation_dates_view, create_first_booking_creation_dates_view, create_number_of_offers_view, \
    create_number_of_bookings_not_cancelled_view, create_materialized_enriched_offerer_view


def create_enriched_offerer_data():
    create_first_stock_creation_dates_view()
    create_first_booking_creation_dates_view()
    create_number_of_offers_view()
    create_number_of_bookings_not_cancelled_view()
    create_offerer_cultural_activity_data()
    create_materialized_enriched_offerer_view()

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
    create_materialized_enriched_user_view()

def create_enriched_stock_data():
    create_stocks_booking_view()
    create_available_stocks_view()
    create_enriched_stock_view()
