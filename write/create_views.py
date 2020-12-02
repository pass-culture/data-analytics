from write.create_table_regions_departments import (
    create_table_regions_departments_data,
)
from write.create_humanized_id import create_humanized_id_data
from write.offerer_view.create_departement_code import (
    create_offerer_departement_code_data,
)
from write.create_intermediate_views_for_stock import (
    create_stocks_booking_view,
    create_available_stocks_view,
    create_materialized_enriched_stock_view,
)
from write.create_intermediate_views_for_user import (
    create_experimentation_sessions_view,
    create_activation_dates_view,
    create_date_of_first_bookings_view,
    create_date_of_second_bookings_view,
    create_date_of_bookings_on_third_product_view,
    create_number_of_bookings_view,
    create_number_of_non_cancelled_bookings_view,
    create_users_seniority_view,
    create_actual_amount_spent_view,
    create_theoric_amount_spent_view,
    create_theoric_amount_spent_in_digital_goods_view,
    create_theoric_amount_spent_in_physical_goods_view,
    create_theoric_amount_spent_in_outings_view,
    create_date_of_last_booking_view,
    create_materialized_enriched_user_view,
)
from write.create_intermediate_views_for_offer import (
    create_is_physical_view,
    create_is_outing_view,
    create_booking_information_view,
    create_count_favorites_view,
    create_count_first_booking_query,
    create_sum_stock_view,
    create_materialized_enriched_offer_view,
)
from write.offerer_view.create_intermediate_views_for_offerer import (
    create_first_stock_creation_dates_view,
    create_first_booking_creation_dates_view,
    create_number_of_offers_view,
    create_number_of_bookings_not_cancelled_view,
    create_number_of_venues_view,
    create_number_of_venues_without_offer_view,
    create_current_year_revenue_view,
    create_materialized_enriched_offerer_view,
)
from write.create_intermediate_views_for_venue import (
    create_total_bookings_per_venue_view,
    create_used_bookings_per_venue_view,
    create_non_cancelled_bookings_per_venue_view,
    create_first_offer_creation_date_view,
    create_last_offer_creation_date_view,
    create_offers_created_per_venue_view,
    create_theoretic_revenue_per_venue,
    create_real_revenue_per_venue,
    create_materialized_enriched_venue_view,
)
from write.create_intermediate_views_for_booking import (
    create_booking_amount_view,
    create_booking_payment_status_view,
    create_booking_ranking_in_category_view,
    create_booking_ranking_view,
    create_materialized_booking_intermediary_view,
    create_materialized_enriched_booking_view,
)


def create_table_regions_departments(ENGINE):
    create_table_regions_departments_data(ENGINE)


def create_enriched_offerer_data(ENGINE):
    create_first_stock_creation_dates_view(ENGINE)
    create_first_booking_creation_dates_view(ENGINE)
    create_number_of_offers_view(ENGINE)
    create_number_of_bookings_not_cancelled_view(ENGINE)
    create_offerer_departement_code_data(ENGINE)
    create_number_of_venues_view(ENGINE)
    create_number_of_venues_without_offer_view(ENGINE)
    create_humanized_id_data(ENGINE, "offerer")
    create_current_year_revenue_view(ENGINE)
    create_materialized_enriched_offerer_view(ENGINE)


def create_enriched_user_data(ENGINE):
    create_experimentation_sessions_view(ENGINE)
    create_activation_dates_view(ENGINE)
    create_date_of_first_bookings_view(ENGINE)
    create_date_of_second_bookings_view(ENGINE)
    create_date_of_bookings_on_third_product_view(ENGINE)
    create_number_of_bookings_view(ENGINE)
    create_number_of_non_cancelled_bookings_view(ENGINE)
    create_users_seniority_view(ENGINE)
    create_actual_amount_spent_view(ENGINE)
    create_theoric_amount_spent_view(ENGINE)
    create_theoric_amount_spent_in_digital_goods_view(ENGINE)
    create_theoric_amount_spent_in_physical_goods_view(ENGINE)
    create_theoric_amount_spent_in_outings_view(ENGINE)
    create_humanized_id_data(ENGINE, '"user"')
    create_date_of_last_booking_view(ENGINE)
    create_materialized_enriched_user_view(ENGINE)


def create_enriched_stock_data(ENGINE):
    create_stocks_booking_view(ENGINE)
    create_available_stocks_view(ENGINE)
    create_materialized_enriched_stock_view(ENGINE)


def create_enriched_offer_data(ENGINE):
    create_is_physical_view(ENGINE)
    create_is_outing_view(ENGINE)
    create_booking_information_view(ENGINE)
    create_count_favorites_view(ENGINE)
    create_count_first_booking_query(ENGINE)
    create_sum_stock_view(ENGINE)
    create_humanized_id_data(ENGINE, "offer")
    create_materialized_enriched_offer_view(ENGINE)


def create_enriched_venue_data(ENGINE):
    create_total_bookings_per_venue_view(ENGINE)
    create_non_cancelled_bookings_per_venue_view(ENGINE)
    create_used_bookings_per_venue_view(ENGINE)
    create_first_offer_creation_date_view(ENGINE)
    create_last_offer_creation_date_view(ENGINE)
    create_offers_created_per_venue_view(ENGINE)
    create_theoretic_revenue_per_venue(ENGINE)
    create_real_revenue_per_venue(ENGINE)
    create_humanized_id_data(ENGINE, "venue")
    create_materialized_enriched_venue_view(ENGINE)


def create_enriched_booking_data(ENGINE):
    create_booking_ranking_view(ENGINE)
    create_booking_ranking_in_category_view(ENGINE)
    create_booking_payment_status_view(ENGINE)
    create_booking_amount_view(ENGINE)
    create_materialized_booking_intermediary_view(ENGINE)
    create_materialized_enriched_booking_view(ENGINE)

