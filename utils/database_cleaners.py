from db import CONNECTION

def clean_database():
    CONNECTION.execute('''
    DELETE FROM "deposit";
    DELETE FROM "favorite";
    DELETE FROM "recommendation";
    DELETE FROM payment_status;
    DELETE FROM payment;
    DELETE FROM "booking";
    DELETE FROM "stock";
    DELETE FROM "offer";
    DELETE FROM "product";
    DELETE FROM "venue";
    DELETE FROM "mediation";
    DELETE FROM "offerer";
    DELETE FROM "user";
    ''')


def clean_tables():
    CONNECTION.execute('DROP TABLE IF EXISTS offerer_cultural_activity;')

def clean_views():
    CONNECTION.execute('DROP VIEW IF EXISTS enriched_stock_data CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS stock_booking_information CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS available_stock_information CASCADE;')
    CONNECTION.execute('DROP MATERIALIZED VIEW IF EXISTS enriched_offerer_data CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS related_stocks CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS related_bookings CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS related_offers CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS related_non_cancelled_bookings CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS related_venues CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS related_venues_with_offer CASCADE;')
    CONNECTION.execute('DROP MATERIALIZED VIEW IF EXISTS enriched_user_data CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS experimentation_sessions CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS activation_dates CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS first_connection_dates CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS date_of_first_bookings CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS date_of_second_bookings CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS date_of_bookings_on_third_product CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS last_recommendation_dates CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS number_of_bookings CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS number_of_non_cancelled_bookings CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS users_seniority CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS actual_amount_spent CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS theoric_amount_spent CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS theoric_amount_spent_in_digital_goods CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS theoric_amount_spent_in_physical_goods CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS enriched_offer_data CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS is_physical_view CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS is_outing_view CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS offer_booking_information_view CASCADE;')
    CONNECTION.execute('DROP VIEW IF EXISTS count_favorites_view CASCADE;')
