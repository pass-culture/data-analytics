from db import ENGINE


def clean_database():
    with ENGINE.connect() as connection:
        connection.execute(
            """
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
        """
        )


def drop_offerer_cultural_activity_table():
    with ENGINE.connect() as connection:
        connection.execute("DROP TABLE IF EXISTS offerer_cultural_activity;")


def clean_views():
    with ENGINE.connect() as connection:
        connection.execute("DROP VIEW IF EXISTS enriched_stock_data CASCADE;")
        connection.execute("DROP VIEW IF EXISTS stock_booking_information CASCADE;")
        connection.execute("DROP VIEW IF EXISTS available_stock_information CASCADE;")
        connection.execute(
            "DROP MATERIALIZED VIEW IF EXISTS enriched_offerer_data CASCADE;"
        )
        connection.execute("DROP VIEW IF EXISTS related_stocks CASCADE;")
        connection.execute("DROP VIEW IF EXISTS related_bookings CASCADE;")
        connection.execute("DROP VIEW IF EXISTS related_offers CASCADE;")
        connection.execute(
            "DROP VIEW IF EXISTS related_non_cancelled_bookings CASCADE;"
        )
        connection.execute("DROP VIEW IF EXISTS related_venues CASCADE;")
        connection.execute("DROP VIEW IF EXISTS related_venues_with_offer CASCADE;")
        connection.execute(
            "DROP MATERIALIZED VIEW IF EXISTS enriched_user_data CASCADE;"
        )
        connection.execute("DROP VIEW IF EXISTS experimentation_sessions CASCADE;")
        connection.execute("DROP VIEW IF EXISTS activation_dates CASCADE;")
        connection.execute("DROP VIEW IF EXISTS first_connection_dates CASCADE;")
        connection.execute("DROP VIEW IF EXISTS date_of_first_bookings CASCADE;")
        connection.execute("DROP VIEW IF EXISTS date_of_second_bookings CASCADE;")
        connection.execute(
            "DROP VIEW IF EXISTS date_of_bookings_on_third_product CASCADE;"
        )
        connection.execute("DROP VIEW IF EXISTS last_recommendation_dates CASCADE;")
        connection.execute("DROP VIEW IF EXISTS number_of_bookings CASCADE;")
        connection.execute(
            "DROP VIEW IF EXISTS number_of_non_cancelled_bookings CASCADE;"
        )
        connection.execute("DROP VIEW IF EXISTS users_seniority CASCADE;")
        connection.execute("DROP VIEW IF EXISTS actual_amount_spent CASCADE;")
        connection.execute("DROP VIEW IF EXISTS theoric_amount_spent CASCADE;")
        connection.execute(
            "DROP VIEW IF EXISTS theoric_amount_spent_in_digital_goods CASCADE;"
        )
        connection.execute(
            "DROP VIEW IF EXISTS theoric_amount_spent_in_physical_goods CASCADE;"
        )
        connection.execute(
            "DROP MATERIALIZED VIEW IF EXISTS enriched_offer_data CASCADE;"
        )
        connection.execute(
            "DROP MATERIALIZED VIEW IF EXISTS enriched_venue_data CASCADE;"
        )
        connection.execute("DROP VIEW IF EXISTS enriched_venue_data CASCADE;")
        connection.execute("DROP VIEW IF EXISTS is_physical_view CASCADE;")
        connection.execute("DROP VIEW IF EXISTS is_outing_view CASCADE;")
        connection.execute(
            "DROP VIEW IF EXISTS offer_booking_information_view CASCADE;"
        )
        connection.execute("DROP VIEW IF EXISTS count_favorites_view CASCADE;")


def drop_offerer_humanized_id_table():
    with ENGINE.connect() as connection:
        connection.execute("DROP TABLE IF EXISTS offerer_humanized_id CASCADE;")
