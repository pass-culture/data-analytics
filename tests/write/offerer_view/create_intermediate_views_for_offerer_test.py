from datetime import datetime

import pandas

from db import ENGINE
from tests.data_creators import create_offerer, create_venue, create_product, \
    create_offer, create_stock, create_user, create_booking
from utils.database_cleaners import clean_database, clean_views
from write.offerer_view.create_intermediate_views_for_offerer import _get_first_stock_creation_dates_query, \
    _get_first_booking_creation_dates_query, _get_number_of_offers_query, _get_number_of_bookings_not_cancelled_query, \
    _get_number_of_venues_per_offerer_query, _get_number_of_venues_with_offer_per_offerer_query


class OffererQueriesTest:
    def teardown_method(self):
        clean_database()
        clean_views()

    class GetFirstStockCreationDatesQueryTest:
        def test_should_return_the_creation_date_of_the_offer_s_first_stock(self):
            # Given
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=1)
            create_offer(venue_id=1, product_id=1, id=1)
            create_stock(offer_id=1, date_created='2019-12-01')
            create_stock(offer_id=1, id=2, date_created='2019-12-09')

            # When
            query = _get_first_stock_creation_dates_query()

            # Then
            with ENGINE.connect() as connection:
                first_stock_dates = pandas.read_sql(query, connection, index_col='offerer_id')
            assert first_stock_dates.loc[
                       1, "Date de création du premier stock"] == datetime(2019, 12, 1)

        def test_should_return_None_if_the_offerer_has_no_stock(self):
            # Given
            create_offerer()

            # When
            query = _get_first_stock_creation_dates_query()

            # Then
            with ENGINE.connect() as connection:
                first_stock_dates = pandas.read_sql(query, connection, index_col='offerer_id')
            assert first_stock_dates.loc[1, "Date de création du premier stock"] is None

    class GetFirstBookingCreationDatesQueryTest:
        def test_should_return_the_creation_date_of_the_offer_s_first_booking(self):
            # Given
            create_user(id=1)
            create_offerer(id=1)
            create_venue(offerer_id=1)
            create_product(id=1)
            create_offer(venue_id=1, product_id=1)
            create_stock(offer_id=1)
            first_booking_date = datetime(2019, 9, 20, 12, 0, 0)
            second_booking_date = datetime(2019, 9, 22, 12, 0, 0)
            create_booking(user_id=1, stock_id=1, id=1, date_created=first_booking_date, token='123456')
            create_booking(user_id=1, stock_id=1, id=2, date_created=second_booking_date, token='AZERTY')

            # When
            query = _get_first_booking_creation_dates_query()

            # Then
            with ENGINE.connect() as connection:
                first_booking_dates = pandas.read_sql(query, connection, index_col='offerer_id')
            assert first_booking_dates.loc[1, "Date de première réservation"] == first_booking_date

        def test_should_return_None_if_the_offerer_has_no_booking(self):
            # Given
            create_offerer()

            # When
            query = _get_first_booking_creation_dates_query()

            # Then
            with ENGINE.connect() as connection:
                first_booking_dates = pandas.read_sql(query, connection, index_col='offerer_id')
            assert first_booking_dates.loc[1, "Date de première réservation"] is None

    class GetNumberOfOffersQueryTest:
        def test_should_return_the_number_of_offers(self):
            # Given
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=1)
            create_product(id=2)
            create_offer(venue_id=1, product_id=1, id=1)
            create_offer(venue_id=1, product_id=2, id=2)

            # When
            query = _get_number_of_offers_query()

            # Then
            with ENGINE.connect() as connection:
                number_of_offers = pandas.read_sql(query, connection, index_col='offerer_id')
            assert number_of_offers.loc[1, "Nombre d’offres"] == 2

        def test_should_return_zero_if_the_offerer_has_no_offer(self):
            # Given
            create_offerer()

            # When
            query = _get_number_of_offers_query()

            # Then
            with ENGINE.connect() as connection:
                number_of_offers = pandas.read_sql(query, connection, index_col='offerer_id')
            assert number_of_offers.loc[1, "Nombre d’offres"] == 0

    class GetNumberOfBookingsNotCancelledQueryTest:
        def test_should_return_the_number_of_bookings_not_cancelled(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='user+plus@email.fr')
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=1)
            create_offer(venue_id=1, product_id=1, id=1)
            create_stock(offer_id=1, id=1)
            create_booking(user_id=1, stock_id=1, id=1, token='123455')
            create_booking(user_id=2, stock_id=1, id=2, token='567UA0')
            create_booking(user_id=2, stock_id=1, id=3, token='6YHA08', is_cancelled=True)

            # When
            query = _get_number_of_bookings_not_cancelled_query()

            # Then
            with ENGINE.connect() as connection:
                number_of_bookings_not_cancelled = pandas.read_sql(query, connection, index_col='offerer_id')
            assert number_of_bookings_not_cancelled.loc[1, "Nombre de réservations non annulées"] == 2

        def test_should_return_zero_if_the_offerer_has_no_booking(self):
            # Given
            create_offerer()

            # When
            query = _get_number_of_bookings_not_cancelled_query()

            # Then
            with ENGINE.connect() as connection:
                number_of_bookings_not_cancelled = pandas.read_sql(query, connection, index_col='offerer_id')
            assert number_of_bookings_not_cancelled.loc[1, "Nombre de réservations non annulées"] == 0

    class GetNumberOfVenuesPerOffererQueryTest:
        def test_should_return_the_number_of_venue(self):
            # Given
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)

            # When
            query = _get_number_of_venues_per_offerer_query()

            # Then
            with ENGINE.connect() as connection:
                number_of_venue = pandas.read_sql(query, connection, index_col='offerer_id')
            assert number_of_venue.loc[1, "Nombre de lieux"] == 1

        def test_should_return_zero_if_the_offerer_has_no_venue(self):
            # Given
            create_offerer()

            # When
            query = _get_number_of_venues_per_offerer_query()

            # Then
            with ENGINE.connect() as connection:
                number_of_venue = pandas.read_sql(query, connection, index_col='offerer_id')
            assert number_of_venue.loc[1, "Nombre de lieux"] == 0

    class GetNumberOfVenueWithOfferPerOffererQueryTest:
        def test_should_return_the_number_of_venue_with_at_least_one_offer(self):
            # Given
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_venue(offerer_id=1, id=2, siret='12345678998765')
            create_product(id=1)
            create_offer(venue_id=1, product_id=1, id=1)
            create_offer(venue_id=1, product_id=1, id=2)

            # When
            query = _get_number_of_venues_with_offer_per_offerer_query()

            # Then
            with ENGINE.connect() as connection:
                number_of_venue_with_offer = pandas.read_sql(query, connection, index_col='offerer_id')
            assert number_of_venue_with_offer.loc[1, "Nombre de lieux avec offres"] == 1

        def test_should_return_zero_if_the_offerer_has_no_venue(self):
            # Given
            create_offerer()

            # When
            query = _get_number_of_venues_with_offer_per_offerer_query()

            # Then
            with ENGINE.connect() as connection:
                number_of_venue_with_offer = pandas.read_sql(query, connection, index_col='offerer_id')
            assert number_of_venue_with_offer.loc[1, "Nombre de lieux avec offres"] == 0

        def test_should_return_zero_if_the_offerer_s_venue_has_no_offer(self):
            # Given
            create_offerer()
            create_venue(offerer_id=1, id=1)

            # When
            query = _get_number_of_venues_with_offer_per_offerer_query()

            # Then
            with ENGINE.connect() as connection:
                number_of_venue_with_offer = pandas.read_sql(query, connection, index_col='offerer_id')
            assert number_of_venue_with_offer.loc[1, "Nombre de lieux avec offres"] == 0
