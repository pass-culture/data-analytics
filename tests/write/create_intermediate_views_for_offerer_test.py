from datetime import datetime

import pandas
import pytest

from db import CONNECTION
from tests.data_creators import clean_database, clean_views, create_offerer, create_venue, create_product, \
    create_offer, create_stock, create_user, create_booking
from write.create_intermediate_views_for_offerer import _get_first_stock_creation_dates_query, \
    _get_first_booking_creation_dates_query, _get_number_of_offers_query, _get_number_of_bookings_not_cancelled_query


class OffererQueriesTest:
    @pytest.fixture(autouse=True)
    def setup_method(self, app):
        yield
        clean_database(app)
        clean_views()

    class GetFirstStockCreationDatesQueryTest:
        def test_should_return_the_creation_date_of_the_offer_s_first_stock(self, app):
            # Given
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1, id=1)
            create_product(app, id=1)
            create_offer(app, venue_id=1, product_id=1, id=1)
            create_stock(app, offer_id=1, date_created='2019-12-01')
            create_stock(app, offer_id=1, id=2, date_created='2019-12-09')

            # When
            query = _get_first_stock_creation_dates_query()

            # Then
            first_stock_dates = pandas.read_sql(query, CONNECTION, index_col='offerer_id')
            assert first_stock_dates.loc[
                       1, "Date de création du premier stock"] == datetime(2019, 12, 1)

        def test_should_return_None_if_the_offerer_has_no_stock(self, app):
            # Given
            create_offerer(app)

            # When
            query = _get_first_stock_creation_dates_query()

            # Then
            first_stock_dates = pandas.read_sql(query, CONNECTION, index_col='offerer_id')
            assert first_stock_dates.loc[1, "Date de création du premier stock"] is None

    class GetFirstBookingCreationDatesQueryTest:
        def test_should_return_the_creation_date_of_the_offer_s_first_booking(self, app):
            # Given
            create_user(app, id=1)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1)
            create_product(app, id=1)
            create_offer(app, venue_id=1, product_id=1)
            create_stock(app, offer_id=1)
            first_booking_date = datetime(2019, 9, 20, 12, 0, 0)
            second_booking_date = datetime(2019, 9, 22, 12, 0, 0)
            create_booking(app, user_id=1, stock_id=1, id=1, date_created=first_booking_date, token='123456')
            create_booking(app, user_id=1, stock_id=1, id=2, date_created=second_booking_date, token='AZERTY')

            # When
            query = _get_first_booking_creation_dates_query()

            # Then
            first_booking_dates = pandas.read_sql(query, CONNECTION, index_col='offerer_id')
            assert first_booking_dates.loc[1, "Date de première réservation"] == first_booking_date

        def test_should_return_None_if_the_offerer_has_no_booking(self, app):
            # Given
            create_offerer(app)

            # When
            query = _get_first_booking_creation_dates_query()

            # Then
            first_booking_dates = pandas.read_sql(query, CONNECTION, index_col='offerer_id')
            assert first_booking_dates.loc[1, "Date de première réservation"] is None

    class GetNumberOfOffersQueryTest:
        def test_should_return_the_number_of_offers(self, app):
            # Given
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1, id=1)
            create_product(app, id=1)
            create_product(app, id=2)
            create_offer(app, venue_id=1, product_id=1, id=1)
            create_offer(app, venue_id=1, product_id=2, id=2)

            # When
            query = _get_number_of_offers_query()

            # Then
            number_of_offers = pandas.read_sql(query, CONNECTION, index_col='offerer_id')
            assert number_of_offers.loc[1, "Nombre d’offres"] == 2

        def test_should_return_zero_if_the_offerer_has_no_offer(self, app):
            # Given
            create_offerer(app)

            # When
            query = _get_number_of_offers_query()

            # Then
            number_of_offers = pandas.read_sql(query, CONNECTION, index_col='offerer_id')
            assert number_of_offers.loc[1, "Nombre d’offres"] == 0

    class GetNumberOfBookingsNotCancelledQueryTest:
        def test_should_return_the_number_of_bookings_not_cancelled(self, app):
            # Given
            create_user(app, id=1)
            create_user(app, id=2, email='user+plus@email.fr')
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1, id=1)
            create_product(app, id=1)
            create_offer(app, venue_id=1, product_id=1, id=1)
            create_stock(app, offer_id=1, id=1)
            create_booking(app, user_id=1, stock_id=1, id=1, token='123455')
            create_booking(app, user_id=2, stock_id=1, id=2, token='567UA0')
            create_booking(app, user_id=2, stock_id=1, id=3, token='6YHA08', is_cancelled=True)

            # When
            query = _get_number_of_bookings_not_cancelled_query()

            # Then
            number_of_bookings_not_cancelled = pandas.read_sql(query, CONNECTION, index_col='offerer_id')
            assert number_of_bookings_not_cancelled.loc[1, "Nombre de réservations non annulées"] == 2

        def test_should_return_zero_if_the_offerer_has_no_booking(self, app):
            # Given
            create_offerer(app)

            # When
            query = _get_number_of_bookings_not_cancelled_query()

            # Then
            number_of_bookings_not_cancelled = pandas.read_sql(query, CONNECTION, index_col='offerer_id')
            assert number_of_bookings_not_cancelled.loc[1, "Nombre de réservations non annulées"] == 0