import pandas
import pytest

from models.db import CONNECTION
from repository.stock_queries import _get_stocks_booking_information_query
from tests.repository.utils import create_stock, create_offer, create_venue, create_offerer, create_product, create_booking, \
    create_user, create_payment, create_payment_status, clean_database, clean_views


class StockQueriesTest:
    @pytest.fixture(autouse=True)
    def setup_method(self, app):
        yield
        clean_database(app)
        clean_views()

    class GetStocksBookingInformationQueryTest:
        def test_should_return_column_with_total_number_of_bookings_cancelled_and_not(self, app):
            # Given
            create_user(app, id=1)
            create_user(app, id=2, email='other@test.com')
            create_product(app, id=1)
            create_product(app, id=2)
            create_offerer(app, id=3)
            create_venue(app, offerer_id=3, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=3)
            create_stock(app, offer_id=3, id=1)
            create_offer(app, venue_id=1, product_id=2, id=2)
            create_stock(app, offer_id=2, id=2)
            create_booking(app, user_id=1, stock_id=1, id=4, quantity=2, is_cancelled=True)
            create_booking(app, user_id=2, stock_id=2, id=5, quantity=1, token='IS02JE')

            expected_stocks_booking_information = pandas.Series(
                index=pandas.Index(data=[1, 2], name='stock_id'),
                data=[2, 1],
                name="Nombre total de réservations")

            # When
            query = _get_stocks_booking_information_query()

            # Then
            stocks_booking_information = pandas.read_sql(query, CONNECTION, index_col='stock_id')
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre total de réservations"],
                                               expected_stocks_booking_information)

        def test_should_return_column_with_total_number_of_bookings_at_0_when_no_booking(self, app):
            # Given
            create_user(app, id=1)
            create_user(app, id=2, email='other@test.com')
            create_product(app, id=1)
            create_product(app, id=2)
            create_offerer(app, id=3)
            create_venue(app, offerer_id=3, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=3)
            create_stock(app, offer_id=3, id=1)
            create_offer(app, venue_id=1, product_id=2, id=2)
            create_stock(app, offer_id=2, id=2)
            create_booking(app, user_id=1, stock_id=1, id=4, quantity=2, is_cancelled=True)

            expected_stocks_booking_information = pandas.Series(
                index=pandas.Index(data=[1, 2], name='stock_id'),
                data=[2, 0],
                name="Nombre total de réservations")

            # When
            query = _get_stocks_booking_information_query()

            # Then
            stocks_booking_information = pandas.read_sql(query, CONNECTION, index_col='stock_id')
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre total de réservations"],
                                               expected_stocks_booking_information)

        def test_should_return_column_with_number_of_cancelled_bookings(self, app):
            # Given
            create_user(app, id=1)
            create_user(app, id=2, email='other@test.com')
            create_product(app, id=1)
            create_product(app, id=2)
            create_offerer(app, id=3)
            create_venue(app, offerer_id=3, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=3)
            create_stock(app, offer_id=3, id=1)
            create_offer(app, venue_id=1, product_id=2, id=2)
            create_stock(app, offer_id=2, id=2)
            create_booking(app, user_id=1, stock_id=1, id=4, quantity=2, is_cancelled=True)
            create_booking(app, user_id=2, stock_id=2, id=5, quantity=1, token='IS02JE')

            expected_stocks_booking_information = pandas.Series(
                index=pandas.Index(data=[1, 2], name='stock_id'),
                data=[2, 0],
                name="Nombre de réservations annulées")

            # When
            query = _get_stocks_booking_information_query()

            # Then
            stocks_booking_information = pandas.read_sql(query, CONNECTION, index_col='stock_id')
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations annulées"],
                                               expected_stocks_booking_information)

        def test_should_return_column_with_0_cancelled_bookings_if_no_booking(self, app):
            # Given
            create_user(app, id=1)
            create_user(app, id=2, email='other@test.com')
            create_product(app, id=1)
            create_product(app, id=2)
            create_offerer(app, id=3)
            create_venue(app, offerer_id=3, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=3)
            create_stock(app, offer_id=3, id=1)

            expected_stocks_booking_information = pandas.Series(
                index=pandas.Index(data=[1], name='stock_id'),
                data=[0],
                name="Nombre de réservations annulées")

            # When
            query = _get_stocks_booking_information_query()

            # Then
            stocks_booking_information = pandas.read_sql(query, CONNECTION, index_col='stock_id')
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations annulées"],
                                               expected_stocks_booking_information)

        def test_should_return_column_with_number_of_bookings_appearing_on_payment(self, app):
            # Given
            create_user(app, id=1)
            create_user(app, id=2, email='other@test.com')
            create_product(app, id=1)
            create_product(app, id=2)
            create_offerer(app, id=3)
            create_venue(app, offerer_id=3, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=3)
            create_stock(app, offer_id=3, id=1)
            create_offer(app, venue_id=1, product_id=2, id=2)
            create_stock(app, offer_id=2, id=2)
            create_booking(app, user_id=1, stock_id=1, id=4, quantity=2)
            create_booking(app, user_id=2, stock_id=2, id=5, quantity=1, token='IS02JE')
            create_payment(app, booking_id=4, id=1)
            create_payment_status(app, payment_id=1)

            expected_stocks_booking_information = pandas.Series(
                index=pandas.Index(data=[1, 2], name='stock_id'),
                data=[2, 0],
                name="Nombre de réservations ayant un paiement")

            # When
            query = _get_stocks_booking_information_query()

            # Then
            stocks_booking_information = pandas.read_sql(query, CONNECTION, index_col='stock_id')
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations ayant un paiement"],
                                               expected_stocks_booking_information)

        def test_should_not_count_bookings_appearing_on_payment_if_payment_s_current_status_is_banned(self, app):
            # Given
            create_user(app, id=1)
            create_user(app, id=2, email='other@test.com')
            create_product(app, id=1)
            create_product(app, id=2)
            create_offerer(app, id=3)
            create_venue(app, offerer_id=3, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=3)
            create_stock(app, offer_id=3, id=1)
            create_offer(app, venue_id=1, product_id=2, id=2)
            create_stock(app, offer_id=2, id=2)
            create_booking(app, user_id=1, stock_id=1, id=4, quantity=2)
            create_payment(app, booking_id=4, id=1)
            create_payment_status(app, payment_id=1, id=1, date='2019-01-01', status='PENDING')
            create_payment_status(app, payment_id=1, id=2, date='2019-01-02', status='BANNED')

            expected_stocks_booking_information = pandas.Series(
                index=pandas.Index(data=[1, 2], name='stock_id'),
                data=[0, 0],
                name="Nombre de réservations ayant un paiement")

            # When
            query = _get_stocks_booking_information_query()

            # Then
            stocks_booking_information = pandas.read_sql(query, CONNECTION,
                                                               index_col='stock_id')
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations ayant un paiement"],
                                               expected_stocks_booking_information)

            # Then
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations ayant un paiement"],
                                               expected_stocks_booking_information)

        def test_should_count_bookings_appearing_on_payment_if_payment_is_no_longer_banned(self, app):
            # Given
            create_user(app, id=1)
            create_user(app, id=2, email='other@test.com')
            create_product(app, id=1)
            create_product(app, id=2)
            create_offerer(app, id=3)
            create_venue(app, offerer_id=3, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=3)
            create_stock(app, offer_id=3, id=1)
            create_offer(app, venue_id=1, product_id=2, id=2)
            create_stock(app, offer_id=2, id=2)
            create_booking(app, user_id=1, stock_id=1, id=4, quantity=2)
            create_payment(app, booking_id=4, id=1)
            create_payment_status(app, payment_id=1, id=1, date='2019-07-18', status='BANNED')
            create_payment_status(app, payment_id=1, id=2, date='2019-07-19', status='SENT')

            expected_stocks_booking_information = pandas.Series(
                index=pandas.Index(data=[1, 2], name='stock_id'),
                data=[2, 0],
                name="Nombre de réservations ayant un paiement")

            # When
            query = _get_stocks_booking_information_query()


            # Then
            stocks_booking_information = pandas.read_sql(query, CONNECTION, index_col='stock_id')
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations ayant un paiement"],
                                               expected_stocks_booking_information)