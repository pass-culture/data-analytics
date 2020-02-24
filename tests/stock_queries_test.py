from datetime import datetime

import pandas
import pytest
from numpy import datetime64

from db import CONNECTION
from stock_queries import _get_stock_information_query, \
    _get_stocks_offer_information_query, _get_stock_venue_information_query, _get_stocks_booking_information_query
from tests.utils import create_stock, create_offer, create_venue, create_offerer, create_product, create_booking, \
    create_user, create_payment, create_payment_status, clean_database


class StockQueriesTest:
    @pytest.fixture(autouse=True)
    def setup_class(self):
        clean_database()

    class GetStockInformationQueryTest:
        def test_should_query_information_directly_linked_to_offer(self):
            # Given
            create_product(id=1)
            create_offerer(id=1)
            create_venue(id=1, offerer_id=1)
            create_offer(id=2, venue_id=1, product_id=1)
            create_stock(id=1, offer_id=2, available=10, booking_limit_datetime='2019-11-23',
                         beginning_datetime=None, date_created='2019-11-01')

            expected_stocks_information = pandas.DataFrame(
                index=pandas.Index(data=[1], name='stock_id'),
                data={"Identifiant de l'offre": 2,
                      "Date de création du stock": datetime(2019, 11, 1),
                      "Date limite de réservation": datetime(2019, 11, 23),
                      "Date de début de l'évènement": pandas.NaT,
                      "Stock disponible brut de réservations": 10})

            # When
            query = _get_stock_information_query()

            # Then
            stock_information = pandas.read_sql(query, CONNECTION, index_col='stock_id')
            stock_information["Date de début de l'évènement"] = stock_information["Date de début de l'évènement"].astype(datetime64)
            pandas.testing.assert_frame_equal(stock_information, expected_stocks_information)

    class GetStockOfferInformationQueryTest:
        def test_should_query_information_directly_linked_to_offer(self):
            # Given
            create_product(id=1, name='Test offer', product_type='EventType.CINEMA')
            create_offerer(id=1)
            create_venue(id=1, offerer_id=1)
            create_offer(id=2, venue_id=1, product_id=1, name='Test offer', product_type='EventType.CINEMA')
            create_stock(id=1, offer_id=2)

            expected_stocks_information = pandas.DataFrame(index=pandas.Index(data=[1], name='stock_id'),
                                                           data={"Nom de l'offre": "Test offer",
                                                                 "Type d'offre": "EventType.CINEMA"})

            # When
            query = _get_stocks_offer_information_query()

            # Then
            stock_offer_information = pandas.read_sql(query, CONNECTION, index_col='stock_id')
            pandas.testing.assert_frame_equal(stock_offer_information, expected_stocks_information)

    class GetStockVenueInformationQueryTest:
        def test_should_query_all_information_directly_linked_to_venue_without_departement_code_when_virtual_venue(
                self):
            # Given
            create_product(id=1)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None,
                         is_virtual=True)
            create_offer(id=2, venue_id=1, product_id=1)
            create_stock(id=1, offer_id=2)

            create_product(id=2)
            create_venue(id=2, offerer_id=3, departement_code='06', postal_code='06000')
            create_offer(id=3, venue_id=2, product_id=2)
            create_stock(id=2, offer_id=3)
            expected_stocks_information = pandas.DataFrame(index=pandas.Index(data=[1, 2], name='stock_id'),
                                                           data={"offerer_id": [3, 3],
                                                                 "Département": [None, "06"]})

            # When
            query = _get_stock_venue_information_query()

            # Then
            stocks_venue_information = pandas.read_sql(query, CONNECTION, index_col='stock_id')
            pandas.testing.assert_frame_equal(stocks_venue_information, expected_stocks_information)

    class GetStocksBookingInformationQueryTest:
        def test_should_return_column_with_total_number_of_bookings_cancelled_and_not(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1)
            create_product(id=2)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None,
                         is_virtual=True)
            create_offer(id=3, venue_id=1, product_id=1)
            create_stock(id=1, offer_id=3)
            create_offer(id=2, venue_id=1, product_id=2)
            create_stock(id=2, offer_id=2)
            create_booking(user_id=1, stock_id=1, quantity=2, id=4, is_cancelled=True)
            create_booking(user_id=2, stock_id=2, quantity=1, id=5, token='IS02JE')

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

        def test_should_return_column_with_total_number_of_bookings_at_0_when_no_booking(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1)
            create_product(id=2)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None,
                         is_virtual=True)
            create_offer(id=3, venue_id=1, product_id=1)
            create_stock(id=1, offer_id=3)
            create_offer(id=2, venue_id=1, product_id=2)
            create_stock(id=2, offer_id=2)
            create_booking(user_id=1, stock_id=1, quantity=2, id=4, is_cancelled=True)

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

        def test_should_return_column_with_number_of_cancelled_bookings(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1)
            create_product(id=2)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None,
                         is_virtual=True)
            create_offer(id=3, venue_id=1, product_id=1)
            create_stock(id=1, offer_id=3)
            create_offer(id=2, venue_id=1, product_id=2)
            create_stock(id=2, offer_id=2)
            create_booking(user_id=1, stock_id=1, quantity=2, id=4, is_cancelled=True)
            create_booking(user_id=2, stock_id=2, quantity=1, id=5, token='IS02JE')

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

        def test_should_return_column_with_0_cancelled_bookings_if_no_booking(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1)
            create_product(id=2)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None,
                         is_virtual=True)
            create_offer(id=3, venue_id=1, product_id=1)
            create_stock(id=1, offer_id=3)

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

        def test_should_return_column_with_number_of_bookings_appearing_on_payment(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1)
            create_product(id=2)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None,
                         is_virtual=True)
            create_offer(id=3, venue_id=1, product_id=1)
            create_stock(id=1, offer_id=3)
            create_offer(id=2, venue_id=1, product_id=2)
            create_stock(id=2, offer_id=2)
            create_booking(user_id=1, stock_id=1, quantity=2, id=4)
            create_booking(user_id=2, stock_id=2, quantity=1, id=5, token='IS02JE')
            create_payment(booking_id=4, id=1)
            create_payment_status(payment_id=1)

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

        def test_should_not_count_bookings_appearing_on_payment_if_payment_s_current_status_is_banned(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1)
            create_product(id=2)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None,
                         is_virtual=True)
            create_offer(id=3, venue_id=1, product_id=1)
            create_stock(id=1, offer_id=3)
            create_offer(id=2, venue_id=1, product_id=2)
            create_stock(id=2, offer_id=2)
            create_booking(user_id=1, stock_id=1, quantity=2, id=4)
            create_payment(booking_id=4, id=1)
            create_payment_status(payment_id=1, status='PENDING', date='2019-01-01', id=1)
            create_payment_status(payment_id=1, status='BANNED', date='2019-01-02', id=2)

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

        def test_should_count_bookings_appearing_on_payment_if_payment_is_no_longer_banned(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1)
            create_product(id=2)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None,
                         is_virtual=True)
            create_offer(id=3, venue_id=1, product_id=1)
            create_stock(id=1, offer_id=3)
            create_offer(id=2, venue_id=1, product_id=2)
            create_stock(id=2, offer_id=2)
            create_booking(user_id=1, stock_id=1, quantity=2, id=4)
            create_payment(booking_id=4, id=1)
            create_payment_status(payment_id=1, status='BANNED', date='2019-07-18', id=1)
            create_payment_status(payment_id=1, status='SENT', date='2019-07-19', id=2)

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
