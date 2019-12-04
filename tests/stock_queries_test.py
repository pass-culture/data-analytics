from datetime import datetime

import pandas
import pytest

from db import ENGINE, CONNECTION
from stock_queries import get_stocks_information, get_stocks_offer_information, get_stocks_venue_information, \
    get_stocks_booking_information, get_stock_creation_date, get_stocks_details
from tests.utils import create_stock, create_offer, create_venue, create_offerer, create_product, create_booking, \
    create_user, create_payment, create_payment_status


class StockQueriesTest:
    @pytest.fixture(autouse=True)
    def setup_class(self):
        ENGINE.execute('''
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
                        DELETE FROM activity;
                        ''')

    class GetStocksDetailsTest:
        def test_should_return_all_values(self):
            # Given
            datetime_before_first_stock_creation = datetime.utcnow()
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1, product_type='EventType.CINEMA')
            create_product(id=2, product_type='ThingType.LIVRE_EDITION')
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None, is_virtual=True)
            create_offer(id=3, venue_id=1, product_id=1, product_type='EventType.CINEMA', name="Test")
            create_stock(id=1, offer_id=3, booking_limit_datetime='2019-11-23',
                         beginning_datetime='2019-11-24', available=10)
            datetime_between_stocks_creation = datetime.utcnow()
            create_offer(id=2, venue_id=1, product_id=2, name="Test bis", product_type='ThingType.LIVRE_EDITION')
            create_stock(id=2, offer_id=2, available=12)
            create_booking(user_id=1, stock_id=1, quantity=2, id=4)
            create_payment(booking_id=4, id=1)
            create_payment_status(payment_id=1, status='PENDING', date='2019-01-01', id=1)

            expected_stocks_details = pandas.DataFrame(
                index=pandas.Index(data=[1, 2], name='stock_id'),
                data={"Identifiant de l'offre": [3, 2],
                      "Nom de l'offre": ["Test", "Test bis"],
                      "offerer_id": [3, 3],
                      "Type d'offre": ["EventType.CINEMA", 'ThingType.LIVRE_EDITION'],
                      "Département": [None, None],
                      "Date limite de réservation": [datetime(2019, 11, 23), pandas.NaT],
                      "Date de début de l'évènement": [datetime(2019, 11, 24), pandas.NaT],
                      "Stock disponible brut de réservations": [10, 12],
                      "Nombre total de réservations": [2, 0],
                      "Nombre de réservations annulées": [0, 0],
                      "Nombre de réservations ayant un paiement": [2, 0]

                      }
            )

            # When
            stocks_details = get_stocks_details(CONNECTION)

            # Then
            datetime_after_second_stock_creation = datetime.utcnow()

            assert datetime_before_first_stock_creation < stocks_details.loc[1, "Date de création du stock"] \
                   < datetime_between_stocks_creation
            assert datetime_between_stocks_creation < stocks_details.loc[2, "Date de création du stock"] \
                   < datetime_after_second_stock_creation
            pandas.testing.assert_frame_equal(stocks_details.drop("Date de création du stock", axis=1), expected_stocks_details)
            assert list(stocks_details.columns) == [
                "Identifiant de l'offre", "Nom de l'offre", "offerer_id", "Type d'offre", "Département",
                "Date de création du stock", "Date limite de réservation", "Date de début de l'évènement",
                "Stock disponible brut de réservations", "Nombre total de réservations",
                "Nombre de réservations annulées", "Nombre de réservations ayant un paiement"
            ]

    class GetStockInformationTest:
        def test_should_return_all_information_directly_linked_to_stock(self):
            # Given
            create_product(id=1)
            create_offerer(id=1)
            create_venue(id=1, offerer_id=1)
            create_offer(id=2, venue_id=1, product_id=1)
            create_stock(id=1, offer_id=2, available=10, booking_limit_datetime='2019-11-23',
                         beginning_datetime=None)

            expected_stocks_information = pandas.DataFrame(index=pandas.Index(data=[1], name='stock_id'),
                                                           data={"Identifiant de l'offre": 2,
                                                                 "Date limite de réservation": datetime(2019, 11, 23),
                                                                 "Date de début de l'évènement": pandas.NaT,
                                                                 "Stock disponible brut de réservations": 10})

            # When
            stocks_information = get_stocks_information(CONNECTION)

            # Then
            pandas.testing.assert_frame_equal(stocks_information, expected_stocks_information)

    class GetStocksOfferInformationTest:
        def test_should_return_all_information_directly_linked_to_offer(self):
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
            stocks_offer_information = get_stocks_offer_information(CONNECTION)

            # Then
            pandas.testing.assert_frame_equal(stocks_offer_information, expected_stocks_information)

    class GetStocksVenueInformationTest:
        def test_should_return_all_information_directly_linked_to_venue_with_departement_code_when_physical_venue(self):
            # Given
            create_product(id=1)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, departement_code='06', postal_code='06000')
            create_offer(id=2, venue_id=1, product_id=1)
            create_stock(id=1, offer_id=2)

            expected_stocks_information = pandas.DataFrame(index=pandas.Index(data=[1], name='stock_id'),
                                                           data={"offerer_id": 3,
                                                                 "Département": "06"})

            # When
            stocks_venue_information = get_stocks_venue_information(CONNECTION)

            # Then
            pandas.testing.assert_frame_equal(stocks_venue_information, expected_stocks_information)

        def test_should_return_all_information_directly_linked_to_venue_without_departement_code_when_virtual_venue(self):
            # Given
            create_product(id=1)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None, is_virtual=True)
            create_offer(id=2, venue_id=1, product_id=1)
            create_stock(id=1, offer_id=2)

            expected_stocks_information = pandas.DataFrame(index=pandas.Index(data=[1], name='stock_id'),
                                                           data={"offerer_id": 3,
                                                                 "Département": None})

            # When
            stocks_venue_information = get_stocks_venue_information(CONNECTION)

            # Then
            pandas.testing.assert_frame_equal(stocks_venue_information, expected_stocks_information)

    class GetStocksBookingInformationTest:
        def test_should_return_column_with_total_number_of_bookings_cancelled_and_not(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1)
            create_product(id=2)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None, is_virtual=True)
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
            stocks_booking_information = get_stocks_booking_information(CONNECTION)

            # Then
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre total de réservations"], expected_stocks_booking_information)

        def test_should_return_column_with_total_number_of_bookings_at_0_when_no_booking(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1)
            create_product(id=2)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None, is_virtual=True)
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
            stocks_booking_information = get_stocks_booking_information(CONNECTION)

            # Then
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre total de réservations"], expected_stocks_booking_information)

        def test_should_return_column_with_number_of_cancelled_bookings(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1)
            create_product(id=2)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None, is_virtual=True)
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
            stocks_booking_information = get_stocks_booking_information(CONNECTION)

            # Then
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations annulées"], expected_stocks_booking_information)

        def test_should_return_column_with_0_cancelled_bookings_if_no_booking(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1)
            create_product(id=2)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None, is_virtual=True)
            create_offer(id=3, venue_id=1, product_id=1)
            create_stock(id=1, offer_id=3)

            expected_stocks_booking_information = pandas.Series(
                index=pandas.Index(data=[1], name='stock_id'),
                data=[0],
                name="Nombre de réservations annulées")

            # When
            stocks_booking_information = get_stocks_booking_information(CONNECTION)

            # Then
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations annulées"], expected_stocks_booking_information)

        def test_should_return_column_with_number_of_bookings_appearing_on_payment(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1)
            create_product(id=2)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None, is_virtual=True)
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
            stocks_booking_information = get_stocks_booking_information(CONNECTION)

            # Then
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations ayant un paiement"], expected_stocks_booking_information)

        def test_should_not_count_bookings_appearing_on_payment_if_payment_s_current_status_is_banned(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1)
            create_product(id=2)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None, is_virtual=True)
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
            stocks_booking_information = get_stocks_booking_information(CONNECTION)

            # Then
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations ayant un paiement"], expected_stocks_booking_information)

            # Then
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations ayant un paiement"], expected_stocks_booking_information)

        def test_should_count_bookings_appearing_on_payment_if_payment_is_no_longer_banned(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1)
            create_product(id=2)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None, is_virtual=True)
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
            stocks_booking_information = get_stocks_booking_information(CONNECTION)

            # Then
            pandas.testing.assert_series_equal(stocks_booking_information["Nombre de réservations ayant un paiement"], expected_stocks_booking_information)


    class GetStockCreationDateTest:
        def test_should_get_stock_creation_date(self):
            # Given
            datetime_before_first_stock_creation = datetime.utcnow()
            create_product(id=1)
            create_product(id=2)
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None, is_virtual=True)
            create_offer(id=3, venue_id=1, product_id=1)
            create_stock(id=1, offer_id=3)
            datetime_between_stock_creations = datetime.utcnow()
            create_stock(id=2, offer_id=3)

            # When
            stock_creation_date = get_stock_creation_date(CONNECTION)

            # Then
            datetime_after_second_stock_creation = datetime.utcnow()
            assert datetime_before_first_stock_creation < stock_creation_date.loc[1, "Date de création du stock"] < datetime_between_stock_creations
            assert datetime_between_stock_creations < stock_creation_date.loc[2, "Date de création du stock"] < datetime_after_second_stock_creation
