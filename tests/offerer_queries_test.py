from datetime import datetime

import pandas
import pytest

from db import CONNECTION, ENGINE
from offerer_queries import get_first_stock_creation_dates, \
    get_first_booking_creation_dates, get_creation_dates, get_number_of_offers, \
    get_number_of_bookings_not_cancelled, get_offerers_details
from tests.utils import create_user, create_offerer, create_venue, create_offer, create_stock, \
    create_booking, create_product, clean_database


class OffererQueriesTest:
    @pytest.fixture(autouse=True)
    def setup_class(self):
        clean_database()

    class GetOffererCreationDatesTest:
        def test_should_return_offerer_s_creation_date(self):
            # Given
            creation_date = datetime(2019, 9, 20, 12, 0, 0)
            create_offerer(date_created=creation_date)

            # When
            creation_dates = get_creation_dates(CONNECTION)

            # Then
            assert creation_dates.loc[1, "Date de création"] == creation_date

    class GetFirstStockCreationDatesTest:
        def test_should_return_the_creation_date_of_the_offer_s_first_stock(self):
            # Given
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=1)
            create_offer(venue_id=1, product_id=1, id=1)
            create_stock(offer_id=1, date_created='2019-12-01')
            create_stock(offer_id=1, id=2, date_created='2019-12-09')

            # When
            first_stock_dates = get_first_stock_creation_dates(CONNECTION)

            # Then
            assert first_stock_dates.loc[
                1, "Date de création du premier stock"] == datetime(2019, 12, 1)

        def test_should_return_None_if_the_offerer_has_no_stock(self):
            # Given
            create_offerer()

            # When
            first_stock_dates = get_first_stock_creation_dates(CONNECTION)

            # Then
            assert first_stock_dates.loc[1, "Date de création du premier stock"] is None

    class GetFirstBookingCreationDatesTest:
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
            create_booking(user_id=1, stock_id=1, date_created=first_booking_date, id=1, token='123456')
            create_booking(user_id=1, stock_id=1, date_created=second_booking_date, id=2, token='AZERTY')

            # When
            first_booking_dates = get_first_booking_creation_dates(CONNECTION)

            # Then
            assert first_booking_dates.loc[1, "Date de première réservation"] == first_booking_date

        def test_should_return_None_if_the_offerer_has_no_booking(self):
            # Given
            create_offerer()

            # When
            first_booking_dates = get_first_booking_creation_dates(CONNECTION)

            # Then
            assert first_booking_dates.loc[1, "Date de première réservation"] is None

    class GetNumberOfOffersTest:
        def test_should_return_the_number_of_offers(self):
            # Given
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=1)
            create_product(id=2)
            create_offer(venue_id=1, product_id=1, id=1)
            create_offer(venue_id=1, product_id=2, id=2)
            # When
            number_of_offers = get_number_of_offers(CONNECTION)
            # Then
            assert number_of_offers.loc[1, "Nombre d’offres"] == 2

        def test_should_return_zero_if_the_offerer_has_no_offer(self):
            # Given
            create_offerer()

            # When
            number_of_offers = get_number_of_offers(CONNECTION)

            # Then
            assert number_of_offers.loc[1, "Nombre d’offres"] == 0

    class GetNumberOfBookingsNotCancelledTest:
        def test_should_return_the_number_of_bookings_not_cancelled(self):
            # Given
            create_user(id=1)
            create_user(email='user+plus@email.fr', id=2)
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=1)
            create_offer(venue_id=1, product_id=1, id=1)
            create_stock(offer_id=1, id=1)
            create_booking(user_id=1, stock_id=1, id=1, token='123455')
            create_booking(user_id=2, stock_id=1, id=2, token='567UA0')
            create_booking(user_id=2, stock_id=1, is_cancelled=True, id=3, token='6YHA08')

            # When
            number_of_bookings_not_cancelled = get_number_of_bookings_not_cancelled(CONNECTION)

            # Then
            assert number_of_bookings_not_cancelled.loc[1, "Nombre de réservations non annulées"] == 2

        def test_should_return_zero_if_the_offerer_has_no_booking(self):
            # Given
            create_offerer()

            # When
            number_of_bookings_not_cancelled = get_number_of_bookings_not_cancelled(CONNECTION)

            # Then
            assert number_of_bookings_not_cancelled.loc[1, "Nombre de réservations non annulées"] == 0

    class GetOfferersDetailsTest:
        def test_should_return_values_for_offers(self):
            # Given
            create_user(can_book_free_offers=True, departement_code="93",
                        date_created=datetime(2019, 1, 1, 12, 0, 0), needs_to_fill_cultural_survey=True, id=1)
            date_creation_offerer_1 = datetime(2019, 1, 1, 12, 0, 0)
            date_creation_offerer_2 = datetime(2019, 1, 2, 12, 0, 0)
            create_offerer(date_created=date_creation_offerer_1, id=1)
            create_offerer(siren='987654321', date_created=date_creation_offerer_2, id=2)
            create_venue(offerer_id=1)
            create_product(product_type='ThingType.JEUX_VIDEO', id=1)
            create_product(product_type='ThingType.AUDIOVISUEL', id=2)
            create_product(product_type='ThingType.CINEMA_ABO', id=3)
            create_offer(venue_id=1, product_id=1, id=1, product_type='ThingType.JEUX_VIDEO')
            create_offer(venue_id=1, product_id=2, id=2, product_type='ThingType.AUDIOVISUEL')
            create_offer(venue_id=1, product_id=3, id=3, product_type='ThingType.CINEMA_ABO')
            create_stock(offer_id=1, id=1, date_created='2019-12-01')
            create_stock(offer_id=2, id=2, date_created='2019-12-02')
            create_stock(offer_id=3, id=3, date_created='2019-12-03')
            date_creation_booking_1 = datetime(2019, 3, 7)
            create_booking(user_id=1, stock_id=1, id=1, date_created=date_creation_booking_1, token='92IZKA')
            create_booking(user_id=1, stock_id=2, id=2, date_created=datetime(2019, 4, 7), token='ZIZ93K')
            create_booking(user_id=1, stock_id=3, id=3, date_created=datetime(2019, 5, 7), is_cancelled=True,
                           token='9ZKZ0A')

            # When
            offerers_details = get_offerers_details(CONNECTION)
            # Then
            assert offerers_details.shape == (2, 5)
            assert offerers_details.loc[1, "Date de création"] == date_creation_offerer_1
            assert offerers_details.loc[2, "Date de création"] == date_creation_offerer_2
            assert offerers_details.loc[
                1, "Date de création du premier stock"] == datetime(2019, 12, 1)
            assert pandas.isnull(offerers_details.loc[2, "Date de création du premier stock"])
            assert offerers_details.loc[1, "Date de première réservation"] == date_creation_booking_1
            assert pandas.isnull(offerers_details.loc[2, "Date de première réservation"])
            assert offerers_details.loc[1, "Nombre d’offres"] == 3
            assert offerers_details.loc[2, "Nombre d’offres"] == 0
            assert offerers_details.loc[2, "Nombre de réservations non annulées"] == 0
            assert offerers_details.loc[1, "Nombre de réservations non annulées"] == 2
