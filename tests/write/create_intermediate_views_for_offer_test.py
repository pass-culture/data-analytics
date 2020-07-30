import pandas
import pytest

from db import CONNECTION
from write.create_intermediate_views_for_offer import _get_is_physical_information_query, _get_is_outing_information_query, \
    _get_offer_booking_information_query, _get_count_favorites_query, _get_offer_info_with_quantity

from utils.database_cleaners import clean_database, clean_views
from tests.data_creators import create_user, create_product, create_offerer, create_venue, \
    create_offer, create_stock, create_booking, create_deposit, create_favorite


class OfferQueriesTest:

    class GetIsPhysicalInformationQueryTest:
        def teardown_method(self):
            clean_database()
            clean_views()

        @pytest.mark.parametrize('offer_type', [
            'ThingType.INSTRUMENT',
            'ThingType.JEUX',
            'ThingType.LIVRE_EDITION',
            'ThingType.MUSIQUE',
            'ThingType.OEUVRE_ART',
            'ThingType.AUDIOVISUEL'
        ])
        def test_should_return_physical_product_column_with_true_if_relevant_product_type(self, app, offer_type):
            # Given
            create_user(app, id=1)
            create_product(app, id=1)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=1, product_type=offer_type)

            expected_is_physical_information = pandas.Series(
                index=pandas.Index(data=[1], name='offer_id'),
                data=[True],
                name="Bien physique")

            # When
            query = _get_is_physical_information_query()

            # Then
            is_physical_information = pandas.read_sql(query, CONNECTION, index_col='offer_id')
            pandas.testing.assert_series_equal(is_physical_information["Bien physique"], expected_is_physical_information)

        def test_should_return_physical_product_column_with_false_if_not_relevant_product_type(self, app):
            # Given
            create_user(app, id=1)
            create_product(app, id=1)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=1, product_type='ThingType.CINEMA_CARD')

            expected_is_physical_information = pandas.Series(
                index=pandas.Index(data=[1], name='offer_id'),
                data=[False],
                name="Bien physique")

            # When
            query = _get_is_physical_information_query()

            # Then
            is_physical_information = pandas.read_sql(query, CONNECTION, index_col='offer_id')
            pandas.testing.assert_series_equal(is_physical_information["Bien physique"], expected_is_physical_information)


    class GetIsOutingInformationQueryTest:
        def teardown_method(self):
            clean_database()
            clean_views()

        @pytest.mark.parametrize('offer_type', [
            'EventType.CINEMA'
            , 'EventType.JEUX'
            , 'ThingType.SPECTACLE_VIVANT_ABO'
            , 'EventType.MUSIQUE'
            , 'ThingType.MUSEES_PATRIMOINE_ABO'
            , 'ThingType.CINEMA_CARD'
            , 'ThingType.PRATIQUE_ARTISTIQUE_ABO'
            , 'ThingType.CINEMA_ABO'
            , 'EventType.MUSEES_PATRIMOINE'
            , 'EventType.PRATIQUE_ARTISTIQUE'
            , 'EventType.CONFERENCE_DEBAT_DEDICACE'
        ])
        def test_should_return_outings_column_with_true_if_relevant_product_type(self, app, offer_type):
            # Given
            create_user(app, id=1)
            create_product(app, id=1)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=1, product_type=offer_type)

            expected_is_outing_information = pandas.Series(
                index=pandas.Index(data=[1], name='offer_id'),
                data=[True],
                name="Sortie")

            # When
            query = _get_is_outing_information_query()

            # Then
            is_outing_information = pandas.read_sql(query, CONNECTION, index_col='offer_id')
            pandas.testing.assert_series_equal(is_outing_information["Sortie"], expected_is_outing_information)

        def test_should_return_outings_column_with_false_if_not_relevant_product_type(self, app):
            # Given
            create_user(app, id=1)
            create_product(app, id=1)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=1, product_type='ThingType.LIVRE_EDITION')

            expected_is_outing_information = pandas.Series(
                index=pandas.Index(data=[1], name='offer_id'),
                data=[False],
                name="Sortie")

            # When
            query = _get_is_outing_information_query()

            # Then
            is_outing_information = pandas.read_sql(query, CONNECTION, index_col='offer_id')
            pandas.testing.assert_series_equal(is_outing_information["Sortie"], expected_is_outing_information)


    class GetOfferBookingInformationQueryTest:
        def teardown_method(self):
            clean_database()
            clean_views()

        def test_should_return_booking_cancelled_booking_and_used_booking_number_columns(self, app):
            # Given
            create_user(app, id=1)
            create_deposit(app, amount=500)
            create_offerer(app, id=10)
            create_product(app, id=1, product_type='ThingType.MUSEES_PATRIMOINE_ABO')
            create_venue(app, id=15, offerer_id=10)
            create_offer(app, id=30, venue_id=15, product_type='ThingType.MUSEES_PATRIMOINE_ABO', product_id=1)
            create_stock(app, id=20, offer_id=30)
            create_booking(app, id=1, user_id=1, amount=10, quantity=1, stock_id=20, is_used=True)
            create_booking(app, id=2, user_id=1, amount=10, quantity=1, stock_id=20, token='ABC321', is_used=True)
            create_booking(app, id=3, user_id=1, amount=10, quantity=3, stock_id=20, token='FAM321', is_cancelled=True)

            expected_booking_number = pandas.Series(
                index=pandas.Index(data=[30], name='offer_id'),
                data=[5],
                name="Nombre de réservations")

            expected_cancelled_booking_number = pandas.Series(
                index=pandas.Index(data=[30], name='offer_id'),
                data=[3],
                name="Nombre de réservations annulées")

            expected_used_booking_number = pandas.Series(
                index=pandas.Index(data=[30], name='offer_id'),
                data=[2],
                name="Nombre de réservations validées")

            # When
            query = _get_offer_booking_information_query()

            # Then
            offer_booking_information = pandas.read_sql(query, CONNECTION, index_col='offer_id')
            pandas.testing.assert_series_equal(offer_booking_information["Nombre de réservations"], expected_booking_number)
            pandas.testing.assert_series_equal(offer_booking_information["Nombre de réservations annulées"], expected_cancelled_booking_number)
            pandas.testing.assert_series_equal(offer_booking_information["Nombre de réservations validées"], expected_used_booking_number)


    class GetCountFavoritesQueryTest:
        def teardown_method(self):
            clean_database()
            clean_views()

        def test_should_return_how_many_time_in_favorite_columns(self, app):
            # Given
            create_user(app, id=1)
            create_product(app, id=1)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=1, product_type='ThingType.LIVRE_EDITION')
            create_favorite(app, id=1, offer_id=1, user_id=1)

            expected_favorites_number = pandas.Series(
                index=pandas.Index(data=[1], name='offer_id'),
                data=[1],
                name="Nombre de fois où l'offre a été mise en favoris")

            # When
            query = _get_count_favorites_query()

            # Then
            count_favorites = pandas.read_sql(query, CONNECTION, index_col='offer_id')
            pandas.testing.assert_series_equal(count_favorites["Nombre de fois où l'offre a été mise en favoris"], expected_favorites_number)

    class GetSumStockTest:
        def teardown_method(self):
            clean_database()
            clean_views()

        def test_should_return_how_many_stocks(self, app):
            # Given
            create_offerer(app, id=10)
            create_venue(app, id=15, offerer_id=10)
            create_product(app, id=1)
            create_offer(app, id=30, venue_id=15,product_id=1)
            create_stock(app, id=20, offer_id=30)
            create_stock(app, id=21, offer_id=30)

            expected_offer_stock = pandas.Series(
                index=pandas.Index(data=[30], name='offer_id'),
                data=[20],
                name="Stock")
            # When
            query = _get_offer_info_with_quantity()
            # Then
            offer_stock = pandas.read_sql(query, CONNECTION, index_col='offer_id')
            pandas.testing.assert_series_equal(offer_stock["Stock"],expected_offer_stock)
