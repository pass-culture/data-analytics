import pandas
import pytest

from db import CONNECTION
from write.create_intermediate_views_for_offer import _get_is_physical_information_query, _get_is_outing_information_query, \
    _get_offer_booking_information_query, _get_count_favorites_query

from tests.data_creators import clean_database, clean_views, create_user, create_product, create_offerer, create_venue, \
    create_offer, create_stock, create_booking, create_deposit, create_favorite


class OfferQueriesTest:

    class GetIsPhysicalInformationQueryTest:
        @pytest.fixture(autouse=True)
        def setup_method(self, app):
            yield
            clean_database(app)
            clean_views()

        def test_should_return_physical_product_column_with_true_if_relevant_product_type(self, app):
            # Given
            create_user(app, id=1)
            create_product(app, id=1)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=1, product_type='ThingType.LIVRE_EDITION')

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
        @pytest.fixture(autouse=True)
        def setup_method(self, app):
            yield
            clean_database(app)
            clean_views()

        def test_should_return_outings_column_with_true_if_relevant_product_type(self, app):
            # Given
            create_user(app, id=1)
            create_product(app, id=1)
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=1, product_type='EventType.MUSIQUE')

            expected_is_outing_information = pandas.Series(
                index=pandas.Index(data=[1], name='offer_id'),
                data=[True],
                name="Sorties")

            # When
            query = _get_is_outing_information_query()

            # Then
            is_outing_information = pandas.read_sql(query, CONNECTION, index_col='offer_id')
            pandas.testing.assert_series_equal(is_outing_information["Sorties"], expected_is_outing_information)

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
                name="Sorties")

            # When
            query = _get_is_outing_information_query()

            # Then
            is_outing_information = pandas.read_sql(query, CONNECTION, index_col='offer_id')
            pandas.testing.assert_series_equal(is_outing_information["Sorties"], expected_is_outing_information)


    class GetOfferBookingInformationQueryTest:
        @pytest.fixture(autouse=True)
        def setup_method(self, app):
            yield
            clean_database(app)
            clean_views()

        def test_should_return_booking_cancled_booking_and_used_booking_number_columns(self, app):
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

            expected_cancled_booking_number = pandas.Series(
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
            pandas.testing.assert_series_equal(offer_booking_information["Nombre de réservations annulées"], expected_cancled_booking_number)
            pandas.testing.assert_series_equal(offer_booking_information["Nombre de réservations validées"], expected_used_booking_number)


    class GetCountFavoritesQueryTest:
        @pytest.fixture(autouse=True)
        def setup_method(self, app):
            yield
            clean_database(app)
            clean_views()

        def test_(self, app):
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
