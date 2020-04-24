from datetime import datetime
import pandas
import pytest

from models.db import CONNECTION
from repository.query_enriched_data_views import create_enriched_user_data, create_enriched_offerer_data
from repository.stock_queries import create_stocks_booking_view, create_available_stocks_view
from repository.view_queries import create_enriched_stock_view, create_all_bookable_offers_view
from tests.repository.utils import clean_database, clean_views, create_user, create_product, create_offerer, create_venue, \
    create_offer, create_stock, create_booking, create_payment_status, create_payment


class ViewQueriesTest:
    @pytest.fixture(autouse=True)
    def setup_method(self, app):
        yield
        clean_database(app)
        clean_views()

    class CreateEnrichedStockViewTest:
        def test_should_return_all_values(self, app):
            # Given
            create_user(app, id=1)
            create_user(app, id=2, email='other@test.com')
            create_product(app, id=1, product_type='EventType.CINEMA')
            create_product(app, id=2, product_type='ThingType.LIVRE_EDITION')
            create_offerer(app, id=3)
            create_venue(app, offerer_id=3, id=1, siret=None, postal_code=None, city=None, departement_code=None,
                         is_virtual=True)
            create_offer(app, venue_id=1, product_id=1, id=3, product_type='EventType.CINEMA', name="Test")
            create_stock(app, offer_id=3, id=1, date_created='2019-11-01', quantity=10,
                         booking_limit_datetime='2019-11-23', beginning_datetime='2019-11-24')
            create_offer(app, venue_id=1, product_id=2, id=2, product_type='ThingType.LIVRE_EDITION', name="Test bis")
            create_stock(app, offer_id=2, id=2, date_created='2019-10-01', quantity=12)
            create_booking(app, user_id=1, stock_id=1, id=4, quantity=2)
            create_payment(app, booking_id=4, id=1)
            create_payment_status(app, payment_id=1, id=1, date='2019-01-01', status='PENDING')

            with app.app_context():
                create_stocks_booking_view()
                create_available_stocks_view()

            expected_stocks_details = pandas.DataFrame(
                index=pandas.Index(data=[1, 2], name='stock_id'),
                data={"Identifiant de l'offre": [3, 2],
                      "Nom de l'offre": ["Test", "Test bis"],
                      "offerer_id": [3, 3],
                      "Type d'offre": ["EventType.CINEMA", 'ThingType.LIVRE_EDITION'],
                      "Département": [None, None],
                      "Date de création du stock": [datetime(2019, 11, 1), datetime(2019, 10, 1)],
                      "Date limite de réservation": [datetime(2019, 11, 23), pandas.NaT],
                      "Date de début de l'évènement": [datetime(2019, 11, 24), pandas.NaT],
                      "Stock disponible réel": [8, 12],
                      "Stock disponible brut de réservations": [10, 12],
                      "Nombre total de réservations": [2, 0],
                      "Nombre de réservations annulées": [0, 0],
                      "Nombre de réservations ayant un paiement": [2, 0]
                      }
            )

            # When
            with app.app_context():
                create_enriched_stock_view()

            # Then
            stocks_details = pandas.read_sql_table('enriched_stock_data', CONNECTION, index_col='stock_id')
            pandas.testing.assert_frame_equal(stocks_details, expected_stocks_details)

    class CreateEnrichedUserViewTest:
        def test_should_create_enriched_user_data_view_with_columns(self, app):
            # When
            with app.app_context():
                create_enriched_user_data()

            # Then
            expected_columns = ["Vague d'expérimentation", "Département", "Date d'activation",
                                "Date de remplissage du typeform",
                                "Date de première connexion", "Date de première réservation",
                                "Date de deuxième réservation",
                                "Date de première réservation dans 3 catégories différentes",
                                "Date de dernière recommandation",
                                "Nombre de réservations totales", "Nombre de réservations non annulées",
                                "Ancienneté en jours",
                                "Montant réél dépensé", "Montant théorique dépensé", "Dépenses numériques",
                                "Dépenses physiques"]

            beneficiary_users_details = pandas.read_sql_table('enriched_user_data', CONNECTION, index_col='user_id')
            assert sorted(expected_columns) == sorted(beneficiary_users_details.columns)
            

    class CreateEnrichedOffererViewTest:
        def test_should_create_enriched_offerer_data_view_with_columns(self, app):
            # When
            with app.app_context():
                create_enriched_offerer_data()

            # Then
            expected_columns = ["Date de création", "Date de création du premier stock",
                                "Date de première réservation", "Nombre d’offres",
                                "Nombre de réservations non annulées", "Activité principale"]

            offerers_details = pandas.read_sql_table('enriched_offerer_data', CONNECTION, index_col='offerer_id')
            assert sorted(expected_columns) == sorted(offerers_details.columns)


    class CreateBookableOffersTest:
        def test_should_create_all_bookable_offers_view(self, app):
            # when
            with app.app_context():
                create_all_bookable_offers_view()

            # then
            expected_columns = ["idAtProviders", "dateModifiedAtLastProvider", "dateCreated", "productId", 
                                "venueId", "lastProviderId", "bookingEmail", "isActive", "type", "name", "description", 
                                "conditions", "ageMin", "ageMax", "url", "mediaUrls", "durationMinutes", "isNational", 
                                "extraData", "isDuo", "fieldsUpdated"]

            bookable_offers_df = pandas.read_sql_table('all_bookable_offers', CONNECTION, index_col='id')            
            assert sorted(expected_columns) == sorted(bookable_offers_df.columns)
