from datetime import datetime

import pandas
import pytest
from freezegun import freeze_time

from db import CONNECTION
from db import db
from query_enriched_data_tables import create_enriched_user_data
from stock_queries import create_stocks_booking_view
from tests.utils import clean_database, create_user, create_product, create_offerer, create_venue, create_offer, \
    create_stock, create_booking, create_payment, create_payment_status, create_recommendation, create_deposit, \
    update_table_column
from user_queries import create_experimentation_sessions_view, create_activation_dates_view, \
    create_first_connection_dates_view, create_date_of_first_bookings_view, create_date_of_second_bookings_view, \
    create_date_of_bookings_on_third_product_view, create_last_recommendation_dates_view, \
    create_number_of_bookings_view, create_number_of_non_cancelled_bookings_view, create_users_seniority_view, \
    create_actual_amount_spent_view, create_theoric_amount_spent_view, \
    create_theoric_amount_spent_in_digital_goods_view, create_theoric_amount_spent_in_physical_goods_view
from view_queries import create_enriched_stock_view, create_enriched_user_view


class ViewQueriesTest:
    @pytest.fixture(autouse=True)
    def setup_class(self, app):
        clean_database(app)

    @pytest.fixture(scope='session')
    def drop_view(self, app):
        db.session.execute('DROP VIEW enriched_stock_data;')
        db.session.commit()

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
            create_stock(app, offer_id=3, id=1, date_created='2019-11-01', available=10,
                         booking_limit_datetime='2019-11-23', beginning_datetime='2019-11-24')
            create_offer(app, venue_id=1, product_id=2, id=2, product_type='ThingType.LIVRE_EDITION', name="Test bis")
            create_stock(app, offer_id=2, id=2, date_created='2019-10-01', available=12)
            create_booking(app, user_id=1, stock_id=1, id=4, quantity=2)
            create_payment(app, booking_id=4, id=1)
            create_payment_status(app, payment_id=1, id=1, date='2019-01-01', status='PENDING')

            with app.app_context():
                create_stocks_booking_view()

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
        def test_should_not_return_details_when_user_cannot_book_free_offers(self, app):
            # Given
            create_user(app, can_book_free_offers=False)

            expected_beneficiary_users_details = pandas.DataFrame(data=[])

            # When
            with app.app_context():
                create_enriched_user_data()

            # Then
            beneficiary_users_details = pandas.read_sql_table('enriched_user_data', CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(beneficiary_users_details, expected_beneficiary_users_details)

        @freeze_time('2020-01-21 11:00:00')
        def test_should_return_values_for_users_who_can_book_free_offers(self, app):
            # Given
            activation_id = 1
            active_user_id = 2
            create_user(app, id=1, can_book_free_offers=True, departement_code="93",
                        date_created=datetime(2019, 1, 1, 12, 0, 0), needs_to_fill_cultural_survey=True)
            create_user(app, id=active_user_id, email="em@a.il", can_book_free_offers=True, departement_code="08",
                        needs_to_fill_cultural_survey=False, cultural_survey_filled_date='2019-12-08')
            create_offerer(app, id=1)
            create_venue(app, offerer_id=1, id=1)
            create_product(app, id=activation_id, product_type='ThingType.ACTIVATION')
            create_offer(app, venue_id=1, product_id=activation_id, id=1, product_type='ThingType.ACTIVATION')
            create_stock(app, offer_id=activation_id, id=1)
            create_booking(app, user_id=active_user_id, stock_id=activation_id, id=1, is_used=True,
                           date_used='2019-12-09')
            create_product(app, id=2, product_type='ThingType.JEUX_VIDEO')
            create_product(app, id=3, product_type='ThingType.AUDIOVISUEL')
            create_product(app, id=4, product_type='ThingType.CINEMA_ABO')
            create_offer(app, venue_id=1, product_id=2, id=2, product_type='ThingType.JEUX_VIDEO', url='u.rl')
            create_offer(app, venue_id=1, product_id=3, id=3, product_type='ThingType.AUDIOVISUEL')
            create_offer(app, venue_id=1, product_id=4, id=4, product_type='ThingType.CINEMA_ABO')

            create_recommendation(app, offer_id=activation_id, user_id=active_user_id, id=1,
                                  date_created=datetime(2019, 2, 3))
            create_stock(app, offer_id=2, id=2)
            create_stock(app, offer_id=3, id=3)
            create_stock(app, offer_id=4, id=4)
            create_deposit(app, user_id=active_user_id)
            create_booking(app, user_id=active_user_id, stock_id=2, id=2, date_created=datetime(2019, 3, 7),
                           token='18J2K1', amount=20, is_used=False)
            create_booking(app, user_id=active_user_id, stock_id=3, id=3, date_created=datetime(2019, 4, 7),
                           token='1U2I12', amount=10, is_used=True)
            create_booking(app, user_id=active_user_id, stock_id=4, id=4, date_created=datetime(2019, 5, 7),
                           token='J91U21', amount=5, is_cancelled=True)
            update_table_column(app, id=activation_id, table_name='booking', column='"isUsed"', value='True')
            recommendation_creation_date = datetime.utcnow()
            create_recommendation(app, offer_id=3, user_id=active_user_id, id=2,
                                  date_created=recommendation_creation_date)

            columns = ["Vague d'expérimentation", "Département", "Date d'activation",
                       "Date de remplissage du typeform",
                       "Date de première connexion", "Date de première réservation", "Date de deuxième réservation",
                       "Date de première réservation dans 3 catégories différentes",
                       "Date de dernière recommandation",
                       "Nombre de réservations totales", "Nombre de réservations non annulées",
                       "Ancienneté en jours",
                       "Montant réél dépensé", "Montant théorique dépensé", "Dépenses numériques",
                       "Dépenses physiques"]


            expected_beneficiary_users_details = pandas.DataFrame(
                index=pandas.RangeIndex(start=0, stop=2, step=1),
                data=[
                    [2, "93", datetime(2019, 1, 1, 12, 0, 0), pandas.NaT, pandas.NaT, pandas.NaT,
                     pandas.NaT, pandas.NaT, pandas.NaT, 0, 0, 384, 0., 0., 0., 0.],
                    [1, "08", datetime(2019, 12, 9), datetime(2019, 12, 8), datetime(2019, 2, 3),
                     datetime(2019, 3, 7),
                     datetime(2019, 4, 7), datetime(2019, 5, 7), recommendation_creation_date, 3, 2, 43, 10., 30.,
                     20.,
                     10.]
                ],
                columns=columns
            )

            # When
            with app.app_context():
                create_enriched_user_data()

            # Then
            beneficiary_users_details = pandas.read_sql_table('enriched_user_data', CONNECTION, index_col='user_id')
            pandas.testing.assert_frame_equal(beneficiary_users_details, expected_beneficiary_users_details)
