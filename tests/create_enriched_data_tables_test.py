from datetime import datetime
from unittest.mock import patch, MagicMock

import pandas
import pytest

from create_enriched_data_tables import create_enriched_offerer_data, \
    create_enriched_user_data
from db import CONNECTION, SESSION
from query_enriched_data_tables import create_enriched_stock_data, create_enriched_stock_data
from stock_queries import create_stock_information, create_stocks_offer_information, create_stock_venue_information, \
    create_stocks_booking_information
from tests.utils import create_offerer, create_user, create_venue, create_offer, create_product, create_stock, \
    clean_database, create_booking, create_payment, create_payment_status

connection = CONNECTION


class EnrichedDataTest:
    @pytest.fixture(scope='session')
    def drop_view(self):
        SESSION.execute('DROP VIEW enriched_stock_data_v2;')
        SESSION.commit()

    @pytest.fixture(autouse=True)
    def setup_class(self):
        clean_database()

    class CreateEnrichedOffererDataTest:

        def test_creates_enriched_offerer_data_table(self):
            # Given
            query = 'SELECT COUNT(*) FROM enriched_offerer_data'

            # When
            create_enriched_offerer_data(connection)

            # Then
            assert SESSION.execute(query).fetchall() == [(0,)]
            SESSION.commit()

        def test_populates_table_when_existing_offerer(self):
            # Given
            create_offerer()

            query = 'SELECT COUNT(*) FROM enriched_offerer_data'

            # When
            create_enriched_offerer_data(connection)

            # Then
            assert SESSION.execute(query).fetchall() == [(1,)]
            SESSION.commit()

        @patch('query_enriched_data_tables.get_offerers_details')
        def test_saves_offerers_details(self, get_offerers_details):
            # Given
            get_offerers_details.return_value = pandas.DataFrame()

            # When
            create_enriched_offerer_data(connection)

            # Then
            get_offerers_details.assert_called_once_with(connection)

        def test_creates_index_on_offerer_id(self):
            # Given
            query = """
            SELECT
                indexname
            FROM
                pg_indexes
            WHERE
                tablename = 'enriched_offerer_data';
            """

            # When
            create_enriched_offerer_data(connection)

            # Then
            assert SESSION.execute(query).fetchall() == [('ix_enriched_offerer_data_offerer_id',)]
            SESSION.commit()

        def test_replaces_table_if_exists(self):
            # Given
            enriched_offerer_data = pandas.DataFrame(
                {'Date de création': '2019-11-18', 'Date de création du premier stock': '2019-11-18',
                 'Date de première réservation': '2019-11-18', 'Nombre d’offres': 0,
                 'Nombre de réservations non annulées': 0}, index={'offerer_id': 1})
            enriched_offerer_data.to_sql(name='enriched_offerer_data',
                                         con=connection)
            query = 'SELECT COUNT(*) FROM enriched_offerer_data'

            # When
            create_enriched_offerer_data(connection)

            # Then
            assert SESSION.execute(query).fetchall() == [(0,)]
            SESSION.commit()

    class CreateEnrichedUserDataTest:

        def test_creates_enriched_user_data_table(self):
            # Given
            query = 'SELECT COUNT(*) FROM enriched_user_data'

            # When
            create_enriched_user_data(connection)

            # Then
            assert SESSION.execute(query).fetchall() == [(0,)]
            SESSION.commit()

        def test_populates_table_when_existing_user(self):
            # Given
            create_user()

            query = 'SELECT COUNT(*) FROM enriched_user_data'

            # When
            create_enriched_user_data(connection)

            # Then
            assert SESSION.execute(query).fetchall() == [(1,)]
            SESSION.commit()

        @patch('query_enriched_data_tables.get_beneficiary_users_details')
        def test_saves_users_details(self, get_beneficiary_users_details):
            # Given
            get_beneficiary_users_details.return_value = pandas.DataFrame()

            # When
            create_enriched_user_data(connection)

            # Then
            get_beneficiary_users_details.assert_called_once_with(connection)

        def test_creates_index(self):
            # Given
            query = """
                SELECT
                    indexname
                FROM
                    pg_indexes
                WHERE
                    tablename = 'enriched_user_data';
                """

            # When
            create_enriched_user_data(connection)

            # Then
            assert SESSION.execute(query).fetchall() == [('ix_enriched_user_data_index',)]
            SESSION.commit()

        @patch('query_enriched_data_tables.get_beneficiary_users_details')
        def test_shuffles_index(self, get_beneficiary_users_details):
            # Given
            enriched_user_data = MagicMock()
            get_beneficiary_users_details.return_value = enriched_user_data

            # When
            create_enriched_user_data(connection)

            # Then
            enriched_user_data.sample.assert_called_once_with(frac=1)

        def test_replaces_table_if_exists(self):
            # Given
            enriched_user_data = pandas.DataFrame(
                {'Vague d\'expérimentation': 1, 'Département': '78', 'Date d\'activation': '2019-11-18',
                 'Date de remplissage du typeform': '2019-11-18', 'Date de première connexion': '2019-11-18',
                 'Date de première réservation': '2019-11-18', 'Date de deuxième réservation': '2019-11-18',
                 'Date de première réservation dans 3 catégories différentes': '2019-11-18',
                 'Date de dernière recommandation': '2019-11-18', 'Nombre de réservations totales': 3,
                 'Nombre de réservations non annulées': 3}, index={'index': 1})
            enriched_user_data.to_sql(name='enriched_user_data',
                                      con=connection)
            query = 'SELECT COUNT(*) FROM enriched_user_data'

            # When
            create_enriched_user_data(connection)

            # Then
            assert SESSION.execute(query).fetchall() == [(0,)]
            SESSION.commit()

    class CreateEnrichedStockDataTest:
        def test_should_return_all_values(self):
            # Given
            create_user(id=1)
            create_user(id=2, email='other@test.com')
            create_product(id=1, product_type='EventType.CINEMA')
            create_product(id=2, product_type='ThingType.LIVRE_EDITION')
            create_offerer(id=3)
            create_venue(id=1, offerer_id=3, siret=None, departement_code=None, postal_code=None, city=None,
                         is_virtual=True)
            create_offer(id=3, venue_id=1, product_id=1, product_type='EventType.CINEMA', name="Test")
            create_stock(id=1, offer_id=3, booking_limit_datetime='2019-11-23',
                         beginning_datetime='2019-11-24', available=10, date_created='2019-11-01')
            create_offer(id=2, venue_id=1, product_id=2, name="Test bis", product_type='ThingType.LIVRE_EDITION')
            create_stock(id=2, offer_id=2, available=12, date_created='2019-10-01')
            create_booking(user_id=1, stock_id=1, quantity=2, id=4)
            create_payment(booking_id=4, id=1)
            create_payment_status(payment_id=1, status='PENDING', date='2019-01-01', id=1)

            create_stock_information()
            create_stocks_offer_information()
            create_stock_venue_information()
            create_stocks_booking_information()

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
            create_enriched_stock_data()

            # Then
            stocks_details = pandas.read_sql_table('enriched_stock_data_v2', CONNECTION, index_col='stock_id')
            pandas.testing.assert_frame_equal(stocks_details, expected_stocks_details)
