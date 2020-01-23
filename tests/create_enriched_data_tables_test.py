from unittest.mock import patch, MagicMock

import pandas
import pytest

from create_enriched_data_tables import create_enriched_offerer_data, \
    create_enriched_user_data
from db import CONNECTION, ENGINE
from query_enriched_data_tables import create_enriched_stock_data
from tests.utils import create_offerer, create_user, create_venue, create_offer, create_product, create_stock, \
    clean_database

connection = CONNECTION


class EnrichedDataTest:
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
            assert ENGINE.execute(query).fetchall() == [(0,)]

        def test_populates_table_when_existing_offerer(self):
            # Given
            create_offerer()

            query = 'SELECT COUNT(*) FROM enriched_offerer_data'

            # When
            create_enriched_offerer_data(connection)

            # Then
            assert ENGINE.execute(query).fetchall() == [(1,)]

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
            assert ENGINE.execute(query).fetchall() == [('ix_enriched_offerer_data_offerer_id',)]

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
            assert ENGINE.execute(query).fetchall() == [(0,)]

    class CreateEnrichedUserDataTest:

        def test_creates_enriched_user_data_table(self):
            # Given
            query = 'SELECT COUNT(*) FROM enriched_user_data'

            # When
            create_enriched_user_data(connection)

            # Then
            assert ENGINE.execute(query).fetchall() == [(0,)]

        def test_populates_table_when_existing_user(self):
            # Given
            create_user()

            query = 'SELECT COUNT(*) FROM enriched_user_data'

            # When
            create_enriched_user_data(connection)

            # Then
            assert ENGINE.execute(query).fetchall() == [(1,)]

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
            assert ENGINE.execute(query).fetchall() == [('ix_enriched_user_data_index',)]

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
            assert ENGINE.execute(query).fetchall() == [(0,)]

    class CreateEnrichedStockDataTest:

        def test_initializes_empty_table_when_no_existing_offerer(self):
            # Given
            query = 'SELECT COUNT(*) FROM enriched_stock_data'

            # When
            create_enriched_stock_data(connection)

            # Then
            assert ENGINE.execute(query).fetchall() == [(0,)]

        def test_populates_table_when_existing_offerer(self):
            # Given
            create_offerer(id=1)
            create_venue(offerer_id=1, id=1)
            create_product(id=1)
            create_offer(venue_id=1, product_id=1, id=1)
            create_stock(offer_id=1, id=1)

            query = 'SELECT COUNT(*) FROM enriched_stock_data'

            # When
            create_enriched_stock_data(connection)

            # Then
            assert ENGINE.execute(query).fetchall() == [(1,)]

        @patch('query_enriched_data_tables.get_stocks_details')
        def test_saves_stocks_details(self, get_stocks_details):
            # Given
            mocked_dataframe = MagicMock()
            mocked_dataframe.to_sql.return_value = MagicMock()
            get_stocks_details.return_value = mocked_dataframe

            # When
            create_enriched_stock_data(connection)

            # Then
            get_stocks_details.assert_called_once_with(connection)
            mocked_dataframe.to_sql.assert_called_once_with(chunksize=100000, name='enriched_stock_data',
                                 con=connection,
                                 if_exists='replace')

        def test_creates_index_on_stock_id(self):
            # Given
            query = """
            SELECT
                indexname
            FROM
                pg_indexes
            WHERE
                tablename = 'enriched_stock_data';
            """

            # When
            create_enriched_stock_data(connection)

            # Then
            assert ENGINE.execute(query).fetchall() == [('ix_enriched_stock_data_stock_id',)]

        def test_replaces_table_if_exists(self):
            # Given
            enriched_stock_data = pandas.DataFrame(
                {'Date de création': '2019-11-18',
                 'Date de création du premier stock': '2019-11-18',
                 'Date de première réservation': '2019-11-18',
                 'Nombre d’offres': 0,
                 'Nombre de réservations non annulées': 0},
                index={'offerer_id': 1})
            enriched_stock_data.to_sql(name='enriched_stock_data',
                                         con=connection)
            query = 'SELECT COUNT(*) FROM enriched_stock_data'

            # When
            create_enriched_stock_data(connection)

            # Then
            assert ENGINE.execute(query).fetchall() == [(0,)]
