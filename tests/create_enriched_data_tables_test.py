from unittest.mock import patch, MagicMock

import pandas
import pytest

from db import db
from db import CONNECTION
from create_enriched_data_tables import create_enriched_offerer_data, \
    create_enriched_user_data
from tests.utils import create_offerer, create_user, clean_database

class EnrichedDataTest:
    @pytest.fixture(autouse=True)
    def setup_class(self, app):
        with app.app_context():
            clean_database(app)

    class CreateEnrichedOffererDataTest:

        def test_creates_enriched_offerer_data_table(self, app):
            # Given
            query = 'SELECT COUNT(*) FROM enriched_offerer_data'

            # When
            create_enriched_offerer_data(CONNECTION)

            # Then
            with app.app_context():
                assert db.session.execute(query).fetchall() == [(0,)]
                db.session.commit()

        def test_populates_table_when_existing_offerer(self, app):
            # Given
            create_offerer(app)

            query = 'SELECT COUNT(*) FROM enriched_offerer_data'

            # When
            create_enriched_offerer_data(CONNECTION)

            # Then
            with app.app_context():
                assert db.session.execute(query).fetchall() == [(1,)]
                db.session.commit()

        @patch('query_enriched_data_tables.get_offerers_details')
        def test_saves_offerers_details(self, get_offerers_details, app):
            # Given
            get_offerers_details.return_value = pandas.DataFrame()

            # When
            create_enriched_offerer_data(CONNECTION)

            # Then
            get_offerers_details.assert_called_once_with(CONNECTION)

        def test_creates_index_on_offerer_id(self, app):
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
            create_enriched_offerer_data(CONNECTION)

            # Then
            with app.app_context():
                assert db.session.execute(query).fetchall() == [('ix_enriched_offerer_data_offerer_id',)]
                db.session.commit()

        def test_replaces_table_if_exists(self, app):
            # Given
            enriched_offerer_data = pandas.DataFrame(
                {'Date de création': '2019-11-18', 'Date de création du premier stock': '2019-11-18',
                 'Date de première réservation': '2019-11-18', 'Nombre d’offres': 0,
                 'Nombre de réservations non annulées': 0}, index={'offerer_id': 1})
            enriched_offerer_data.to_sql(name='enriched_offerer_data',
                                         con=CONNECTION)
            query = 'SELECT COUNT(*) FROM enriched_offerer_data'

            # When
            create_enriched_offerer_data(CONNECTION)

            # Then
            with app.app_context():
                assert db.session.execute(query).fetchall() == [(0,)]
                db.session.commit()

    class CreateEnrichedUserDataTest:

        def test_creates_enriched_user_data_table(self, app):
            # Given
            query = 'SELECT COUNT(*) FROM enriched_user_data'

            # When
            create_enriched_user_data(CONNECTION)

            # Then
            with app.app_context():
                assert db.session.execute(query).fetchall() == [(0,)]
                db.session.commit()

        def test_populates_table_when_existing_user(self, app):
            # Given
            create_user(app)

            query = 'SELECT COUNT(*) FROM enriched_user_data'

            # When
            create_enriched_user_data(CONNECTION)

            # Then
            with app.app_context():
                assert db.session.execute(query).fetchall() == [(1,)]
                db.session.commit()

        @patch('query_enriched_data_tables.get_beneficiary_users_details')
        def test_saves_users_details(self, get_beneficiary_users_details, app):
            # Given
            get_beneficiary_users_details.return_value = pandas.DataFrame()

            # When
            create_enriched_user_data(CONNECTION)

            # Then
            get_beneficiary_users_details.assert_called_once_with(CONNECTION)

        def test_creates_index(self, app):
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
            create_enriched_user_data(CONNECTION)

            # Then
            with app.app_context():
                assert db.session.execute(query).fetchall() == [('ix_enriched_user_data_index',)]
                db.session.commit()

        @patch('query_enriched_data_tables.get_beneficiary_users_details')
        def test_shuffles_index(self, get_beneficiary_users_details, app):
            # Given
            enriched_user_data = MagicMock()
            get_beneficiary_users_details.return_value = enriched_user_data

            # When
            create_enriched_user_data(CONNECTION)

            # Then
            enriched_user_data.sample.assert_called_once_with(frac=1)

        def test_replaces_table_if_exists(self, app):
            # Given
            enriched_user_data = pandas.DataFrame(
                {'Vague d\'expérimentation': 1, 'Département': '78', 'Date d\'activation': '2019-11-18',
                 'Date de remplissage du typeform': '2019-11-18', 'Date de première connexion': '2019-11-18',
                 'Date de première réservation': '2019-11-18', 'Date de deuxième réservation': '2019-11-18',
                 'Date de première réservation dans 3 catégories différentes': '2019-11-18',
                 'Date de dernière recommandation': '2019-11-18', 'Nombre de réservations totales': 3,
                 'Nombre de réservations non annulées': 3}, index={'index': 1})
            enriched_user_data.to_sql(name='enriched_user_data',
                                      con=CONNECTION)
            query = 'SELECT COUNT(*) FROM enriched_user_data'

            # When
            create_enriched_user_data(CONNECTION)

            # Then
            with app.app_context():
                assert db.session.execute(query).fetchall() == [(0,)]
                db.session.commit()
