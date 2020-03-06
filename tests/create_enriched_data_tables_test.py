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
