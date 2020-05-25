import pandas
from models.create_offerer_cultural_activity import create_offerer_cultural_activity_dataframe, create_table_offerer_cultural_activity
from repository.offerer_queries import create_siren_dataframe
import pytest
from unittest.mock import patch, MagicMock
from utils.get_label_from_given_ape_code import get_label_from_given_ape_code
from models.db import db
from tests.repository.utils import create_offerer, clean_database, clean_tables


class CreateOffererCulturalActivityTest:
    @pytest.fixture(autouse=True)
    def setup_method(self, app):
        yield
        clean_database(app)

    def test_should_return_empty_dataframe_when_given_empty_dataframe(self):
        # Given
        empty_siren_dataframe = pandas.DataFrame(columns=['id', 'siren'])
        expected_dataframe = pandas.DataFrame(columns=['id', 'APE_label'])

        # When
        result = create_offerer_cultural_activity_dataframe(empty_siren_dataframe)

        # Then
        pandas.testing.assert_frame_equal(expected_dataframe, result)
    
    def test_should_return_dataframe_with_activity_label(self, app):
        # Given
        siren = '345678123'
        create_offerer(app, id=1, siren=siren)
        siren_dataframe = create_siren_dataframe()
        expected_ape_code = '7021ZX'
        expected_label = get_label_from_given_ape_code(expected_ape_code)
        expected_dataframe = pandas.DataFrame(data={'id': [1], 'APE_label': [expected_label]})

        # When
        result = create_offerer_cultural_activity_dataframe(siren_dataframe)

        # Then
        pandas.testing.assert_frame_equal(expected_dataframe, result)


class CreateTableOffererCulturalActivityTest:
    @pytest.fixture(autouse=True)
    def setup_method(self, app):
        yield
        clean_database(app)
        clean_tables()

    def test_should_create_table(self, app):
        # Given
        offerer_cultural_activity_dataframe = pandas.DataFrame()

        # When
        with app.app_context():
            create_table_offerer_cultural_activity(offerer_cultural_activity_dataframe)

        # Then
        query = '''SELECT * FROM information_schema.tables WHERE table_name = 'offerer_cultural_activity';'''
        results = db.session.execute(query).fetchall()
        assert len(results) == 1
