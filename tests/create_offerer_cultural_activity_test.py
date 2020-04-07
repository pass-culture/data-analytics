import pandas
from create_offerer_cultural_activity import create_offerer_cultural_activity_dataframe
from repository.offerer_queries import create_siren_dataframe
from tests.utils import create_offerer
import pytest
from tests.utils import clean_database, clean_views, clean_tables
from unittest.mock import patch, MagicMock
import api_sirene_connector 
from get_label_from_given_ape_code import get_label_from_given_ape_code
from create_offerer_cultural_activity import create_table_offerer_cultural_activity
from db import db

class CreateOffererCulturalActivityTest:
    @pytest.fixture(autouse=True)
    def setup_method(self, app):
        yield
        clean_database(app)

    def test_should_return_empty_dataframe_when_given_empty_dataframe(self):
        # Given
        siren_dataframe = create_siren_dataframe()
        expected_dataframe = pandas.DataFrame(columns=['APE_label'], index=pandas.Index(data=[], name='id'))

        # When
        result = create_offerer_cultural_activity_dataframe(siren_dataframe)

        # Then
        pandas.testing.assert_frame_equal(expected_dataframe, result)
    
    @patch('api_sirene_connector.requests.get')
    def test_should_return_dataframe_with_activity_label(self, request_get, app):
        # Given
        create_offerer(app, id=1, siren='345678123')
        siren_dataframe = create_siren_dataframe()
        expected_result = '1610B'
        response_return_value = MagicMock(status_code=200)
        response_return_value.json = MagicMock(return_value=expected_result)
        request_get.return_value = response_return_value
        expected_label = get_label_from_given_ape_code(expected_result)
        expected_dataframe = pandas.DataFrame(columns=['APE_label'], data=[expected_label], index=pandas.Index(data=[1], name='id'))

        # When
        result = create_offerer_cultural_activity_dataframe(siren_dataframe)

        # Then
        pandas.testing.assert_frame_equal(expected_dataframe, result)

class CreateTableOffererCulturalActivityTest:
    @pytest.fixture(autouse=True)
    def setup_method(self, app):
        yield
        clean_database(app)
        clean_tables

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
