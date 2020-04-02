import pandas
from create_offerer_cultural_activity import create_offerer_cultural_activity_dataframe
from repository.offerer_queries import create_siren_dataframe
from tests.utils import create_offerer
from pprint import pprint

import pytest
from tests.utils import clean_database, clean_views

class CreateOffererCulturalActivityTest:
    @pytest.fixture(autouse=True)
    def setup_method(self, app):
        yield
        clean_database(app)
        clean_views()

    def test_should_return_empty_dataframe_when_given_empty_dataframe(self):
        # Given
        siren_dataframe = create_siren_dataframe()
        expected_dataframe = pandas.DataFrame(columns=['APE_label'], index=pandas.Index(data=[], name='id'))

        # When
        result = create_offerer_cultural_activity_dataframe(siren_dataframe)

        # Then
        pandas.testing.assert_frame_equal(expected_dataframe, result)

    def test_should_return_dataframe_with_activity_label(self, app):
        # Given
        create_offerer(app, id=1, siren='345678123')
        create_offerer(app, id=3, siren='123456789')
        siren_dataframe = create_siren_dataframe()
        expected_dataframe = pandas.DataFrame(columns=['APE_label'], data=['', ''], index=pandas.Index(data=[1, 3], name='id'))

        # When
        result = create_offerer_cultural_activity_dataframe(siren_dataframe)

        # Then
        pprint(expected_dataframe)
        
        pprint(result)
        pandas.testing.assert_frame_equal(expected_dataframe, result)
