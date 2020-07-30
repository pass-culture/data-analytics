import pandas

from read.postgresql_database.offerer_queries import get_siren_dataframe
from tests.data_creators import create_offerer
from transform.compute_offerer_cultural_activity import get_offerer_cultural_activity_dataframe, \
    get_label_from_given_ape_code
from utils.database_cleaners import clean_database


class GetOffererCulturalActivityDataframeTest:
    def test_should_return_empty_dataframe_when_given_empty_dataframe(self):
        # Given
        empty_siren_dataframe = pandas.DataFrame(columns=['id', 'siren'])
        expected_dataframe = pandas.DataFrame(columns=['id', 'APE_label'])

        # When
        result = get_offerer_cultural_activity_dataframe(empty_siren_dataframe)

        # Then
        pandas.testing.assert_frame_equal(expected_dataframe, result)

    def test_should_return_dataframe_with_activity_label(self, app):
        # Given
        siren_dataframe = pandas.DataFrame(data={'id': [1], 'siren': [345678123]})
        expected_ape_code = '7021ZX'
        expected_label = get_label_from_given_ape_code(expected_ape_code)
        expected_dataframe = pandas.DataFrame(data={'id': [1], 'APE_label': [expected_label]})

        # When
        result = get_offerer_cultural_activity_dataframe(siren_dataframe)
        print(siren_dataframe)

        # Then
        pandas.testing.assert_frame_equal(expected_dataframe, result)


class GetLabelFromGivenApeCode:
    def test_should_return_empty_string_when_no_APE_code_is_given(self):
        # Given
        ape_code = ''

        # When
        label = get_label_from_given_ape_code(ape_code)

        # Then
        assert label == ''

    def test_should_return_empty_string_when_given_APE_code_does_not_exist_in_mapping_table(self):
        # Given
        ape_code = 'ABCDEF'

        # When
        label = get_label_from_given_ape_code(ape_code)

        # Then
        assert label == ''

    def test_should_return_label_when_given_APE_code_exists_in_mapping_table(self):
        # Given
        ape_code = '5320Z'

        # When
        label = get_label_from_given_ape_code(ape_code)

        # Then
        assert label == 'Autres activités de poste et de courrier'
