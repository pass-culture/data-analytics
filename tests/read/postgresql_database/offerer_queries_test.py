import pandas

from read.postgresql_database.offerer_queries import get_siren_dataframe, get_postal_code_dataframe
from tests.data_creators import create_offerer
from utils.database_cleaners import clean_database, clean_views


class OffererQueriesTest:
    def teardown_method(self):
        clean_database()
        clean_views()

    class GetSirenDataFrameTest:
        def test_should_return_empty_dataframe_if_no_offerer(self, app):
            # When
            df = get_siren_dataframe()

            # Then
            assert df.empty

        def test_should_return_siren_related_to_existing_offerers(self, app):
            # Given
            create_offerer(app, id=1, siren='345678123')
            create_offerer(app, id=2, siren=None)
            create_offerer(app, id=3, siren='123456789')
            expected_siren_dataframe = pandas.DataFrame(data={"id": [1, 3], "siren": ['345678123', '123456789']})

            # When
            siren_dataframe = get_siren_dataframe()

            # Then
            pandas.testing.assert_frame_equal(siren_dataframe, expected_siren_dataframe)


    class GetPostalCodeDataFrameTest:
        def test_should_return_empty_dataframe_if_no_offerer(self, app):
            # When
            df = get_postal_code_dataframe()

            # Then
            assert df.empty

        def test_should_return_postal_code_related_to_existing_offerers(self, app):
            # Given
            create_offerer(app, id=1, postal_code='75003')
            create_offerer(app, id=2, postal_code='97459', siren='123456788')
            expected_postal_code_dataframe = pandas.DataFrame(data={"id": [1, 2], "postalCode": ['75003', '97459']})

            # When
            postal_code_dataframe = get_postal_code_dataframe()

            # Then
            pandas.testing.assert_frame_equal(postal_code_dataframe, expected_postal_code_dataframe)
