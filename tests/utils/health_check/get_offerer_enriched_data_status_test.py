from unittest.mock import MagicMock

from utils.health_check.get_offerer_enriched_data_status import get_offerer_enriched_data_status


class GetEnrichedDataStatusTest:
    class OffererStatusTest:
        def test_should_return_a_dict_with_offerer_table_status(self):
            # Given
            is_enriched_offerer_contains_data = MagicMock(return_value=False)
            is_enriched_offerer_data_exists = MagicMock(return_value=False)

            is_enriched_offerer_data_exists.return_value = True

            # When
            status = get_offerer_enriched_data_status(
                is_enriched_offerer_data_exists=is_enriched_offerer_data_exists,
                is_enriched_offerer_contains_data=is_enriched_offerer_contains_data,
            )

            # Then
            assert status['is_enriched_offerer_datasource_exists']
            assert status['is_offerer_ok'] is False

        def test_should_return_is_offerer_ok_as_true_when_table_exists_with_data(self):
            # Given
            is_enriched_offerer_contains_data = MagicMock(return_value=False)
            is_enriched_offerer_data_exists = MagicMock(return_value=False)

            is_enriched_offerer_data_exists.return_value = True
            is_enriched_offerer_contains_data.return_value = True

            # When
            status = get_offerer_enriched_data_status(
                is_enriched_offerer_data_exists=is_enriched_offerer_data_exists,
                is_enriched_offerer_contains_data=is_enriched_offerer_contains_data,
            )

            # Then
            assert status['is_enriched_offerer_datasource_exists']
            assert status['is_offerer_ok']

        def test_should_return_is_offerer_ok_as_false_when_table_exists_without_data(self):
            # Given
            is_enriched_offerer_contains_data = MagicMock(return_value=False)
            is_enriched_offerer_data_exists = MagicMock(return_value=False)

            is_enriched_offerer_data_exists.return_value = True
            is_enriched_offerer_contains_data.return_value = False

            # When
            status = get_offerer_enriched_data_status(
                is_enriched_offerer_data_exists=is_enriched_offerer_data_exists,
                is_enriched_offerer_contains_data=is_enriched_offerer_contains_data,
            )

            # Then
            assert status['is_enriched_offerer_datasource_exists']
            assert status['is_offerer_ok'] is False
