from unittest.mock import MagicMock

from application.get_enriched_data_status import get_enriched_data_status


class GetEnrichedDataStatusTest:

    def test_should_return_a_dict_with_offerer_table_status(self):
        # Given
        is_enriched_stock_data_exists = MagicMock()
        is_enriched_user_data_exists = MagicMock()
        is_enriched_offerer_data_exists = MagicMock()
        is_enriched_offerer_data_exists.return_value = True

        # When
        status = get_enriched_data_status(
            is_enriched_offerer_data_exists=is_enriched_offerer_data_exists,
            is_enriched_user_data_exists=is_enriched_user_data_exists,
            is_enriched_stock_data_exists=is_enriched_stock_data_exists
        )

        # Then
        assert status['is_enriched_offerer_datasource_exists'] == True

    def test_should_return_a_dict_with_user_status(self):
        # Given
        is_enriched_stock_data_exists = MagicMock()
        is_enriched_offerer_data_exists = MagicMock()
        is_enriched_user_data_exists = MagicMock()
        is_enriched_user_data_exists.return_value = False

        # When
        status = get_enriched_data_status(
            is_enriched_offerer_data_exists=is_enriched_offerer_data_exists,
            is_enriched_user_data_exists=is_enriched_user_data_exists,
            is_enriched_stock_data_exists=is_enriched_stock_data_exists
        )

        # Then
        assert status['is_enriched_user_datasource_exists'] == False


    def test_should_return_a_dict_with_stock_status(self):
        # Given
        is_enriched_offerer_data_exists = MagicMock()
        is_enriched_user_data_exists = MagicMock()
        is_enriched_stock_data_exists = MagicMock()
        is_enriched_stock_data_exists.return_value = True

        # When
        status = get_enriched_data_status(
            is_enriched_offerer_data_exists=is_enriched_offerer_data_exists,
            is_enriched_user_data_exists=is_enriched_user_data_exists,
            is_enriched_stock_data_exists=is_enriched_stock_data_exists
        )

        # Then
        assert status['is_enriched_stock_datasource_exists'] == True
