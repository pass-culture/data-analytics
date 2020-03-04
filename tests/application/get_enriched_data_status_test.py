from unittest.mock import MagicMock

from application.get_enriched_data_status import get_enriched_data_status


class GetEnrichedDataStatusTest:

    class OffererStatusTest:
        def test_should_return_a_dict_with_offerer_table_status(self):
            # Given
            is_enriched_offerer_contains_data = MagicMock()
            is_enriched_users_contains_data = MagicMock()
            is_enriched_stocks_contains_data = MagicMock()
            is_enriched_stock_data_exists = MagicMock()
            is_enriched_user_data_exists = MagicMock()
            is_enriched_offerer_data_exists = MagicMock()

            is_enriched_offerer_data_exists.return_value = True

            # When
            status = get_enriched_data_status(
                is_enriched_offerer_data_exists=is_enriched_offerer_data_exists,
                is_enriched_user_data_exists=is_enriched_user_data_exists,
                is_enriched_stock_data_exists=is_enriched_stock_data_exists,
                is_enriched_offerer_contains_data=is_enriched_offerer_contains_data,
                is_enriched_users_contains_data=is_enriched_users_contains_data,
                is_enriched_stocks_contains_data=is_enriched_stocks_contains_data
            )

            # Then
            assert status['is_enriched_offerer_datasource_exists'] == True

        def test_should_return_is_offerer_ok_as_true_when_table_exists_with_data(self):
            # Given
            is_enriched_offerer_contains_data = MagicMock()
            is_enriched_users_contains_data = MagicMock()
            is_enriched_stocks_contains_data = MagicMock()
            is_enriched_stock_data_exists = MagicMock()
            is_enriched_user_data_exists = MagicMock()
            is_enriched_offerer_data_exists = MagicMock()

            is_enriched_offerer_data_exists.return_value = True
            is_enriched_offerer_contains_data.return_value = True

            # When
            status = get_enriched_data_status(
                is_enriched_offerer_data_exists=is_enriched_offerer_data_exists,
                is_enriched_user_data_exists=is_enriched_user_data_exists,
                is_enriched_stock_data_exists=is_enriched_stock_data_exists,
                is_enriched_offerer_contains_data=is_enriched_offerer_contains_data,
                is_enriched_users_contains_data=is_enriched_users_contains_data,
                is_enriched_stocks_contains_data=is_enriched_stocks_contains_data
            )

            # Then
            assert status['is_enriched_offerer_datasource_exists'] == True
            assert status['is_offerer_ok'] == True

        def test_should_return_is_offerer_ok_as_false_when_table_exists_without_data(self):
            # Given
            is_enriched_offerer_contains_data = MagicMock()
            is_enriched_users_contains_data = MagicMock()
            is_enriched_stocks_contains_data = MagicMock()
            is_enriched_stock_data_exists = MagicMock()
            is_enriched_user_data_exists = MagicMock()
            is_enriched_offerer_data_exists = MagicMock()

            is_enriched_offerer_data_exists.return_value = True
            is_enriched_offerer_contains_data.return_value = False

            # When
            status = get_enriched_data_status(
                is_enriched_offerer_data_exists=is_enriched_offerer_data_exists,
                is_enriched_user_data_exists=is_enriched_user_data_exists,
                is_enriched_stock_data_exists=is_enriched_stock_data_exists,
                is_enriched_offerer_contains_data=is_enriched_offerer_contains_data,
                is_enriched_users_contains_data=is_enriched_users_contains_data,
                is_enriched_stocks_contains_data=is_enriched_stocks_contains_data
            )

            # Then
            assert status['is_enriched_offerer_datasource_exists'] == True
            assert status['is_offerer_ok'] == False

    class UserStatusTest:
        def test_should_return_a_dict_with_user_table_status(self):
            # Given
            is_enriched_offerer_contains_data = MagicMock()
            is_enriched_users_contains_data = MagicMock()
            is_enriched_stocks_contains_data = MagicMock()
            is_enriched_stock_data_exists = MagicMock()
            is_enriched_user_data_exists = MagicMock()
            is_enriched_offerer_data_exists = MagicMock()
            is_enriched_user_data_exists.return_value = False

            # When
            status = get_enriched_data_status(
                is_enriched_offerer_data_exists=is_enriched_offerer_data_exists,
                is_enriched_user_data_exists=is_enriched_user_data_exists,
                is_enriched_stock_data_exists=is_enriched_stock_data_exists,
                is_enriched_offerer_contains_data=is_enriched_offerer_contains_data,
                is_enriched_users_contains_data=is_enriched_users_contains_data,
                is_enriched_stocks_contains_data=is_enriched_stocks_contains_data
            )

            # Then
            assert status['is_enriched_user_datasource_exists'] == False

        def test_should_return_is_user_ok_as_true_when_table_exists_with_data(self):
            # Given
            is_enriched_offerer_contains_data = MagicMock()
            is_enriched_users_contains_data = MagicMock()
            is_enriched_stocks_contains_data = MagicMock()
            is_enriched_stock_data_exists = MagicMock()
            is_enriched_user_data_exists = MagicMock()
            is_enriched_offerer_data_exists = MagicMock()

            is_enriched_user_data_exists.return_value = True
            is_enriched_users_contains_data.return_value = True

            # When
            status = get_enriched_data_status(
                is_enriched_offerer_data_exists=is_enriched_offerer_data_exists,
                is_enriched_user_data_exists=is_enriched_user_data_exists,
                is_enriched_stock_data_exists=is_enriched_stock_data_exists,
                is_enriched_offerer_contains_data=is_enriched_offerer_contains_data,
                is_enriched_users_contains_data=is_enriched_users_contains_data,
                is_enriched_stocks_contains_data=is_enriched_stocks_contains_data
            )

            # Then
            assert status['is_enriched_user_datasource_exists'] == True
            assert status['is_user_ok'] == True

        def test_should_return_is_user_ok_as_false_when_table_exists_without_data(self):
            # Given
            is_enriched_offerer_contains_data = MagicMock()
            is_enriched_users_contains_data = MagicMock()
            is_enriched_stocks_contains_data = MagicMock()
            is_enriched_stock_data_exists = MagicMock()
            is_enriched_user_data_exists = MagicMock()
            is_enriched_offerer_data_exists = MagicMock()

            is_enriched_user_data_exists.return_value = True
            is_enriched_users_contains_data.return_value = False

            # When
            status = get_enriched_data_status(
                is_enriched_offerer_data_exists=is_enriched_offerer_data_exists,
                is_enriched_user_data_exists=is_enriched_user_data_exists,
                is_enriched_stock_data_exists=is_enriched_stock_data_exists,
                is_enriched_offerer_contains_data=is_enriched_offerer_contains_data,
                is_enriched_users_contains_data=is_enriched_users_contains_data,
                is_enriched_stocks_contains_data=is_enriched_stocks_contains_data
            )

            # Then
            assert status['is_enriched_user_datasource_exists'] == True
            assert status['is_user_ok'] == False

    class StockStatusTest:
        def test_should_return_a_dict_with_stock_status(self):
            # Given
            is_enriched_offerer_contains_data = MagicMock()
            is_enriched_users_contains_data = MagicMock()
            is_enriched_stocks_contains_data = MagicMock()
            is_enriched_stock_data_exists = MagicMock()
            is_enriched_user_data_exists = MagicMock()
            is_enriched_offerer_data_exists = MagicMock()

            is_enriched_stock_data_exists.return_value = True

            # When
            status = get_enriched_data_status(
                is_enriched_offerer_data_exists=is_enriched_offerer_data_exists,
                is_enriched_user_data_exists=is_enriched_user_data_exists,
                is_enriched_stock_data_exists=is_enriched_stock_data_exists,
                is_enriched_offerer_contains_data=is_enriched_offerer_contains_data,
                is_enriched_users_contains_data=is_enriched_users_contains_data,
                is_enriched_stocks_contains_data=is_enriched_stocks_contains_data
            )

            # Then
            assert status['is_enriched_stock_datasource_exists'] == True


        def test_should_return_is_stock_ok_as_true_when_table_exists_with_data(self):
            # Given
            is_enriched_offerer_contains_data = MagicMock()
            is_enriched_users_contains_data = MagicMock()
            is_enriched_stocks_contains_data = MagicMock()
            is_enriched_stock_data_exists = MagicMock()
            is_enriched_user_data_exists = MagicMock()
            is_enriched_offerer_data_exists = MagicMock()

            is_enriched_stock_data_exists.return_value = True
            is_enriched_stocks_contains_data.return_value = True

            # When
            status = get_enriched_data_status(
                is_enriched_offerer_data_exists=is_enriched_offerer_data_exists,
                is_enriched_user_data_exists=is_enriched_user_data_exists,
                is_enriched_stock_data_exists=is_enriched_stock_data_exists,
                is_enriched_offerer_contains_data=is_enriched_offerer_contains_data,
                is_enriched_users_contains_data=is_enriched_users_contains_data,
                is_enriched_stocks_contains_data=is_enriched_stocks_contains_data
            )

            # Then
            assert status['is_enriched_stock_datasource_exists'] == True
            assert status['is_stock_ok'] == True


        def test_should_return_is_stock_ok_as_true_when_table_exists_without_data(self):
            # Given
            is_enriched_offerer_contains_data = MagicMock()
            is_enriched_users_contains_data = MagicMock()
            is_enriched_stocks_contains_data = MagicMock()
            is_enriched_stock_data_exists = MagicMock()
            is_enriched_user_data_exists = MagicMock()
            is_enriched_offerer_data_exists = MagicMock()

            is_enriched_stock_data_exists.return_value = True
            is_enriched_stocks_contains_data.return_value = False

            # When
            status = get_enriched_data_status(
                is_enriched_offerer_data_exists=is_enriched_offerer_data_exists,
                is_enriched_user_data_exists=is_enriched_user_data_exists,
                is_enriched_stock_data_exists=is_enriched_stock_data_exists,
                is_enriched_offerer_contains_data=is_enriched_offerer_contains_data,
                is_enriched_users_contains_data=is_enriched_users_contains_data,
                is_enriched_stocks_contains_data=is_enriched_stocks_contains_data
            )

            # Then
            assert status['is_enriched_stock_datasource_exists'] == True
            assert status['is_stock_ok'] == False
