from unittest.mock import MagicMock

from application.get_user_enriched_data_status import get_user_enriched_data_status


class GetEnrichedDataStatusTest:
    class UserStatusTest:
        def test_should_return_a_dict_with_user_table_status(self):
            # Given
            is_enriched_users_contains_data = MagicMock(return_value=False)
            is_enriched_user_data_exists = MagicMock(return_value=False)
            is_enriched_user_data_exists.return_value = False

            # When
            status = get_user_enriched_data_status(
                is_enriched_user_data_exists=is_enriched_user_data_exists,
                is_enriched_users_contains_data=is_enriched_users_contains_data,
            )

            # Then
            assert status['is_enriched_user_datasource_exists']  == False

        def test_should_return_is_user_ok_as_true_when_table_exists_with_data(self):
            # Given
            is_enriched_users_contains_data = MagicMock(return_value=False)
            is_enriched_user_data_exists = MagicMock(return_value=False)

            is_enriched_user_data_exists.return_value = True
            is_enriched_users_contains_data.return_value = True

            # When
            status = get_user_enriched_data_status(
                is_enriched_user_data_exists=is_enriched_user_data_exists,
                is_enriched_users_contains_data=is_enriched_users_contains_data,
            )

            # Then
            assert status['is_enriched_user_datasource_exists'] == True
            assert status['is_user_ok'] == True

        def test_should_return_is_user_ok_as_false_when_table_exists_without_data(self):
            # Given
            is_enriched_users_contains_data = MagicMock(return_value=False)
            is_enriched_user_data_exists = MagicMock(return_value=False)

            is_enriched_user_data_exists.return_value = True
            is_enriched_users_contains_data.return_value = False

            # When
            status = get_user_enriched_data_status(
                is_enriched_user_data_exists=is_enriched_user_data_exists,
                is_enriched_users_contains_data=is_enriched_users_contains_data,
            )

            # Then
            assert status['is_enriched_user_datasource_exists'] == True
            assert status['is_user_ok'] == False
