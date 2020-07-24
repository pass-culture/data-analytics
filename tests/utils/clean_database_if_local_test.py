from unittest.mock import patch

from utils.clean_database_if_local import clean_database_if_local

class CleanDatabaseIfLocalTest:
    @patch('utils.clean_database_if_local.clean_database')
    @patch('utils.clean_database_if_local.clean_views')
    @patch('utils.clean_database_if_local.LOCAL_DATABASE_URL', 'postgresql://pass_culture:passq@localhost:5435/pass_culture')
    @patch('utils.clean_database_if_local.DATABASE_URL', 'postgresql://pass_culture:passq@remote.host:5435/pass_culture')
    def test_should_not_clean_database_when_url_is_remote(self, mock_clean_views, mock_clean_database):
        # Given / When
        clean_database_if_local()

        # Then
        mock_clean_views.assert_not_called()
        mock_clean_database.assert_not_called()


    @patch('utils.clean_database_if_local.clean_database')
    @patch('utils.clean_database_if_local.clean_views')
    @patch('utils.clean_database_if_local.LOCAL_DATABASE_URL', 'postgresql://pass_culture:passq@localhost:5435/pass_culture')
    @patch('utils.clean_database_if_local.DATABASE_URL', 'postgresql://pass_culture:passq@localhost:5435/pass_culture')
    def test_should_clean_database_when_url_is_local(self, mock_clean_views, mock_clean_database):
        # Given / When
        clean_database_if_local()

        # Then
        mock_clean_views.assert_called_once()
        mock_clean_database.assert_called_once()

