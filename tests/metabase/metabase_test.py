from unittest.mock import patch, MagicMock

from metabase.commands import configure_new_metabase_session, get_connected_database_id, get_connected_database_host_name, \
     get_db_details_by_app_name, get_app_name_for_restore, switch_metabase_database_connection, clean_database_if_local


ENV_VAR = {
    "METABASE_USER_NAME": "admin.metabase@example.com",
    "METABASE_PASSWORD": "password",
    "METABASE_DBNAME": "table_name",
    "BLUE_DB_INFO": """{
        "app_name": "app_name_blue",
        "details": {
            "port": "12345",
            "host": "db_blue.postgresql.example.com",
            "dbname": "db_blue",
            "user": "db_user",
            "password": "password_blue"
        }
    }""",
    "GREEN_DB_INFO": """{
        "app_name": "app_name_green",
        "details": {
            "port": "12346",
            "host": "db_green.postgresql.example.com",
            "dbname": "db_green",
            "user": "db_user",
            "password": "password_green"
        }
    }"""
}

class ConfigureNewMetabaseSessionTest:
    @patch('metabase.commands.METABASE_URL', 'metabase.example.com')
    @patch('metabase.commands.requests.post')
    def test_should_return_session_id_from_api_session(self, mock_request):
        # Given
        user_name = 'admin.metabase@example.com'
        password = 'password'
        request_json = {"id": "session_id"}
        response_return_value = MagicMock(status_code=200, text='')
        response_return_value.json = MagicMock(return_value=request_json)
        mock_request.return_value = response_return_value

        # When
        result = configure_new_metabase_session(user_name, password)

        # Then
        assert result == 'session_id'
        mock_request.assert_called_once_with('metabase.example.com/api/session/',
         json={'username': 'admin.metabase@example.com',
        'password': 'password'})


class GetDumpTableIdTest:
    @patch('metabase.commands.METABASE_URL', 'metabase.example.com')
    @patch('metabase.commands.requests.get')
    def test_should_return_table_id_in_metabase_from_api_database(self, mock_request):
        # Given
        session_id = 42
        request_json = [{'name': 'table_name', 'id': 'table_id_in_metabase'}]
        response_return_value = MagicMock(status_code=200, text='')
        response_return_value.json = MagicMock(return_value=request_json)
        mock_request.return_value = response_return_value

        # When
        result = get_connected_database_id('table_name', session_id)

        # Then
        assert result == 'table_id_in_metabase'
        mock_request.assert_called_once_with('metabase.example.com/api/database/',
         headers={'cookie': 'metabase.SESSION=42'})


class GetDumpTableInformationTest:
    @patch('metabase.commands.METABASE_URL', 'metabase.example.com')
    @patch('metabase.commands.get_connected_database_id')
    @patch('metabase.commands.requests.get')
    def test_should_return_host_from_api_database_table_id_in_metabase(self, mock_request, mock_get_connected_database_id):
        # Given
        session_id = 42
        request_json = {
            'name': 'table_name',
            'id': 'table_id_in_metabase',
            'details': {'port': '12345',
                        'host': 'db_name.postgresql.example.com',
                        'dbname': 'db_name',
                        'user': 'db_user',
                        'password': 'password',
                        'ssl': True},
        }
        response_return_value = MagicMock(status_code=200, text='')
        response_return_value.json = MagicMock(return_value=request_json)
        mock_request.return_value = response_return_value
        mock_get_connected_database_id.return_value = 'table_id_in_metabase'

        # When
        result = get_connected_database_host_name('table_name', session_id)

        # Then
        assert result == 'db_name.postgresql.example.com'
        mock_request.assert_called_once_with('metabase.example.com/api/database/table_id_in_metabase',
         headers={'cookie': 'metabase.SESSION=42'})


class GetDbDetailsByAppNameTest:
    @patch.dict('os.environ', ENV_VAR)
    def test_should_return_blue_db_details_when_app_name_match(self):
        # Given / When
        result = get_db_details_by_app_name('app_name_blue')

        # Then
        assert result == {
            "port": "12345",
            "host": "db_blue.postgresql.example.com",
            "dbname": "db_blue",
            "user": "db_user",
            "password": "password_blue"
        }


    @patch.dict('os.environ', ENV_VAR)
    def test_should_return_green_db_details_when_app_name_does_not_match(self):
        # Given / When
        result = get_db_details_by_app_name('other_app_name')

        # Then
        assert result == {
            "port": "12346",
            "host": "db_green.postgresql.example.com",
            "dbname": "db_green",
            "user": "db_user",
            "password": "password_green"
        }


class GetAppNameForRestoreTest:
    @patch.dict('os.environ', ENV_VAR)
    @patch('metabase.commands.get_connected_database_host_name')
    @patch('metabase.commands.configure_new_metabase_session')
    def test_should_return_green_app_name_when_blue_db_is_on_metabase(self, mock_configure_new_metabase_session, mock_get_connected_database_host_name):
        # Given
        mock_configure_new_metabase_session.return_value = 'session_id'
        mock_get_connected_database_host_name.return_value = 'db_blue.postgresql.example.com'

        # When
        result = get_app_name_for_restore()

        # Then
        assert result == 'app_name_green'


    @patch.dict('os.environ', ENV_VAR)
    @patch('metabase.commands.get_connected_database_host_name')
    @patch('metabase.commands.configure_new_metabase_session')
    def test_should_return_blue_app_name_when_green_db_is_on_metabase(self, mock_configure_new_metabase_session, mock_get_connected_database_host_name):
        # Given
        mock_configure_new_metabase_session.return_value = 'session_id'
        mock_get_connected_database_host_name.return_value = 'db_green.postgresql.example.com'

        # When
        result = get_app_name_for_restore()

        # Then
        assert result == 'app_name_blue'


class SwitchMetabaseDatabaseConnectionTest:
    @patch.dict('os.environ', ENV_VAR)
    @patch('metabase.commands.METABASE_URL', 'metabase.example.com')
    @patch('metabase.commands.requests.put')
    @patch('metabase.commands.get_db_details_by_app_name')
    @patch('metabase.commands.get_app_name_for_restore')
    @patch('metabase.commands.get_connected_database_id')
    @patch('metabase.commands.configure_new_metabase_session')
    @patch('metabase.commands.LOCAL_DATABASE_URL', 'postgresql://pass_culture:passq@localhost:5435/pass_culture')
    @patch('metabase.commands.DATABASE_URL', 'postgresql://pass_culture:passq@remote.host:5435/pass_culture')
    def test_should_switch_connection_to_dump_table(self, mock_configure_new_metabase_session,
                                                          mock_get_connected_database_id, mock_get_app_name_for_restore,
                                                          mock_get_db_details_by_app_name, mock_request):
        # Given
        table_name = 'table_name'
        user_name = 'user_name'
        password = 'password'
        mock_configure_new_metabase_session.return_value = 'session_id'
        mock_get_connected_database_id.return_value = 'table_id'
        mock_get_app_name_for_restore.return_value = 'app_name_blue'
        mock_get_db_details_by_app_name.return_value = {
            "port": "12345",
            "host": "db_blue.postgresql.example.com",
            "dbname": "db_blue",
            "user": "db_user",
            "password": "password_blue"
        }

        # When
        switch_metabase_database_connection(table_name, user_name, password)

        # Then
        mock_request.assert_called_once_with('metabase.example.com/api/database/table_id',
                                             headers={'cookie': 'metabase.SESSION=session_id'},
                                             json={'details': {'port': '12345',
                                                               'host': 'db_blue.postgresql.example.com',
                                                               'dbname': 'db_blue',
                                                               'user': 'db_user',
                                                               'password': 'password_blue',
                                                               'ssl': True},
                                                   'name': 'table_name', 'engine': 'postgres'})

    @patch.dict('os.environ', ENV_VAR)
    @patch('metabase.commands.METABASE_URL', 'metabase.example.com')
    @patch('metabase.commands.requests.put')
    @patch('metabase.commands.get_db_details_by_app_name')
    @patch('metabase.commands.get_app_name_for_restore')
    @patch('metabase.commands.get_connected_database_id')
    @patch('metabase.commands.configure_new_metabase_session')
    @patch('metabase.commands.LOCAL_DATABASE_URL', 'postgresql://pass_culture:passq@localhost:5435/pass_culture')
    @patch('metabase.commands.DATABASE_URL', 'postgresql://pass_culture:passq@localhost:5435/pass_culture')
    def test_should_not_use_ssl_connection_when_url_is_local(self, mock_configure_new_metabase_session, mock_get_connected_database_id, mock_get_app_name_for_restore, mock_get_db_details_by_app_name, mock_request):
        # Given
        table_name = 'table_name'
        user_name = 'user_name'
        password = 'password'
        mock_configure_new_metabase_session.return_value = 'session_id'
        mock_get_connected_database_id.return_value = 'table_id'
        mock_get_app_name_for_restore.return_value = 'app_name_blue'
        mock_get_db_details_by_app_name.return_value = {
            "port": "12345",
            "host": "db_blue.postgresql.example.com",
            "dbname": "db_blue",
            "user": "db_user",
            "password": "password_blue"
        }

        # When
        switch_metabase_database_connection(table_name, user_name, password)

        # Then
        mock_request.assert_called_once_with('metabase.example.com/api/database/table_id',
         headers={'cookie': 'metabase.SESSION=session_id'},
         json={'details': {'port': '12345',
         'host': 'db_blue.postgresql.example.com',
         'dbname': 'db_blue',
         'user': 'db_user',
         'password': 'password_blue',
         'ssl': False},
         'name': 'table_name', 'engine': 'postgres'})


class CleanDatabaseIfLocalTest:
    @patch('metabase.commands.clean_database')
    @patch('metabase.commands.clean_views')
    @patch('metabase.commands.LOCAL_DATABASE_URL', 'postgresql://pass_culture:passq@localhost:5435/pass_culture')
    @patch('metabase.commands.DATABASE_URL', 'postgresql://pass_culture:passq@remote.host:5435/pass_culture')
    def test_should_not_clean_database_when_url_is_remote(self, mock_clean_views, mock_clean_database):
        # Given / When
        clean_database_if_local()

        # Then
        mock_clean_views.assert_not_called()
        mock_clean_database.assert_not_called()


    @patch('metabase.commands.clean_database')
    @patch('metabase.commands.clean_views')
    @patch('metabase.commands.LOCAL_DATABASE_URL', 'postgresql://pass_culture:passq@localhost:5435/pass_culture')
    @patch('metabase.commands.DATABASE_URL', 'postgresql://pass_culture:passq@localhost:5435/pass_culture')
    def test_should_clean_database_when_url_is_local(self, mock_clean_views, mock_clean_database):
        # Given / When
        clean_database_if_local()

        # Then
        mock_clean_views.assert_called_once()
        mock_clean_database.assert_called_once()
