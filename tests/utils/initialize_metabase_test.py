from unittest.mock import call, patch, MagicMock


from utils.initialize_metabase import get_setup_token, initialize_metabase_if_local, post_create_metabase_superuser


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
}

class InitializeMetabaseIfLocalTest:
    @patch('utils.initialize_metabase.METABASE_URL', 'metabase.example.com')
    @patch('utils.initialize_metabase.requests.get')
    def test_get_setup_token_should_return_token_setup(self, mock_request):
        # Given
        request_json ={'setup_token': "my-setup-token"}
        response_return_value = MagicMock(status_code=200)
        response_return_value.json = MagicMock(return_value=request_json)
        mock_request.return_value = response_return_value

        # When
        result = get_setup_token()

        # Then
        assert result == "my-setup-token"
        calls = [call('metabase.example.com/setup'), call('metabase.example.com/api/session/properties')]
        mock_request.assert_has_calls(calls)


    @patch.dict('os.environ', ENV_VAR)
    @patch('utils.initialize_metabase.METABASE_URL', 'metabase.example.com')
    @patch('utils.initialize_metabase.requests.post')
    def test_post_create_metabase_superuser_should_call_metabase_api(self, mock_request):
        # Given / When
        result = post_create_metabase_superuser('setup_token')

        # Then
        mock_request.assert_called_once_with('metabase.example.com/api/setup',
            json={
                'token': 'setup_token',
                'prefs': {
                    'site_name': 'pc',
                    'allow_tracking': 'true'},
                'database': {
                    'engine': 'postgres',
                    'name': 'Produit',
                    'details': {
                        'host': 'db_blue.postgresql.example.com',
                        'port': '12345',
                        'dbname': 'db_blue',
                        'user': 'db_user',
                        'password': 'password_blue',
                        'ssl': False,
                        'additional-options': None,
                        'tunnel-enabled': False},
                    'auto_run_queries': True,
                    'is_full_sync': True,
                    'schedules': {
                        'cache_field_values': {
                        'schedule_day': None,
                            'schedule_frame': None,
                            'schedule_hour': 0,
                            'schedule_type': 'daily'},
                        'metadata_sync': {
                            'schedule_day': None,
                            'schedule_frame': None,
                            'schedule_hour': None,
                            'schedule_type': 'hourly'}}},
                'user': {
                    'first_name': 'pc',
                    'last_name': 'admin',
                    'email': 'admin.metabase@example.com',
                    'password': 'password',
                    'site_name': 'pc'}
                }
            )

    @patch('utils.initialize_metabase.post_create_metabase_superuser')
    @patch('utils.initialize_metabase.get_setup_token')
    def test_initialize_metabase_if_local_should_call_setup_functions(self, mock_get_setup_token, mock_post_create_metabase_superuser):
        # Given
        setup_token = 'my_fancy_setup_token'
        mock_get_setup_token.return_value = setup_token

        #  When
        initialize_metabase_if_local()

        # Then
        mock_get_setup_token.assert_called_once()
        mock_post_create_metabase_superuser.assert_called_once_with(setup_token)

