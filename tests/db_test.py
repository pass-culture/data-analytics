from unittest.mock import patch, MagicMock

import psycopg2
import pytest

from db import connect_to_database


@patch('db.psycopg2.connect')
def test_should_raise_exception_and_close_connection_when_there_is_a_query_error(mocked_connect):
    # Given
    @connect_to_database
    def decorated_function(connection):
        raise psycopg2.Error

    mocked_connection = MagicMock()
    mocked_connect.return_value = mocked_connection

    # When
    with pytest.raises(psycopg2.Error):
        decorated_function()

        # Then
        mocked_connection.close.assert_called_once()


@patch('db.psycopg2.connect')
def test_should_return_decorated_function_value_and_close_connection_when_there_is_not_a_query_error(mocked_connect):
    # Given
    @connect_to_database
    def decorated_function(connection):
        return 'value'

    mocked_connection = MagicMock()
    mocked_connect.return_value = mocked_connection

    # When
    output = decorated_function()

    # Then
    mocked_connection.close.assert_called_once()
    assert output == 'value'