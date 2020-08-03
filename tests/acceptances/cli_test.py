from unittest.mock import patch

from click.testing import CliRunner
from db import DATABASE_URL
from metabase.cli import cli
from utils.database_cleaners import clean_views


class CreateTest:
    def setup_class(self):
        clean_views()

    def test_exits_with_0_when_called_with_right_db_url(self):
        # Given
        runner = CliRunner()

        # When
        result = runner.invoke(cli, ['create', '-u', DATABASE_URL])

        # Then
        assert result.exit_code == 0

    def test_exits_with_1_when_called_without_db_url(self):
        # Given
        runner = CliRunner()

        # When
        result = runner.invoke(cli, ['create'])

        # Then
        assert result.exit_code == 1
        assert type(result.exception) == AttributeError

    def test_exits_with_1_when_called_without_wrong_db_url(self):
        # Given
        runner = CliRunner()

        # When
        unknown_url = 'postgresql://user:pswd@host:port/db_name'
        result = runner.invoke(cli, ['create', '-u', unknown_url])

        # Then
        assert result.exit_code == 1
        assert type(result.exception) == ValueError

    @patch('metabase.cli.create_enriched_data_views')
    def test_exits_with_1_when_exception_in_command(self, mocked_create_enriched_data_views):
        # Given
        runner = CliRunner()
        mocked_create_enriched_data_views.side_effect = Exception

        # When
        result = runner.invoke(cli, ['create', '-u', DATABASE_URL])

        # Then
        assert result.exit_code == 1
        assert type(result.exception) == Exception

