import pytest

from db import CONNECTION, db
from query_enriched_data_tables import create_enriched_offerer_data, create_enriched_user_data
from repository.health_check_repository import is_enriched_stock_data_exists, is_enriched_offerer_data_exists, \
    is_enriched_user_data_exists
from tests.utils import clean_database
from view_queries import create_enriched_stock_view


class IsEnrichedOffererDataExistsTest:
    @pytest.fixture(autouse=True)
    def setup_class(self, app):
        clean_database(app)

    def test_should_return_false_when_no_table_exists(self, app):
        # When
        result = is_enriched_offerer_data_exists()

        # Then
        assert result is False


    def test_should_return_true_when_table_exists(self, app):
        # Given
        create_enriched_offerer_data(CONNECTION)

        # When
        result = is_enriched_offerer_data_exists()

        # Then
        assert result is True


class IsEnrichedUserDataExistsTest:
    @pytest.fixture(autouse=True)
    def setup_class(self, app):
        clean_database(app)

    def test_should_return_false_when_no_table_exists(self, app):
        # When
        result = is_enriched_user_data_exists()

        # Then
        assert result is False


    def test_should_return_true_when_table_exists(self, app):
        # Given
        create_enriched_user_data(CONNECTION)

        # When
        result = is_enriched_user_data_exists()

        # Then
        assert result is True


class IsEnrichedStockDataExistsTest:
    @pytest.fixture(autouse=True)
    def setup_class(self, app):
        clean_database(app)
        with app.app_context():
            db.session.execute('''
                DROP VIEW IF EXISTS enriched_stock_data;
            ''')
            db.session.commit()

    def test_should_return_false_when_no_table_exists(self, app):
        # When
        result = is_enriched_stock_data_exists()

        # Then
        assert result is False


    def test_should_return_true_when_table_exists(self, app):
        # Given
        with app.app_context():
            create_enriched_stock_view()

        # When
        result = is_enriched_stock_data_exists()

        # Then
        assert result is True
