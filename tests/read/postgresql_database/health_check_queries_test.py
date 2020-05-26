import pytest
from read.postgresql_database.health_check_queries import does_enriched_offerer_data_exists, does_enriched_user_data_exists, does_enriched_stock_data_exists, \
    does_enriched_offerer_contains_data, does_enriched_stocks_contains_data, does_enriched_users_contains_data
from tests.data_creators import clean_database, clean_views, clean_tables, create_offerer, create_user, create_venue, create_product, create_offer, \
    create_stock
from write.create_views import create_enriched_offerer_data, create_enriched_stock_data, create_enriched_user_data


class IsEnrichedOffererDataExistsTest:
    @pytest.fixture(autouse=True)
    def setup_method(self, app):
        yield
        clean_database(app)
        clean_views()
        clean_tables()

    def test_should_return_false_when_no_table_exists(self, app):
        # When
        result = does_enriched_offerer_data_exists() 

        # Then
        assert result is False

    def test_should_return_true_when_table_exists(self, app):
        # Given
        with app.app_context():
            create_enriched_offerer_data()

        # When
        result = does_enriched_offerer_data_exists()

        # Then
        assert result is True


class IsEnrichedUserDataExistsTest:
    @pytest.fixture(autouse=True)
    def setup_class(self, app):
        clean_database(app)

    def test_should_return_false_when_no_table_exists(self, app):
        # When
        result = does_enriched_user_data_exists()

        # Then
        assert result is False


    def test_should_return_true_when_table_exists(self, app):
        # Given
        create_enriched_user_data()

        # When
        result = does_enriched_user_data_exists()

        # Then
        assert result is True


class IsEnrichedStockDataExistsTest:
    @pytest.fixture(autouse=True)
    def setup_class(self, app):
        clean_database(app)
        clean_views()

    def test_should_return_false_when_no_table_exists(self, app):
        # When
        result = does_enriched_stock_data_exists()

        # Then
        assert result is False


    def test_should_return_true_when_table_exists(self, app):
        # Given
        with app.app_context():
            create_enriched_stock_data()

        # When
        result = does_enriched_stock_data_exists()

        # Then
        assert result is True


class IsEnrichedOffererSourceContainsDataTest:
    @pytest.fixture(autouse=True)
    def setup_class(self, app):
        clean_database(app)
        clean_views()

    def test_should_return_false_when_contains_no_data(self, app):
        # When
        result = does_enriched_offerer_contains_data()

        # Then
        assert result is False

    def test_should_return_true_when_table_exists_and_contains_an_offerer(self, app):
        # Given
        create_offerer(app, id=1)
        with app.app_context():
            create_enriched_offerer_data()

        # When
        result = does_enriched_offerer_contains_data()

        # Then
        assert result is True


    def test_should_return_false_when_table_exists_and_contains_no_offerer(self, app):
        # Given
        create_enriched_offerer_data()

        # When
        result = does_enriched_offerer_contains_data()

        # Then
        assert result is False


class IsEnrichedUserSourceContainsDataTest:
    @pytest.fixture(autouse=True)
    def setup_class(self, app):
        clean_database(app)
        clean_views()

    def test_should_return_false_when_contains_no_data(self, app):
        # When
        result = does_enriched_users_contains_data()

        # Then
        assert result is False

    def test_should_return_true_when_table_exists_and_contains_at_least_a_user(self, app):
        # Given
        create_user(app, id=1)
        create_enriched_user_data()

        # When
        result = does_enriched_users_contains_data()

        # Then
        assert result is True

    def test_should_return_false_when_table_exists_and_contains_no_least_a_user(self, app):
        # Given
        create_enriched_user_data()

        # When
        result = does_enriched_users_contains_data()

        # Then
        assert result is False


class IsEnrichedStocksSourceContainsDataTest:
    @pytest.fixture(autouse=True)
    def setup_class(self, app):
        clean_database(app)
        clean_views()


    def test_should_return_false_when_contains_no_data(self, app):
        # When
        result = does_enriched_stocks_contains_data()

        # Then
        assert result is False

    def test_should_return_true_when_table_exists_and_contains_at_least_one_stock(self, app):
        # Given
        create_offerer(app, id=1)
        create_venue(app, offerer_id=1, id=1)
        create_product(app, id=1)
        create_offer(app, venue_id=1, product_id=1, id=1)
        create_stock(app, offer_id=1, date_created='2019-12-01')
        with app.app_context():
            create_enriched_stock_data()

        # When
        result = does_enriched_stocks_contains_data()

        # Then
        assert result is True

    def test_should_return_false_when_table_exists_and_contains_no_data(self, app):
        # Given
        with app.app_context():
            create_enriched_stock_data()

        # When
        result = does_enriched_stocks_contains_data()

        # Then
        assert result is False
