from app import app
from utils.database_cleaners import clean_database, clean_views

app.testing = True


class HealthCheckTest:
    def setup_method(self):
        clean_database()
        clean_views()

    def test_health_check_on_offerer(self):
        with app.test_client() as client:
            # When
            response = client.get("/health/offerer")

            # Then
            assert response.status_code == 200
            assert response.json == {
                "is_enriched_offerer_datasource_exists": False,
                "is_offerer_ok": False,
            }

    def test_health_check_on_user(self):
        with app.test_client() as client:
            # When
            response = client.get("/health/user")

            # Then
            assert response.status_code == 200
            assert response.json == {
                "is_enriched_user_datasource_exists": False,
                "is_user_ok": False,
            }

    def test_health_check_on_stock(self):
        with app.test_client() as client:
            # When
            response = client.get("/health/stock")

            # Then
            assert response.status_code == 200
            assert response.json == {
                "is_enriched_stock_datasource_exists": False,
                "is_stock_ok": False,
            }

    def test_health_check_on_offer(self):
        with app.test_client() as client:
            # When
            response = client.get("/health/offer")

            # Then
            assert response.status_code == 200
            assert response.json == {
                "is_enriched_offer_datasource_exists": False,
                "is_offer_ok": False,
            }


class PingTest:
    def test_should_return_200(self):
        with app.test_client() as client:
            # When
            response = client.get("/")

            # Then
            assert response.status_code == 200
