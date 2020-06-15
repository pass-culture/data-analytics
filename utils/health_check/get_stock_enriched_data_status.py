from read.postgresql_database.health_check_queries import HealthCheckSession


def get_stock_enriched_data_status(
        is_enriched_stock_data_exists,
        is_enriched_stocks_contains_data) -> dict:
    enriched_status = dict()
    enriched_status.update({
        'is_enriched_stock_datasource_exists': is_enriched_stock_data_exists(HealthCheckSession),
        'is_stock_ok': is_enriched_stock_data_exists(HealthCheckSession) and is_enriched_stocks_contains_data(
            HealthCheckSession)
    })

    return enriched_status
