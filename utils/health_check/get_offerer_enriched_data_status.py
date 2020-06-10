from read.postgresql_database.health_check_queries import HealthCheckSession


def get_offerer_enriched_data_status(
        is_enriched_offerer_data_exists,
        is_enriched_offerer_contains_data) -> dict:
    enriched_status = dict()
    enriched_status.update({
        'is_enriched_offerer_datasource_exists': is_enriched_offerer_data_exists(HealthCheckSession),
        'is_offerer_ok': is_enriched_offerer_data_exists(HealthCheckSession) and is_enriched_offerer_contains_data(HealthCheckSession),
    })

    return enriched_status
