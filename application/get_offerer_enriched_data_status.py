def get_offerer_enriched_data_status(
        is_enriched_offerer_data_exists,
        is_enriched_offerer_contains_data) -> dict:
    enriched_status = dict()
    enriched_status.update({
        'is_enriched_offerer_datasource_exists': is_enriched_offerer_data_exists(),
        'is_offerer_ok': is_enriched_offerer_data_exists() and is_enriched_offerer_contains_data(),
    })

    return enriched_status
