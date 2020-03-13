def get_user_enriched_data_status(
        is_enriched_user_data_exists,
        is_enriched_users_contains_data) -> dict:
    enriched_status = dict()
    enriched_status.update({
        'is_enriched_user_datasource_exists': is_enriched_user_data_exists(),
        'is_user_ok': is_enriched_user_data_exists() and is_enriched_users_contains_data(),
    })

    return enriched_status
