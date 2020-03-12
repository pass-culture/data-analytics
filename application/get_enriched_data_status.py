def get_enriched_data_status(
        is_enriched_offerer_data_exists,
        is_enriched_user_data_exists,
        is_enriched_stock_data_exists,
        is_enriched_offerer_contains_data,
        is_enriched_users_contains_data,
        is_enriched_stocks_contains_data) -> dict:
    enriched_status = dict()
    enriched_status.update({
        'is_enriched_offerer_datasource_exists': is_enriched_offerer_data_exists(),
        'is_enriched_user_datasource_exists': is_enriched_user_data_exists(),
        'is_enriched_stock_datasource_exists': is_enriched_stock_data_exists(),
        'is_user_ok': is_enriched_user_data_exists() and is_enriched_users_contains_data(),
        'is_offerer_ok': is_enriched_offerer_data_exists() and is_enriched_offerer_contains_data(),
        'is_stock_ok': is_enriched_stock_data_exists() and is_enriched_stocks_contains_data()
    })

    return enriched_status
