def get_enriched_data_status(is_enriched_offerer_data_exists, is_enriched_user_data_exists, is_enriched_stock_data_exists) -> dict:
    enriched_status = dict()
    enriched_status.update({ 'is_enriched_offerer_datasource_exists': is_enriched_offerer_data_exists() })
    enriched_status.update({ 'is_enriched_user_datasource_exists': is_enriched_user_data_exists() })
    enriched_status.update({ 'is_enriched_stock_datasource_exists': is_enriched_stock_data_exists() })

    return enriched_status
