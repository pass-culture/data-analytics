def get_stock_enriched_data_status(
        is_enriched_stock_data_exists,
        is_enriched_stocks_contains_data) -> dict:
    enriched_status = dict()
    enriched_status.update({
        'is_enriched_stock_datasource_exists': is_enriched_stock_data_exists(),
        'is_stock_ok': is_enriched_stock_data_exists() and is_enriched_stocks_contains_data()
    })

    return enriched_status
