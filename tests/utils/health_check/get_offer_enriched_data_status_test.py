from unittest.mock import MagicMock

from utils.health_check.get_offer_enriched_data_status import get_offer_enriched_data_status


class GetEnrichedDataStatusTest:
    class StockStatusTest:
        def test_should_return_a_dict_with_offer_table_status(self):
            # Given
            is_enriched_offers_contains_data = MagicMock(return_value=False)
            is_enriched_offer_present = MagicMock(return_value=False)
            is_enriched_offer_present.return_value = False

            # When
            status = get_offer_enriched_data_status(
                is_enriched_offer_present=is_enriched_offer_present,
                is_enriched_offers_contains_data=is_enriched_offers_contains_data,
            )

            # Then
            assert status['is_enriched_offer_datasource_exists']  == False

        def test_should_return_is_offer_ok_as_true_when_table_exists_with_data(self):
            # Given
            is_enriched_offers_contains_data = MagicMock(return_value=False)
            is_enriched_offer_present = MagicMock(return_value=False)

            is_enriched_offer_present.return_value = True
            is_enriched_offers_contains_data.return_value = True

            # When
            status = get_offer_enriched_data_status(
                is_enriched_offer_present=is_enriched_offer_present,
                is_enriched_offers_contains_data=is_enriched_offers_contains_data,
            )

            # Then
            assert status['is_enriched_offer_datasource_exists'] == True
            assert status['is_offer_ok'] == True

        def test_should_return_is_offer_ok_as_false_when_table_exists_without_data(self):
            # Given
            is_enriched_offers_contains_data = MagicMock(return_value=False)
            is_enriched_offer_present = MagicMock(return_value=False)

            is_enriched_offer_present.return_value = True
            is_enriched_offers_contains_data.return_value = False

            # When
            status = get_offer_enriched_data_status(
                is_enriched_offer_present=is_enriched_offer_present,
                is_enriched_offers_contains_data=is_enriched_offers_contains_data,
            )

            # Then
            assert status['is_enriched_offer_datasource_exists'] == True
            assert status['is_offer_ok'] == False
