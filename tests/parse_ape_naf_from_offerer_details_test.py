from parse_ape_naf_from_offerer_details import parse_ape_naf_from_offerer_details


class ParseApeNafFromOffererDetailsTest:
    def test_if_empty_offerer_details_return_empty_string(self):
        # Given
        offerer_details = {}

        # When
        results = parse_ape_naf_from_offerer_details(offerer_details)

        # Then
        assert results == ''