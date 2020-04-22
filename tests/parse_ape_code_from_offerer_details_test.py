from parse_ape_code_from_offerer_details  import parse_ape_code_from_offerer_details


class ParseApeNafFromOffererDetailsTest:
    def test_if_empty_offerer_details_return_empty_string(self):
        # Given
        offerer_details = {}

        # When
        results = parse_ape_code_from_offerer_details(offerer_details)

        # Then
        assert results == ''

    def test_should_parse_api_result(self):
        # Given
        offerer_details = {
            "unite_legale": {
                "id": 20493806,
                "siren": "853318459",
                "activite_principale": "70.21Z",
                "etablissement_siege": {
                    "id": 48863654,
                    "siren": "853318459",
                    "siret": "85331845900015",
                    "activite_principale": "70.21ZX"
                },
                "etablissements": [
                    {
                        "id": 48863654,
                        "siren": "853318459",
                        "siret": "85331845900015",
                        "activite_principale": "70.21ZZZ"
                    }
                ]
            }
        }

        # When
        results = parse_ape_code_from_offerer_details(offerer_details)

        # Then
        assert results == "7021Z"

    def test_should_return_empty_when_APE_is_not_given(self):
        # Given
        offerer_details = {
            "unite_legale": {
                "id": 20493806,
                "siren": "853318459",
                "etablissement_siege": {
                    "id": 48863654,
                    "siren": "853318459",
                    "siret": "85331845900015",
                    "activite_principale": "70.21ZX"
                },
                "etablissements": [
                    {
                        "id": 48863654,
                        "siren": "853318459",
                        "siret": "85331845900015",
                        "activite_principale": "70.21ZZZ"
                    }
                ]
            }
        }

        # When
        results = parse_ape_code_from_offerer_details(offerer_details)

        # Then
        assert results == ""
