from unittest.mock import patch, MagicMock

from read.connectors.api_sirene_connector import get_offerer_details_by_siren


class GetOffererDetailsBySirenTest:
    @patch("read.connectors.api_sirene_connector.requests.get")
    def test_should_request_correct_endpoint_of_api_sirene(self, request_get):
        # Given
        siren = "123456789"

        # When
        get_offerer_details_by_siren(siren)

        # Then
        request_get.assert_called_once_with(
            f"https://entreprise.data.gouv.fr/api/sirene/v3/unites_legales/123456789"
        )

    @patch("read.connectors.api_sirene_connector.requests.get")
    def test_should_return_api_response_when_status_code_200(self, request_get):
        # Given
        siren = "123456789"
        expected_result = {"toto"}
        response_return_value = MagicMock(status_code=200)
        response_return_value.json = MagicMock(return_value=expected_result)
        request_get.return_value = response_return_value

        # When
        response = get_offerer_details_by_siren(siren)

        # Then
        assert response == {"toto"}

    @patch("read.connectors.api_sirene_connector.requests.get")
    def test_should_return_return_empty_dict_when_status_code_is_not_200(
        self, request_get
    ):
        # Given
        siren = "123456789"
        response_return_value = MagicMock(status_code=400)
        request_get.return_value = response_return_value

        # When
        response = get_offerer_details_by_siren(siren)

        # Then
        assert response == {}
