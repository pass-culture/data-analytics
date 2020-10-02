import pandas

from transform.compute_humanized_id import get_humanized_id_dataframe


class GetHumanizedIdDataframe:
    def test_should_return_dataframe_with_humanized_id_column(self):
        # Given
        input_dataframe = pandas.DataFrame(data={"id": [123, 456]})
        expected_dataframe = pandas.DataFrame(
            data={"id": [123, 456], "humanized_id": ["PM", "AHEA"]}
        )

        # When
        result = get_humanized_id_dataframe(input_dataframe)

        # Then
        pandas.testing.assert_frame_equal(expected_dataframe, result)
