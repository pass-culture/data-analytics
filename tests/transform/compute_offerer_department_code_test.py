import pandas

from transform.compute_offerer_cultural_activity import get_departement_code
from transform.compute_offerer_department_code import get_offerer_with_departement_code_dataframe

def test_should_return_empty_dataframe_when_given_dataframe_is_empty():
    # Given
    empty_postal_code_dataframe = pandas.DataFrame(columns=["id","postalCode"])
    expected_dataframe = pandas.DataFrame(columns=["id","department_code"])
    
    # When
    result = get_offerer_with_departement_code_dataframe(empty_postal_code_dataframe)
    
    # Then
    pandas.testing.assert_frame_equal(expected_dataframe,result)

def test_should_return_dataframe_with_department_code_column():
    # Given
    postal_code_dataframe = pandas.DataFrame(data={"id": [1, 2], "postalCode": ['75003', '97459']})
    expected_dataframe = pandas.DataFrame(data={"id": [1, 2], "department_code": ['75', '974']})
    
    # When
    result = get_offerer_with_departement_code_dataframe(postal_code_dataframe)
    
    # Then
    pandas.testing.assert_frame_equal(expected_dataframe,result)


def test_get_departement_code_for_mainland_France():
    # given
    postal_code = '75012'

    # when
    departement_code = get_departement_code(postal_code)

    # then
    assert departement_code == '75'


def test_get_departement_code_for_overseas_France():
    # given
    postal_code = '97440'

    # when
    departement_code = get_departement_code(postal_code)

    # then
    assert departement_code == '974'