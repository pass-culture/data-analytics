import pandas
from models.create_offerer_with_department_code_dataframe import create_offerer_with_departement_code_dataframe

def test_should_return_empty_dataframe_when_given_dataframe_is_empty():
    # Given
    empty_postal_code_dataframe = pandas.DataFrame(columns=["id","postalCode"])
    expected_dataframe = pandas.DataFrame(columns=["id","department_code"])
    
    # When
    result = create_offerer_with_departement_code_dataframe(empty_postal_code_dataframe)
    
    # Then
    pandas.testing.assert_frame_equal(expected_dataframe,result)

def test_should_return_dataframe_with_department_code_column():
    # Given
    postal_code_dataframe = pandas.DataFrame(data={"id": [1, 2], "postalCode": ['75003', '97459']})
    expected_dataframe = pandas.DataFrame(data={"id": [1, 2], "department_code": ['75', '974']})
    
    # When
    result = create_offerer_with_departement_code_dataframe(postal_code_dataframe)
    
    # Then
    pandas.testing.assert_frame_equal(expected_dataframe,result)
    