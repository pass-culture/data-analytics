import pandas
from models.create_offerer_with_department_code import create_offerer_with_departement_code_dataframe, create_table_offerer_departement_code
from tests.repository.utils import clean_database, clean_tables
import pytest
from models.db import db
from tests.repository.utils import clean_database, clean_tables


class CreateOffererWithDepartmentCodeDataframeTest:
    def test_should_return_empty_dataframe_when_given_dataframe_is_empty(self):
        # Given
        empty_postal_code_dataframe = pandas.DataFrame(columns=["id","postalCode"])
        expected_dataframe = pandas.DataFrame(columns=["id","department_code"])
        
        # When
        result = create_offerer_with_departement_code_dataframe(empty_postal_code_dataframe)
        
        # Then
        pandas.testing.assert_frame_equal(expected_dataframe,result)

    def test_should_return_dataframe_with_department_code_column(self):
        # Given
        postal_code_dataframe = pandas.DataFrame(data={"id": [1, 2], "postalCode": ['75003', '97459']})
        expected_dataframe = pandas.DataFrame(data={"id": [1, 2], "department_code": ['75', '974']})
        
        # When
        result = create_offerer_with_departement_code_dataframe(postal_code_dataframe)
        
        # Then
        pandas.testing.assert_frame_equal(expected_dataframe,result)
        

class CreateTableOffererWithDepartmentCodeTest:
    @pytest.fixture(autouse=True)
    def setup_method(self, app):
        yield
        clean_database(app)
        clean_tables()

    def test_should_create_table(self, app):
        # Given
        department_code_dataframe = pandas.DataFrame()

        # When
        with app.app_context():
            create_table_offerer_departement_code(department_code_dataframe)

        # Then
        query = '''SELECT * FROM information_schema.tables WHERE table_name = 'offerer_departement_code';'''
        results = db.session.execute(query).fetchall()
        assert len(results) == 1
