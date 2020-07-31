import pandas

from db import ENGINE
from utils.database_cleaners import clean_database, drop_offerer_cultural_activity_table
from write.offerer_view.create_departement_code import create_table_offerer_departement_code


class CreateTableOffererWithDepartmentCodeTest:
    def teardown_method(self):
        drop_offerer_cultural_activity_table()

    def test_should_create_table(self):
        # Given
        department_code_dataframe = pandas.DataFrame()

        # When
        create_table_offerer_departement_code(department_code_dataframe, ENGINE)

        # Then
        with ENGINE.connect() as connection:
            query = '''SELECT * FROM information_schema.tables WHERE table_name = 'offerer_departement_code';'''
            results = connection.execute(query).fetchall()
        assert len(results) == 1
