import pandas
import pytest

from db import db
from tests.data_creators import clean_database, clean_tables
from write.offerer_view.create_departement_code import create_table_offerer_departement_code


class CreateTableOffererWithDepartmentCodeTest:
    def teardown_method(self):
        clean_database()
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
