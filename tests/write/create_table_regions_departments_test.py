import pandas as pd

from db import ENGINE
from utils.database_cleaners import drop_regions_departments_table
from write.create_table_regions_departments import (
    create_table_regions_departments,
)


class CreateTableRegionsDepartmentsTest:
    def teardown_method(self):
        drop_regions_departments_table()

    def test_should_create_table(self):
        # Given
        regions_departments_dataframe = pd.DataFrame()

        # When
        create_table_regions_departments(regions_departments_dataframe, ENGINE)

        # Then
        with ENGINE.connect() as connection:
            query = """SELECT * FROM information_schema.tables WHERE table_name = 'regions_departments';"""
            results = connection.execute(query).fetchall()
        assert len(results) == 1

    def test_should_import_data(self):
        # Given
        data = [
            ["19", "Corrèze", "Nouvelle-Aquitaine"],
            ["75", "Paris", "Île-de-France"],
        ]
        regions_departments_dataframe = pd.DataFrame(
            data=data, columns=["num_dep", "dep_name", "region_name"]
        )

        expected_total_number_of_rows = 2

        # When
        create_table_regions_departments(regions_departments_dataframe, ENGINE)

        # Then

        with ENGINE.connect() as connection:
            query = """SELECT * FROM regions_departments"""
            results = connection.execute(query).fetchall()
        assert len(results) == expected_total_number_of_rows
