import pandas

from db import ENGINE
from utils.database_cleaners import drop_offerer_humanized_id_table
from write.create_humanized_id import create_table_humanized_id


class CreateTableOffererWithHumanizedIdTest:
    def teardown_method(self):
        drop_offerer_humanized_id_table()

    def test_should_create_table(self):
        # Given
        humanized_id_dataframe = pandas.DataFrame()

        # When
        create_table_humanized_id(ENGINE, "offerer", humanized_id_dataframe)

        # Then
        with ENGINE.connect() as connection:
            query = """SELECT * FROM information_schema.tables WHERE table_name = 'offerer_humanized_id';"""
            results = connection.execute(query).fetchall()

        assert len(results) == 1
