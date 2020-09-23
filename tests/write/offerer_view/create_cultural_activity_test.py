import pandas

from db import ENGINE
from utils.database_cleaners import drop_offerer_cultural_activity_table
from write.offerer_view.create_cultural_activity import (
    _create_table_offerer_cultural_activity,
)


class CreateTableOffererCulturalActivityTest:
    def teardown_method(self):
        drop_offerer_cultural_activity_table()

    def test_should_create_table(self, app):
        # Given
        offerer_cultural_activity_dataframe = pandas.DataFrame()

        # When
        _create_table_offerer_cultural_activity(
            offerer_cultural_activity_dataframe, ENGINE
        )

        # Then
        with ENGINE.connect() as connection:
            query = """SELECT * FROM information_schema.tables WHERE table_name = 'offerer_cultural_activity';"""
            results = connection.execute(query).fetchall()
        assert len(results) == 1
