import pandas

from utils.humanize_id import humanize


def get_humanized_id_dataframe(id_dataframe: pandas.DataFrame) -> pandas.DataFrame:
    humanized_id_dataframe = id_dataframe.copy()
    humanized_id_dataframe["humanized_id"] = humanized_id_dataframe["id"].apply(
        humanize
    )
    return humanized_id_dataframe


def create_id_dataframe(ENGINE, table_name: str) -> pandas.DataFrame:
    connection = ENGINE.connect()
    query = """
        SELECT id
        FROM {table_name}
        WHERE id is not NULL 
    """.format(
        table_name=table_name
    )

    id_df = pandas.read_sql(query, connection)
    connection.close()
    ENGINE.dispose()
    return id_df
