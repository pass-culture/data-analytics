import pandas
import sqlalchemy

from transform.compute_humanized_id import (
    create_id_dataframe,
    get_humanized_id_dataframe,
)


def create_table_humanized_id(
    ENGINE, table_name: str, humanized_id_dataframe: pandas.DataFrame
) -> None:
    with ENGINE.connect() as connection:
        humanized_id_dataframe.to_sql(
            name="user_humanized_id"
            if table_name == '"user"'
            else "{}_humanized_id".format(table_name),
            con=connection,
            if_exists="replace",
            dtype={
                "id": sqlalchemy.types.BIGINT(),
                "humanized_id": sqlalchemy.types.VARCHAR(length=250),
            },
        )


def create_humanized_id_data(ENGINE, table_name: str) -> None:
    id_dataframe = create_id_dataframe(ENGINE, table_name)
    humanized_id_dataframe = get_humanized_id_dataframe(id_dataframe)
    create_table_humanized_id(ENGINE, table_name, humanized_id_dataframe)
