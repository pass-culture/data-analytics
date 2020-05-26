import pandas
import sqlalchemy

from db import ENGINE
from transform.compute_offerer_department_code import get_offerer_with_departement_code_dataframe, \
    create_postal_code_dataframe


def create_table_offerer_departement_code(department_code_dataframe: pandas.DataFrame) -> None:
    department_code_dataframe.to_sql(
        name='offerer_departement_code',
        con=ENGINE,
        if_exists='replace',
        dtype={
                'id': sqlalchemy.types.BIGINT(),
                'APE_label': sqlalchemy.types.VARCHAR(length=250)
                }
    )


def create_offerer_departement_code_data() -> None:
    postal_code_dataframe = create_postal_code_dataframe()
    department_code_dataframe = get_offerer_with_departement_code_dataframe(postal_code_dataframe)
    create_table_offerer_departement_code(department_code_dataframe)