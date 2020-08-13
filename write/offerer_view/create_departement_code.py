import pandas
import sqlalchemy

from transform.compute_offerer_department_code import get_offerer_with_departement_code_dataframe, \
    create_postal_code_dataframe


def create_table_offerer_departement_code(department_code_dataframe: pandas.DataFrame, ENGINE) -> None:
    with ENGINE.connect() as connection:
        department_code_dataframe.to_sql(
            name='offerer_departement_code',
            con=connection,
            if_exists='replace',
            dtype={
                    'id': sqlalchemy.types.BIGINT(),
                    'APE_label': sqlalchemy.types.VARCHAR(length=250)
                    }
        )


def create_offerer_departement_code_data(ENGINE) -> None:
    postal_code_dataframe = create_postal_code_dataframe(ENGINE)
    department_code_dataframe = get_offerer_with_departement_code_dataframe(postal_code_dataframe)
    create_table_offerer_departement_code(department_code_dataframe, ENGINE)
