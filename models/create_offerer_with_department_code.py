import pandas
from use_case.get_department_code_from_postal_code import get_departement_code
import sqlalchemy 
from models.db import ENGINE


def create_offerer_with_departement_code_dataframe(postal_code_dataframe: pandas.DataFrame) -> pandas.DataFrame:
    department_code_dataframe = postal_code_dataframe.copy()
    department_code_dataframe['department_code'] = department_code_dataframe['postalCode'].apply(get_departement_code)
    return department_code_dataframe.drop('postalCode', axis=1)

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
    department_code_dataframe = create_offerer_with_departement_code_dataframe(postal_code_dataframe)
    create_table_offerer_departement_code(department_code_dataframe)
