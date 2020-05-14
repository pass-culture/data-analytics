import pandas
from use_case.get_department_code_from_postal_code import get_departement_code


def create_offerer_with_departement_code_dataframe(postal_code_dataframe: pandas.DataFrame) -> pandas.DataFrame:
    department_code_dataframe = postal_code_dataframe.copy()
    department_code_dataframe['department_code'] = department_code_dataframe['postalCode'].apply(get_departement_code)
    return department_code_dataframe.drop('postalCode', axis=1)
