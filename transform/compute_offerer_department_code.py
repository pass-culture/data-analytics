import pandas

from transform.compute_offerer_cultural_activity import get_departement_code


def get_offerer_with_departement_code_dataframe(postal_code_dataframe: pandas.DataFrame) -> pandas.DataFrame:
    department_code_dataframe = postal_code_dataframe.copy()
    department_code_dataframe['department_code'] = department_code_dataframe['postalCode'].apply(get_departement_code)
    return department_code_dataframe.drop('postalCode', axis=1)


def create_postal_code_dataframe(ENGINE) -> pandas.DataFrame:
    connection = ENGINE.connect()
    query = '''
    SELECT
        id
        ,"postalCode"
    FROM offerer
    WHERE "postalCode" is not NULL 
    '''
    postalcode_df = pandas.read_sql(query, connection)
    connection.close()
    ENGINE.dispose()
    return postalcode_df
