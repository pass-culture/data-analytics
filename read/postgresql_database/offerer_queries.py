import pandas
from pandas import DataFrame

from db import ENGINE


def get_siren_dataframe() -> DataFrame:
    connection = ENGINE.connect()
    query = '''
    SELECT
        id
        ,siren
    FROM offerer
    WHERE siren IS NOT NULL 
    '''
    siren_df = pandas.read_sql(query, connection)
    connection.close()
    ENGINE.dispose()
    return siren_df


def get_postal_code_dataframe() -> DataFrame:
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
