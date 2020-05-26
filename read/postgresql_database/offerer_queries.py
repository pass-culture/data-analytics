import pandas
from pandas import DataFrame
from db import CONNECTION


def get_siren_dataframe() -> DataFrame:
    query = '''
    SELECT
        id
        ,siren
    FROM offerer
    WHERE siren IS NOT NULL 
    '''
    siren_df = pandas.read_sql(query, CONNECTION)
    return siren_df


def get_postal_code_dataframe() -> DataFrame:
    query = '''
    SELECT
        id
        ,"postalCode"
    FROM offerer
    WHERE "postalCode" is not NULL 
    '''
    postalcode_df = pandas.read_sql(query, CONNECTION)
    return postalcode_df
