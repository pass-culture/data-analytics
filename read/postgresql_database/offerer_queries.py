from typing import Optional

import pandas
from pandas import DataFrame


def get_siren_dataframe(ENGINE) -> Optional[DataFrame]:
    query = '''
    SELECT
        id
        ,siren
    FROM offerer
    WHERE siren IS NOT NULL 
    '''

    with ENGINE.connect() as connection:
        siren_df = pandas.read_sql(query, connection)
    return siren_df


def get_postal_code_dataframe(ENGINE) -> DataFrame:
    query = '''
    SELECT
        id
        ,"postalCode"
    FROM offerer
    WHERE "postalCode" is not NULL 
    '''

    with ENGINE.connect() as connection:
        postalcode_df = pandas.read_sql(query, connection)
    return postalcode_df
