from typing import Optional

import pandas
from pandas import DataFrame

from db import ENGINE, connect_to_database


@connect_to_database
def get_siren_dataframe(connection) -> Optional[DataFrame]:
    query = '''
    SELECT
        id
        ,siren
    FROM offerer
    WHERE siren IS NOT NULL 
    '''
    siren_df = pandas.read_sql(query, connection)
    return siren_df


@connect_to_database
def get_postal_code_dataframe(connection) -> DataFrame:
    query = '''
    SELECT
        id
        ,"postalCode"
    FROM offerer
    WHERE "postalCode" is not NULL 
    '''
    postalcode_df = pandas.read_sql(query, connection)
    return postalcode_df
