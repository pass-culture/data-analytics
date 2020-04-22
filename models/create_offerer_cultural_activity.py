import pandas 
from parse_ape_code_from_offerer_details import get_ape_code_by_siren
from get_label_from_given_ape_code import get_label_from_given_ape_code
from models.db import ENGINE
from repository.offerer_queries import create_siren_dataframe


def create_offerer_cultural_activity_dataframe(siren_dataframe: pandas.DataFrame) -> pandas.DataFrame:
    offerer_cultural_activity_dataframe = siren_dataframe.copy()
    offerer_cultural_activity_dataframe['APE_code'] = offerer_cultural_activity_dataframe['siren'].apply(get_ape_code_by_siren)
    offerer_cultural_activity_dataframe['APE_label'] = offerer_cultural_activity_dataframe['APE_code'].apply(get_label_from_given_ape_code)
    offerer_cultural_activity_dataframe.drop('APE_code', axis=1, inplace=True)
    return offerer_cultural_activity_dataframe.drop('siren', axis=1)

def create_table_offerer_cultural_activity(offerer_cultural_activity_dataframe: pandas.DataFrame) -> None:
    offerer_cultural_activity_dataframe.to_sql('offerer_cultural_activity', con=ENGINE, if_exists='append')

def create_offerer_cultural_activity_data() -> None:
    siren_dataframe = create_siren_dataframe()
    offerer_cultural_activity_dataframe = create_offerer_cultural_activity_dataframe(siren_dataframe)
    create_table_offerer_cultural_activity(offerer_cultural_activity_dataframe)
