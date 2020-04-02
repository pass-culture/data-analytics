import pandas

def create_offerer_cultural_activity_dataframe(siren_dataframe: pandas.DataFrame) -> pandas.DataFrame:
    offerer_cultural_activity_dataframe = siren_dataframe.copy()
    offerer_cultural_activity_dataframe['APE_label'] = ''
    return offerer_cultural_activity_dataframe.drop('siren', axis=1)
