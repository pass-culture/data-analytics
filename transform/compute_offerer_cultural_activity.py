import pandas

from transform.parse_ape_code_from_offerer_details import get_ape_code_by_siren


def get_offerer_cultural_activity_dataframe(siren_dataframe: pandas.DataFrame) -> pandas.DataFrame:
    offerer_cultural_activity_dataframe = siren_dataframe.copy()
    offerer_cultural_activity_dataframe['APE_code'] = offerer_cultural_activity_dataframe['siren'].apply(get_ape_code_by_siren)
    offerer_cultural_activity_dataframe['APE_label'] = offerer_cultural_activity_dataframe['APE_code'].apply(get_label_from_given_ape_code)
    offerer_cultural_activity_dataframe.drop('APE_code', axis=1, inplace=True)
    return offerer_cultural_activity_dataframe.drop('siren', axis=1)


def get_label_from_given_ape_code(ape_code: str) -> str:
    data = pandas.read_csv('transform/nomenclature_naf.csv', delimiter=';')
    try:
        return data.loc[data['Code NAF'] == ape_code].values[0][1]
    except IndexError:
        return ''


MAINLAND_DEPARTEMENT_CODE_LENGTH = 2
OVERSEAS_DEPARTEMENT_CODE_LENGTH = 3
OVERSEAS_DEPARTEMENT_IDENTIFIER = '97'


def get_departement_code(postalCode: str) -> str:
    return postalCode[:OVERSEAS_DEPARTEMENT_CODE_LENGTH] if _is_overseas_departement(postalCode) \
        else postalCode[:MAINLAND_DEPARTEMENT_CODE_LENGTH]


def _is_overseas_departement(postalCode: str) -> bool:
    return postalCode.startswith(OVERSEAS_DEPARTEMENT_IDENTIFIER)