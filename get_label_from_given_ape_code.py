import pandas


def get_label_from_given_ape_code(ape_code: str) -> str:
    data = pandas.read_csv('utils/nomenclature_naf.csv', delimiter=';')
    try:
        return data.loc[data['Code NAF'] == ape_code].values[0][1]
    except IndexError:
        return ''
