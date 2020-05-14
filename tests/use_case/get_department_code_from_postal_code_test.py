from use_case.get_department_code_from_postal_code import get_departement_code


def test_get_departement_code_for_mainland_France():
    # given
    postal_code = '75012'

    # when
    departement_code = get_departement_code(postal_code)

    # then
    assert departement_code == '75'

def test_get_departement_code_for_overseas_France():
    # given
    postal_code = '97440'

    # when
    departement_code = get_departement_code(postal_code)

    # then
    assert departement_code == '974'
