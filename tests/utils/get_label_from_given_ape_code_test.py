from utils.get_label_from_given_ape_code import get_label_from_given_ape_code


def test_should_return_empty_string_when_no_APE_code_is_given():
    # Given
    ape_code = ''

    # When
    label = get_label_from_given_ape_code(ape_code)

    # Then
    assert label == ''

def test_should_return_empty_string_when_given_APE_code_does_not_exist_in_mapping_table():
    # Given
    ape_code = 'ABCDEF'

    # When
    label = get_label_from_given_ape_code(ape_code)

    # Then
    assert label == ''

def test_should_return_label_when_given_APE_code_exists_in_mapping_table():
    # Given
    ape_code = '5320Z'

    # When
    label = get_label_from_given_ape_code(ape_code)

    # Then
    assert label == 'Autres activités de poste et de courrier'
