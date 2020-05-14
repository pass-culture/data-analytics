MAINLAND_DEPARTEMENT_CODE_LENGTH = 2
OVERSEAS_DEPARTEMENT_CODE_LENGTH = 3
OVERSEAS_DEPARTEMENT_IDENTIFIER = '97'


def get_departement_code(postalCode: str) -> str:
    return postalCode[:OVERSEAS_DEPARTEMENT_CODE_LENGTH] if _is_overseas_departement(postalCode) \
        else postalCode[:MAINLAND_DEPARTEMENT_CODE_LENGTH]

def _is_overseas_departement(postalCode: str) -> bool:
    return postalCode.startswith(OVERSEAS_DEPARTEMENT_IDENTIFIER)
