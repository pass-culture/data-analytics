from read.connectors.api_sirene_connector import get_offerer_details_by_siren


def parse_ape_code_from_offerer_details(offerer_details: dict) -> str:
    try:
        ape_code_with_point = offerer_details["unite_legale"]["activite_principale"]
        return ape_code_with_point.replace(".", "")
    except KeyError:
        return ""


def get_ape_code_by_siren(siren: str) -> str:
    offerer_details = get_offerer_details_by_siren(siren)
    return parse_ape_code_from_offerer_details(offerer_details)
