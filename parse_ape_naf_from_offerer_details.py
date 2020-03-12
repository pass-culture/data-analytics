def parse_ape_naf_from_offerer_details(offerer_details: dict) -> str:
    try:
        return offerer_details["unite_legale"]["activite_principale"]
    except KeyError:
        return ''
