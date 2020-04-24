import requests
from utils.logger import logger


def get_offerer_details_by_siren(siren: str) -> dict:
    api_response = requests.get(f"https://entreprise.data.gouv.fr/api/sirene/v3/unites_legales/{siren}")

    if api_response.status_code != 200:
        logger.error(f"API sirene returned status {api_response.status_code} for siren {siren}")
        return {}

    return api_response.json()
