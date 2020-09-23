from utils.load_environment_variables import load_environment_variables

load_environment_variables()

import json
import os
import requests


METABASE_URL = os.environ.get("METABASE_URL", "http://localhost:3002")


def get_setup_token():
    requests.get(f"{METABASE_URL}/setup")
    response = requests.get(f"{METABASE_URL}/api/session/properties")
    return response.json()["setup_token"]


def post_create_metabase_superuser(token):
    db_blue = json.loads(os.environ.get("BLUE_DB_INFO"))
    user_email = os.environ.get("METABASE_USER_NAME", "admin@example.com")
    user_password = os.environ.get("METABASE_PASSWORD", "user@AZERTY123")
    payload = {
        "token": token,
        "prefs": {"site_name": "pc", "allow_tracking": "true"},
        "database": {
            "engine": "postgres",
            "name": "Produit",
            "details": {
                "host": db_blue["details"]["host"],
                "port": db_blue["details"]["port"],
                "dbname": db_blue["details"]["dbname"],
                "user": db_blue["details"]["user"],
                "password": db_blue["details"]["password"],
                "ssl": False,
                "additional-options": None,
                "tunnel-enabled": False,
            },
            "auto_run_queries": True,
            "is_full_sync": True,
            "schedules": {
                "cache_field_values": {
                    "schedule_day": None,
                    "schedule_frame": None,
                    "schedule_hour": 0,
                    "schedule_type": "daily",
                },
                "metadata_sync": {
                    "schedule_day": None,
                    "schedule_frame": None,
                    "schedule_hour": None,
                    "schedule_type": "hourly",
                },
            },
        },
        "user": {
            "first_name": "pc",
            "last_name": "admin",
            "email": user_email,
            "password": user_password,
            "site_name": "pc",
        },
    }
    response = requests.post(f"{METABASE_URL}/api/setup", json=payload)


def initialize_metabase_if_local():
    token = get_setup_token()
    post_create_metabase_superuser(token)


if __name__ == "__main__":
    initialize_metabase_if_local()
