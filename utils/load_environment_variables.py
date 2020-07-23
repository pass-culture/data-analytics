import os
from dotenv import load_dotenv


def load_environment_variables():
    load_dotenv(dotenv_path='.env.local', override=True)
