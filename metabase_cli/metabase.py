import json
import os

import requests

METABASE_URL = 'https://metabase-test.osc-fr1.scalingo.io'


def configure_new_metabase_session(user_name: str, password: str):
    response = requests.post(f'{METABASE_URL}/api/session/',
                             json={"username": user_name, "password": password})
    session = response.json()
    return session['id']


def get_dump_table_id(table_name: str, session_id: str):
    response = requests.get(f'{METABASE_URL}/api/database/',
                            headers={'cookie': f'metabase.SESSION={session_id}'})
    table_infos = [table for table in response.json() if table['name'] == table_name]
    return table_infos[0]['id']


def edit_dump_table_connection(table_name: str, user_name: str, password: str):
    session_id = configure_new_metabase_session(user_name, password)
    table_id = get_dump_table_id(table_name, session_id)
    app_name = get_app_name_for_restore()
    db_details = get_db_details_by_app_name(app_name)
    db_details['ssl'] = True
    db_settings = {'details': db_details,
                 'name': table_name,
                 'engine': "postgres"}
    requests.put(f'{METABASE_URL}/api/database/{table_id}',
                 headers={'cookie': f'metabase.SESSION={session_id}'},
                 json=db_settings)


def get_dump_table_information(table_name: str, session_id: str):
    table_id = get_dump_table_id(table_name, session_id)
    response = requests.get(f'{METABASE_URL}/api/database/{table_id}',
                            headers={'cookie': f'metabase.SESSION={session_id}'})
    jsonified_response = response.json()
    return jsonified_response['details']['host']


def get_app_name_for_restore():
    session_id = configure_new_metabase_session(os.environ.get('METABASE_USER_NAME'), os.environ.get('METABASE_PASSWORD'))
    host = get_dump_table_information(os.environ.get('METABASE_DBNAME'), session_id)
    db_blue = json.loads(os.environ.get('BLUE_DB_INFO'))
    db_green = json.loads(os.environ.get('GREEN_DB_INFO'))
    if db_blue['details']['host'] == host:
        return db_green['app_name']
    else:
        return db_blue['app_name']


def get_db_details_by_app_name(app_name: str):
    db_blue = json.loads(os.environ.get('BLUE_DB_INFO'))
    db_green = json.loads(os.environ.get('GREEN_DB_INFO'))
    if db_blue['app_name'] == app_name:
        return db_blue['details']
    return db_green['details']
