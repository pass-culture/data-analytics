import os
from utils.load_environment_variables import load_environment_variables

load_environment_variables()

import click
from flask import Flask

from metabase.commands import switch_metabase_database_connection, get_app_name_for_restore
from write.create_enriched_data_views import create_enriched_data_views

app = Flask(__name__, static_url_path='/static')

@click.group()
def cli():
    pass


@click.command('create', help='Create enriched views on Analytics Data Source')
@click.option('-u', '--db_url')
def create_enriched_views(db_url):
    create_enriched_data_views(db_url)


@click.command('show', help='Show app name that is not linked to Metabase. The restore has to be done on this app')
def show_app_name_for_restore():
    print(get_app_name_for_restore())


@click.command('switch',
               help='Switch Metabase connection to the other app. Invoke this command after a successful restore')
@click.option('-l', '--local', is_flag=True)
def switch_host_for_restore(local: bool):
    switch_metabase_database_connection(os.environ.get('METABASE_DBNAME'), os.environ.get('METABASE_USER_NAME'), os.environ.get('METABASE_PASSWORD'), local)


cli.add_command(create_enriched_views)
cli.add_command(show_app_name_for_restore)
cli.add_command(switch_host_for_restore)

if __name__ == '__main__':
    cli()
