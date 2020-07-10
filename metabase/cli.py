import json
import os

import click
from flask import Flask

from metabase.commands import switch_metabase_database_connection, get_app_name_for_restore, \
     clean_database_if_local, initialize_metabase_if_local
from write.create_enriched_data_views import create_enriched_data_views


app = Flask(__name__, static_url_path='/static')

@click.group()
def cli():
    pass


@app.cli.command('create_enriched_views')
def create_enriched_views():
    create_enriched_data_views()


@click.command('clean_database_and_view')
def clean_database_and_view():
    clean_database_if_local()


@click.command('show_app_name_for_restore')
def show_app_name_for_restore():
    print (get_app_name_for_restore())


@click.command('switch_host_for_restore')
def switch_host_for_restore():
    switch_metabase_database_connection(os.environ.get('METABASE_DBNAME'), os.environ.get('METABASE_USER_NAME'), os.environ.get('METABASE_PASSWORD'))


@click.command('initialize_metabase')
def initialize_metabase():
    initialize_metabase_if_local()


cli.add_command(clean_database_and_view)
cli.add_command(create_enriched_views)
cli.add_command(initialize_metabase)
cli.add_command(show_app_name_for_restore)
cli.add_command(switch_host_for_restore)

if __name__ == '__main__':
    cli()
