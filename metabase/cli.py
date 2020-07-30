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


@app.cli.command('create_enriched_views')
def create_enriched_views():
    create_enriched_data_views()


@click.command('show_app_name_for_restore')
def show_app_name_for_restore():
    print(get_app_name_for_restore())


@click.command('switch_host_for_restore')
@click.option('-l', '--local', is_flag=True)
def switch_host_for_restore(local: bool):
    switch_metabase_database_connection(os.environ.get('METABASE_DBNAME'), os.environ.get('METABASE_USER_NAME'), os.environ.get('METABASE_PASSWORD'), local)


cli.add_command(create_enriched_views)
cli.add_command(show_app_name_for_restore)
cli.add_command(switch_host_for_restore)

if __name__ == '__main__':
    cli()
