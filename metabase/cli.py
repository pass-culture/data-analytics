import json
import os

import click

from metabase.commands import switch_metabase_database_connection, get_app_name_for_restore


@click.group()
def cli():
    pass


@click.command('show_app_name_for_restore')
def show_app_name_for_restore():
    print (get_app_name_for_restore())


@click.command('switch_host_for_restore')
def switch_host_for_restore():
    switch_metabase_database_connection(os.environ.get('METABASE_DBNAME'), os.environ.get('METABASE_USER_NAME'), os.environ.get('METABASE_PASSWORD'))

cli.add_command(show_app_name_for_restore)
cli.add_command(switch_host_for_restore)

if __name__ == '__main__':
    cli()
