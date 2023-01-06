"""Console script for neo4j_tools."""
import sys
import click
from neo4j_tools import config
import logging


logger = logging.getLogger(__name__)


@click.group(help="Neo4J tools Command Line Utilities")
@click.version_option()
def main():
    """Entry method."""
    pass



@main.command()
@click.option('-u', '--user', help="User")
@click.option('-p', '--password', help="Password")
@click.option('-d', '--database', help="Database name")
@click.option('-s', '--server', default='localhost', help="Server name")
@click.option('-o', '--port', default='7687', help="Server port")
@click.option('-i', '--import_folder', default="/opt/neo4j/import/", help="Path to import folder")
def set_neo_config(user: str, password: str, database: str, server: str, port: str, import_folder: str):
    """Set Neo4J tools settings."""
    config.set_neo_config(
        user=user,
        password=password,
        database=database,
        server=server,
        port=int(port),
        import_folder=import_folder
    )


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
