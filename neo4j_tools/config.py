import os
import logging
from neo4j_tools import defaults
from configparser import RawConfigParser

def set_neo_config(
    user: str,
    password: str,
    database: str,
    server="localhost",
    port=7687,
    import_folder="/opt/neo4j/import/",
):
    config_dict = {
        'user': user,
        'password': password,
        'database': database,
        'server': server,
        'port': port,
        'import_folder': import_folder       
    }
    for param, value in config_dict.items():
        write_to_config('NEO4J', param, value)


def write_to_config(section: str, option: str, value: str) -> None:
    """Write section, option and value to config file.

    Parameters
    ----------
    section : str
        Section name of configuration file.
    option : str
        Option name.
    value : str
        Option value.
    """
    if value:
        cfp = defaults.config_file_path
        print(cfp)
        config = RawConfigParser()

        if not os.path.exists(cfp):
            with open(cfp, 'w') as config_file:
                config[section] = {option: value}
                config.write(config_file)
                logging.info(f'Set in configuration file {cfp} in section {section} {option}={value}')
        else:
            config.read(cfp)
            if not config.has_section(section):
                config.add_section(section)
            config.set(section, option, value)
            with open(cfp, 'w') as configfile:
                config.write(configfile)