import os

HOME = os.path.expanduser('~')
PROJECT_NAME = 'neo4j_tools'
# Path to folder
PROJECT_DIR = os.path.join(HOME, f".{PROJECT_NAME}")
if not os.path.exists(PROJECT_DIR):
    os.mkdir(PROJECT_DIR)

"""This file contains default values for configurations and parameters."""

###############################################################################
# Config
config_file_path = os.path.join(PROJECT_DIR, 'config.ini')