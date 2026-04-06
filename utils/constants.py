import configparser
import os

parser = configparser.ConfigParser()
# This points to your config file
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))


# --- KEEP DATABASE SETTINGS ---
DATABASE_HOST = parser.get('database', 'database_host')
DATABASE_NAME = parser.get('database', 'database_name')
DATABASE_PORT = parser.get('database', 'database_port')
DATABASE_USER = parser.get('database', 'database_username')
DATABASE_PASSWORD = parser.get('database', 'database_password')

# In constants.py
INPUT_PATH = parser.get('file_paths', 'input_path')
OUTPUT_PATH = parser.get('file_paths', 'output_path')