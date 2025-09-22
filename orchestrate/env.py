import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parents[0] / '.env'
load_dotenv(dotenv_path=env_path)

PROJECT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))


EXTRACT_LOAD_FILE = os.environ['EXTRACT_LOAD_FILE']
EXTRACT_LOAD_DIR = os.environ['EXTRACT_LOAD_DIR']
EXTRACT_LOAD_VENV_PATH = os.environ['EXTRACT_LOAD_VENV_PATH']
EXTRACT_LOAD_PYTHON_EXEC_PATH = os.environ['EXTRACT_LOAD_PYTHON_EXEC_PATH']

DBT_DIR = os.environ['DBT_DIR']
DBT_DOTENV_PATH = os.environ['DBT_DOTENV_PATH']
DBT_VENV_PATH = os.environ['DBT_VENV_PATH']
DBT_EXEC_PATH = os.environ['DBT_EXEC_PATH']

DBT_EXEC_PYTHON_PATH = os.environ['DBT_EXEC_PYTHON_PATH']

DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']
DB_READ_PATH = os.environ['DB_READ_PATH']