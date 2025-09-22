import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parents[0] / '.env'
load_dotenv(dotenv_path=env_path)

OUTPUT_PATH_DEV = os.environ['OUTPUT_PATH_DEV']
OUTPUT_PATH_PROD = os.environ['OUTPUT_PATH_PROD']

DBT_PATH = os.environ['DBT_PATH']
