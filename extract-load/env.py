import os
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parents[0] / '.env'
load_dotenv(dotenv_path=env_path)

PROJECT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))

SF_USERNAME = os.environ['SF_USERNAME']
SF_PASSWORD = os.environ['SF_PASSWORD']
SF_TOKEN = os.environ['SF_TOKEN']
SF_LOGIN_URL = os.environ['SF_LOGIN_URL']

EXTRACT_DIR_RELATIVE = os.environ['EXTRACT_DIR_RELATIVE']
EXTRACT_DIR = os.path.join(PROJECT_DIRECTORY, EXTRACT_DIR_RELATIVE)

PIPELINES_DIR_RELATIVE = os.environ['PIPELINES_DIR_RELATIVE']
PIPELINES_DIR = os.path.join(PROJECT_DIRECTORY, PIPELINES_DIR_RELATIVE)

POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
POSTGRES_HOST = os.environ['POSTGRES_HOST']
POSTGRES_PORT = os.environ['POSTGRES_PORT']
POSTGRES_DB = os.environ['POSTGRES_DB']
POSTGRES_CERTIFICATION = os.environ['POSTGRES_CERTIFICATION']
POSTGRES_CERTIFICATION_PATH = os.path.join(PROJECT_DIRECTORY, POSTGRES_CERTIFICATION)

S3_USERNAME = os.environ['S3_USERNAME']
S3_ACCESS_KEY = os.environ['S3_ACCESS_KEY']
S3_SECRET_ACCESS_KEY = os.environ['S3_SECRET_ACCESS_KEY']
S3_REGION = os.environ['S3_REGION']
S3_BUCKET_URL = os.environ['S3_BUCKET_URL']
S3_ENDPOINT_URL = os.environ['S3_ENDPOINT_URL']