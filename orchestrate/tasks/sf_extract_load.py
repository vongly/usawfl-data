import subprocess
from prefect import task

import sys, os
from pathlib import Path
import json

from dotenv import load_dotenv

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from utils.helpers import extract_load

from env import (
    SF_EXTRACT_LOAD_FILE,
    SF_EXTRACT_LOAD_FILE_TEST,
)


@task(name='Salesforce Extract Load')
def sf_extract_load_task():
    extract_load(rel_file_path=SF_EXTRACT_LOAD_FILE)

@task(name='Salesforce Extract Load Test')
def sf_extract_load_test_task():
    extract_load(rel_file_path=SF_EXTRACT_LOAD_FILE_TEST)


if __name__ == '__main__':
    sf_extract_load_task()