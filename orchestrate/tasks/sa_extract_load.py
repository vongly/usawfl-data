import subprocess
from prefect import task

import sys, os
from pathlib import Path

from dotenv import load_dotenv

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from utils.helpers import extract_load

from env import (
    SA_EXTRACT_LOAD_FILE,
    SA_EXTRACT_LOAD_FILE_TEST,
)


@task(name='Stats App Extract Load')
def sa_extract_load_task():
    extract_load(rel_file_path=SA_EXTRACT_LOAD_FILE)

@task(name='Stats App Extract Load Test')
def sa_extract_load_test_task():
    extract_load(rel_file_path=SA_EXTRACT_LOAD_FILE_TEST)

if __name__ == '__main__':
    sa_extract_load_task()