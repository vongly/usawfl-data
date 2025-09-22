import subprocess
from prefect import flow

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from tasks.extract_load import extract_load_task

@flow(name='Extract Load')
def extract_load_flow():
    extract_load_task()

if __name__ == '__main__':
    extract_load_flow()