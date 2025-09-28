import subprocess
from prefect import flow

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from tasks.sf_extract_load import sf_extract_load_test_task


@flow(name='Salesforce Extract Load Test')
def sf_extract_load_test_flow():
    sf_extract_load_test_task()

if __name__ == '__main__':
    sf_extract_load_test_flow()