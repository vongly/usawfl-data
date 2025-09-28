import subprocess
from prefect import flow

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from subflows.sf_extract_load_test import sf_extract_load_test_flow
from subflows.sa_extract_load_test import sa_extract_load_test_flow


@flow(name='Full Extract Load Test')
def full_extract_load_test_flow():
    
    sf_extract_load_test_flow()
    sa_extract_load_test_flow()

if __name__ == '__main__':
    full_extract_load_test_flow()