import subprocess
from prefect import flow

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from tasks.sa_extract_load import sa_extract_load_task


@flow(name='Stats App Extract Load Test')
def sa_extract_load_flow():
    sa_extract_load_task()

if __name__ == '__main__':
    sa_extract_load_flow()