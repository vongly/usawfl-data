import subprocess
from prefect import flow

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from full_transformation import full_transformation_flow

from subflows.sf_extract_load import sf_extract_load_flow
from subflows.sa_extract_load import sa_extract_load_flow
from tasks.dbt import dbt_run_task


@flow(name='Full Extract Load')
def full_extract_load_flow():
    
    sf_extract_load_flow()
    sa_extract_load_flow()

if __name__ == '__main__':
    full_extract_load_flow()