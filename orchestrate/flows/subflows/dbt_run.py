from prefect import flow, get_run_logger

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from tasks.dbt import dbt_run_task


@flow(name='dbt Run')
def dbt_run_flow():

    dbt_run_task('--target prod')

if __name__ == '__main__':
    dbt_run_flow()