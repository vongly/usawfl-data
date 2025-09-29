from prefect import flow, get_run_logger

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(parent_dir))

from tasks.dbt import dbt_build_task


@flow(name='dbt Build')
def dbt_build_flow():

    dbt_build_task('--target prod')

if __name__ == '__main__':
    dbt_build_flow()