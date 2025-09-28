from prefect import flow, get_run_logger

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(parent_dir))

from tasks.build_dbt_output_folders import build_dbt_output_folders_task


@flow(name='Build dbt Output Folders')
def build_dbt_output_folders_flow():

    build_dbt_output_folders_task()

if __name__ == '__main__':
    build_dbt_output_folders_flow()