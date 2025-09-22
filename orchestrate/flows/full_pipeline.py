import subprocess
from prefect import flow

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from tasks.extract_load import extract_load_task
from tasks.dbt import (
    dbt_run_task,
    build_output_folders_task,
    create_psql_views_task,
)


@flow(name='Full Pipeline')
def full_pipeline_flow():
    
    extract_load_task()
    build_output_folders_task()
    dbt_run_task()
    create_psql_views_task()
    

if __name__ == '__main__':
    full_pipeline_flow()