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
    
    extract_load = extract_load_task.submit()
    extract_load.result()

    dbt_run = dbt_run_task.submit()
    dbt_run.result()

    build_output_folders = build_output_folders_task.submit()
    build_output_folders.result()

    create_psql_views = create_psql_views_task.submit()
    create_psql_views.result()

if __name__ == '__main__':
    full_pipeline_flow()