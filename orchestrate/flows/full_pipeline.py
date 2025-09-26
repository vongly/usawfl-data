import subprocess
from prefect import flow

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from tasks.sf_extract_load import sf_extract_load_task
from tasks.sa_extract_load import sa_extract_load_task
from tasks.build_dbt_output_folders import build_output_folders_task
from tasks.create_postgres_views import create_psql_views_task
from tasks.dbt import dbt_run_task


@flow(name='Full Pipeline')
def full_pipeline_flow():
    
    sf_extract_load_task()
    sa_extract_load_task()

    build_output_folders_task()

    dbt_run_task('--target prod')

    create_psql_views_task()
    

if __name__ == '__main__':
    full_pipeline_flow()