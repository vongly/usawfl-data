from prefect import flow, get_run_logger

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from tasks.create_postgres_views import create_psql_views_task
from tasks.dbt import dbt_run_task


@flow(name='Create Postgres Views')
def create_psql_views_flow():

    create_psql_views_task()

if __name__ == '__main__':
    create_psql_views_flow()