from prefect import flow, get_run_logger

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from subflows.build_dbt_output_folders import build_dbt_output_folders_flow
from subflows.dbt_run import dbt_run_flow
from subflows.create_psql_views_flow import create_psql_views_flow


@flow(name='Full Transformation')
def full_transformation_flow():

    build_dbt_output_folders_flow()
    dbt_run_flow()
    create_psql_views_flow()

if __name__ == '__main__':
    full_transformation_flow()