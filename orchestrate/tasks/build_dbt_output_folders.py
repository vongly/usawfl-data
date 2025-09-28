from prefect import flow, task, get_run_logger

import sys, os
from pathlib import Path

from dotenv import load_dotenv

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import DBT_DOTENV_PATH

from utils.helpers import dbtOutputFiles, ConnectPSQL


@task(name='Build dbt Output Folders')
def build_dbt_output_folders_task():

    logger = get_run_logger()

    try:
        load_dotenv(dotenv_path=DBT_DOTENV_PATH)

        output_path_dev = os.getenv('OUTPUT_PATH_DEV')
        output_path_prod = os.getenv('OUTPUT_PATH_PROD')

        if not output_path_dev or not output_path_prod:
            raise ValueError('Missing OUTPUT_PATH_DEV or OUTPUT_PATH_PROD in environment')

        file_structure = dbtOutputFiles()
        file_structure.get_schema_structure()
        file_structure.build_output_folders(
            path_dev=output_path_dev,
            path_prod=output_path_prod,
        )

        logger.info('✅ Successfully built output folders')
        logger.info(f'DEV: {output_path_dev}')
        logger.info(f'PROD: {output_path_prod}')

    except Exception as e:
        logger.error('❌ Failed in "Build Output Folders": %s', e, exc_info=True)
        raise

if __name__ == '__main__':
    build_dbt_output_folders_task()