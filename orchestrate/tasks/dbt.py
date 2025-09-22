import subprocess
from prefect import flow, task, get_run_logger

import sys, os, io
from pathlib import Path
import json

from dotenv import load_dotenv

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from utils.helpers import dbtOutputFiles, ConnectPSQL

from env import (
    DBT_DIR,
    DBT_DOTENV_PATH,
    DBT_EXEC_PATH,
    DB_READ_PATH,
    DB_HOST,
    DB_NAME,
    DB_PORT,
)

load_dotenv(dotenv_path=DBT_DOTENV_PATH)


def run_dbt_commands(command):

    env = os.environ.copy()
    env['DBT_DIR'] = DBT_DIR

    logger = get_run_logger()
    result = subprocess.run(
        [DBT_EXEC_PATH] + command,
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
    )

    logger.info(f'Exit code: {result.returncode}')
    if result.stdout:
        logger.info(f'STDOUT:\n{result.stdout.strip()}')
    if result.stderr:
        logger.error(f'STDERR:\n{result.stderr.strip()}')

    if result.returncode != 0:
        raise RuntimeError(f'dbt run failed with exit code {result.returncode}')

    logger.info(f'Raw output:\n{result.stdout.strip()}')


@task(name='dbt Run')
def dbt_run_task(cmd_suffix=None):
    command = ['run']

    if cmd_suffix:
        cmd_suffix = cmd_suffix.split(' ')
        command = command + cmd_suffix

    run_dbt_commands(command)

@task(name='dbt Seed')
def dbt_seed_task(cmd_suffix=None):
    command = 'seed'

    if cmd_suffix:
        cmd_suffix = cmd_suffix.split(' ')
        command = command + cmd_suffix

    run_dbt_commands(command)

@task(name='Build Output Folders')
def build_output_folders_task(cmd_suffix=None):

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

@task(name='Create Postgres Views')
def create_psql_views_task():
    '''
        automates building pg duck views from parquet files
    '''

    logger = get_run_logger()

    try:
        load_dotenv(dotenv_path=DBT_DOTENV_PATH)

        output_path_prod = os.getenv('OUTPUT_PATH_PROD')

        if not output_path_prod:
            raise ValueError('Missing OUTPUT_PATH_PROD in environment')

        db = ConnectPSQL()
        file_structure = dbtOutputFiles()
        file_structure.get_schema_structure()
        file_structure.get_field_dtypes(path=output_path_prod)

        buffer = io.StringIO()
        sys_stdout = sys.stdout
        sys.stdout = buffer
        try:
            file_structure.create_postgres_views(
                db_read_path=DB_READ_PATH,
                db=db,
            )
        finally:
            sys.stdout = sys_stdout

        output = buffer.getvalue()
        if output.strip():
            logger.info(f"create_postgres_views output:\n{output}")

        logger.info(f'✅ Successfully created views @{DB_HOST}:{DB_PORT}/{DB_NAME}')

    except Exception as e:
        logger.error('❌ Failed in "Create Posgres Views": %s', e, exc_info=True)
        raise


if __name__ == '__main__':
    dbt_run_task('--target prod')