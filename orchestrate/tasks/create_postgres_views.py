from prefect import flow, task, get_run_logger

import sys, os, io
from pathlib import Path

from dotenv import load_dotenv

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import (
    DBT_DOTENV_PATH,
    DB_READ_PATH,
    DB_HOST,
    DB_NAME,
    DB_PORT,
)

from utils.helpers import dbtOutputFiles, ConnectPSQL


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
    create_psql_views_task()