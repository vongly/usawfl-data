from prefect import get_run_logger
import sys
import os
from pathlib import Path
import subprocess
import json
import psycopg
import duckdb

from glob import glob

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import (
    DB_NAME,
    DB_USER,
    DB_PASS,
    DB_HOST,
    DB_PORT,
    DBT_EXEC_PATH,
    DBT_DIR,
    EXTRACT_LOAD_PYTHON_EXEC_PATH,
    EXTRACT_LOAD_DIR
)

def extract_load(rel_file_path):

    logger = get_run_logger()

    result = subprocess.run(
        [
            EXTRACT_LOAD_PYTHON_EXEC_PATH,
            f'{EXTRACT_LOAD_DIR}/{rel_file_path}',
        ],
        capture_output=True,
        text=True,
    )

    logger.info(f'Exit code: {result.returncode}')
    if result.stdout:
        logger.info(f'STDOUT:\n{result.stdout.strip()}')
    if result.stderr:
        logger.error(f'STDERR:\n{result.stderr.strip()}')

    if result.returncode != 0:
        raise RuntimeError(
            f'Pipeline failed with exit code {result.returncode}'
        )

class ConnectPSQL:
    def __init__(
        self,
        db=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
    ):
        self.conn = psycopg.connect(
            dbname=db,
            user=user,
            password=password,
            host=host,
            port=port,
        )

    def cursor(self):
        return self.conn.cursor()

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

class dbtOutputFiles:

    def get_schema_structure(self):

        logger = get_run_logger()

        try:
            result = subprocess.run(
                [DBT_EXEC_PATH, 'ls', '--resource-type', 'model', '--output', 'json'],
                cwd=DBT_DIR,
                capture_output=True,
                text=True,
                check=True
            )
            logger.info("dbt ls output:\n%s", result.stdout)
            if result.stderr:
                logger.warning("dbt ls stderr:\n%s", result.stderr)
            logger.info(result.stdout)

        except subprocess.CalledProcessError as e:
            logger.error("dbt ls failed with return code %s", e.returncode)
            logger.error("STDOUT:\n%s", e.stdout)
            logger.error("STDERR:\n%s", e.stderr)
            raise

        self.schema_structure = []

        for line in result.stdout.splitlines():
            line = line.strip()
            if not line.startswith('{'):
                continue

            node = json.loads(line)

            if 'name' in node:
                schema = {
                    'schema': node['original_file_path'].split('/')[1],            
                    'model': node['name'],
                }
                self.schema_structure.append(schema)

        return self.schema_structure

    def build_output_folders(self, path_dev, path_prod):
        for path in [path_dev, path_prod]:

            for entry in self.schema_structure:
                schema = entry['schema']
                model = entry['model']

                schema_dir = os.path.join(path, schema)
                model_dir = os.path.join(schema_dir, model)

                if not os.path.exists(model_dir):
                    os.makedirs(model_dir)

    def get_field_dtypes(self, path, file_type='parquet'):

        self.dtype_models = []

        for entry in self.schema_structure:
            schema = entry['schema']
            model = entry['model']

            schema_dir = os.path.join(path, schema)
            model_dir = os.path.join(schema_dir, model)

            if os.path.exists(model_dir):
                model_dir_wild_card = model_dir + '/*.' + file_type
                files = glob(model_dir_wild_card)
                if files:
                    results = duckdb.sql(f'''
                        select * from read_parquet('{model_dir_wild_card}')
                    ''')

                    field_descriptions = results.description
                    
                    dtype_model = {'schema': schema, 'model': model, 'file_path': model_dir_wild_card, 'fields': []}
                    for field in field_descriptions:
                        dtype_model['fields'].append({'name': field[0], 'dtype': field[1]})
                    self.dtype_models.append(dtype_model)

        return self.dtype_models

    def create_postgres_views(self, db_read_path, db):
        conn = db.conn
        cursor = conn.cursor()

        for dtype_model in self.dtype_models:
            fields_query = [ f'''cast(a['{field['name']}'] as {field['dtype']}) as {field['name']}''' for field in dtype_model['fields'] ]
            spaces = ' ' * 20
            fields_query_string = f',\n{spaces}'.format().join(fields_query)

            sql = f'''
                create schema if not exists {dtype_model['schema']};
                drop view if exists {dtype_model['schema']}.{dtype_model['model']};
                create view {dtype_model['schema']}.{dtype_model['model']} as

                select
                    {fields_query_string}
                from
                    read_parquet('{db_read_path}/{dtype_model['schema']}/{dtype_model['model']}/*.parquet') a
                ;'''
            print(sql)
            cursor.execute(sql)
            conn.commit()
