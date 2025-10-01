import dlt

import os
import sys
from pathlib import Path
from datetime import datetime, timezone

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core.pipeline import CreatePipeline
from utils.resources.postgres import PostgresResource
from utils.postgres_connection import connect_to_postgres, PostgresCall

from utils.helpers import pretty_all_jsons

from env import (
    S3_ACCESS_KEY,
    S3_SECRET_ACCESS_KEY,
    S3_REGION,
    S3_ENDPOINT_URL,
    STATS_POSTGRES_USER,
    STATS_POSTGRES_PASSWORD,
    STATS_POSTGRES_HOST,
    STATS_POSTGRES_PORT, 
    STATS_POSTGRES_DB,
    STATS_POSTGRES_TABLES,
    STATS_POSTGRES_TABLES_PREFIX,
)

'''

    set to full replace to incorporate hard deletes from source

'''

def run_pipeline(test=False):

    pipeline_name = 'stats_app_to_s3'
    dataset = 'raw_stats_app'
    destination = 'filesystem'

    schema = 'public'
    table_prefix = STATS_POSTGRES_TABLES_PREFIX
    tables = STATS_POSTGRES_TABLES.split(',')

    if test:
        pipeline_name = '_test_' + pipeline_name 
        dataset = '_test_' + dataset

        os.environ['_TEST_STATS_APP_TO_S3__DESTINATION__FILESYSTEM__BUCKET_URL'] = f's3://{pipeline_name}'
        os.environ['_TEST_STATS_APP_TO_S3__DESTINATION__CREDENTIALS__ENDPOINT_URL'] = S3_ENDPOINT_URL
        os.environ['_TEST_STATS_APP_TO_S3__DESTINATION__CREDENTIALS__AWS_ACCESS_KEY_ID'] = S3_ACCESS_KEY
        os.environ['_TEST_STATS_APP_TO_S3__DESTINATION__CREDENTIALS__AWS_SECRET_ACCESS_KEY'] = S3_SECRET_ACCESS_KEY
        os.environ['_TEST_STATS_APP_TO_S3__DESTINATION__CREDENTIALS__AWS_DEFAULT_REGION'] = S3_REGION

    else:
        os.environ['STATS_APP_TO_S3__DESTINATION__FILESYSTEM__BUCKET_URL'] = f's3://{pipeline_name}'
        os.environ['STATS_APP_TO_S3__DESTINATION__CREDENTIALS__ENDPOINT_URL'] = S3_ENDPOINT_URL
        os.environ['STATS_APP_TO_S3__DESTINATION__CREDENTIALS__AWS_ACCESS_KEY_ID'] = S3_ACCESS_KEY
        os.environ['STATS_APP_TO_S3__DESTINATION__CREDENTIALS__AWS_SECRET_ACCESS_KEY'] = S3_SECRET_ACCESS_KEY
        os.environ['STATS_APP_TO_S3__DESTINATION__CREDENTIALS__AWS_DEFAULT_REGION'] = S3_REGION

    connection = connect_to_postgres(
        dbname=STATS_POSTGRES_DB,
        user=STATS_POSTGRES_USER,
        password=STATS_POSTGRES_PASSWORD,
        host=STATS_POSTGRES_HOST,
        port=STATS_POSTGRES_PORT,
    )

    call = PostgresCall(connection=connection, test=test)

    resources = [
        PostgresResource(
            schema=schema,
            table=table_prefix+table,
            call=call,
            write_distposition='replace',
        ).create_resource()

            for table in tables
    ]

    pipeline = CreatePipeline(
        pipeline_name=pipeline_name,
        dataset=dataset,
        destination=destination,
        resources=resources,
    )

    pipeline.run_pipeline()
    print('Expected output:', f's3://{pipeline_name}', '\n')
    pretty_all_jsons()
    print(pipeline.jobs_json)

    return pipeline

if __name__ == '__main__':
    pipeline = run_pipeline()
