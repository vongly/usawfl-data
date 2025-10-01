import dlt

import os
import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core.pipeline import CreatePipeline
from utils.resources.salesforceAPI import SalesforceResource
from utils.salesforce_connection import SalesforceBulkCall

from utils.helpers import pretty_all_jsons

from env import (
    S3_ACCESS_KEY,
    S3_SECRET_ACCESS_KEY,
    S3_REGION,
    S3_ENDPOINT_URL,
    SF_OBJECTS,
)


def run_pipeline(test=False):

    pipeline_name = 'salesforce_to_s3'
    dataset = 'raw_salesforce'
    destination = 'filesystem'

    objects = SF_OBJECTS.split(',')

    if test:
        pipeline_name = '_test_' + pipeline_name
        dataset = '_test_' + dataset

        os.environ['_TEST_SALESFORCE_TO_S3__DESTINATION__FILESYSTEM__BUCKET_URL'] = f's3://{pipeline_name}'
        os.environ['_TEST_SALESFORCE_TO_S3__DESTINATION__CREDENTIALS__ENDPOINT_URL'] = S3_ENDPOINT_URL
        os.environ['_TEST_SALESFORCE_TO_S3__DESTINATION__CREDENTIALS__AWS_ACCESS_KEY_ID'] = S3_ACCESS_KEY
        os.environ['_TEST_SALESFORCE_TO_S3__DESTINATION__CREDENTIALS__AWS_SECRET_ACCESS_KEY'] = S3_SECRET_ACCESS_KEY
        os.environ['_TEST_SALESFORCE_TO_S3__DESTINATION__CREDENTIALS__AWS_DEFAULT_REGION'] = S3_REGION

    else:
        os.environ['SALESFORCE_TO_S3__DESTINATION__FILESYSTEM__BUCKET_URL'] = f's3://{pipeline_name}'
        os.environ['SALESFORCE_TO_S3__DESTINATION__CREDENTIALS__ENDPOINT_URL'] = S3_ENDPOINT_URL
        os.environ['SALESFORCE_TO_S3__DESTINATION__CREDENTIALS__AWS_ACCESS_KEY_ID'] = S3_ACCESS_KEY
        os.environ['SALESFORCE_TO_S3__DESTINATION__CREDENTIALS__AWS_SECRET_ACCESS_KEY'] = S3_SECRET_ACCESS_KEY
        os.environ['SALESFORCE_TO_S3__DESTINATION__CREDENTIALS__AWS_DEFAULT_REGION'] = S3_REGION


    api_call_session = SalesforceBulkCall(test=test)
    resources = [
        SalesforceResource(
            object_name=obj,
            api_call_session=api_call_session,
            incremental_attribute='SystemModstamp',
            write_disposition='append',
        ).create_resource()

            for obj in objects
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
