import dlt

import os
import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core.salesforce_pipeline import SalesforcePipeline
from utils.resources.salesforceAPI import SalesforceResource
from utils.connections import SalesforceBulkCall

from utils.helpers import pretty_all_jsons

from env import (
    S3_ACCESS_KEY,
    S3_SECRET_ACCESS_KEY,
    S3_REGION,
    S3_ENDPOINT_URL,
)



def run_pipeline(test=False):

    pipeline_name = 'usawfl_salesforce_to_s3_file'
    dataset = 'raw_salesforce'
    destination = 'filesystem'

    objects = [
        'USAWFLOfficialLink__c',
        'USAWFL_Officials__c',
        'USAWFL_Team__c',
        'USAWFL_Tournaments__c',
        'USAWFL__c',
        'Contact',
    ]

    if test:
        pipeline_name = pipeline_name + '_test'
        dataset = dataset + '_test'

        os.environ['USAWFL_SALESFORCE_TO_S3_FILE_TEST__DESTINATION__FILESYSTEM__BUCKET_URL'] = f's3://{pipeline_name}'
        os.environ['USAWFL_SALESFORCE_TO_S3_FILE_TEST__DESTINATION__CREDENTIALS__ENDPOINT_URL'] = S3_ENDPOINT_URL
        os.environ['USAWFL_SALESFORCE_TO_S3_FILE_TEST__DESTINATION__CREDENTIALS__AWS_ACCESS_KEY_ID'] = S3_ACCESS_KEY
        os.environ['USAWFL_SALESFORCE_TO_S3_FILE_TEST__DESTINATION__CREDENTIALS__AWS_SECRET_ACCESS_KEY'] = S3_SECRET_ACCESS_KEY
        os.environ['USAWFL_SALESFORCE_TO_S3_FILE_TEST__DESTINATION__CREDENTIALS__AWS_DEFAULT_REGION'] = S3_REGION

    else:
        os.environ['USAWFL_SALESFORCE_TO_S3_FILE__DESTINATION__FILESYSTEM__BUCKET_URL'] = f's3://{pipeline_name}'
        os.environ['USAWFL_SALESFORCE_TO_S3_FILE__DESTINATION__CREDENTIALS__ENDPOINT_URL'] = S3_ENDPOINT_URL
        os.environ['USAWFL_SALESFORCE_TO_S3_FILE__DESTINATION__CREDENTIALS__AWS_ACCESS_KEY_ID'] = S3_ACCESS_KEY
        os.environ['USAWFL_SALESFORCE_TO_S3_FILE__DESTINATION__CREDENTIALS__AWS_SECRET_ACCESS_KEY'] = S3_SECRET_ACCESS_KEY
        os.environ['USAWFL_SALESFORCE_TO_S3_FILE__DESTINATION__CREDENTIALS__AWS_DEFAULT_REGION'] = S3_REGION


    api_call_session = SalesforceBulkCall(test=test)
    resources = [
        SalesforceResource(
            object_name=obj,
            api_call_session=api_call_session,
            incremental_attribute='SystemModstamp',
        ).create_resource()

            for obj in objects
    ]

    pipeline = SalesforcePipeline(
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
