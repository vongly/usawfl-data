import dlt

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core.salesforce_pipeline import SalesforcePipeline
from utils.resources.salesforceAPI import SalesforceResource
from utils.connections import SalesforceBulkCall

from utils.helpers import pretty_all_jsons

from env import (
    EXTRACT_DIR
)

def run_pipeline():

    objects = [
        'USAWFLOfficialLink__c',
        'USAWFL_Officials__c',
        'USAWFL_Team__c',
        'USAWFL_Tournaments__c',
        'USAWFL__c',
        'Contact',
    ]

    api_call_session = SalesforceBulkCall()
    resources = [
        SalesforceResource(
            object_name=obj,
            api_call_session=api_call_session,
            incremental_attribute='SystemModstamp',
        ).create_resource()

            for obj in objects
    ]

    pipeline_name = 'usawfl_salesforce_to_parquet'
    dataset = 'raw_salesforce'
    destination = dlt.destinations.filesystem(
        bucket_url=f'{EXTRACT_DIR}/{pipeline_name}',
    )

    pipeline = SalesforcePipeline(
        pipeline_name=pipeline_name,
        dataset=dataset,
        destination=destination,
        resources=resources,
    )

    pipeline.run_pipeline()
    
    return pipeline

if __name__ == '__main__':
    pipeline = run_pipeline()
    pretty_all_jsons()
    print(pipeline.jobs_json)
