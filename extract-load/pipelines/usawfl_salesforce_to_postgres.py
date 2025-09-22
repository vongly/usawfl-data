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
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_CERTIFICATION_PATH
)

os.environ['DESTINATION__POSTGRES__CREDENTIALS__HOST'] = POSTGRES_HOST
os.environ['DESTINATION__POSTGRES__CREDENTIALS__PORT'] = POSTGRES_PORT
os.environ['DESTINATION__POSTGRES__CREDENTIALS__USERNAME'] = POSTGRES_USER
os.environ['DESTINATION__POSTGRES__CREDENTIALS__PASSWORD'] = POSTGRES_PASSWORD
os.environ['DESTINATION__POSTGRES__CREDENTIALS__DATABASE'] = POSTGRES_DB
os.environ['DESTINATION__POSTGRES__CREDENTIALS__SSLMODE'] = 'verify-full'
os.environ['DESTINATION__POSTGRES__CREDENTIALS__SSLROOTCERT'] = POSTGRES_CERTIFICATION_PATH

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

    pipeline_name = 'usawfl_salesforce_to_postgres'
    dataset = 'raw_salesforce'
    destination = 'postgres'

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