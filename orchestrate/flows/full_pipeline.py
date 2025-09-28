import subprocess
from prefect import flow

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from full_extract_load import full_extract_load_flow
from full_transformation import full_transformation_flow


@flow(name='Full Data Pipeline')
def full_pipeline_flow():
    
    full_extract_load_flow()
    full_transformation_flow()    

if __name__ == '__main__':
    full_pipeline_flow()