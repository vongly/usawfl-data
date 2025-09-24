import subprocess
from prefect import flow, task, get_run_logger

import sys, os
from pathlib import Path
import json

from dotenv import load_dotenv

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import (
    EXTRACT_LOAD_PYTHON_EXEC_PATH,
    EXTRACT_LOAD_DIR,
    EXTRACT_LOAD_FILE,
    EXTRACT_LOAD_FILE_TEST,
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


@task(name='Extract Load')
def extract_load_task():
    extract_load(rel_file_path=EXTRACT_LOAD_FILE)


@task(name='Extract Load Test')
def extract_load_test_task():
    extract_load(rel_file_path=EXTRACT_LOAD_FILE_TEST)


if __name__ == '__main__':
    extract_load_task()