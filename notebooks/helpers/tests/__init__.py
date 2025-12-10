"""
For new test development, include test module name like bellow examples.
This makes sure that the test_runner.py module will load all tests inside this directory.
"""
from . import streaming_autoloader_tests
from . import batch_ingest_copy_into_tests
from . import cdc_merge_tests
from . import gold_queries_and_mvs_tests
from . import transformations_dlt_tests

__all__ = [
    "streaming_autoloader_tests",
    "batch_ingest_copy_into_tests",
    "cdc_merge_tests",
    "gold_queries_and_mvs_tests",
    "transformations_dlt_tests"
]
