from google.cloud import bigquery
import os
from log_config import configure_logging
import logging

configure_logging()

def connect_bq():
    """
    Connects to Google BigQuery using a service account key.
    Returns a BigQuery client instance.
    """

    return bigquery.Client()