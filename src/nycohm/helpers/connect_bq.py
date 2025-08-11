from dagster import Field, resource
from google.cloud import bigquery
import os
from .log_config import configure_logging
import logging

def connect_bq(credentials_path: str | None = None) -> bigquery.Client:
    """Return a BigQuery client using the provided credentials.

    If ``credentials_path`` is ``None``, the path will be read from the
    ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable. If no path is
    available, the default BigQuery client behaviour is used, which relies on
    application default credentials.
    """

    path = credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if path:
        logging.info(f"Using BigQuery credentials from {path}")
        return bigquery.Client.from_service_account_json(path)
    return bigquery.Client()


@resource(config_schema={"credentials_path": Field(str, is_required=False)})
def bq_client(context) -> bigquery.Client:
    """Dagster resource providing a :class:`bigquery.Client` instance."""

    return connect_bq(context.resource_config.get("credentials_path"))