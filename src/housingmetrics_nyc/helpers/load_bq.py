from google.cloud import bigquery
from .log_config import configure_logging
import logging
import pandas as pd

configure_logging()


def load_bq(
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_id: str,
    client: bigquery.Client,
) -> None:
    """
    Save a DataFrame to a BigQuery table, letting BigQuery infer the schema.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        project_id (str): The BigQuery project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.
        client (bigquery.Client): The BigQuery client to use.
    """
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Check if the table exists
    try:
        client.get_table(table_ref)
        table_exists = True
    except Exception:
        table_exists = False

    # Create the table if it does not exist
    if not table_exists:
        table = bigquery.Table(table_ref)
        client.create_table(table)
        logging.info(f"Table {table_ref} created.")
    else:
        # Drop table
        client.delete_table(table_ref)
        logging.info(f"Table {table_ref} dropped.")

    # Save the DataFrame to BigQuery, letting BigQuery infer the schema
    job = client.load_table_from_dataframe(df, table_ref)
    job.result()  # Wait for the job to complete
    logging.info(f"DataFrame saved to {table_ref}")
    logging.info(f"DataFrame shape: {df.shape}")