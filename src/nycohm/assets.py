from dagster import asset
from .helpers.process_datasets import (
    process_housing,
    process_affordable,
    join_sets,
    check_metrics,
)


@asset(
    required_resource_keys={"bq_client"},
    description="Clean and load housing dataset",
)
def housing_processed(context):
    process_housing(context.resources.bq_client)


@asset(
    required_resource_keys={"bq_client"},
    description="Clean and load affordable housing dataset",
)
def affordable_processed(context):
    process_affordable(context.resources.bq_client)


@asset(
    required_resource_keys={"bq_client"},
    description="Join processed housing datasets",
)
def joined_dataset(context, housing_processed, affordable_processed):
    join_sets(context.resources.bq_client)


@asset(
    required_resource_keys={"bq_client"},
    description="Check metrics of the joined dataset",
)
def dataset_metrics(context, joined_dataset):
    check_metrics(context.resources.bq_client)