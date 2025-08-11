from dagster import job, op, Out, Nothing
from .helpers.process_datasets import join_sets, check_metrics
from .helpers.connect_bq import bq_client


@op(
    out=Out(Nothing),
    description="Clean and load housing dataset",
    required_resource_keys={"bq_client"},
)
def process_housing_op(context):
    from .helpers.process_datasets import process_housing
    process_housing(context.resources.bq_client)


@op(
    out=Out(Nothing),
    description="Clean and load affordable housing dataset",
    required_resource_keys={"bq_client"},
)
def process_affordable_op(context):
    from .helpers.process_datasets import process_affordable
    process_affordable(context.resources.bq_client)


@op(
    out=Out(Nothing),
    description="Join processed housing datasets",
    required_resource_keys={"bq_client"},
)
def join_sets_op(context):
    join_sets(context.resources.bq_client)


@op(
    out=Out(Nothing),
    description="Check metrics of the joined dataset",
    required_resource_keys={"bq_client"},
)
def check_metrics_op(context, joined_data):
    check_metrics(context.resources.bq_client)


@job(description="ETL job to process NYC housing datasets and join them")
def nyc_housing_job():
    process_housing_op()
    process_affordable_op()
    joined_data = join_sets_op()
    check_metrics_op(joined_data)


# Dagster entry point
from dagster import Definitions


defs = Definitions(
    jobs=[nyc_housing_job],
    resources={"bq_client": bq_client},
)
