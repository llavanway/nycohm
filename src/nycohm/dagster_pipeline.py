from dagster import job, op, In, Out, Nothing

from .helpers.process_datasets import (
    process_housing,
    process_affordable,
    join_sets,
)
from .helpers.connect_bq import bq_client


@op(
    out=Out(Nothing),
    description="Clean and load housing dataset",
    required_resource_keys={"bq_client"},
)
def process_housing_op(context):
    process_housing(context.resources.bq_client)


@op(
    out=Out(Nothing),
    description="Clean and load affordable housing dataset",
    required_resource_keys={"bq_client"},
)
def process_affordable_op(context):
    process_affordable(context.resources.bq_client)


@op(
    ins={"housing": In(Nothing), "affordable": In(Nothing)},
    out=Out(Nothing),
    description="Join processed housing datasets",
    required_resource_keys={"bq_client"},
)
def join_sets_op(context, housing, affordable):
    join_sets(context.resources.bq_client)


@job(description="ETL job to process NYC housing datasets and join them")
def nyc_housing_job():
    housing = process_housing_op()
    affordable = process_affordable_op()
    join_sets_op(housing, affordable)


# Dagster entry point
from dagster import Definitions


defs = Definitions(
    jobs=[nyc_housing_job],
    resources={"bq_client": bq_client},
)
