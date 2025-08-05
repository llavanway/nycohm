from dagster import job, op, In, Out, Nothing

from helpers.process_datasets import (
    process_housing,
    process_affordable,
    join_sets,
)


@op(out=Out(Nothing), description="Clean and load housing dataset")
def process_housing_op():
    process_housing()


@op(out=Out(Nothing), description="Clean and load affordable housing dataset")
def process_affordable_op():
    process_affordable()


@op(
    ins={"housing": In(Nothing), "affordable": In(Nothing)},
    out=Out(Nothing),
    description="Join processed housing datasets",
)
def join_sets_op(housing, affordable):
    join_sets()


@job(description="ETL job to process NYC housing datasets and join them")
def nyc_housing_job():
    housing = process_housing_op()
    affordable = process_affordable_op()
    join_sets_op(housing, affordable)


# Dagster entry point
from dagster import Definitions

defs = Definitions(jobs=[nyc_housing_job])
