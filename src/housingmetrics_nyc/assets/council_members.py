import pandas as pd
from datetime import datetime, timezone
from dagster import asset, Output, MetadataValue, AssetExecutionContext, AssetIn

from src.housingmetrics_nyc.fetch_data import dataset_source_info, ensure_dataset
from src.housingmetrics_nyc.helpers._read_csv import _read_csv
from src.housingmetrics_nyc.helpers.council_members import load_overrides, sitting_council_members
from src.housingmetrics_nyc.helpers.handle_null import standardize_null_values
from src.housingmetrics_nyc.helpers.log_config import configure_logging
from src.housingmetrics_nyc.helpers.prep_for_bq import sanitize_bq_columns

configure_logging()

DATASET = 'council_members'


# ingest
@asset(
    name="council_members",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="ingest_csv",
)
def council_members(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    # downloaded from NYC Open Data on first use; see src/housingmetrics_nyc/fetch_data.py
    df, csv_path = _read_csv(context, ensure_dataset(DATASET))

    df, col_map = sanitize_bq_columns(df, lowercase=False)
    context.log.info(f"Renamed columns for BigQuery: {col_map}")

    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()

    metadata = {
        "csv_path": MetadataValue.path(str(csv_path)),
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
        "source_owner": "NYC Council",
        "notes": "Every Council Member term since 1999, from the Council's Legistar system.",
    }
    return Output(df, metadata=metadata)


# process
@asset(
    ins={"council_members": AssetIn(key=["council_members"])},
    name="council_members_clean",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def council_members_clean(council_members) -> Output[pd.DataFrame]:
    """The sitting Council Member for each district as of the data refresh."""
    df = standardize_null_values(council_members)
    terms = df.rename(columns={"District": "Council_District", "Council_Member_Name": "Council_Member"})

    # "as of the data refresh" is the day the list was downloaded
    downloaded_at = dataset_source_info(DATASET).get("downloaded_at")
    as_of = pd.Timestamp(downloaded_at).tz_localize(None) if downloaded_at else pd.Timestamp.now()

    sitting = sitting_council_members(terms, as_of, overrides=load_overrides())

    metadata = {
        "rows": len(sitting),
        "as_of": str(as_of.date()),
        "districts_without_a_listed_member": sorted(
            set(range(1, 52)) - set(sitting["Council_District"].astype(int))
        ),
        "preview": MetadataValue.md(sitting.head(10).to_markdown(index=False)),
    }
    return Output(sitting, metadata=metadata)


assets_council_members = [council_members, council_members_clean]
