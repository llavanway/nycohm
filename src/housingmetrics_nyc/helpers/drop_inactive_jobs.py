import logging
import re

import pandas as pd

WITHDRAWN = "9. Withdrawn"
INCOMPLETE_STATUSES = [
    "1. Filed Application",
    "2. Approved Application",
    "3. Permitted for Construction",
]
COMPLETE_STATUSES = [
    "4. Partially Completed Construction",
    "5. Completed Construction",
]
DATE_COLS = ["DateFiled", "DatePermit", "DateLstUpd", "DateComplt"]
STALLED_AFTER_YEARS = 3


def capture_date_from_version(version: str) -> pd.Timestamp:
    """
    Convert a Housing Database version label to its data capture date.
    '26Q2' and '26Q2.1' -> 2026-07-01; '24Q4' -> 2025-01-01.
    """
    match = re.match(r"^(\d{2})Q([1-4])", str(version).strip().upper())
    if not match:
        raise ValueError(f"Unrecognized Housing Database version: {version!r}")
    quarter_end = pd.Period(year=2000 + int(match.group(1)), quarter=int(match.group(2)), freq="Q")
    return quarter_end.end_time.normalize() + pd.Timedelta(days=1)


def drop_inactive_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove inactive jobs from the Housing Database project-level file.

    The NYC Open Data export includes inactive jobs but has no Job_Inactv column,
    so this reapplies the rules DCP uses to set that flag
    (NYCPlanning/data-engineering, products/developments/sql/_status.sql):
      - Withdrawn: Job_Status is '9. Withdrawn'.
      - Stalled: filed, approved or permitted, and not updated in the 3 years
        before the capture date.
      - Duplicate: filed, approved or permitted, and an identical job (same type,
        address and unit counts) with a later update is already complete.
    DCP also applies manual corrections that are not published, so the result is
    close to, but not exactly, DCP's own inactive-excluded file.
    """
    capture_date = capture_date_from_version(df["Version"].dropna().iloc[0])
    dates = pd.concat(
        {col: pd.to_datetime(df[col], errors="coerce") for col in DATE_COLS}, axis=1
    )
    incomplete = df["Job_Status"].isin(INCOMPLETE_STATUSES)

    withdrawn = df["Job_Status"].eq(WITHDRAWN)

    # DCP divides whole days by 365 and drops the remainder
    days_idle = (capture_date - dates.max(axis=1)).dt.days
    stalled = incomplete & (days_idle // 365 >= STALLED_AFTER_YEARS)

    match_cols = ["Job_Type", "_address", "ClassAInit", "ClassAProp"]
    jobs = pd.DataFrame(
        {
            "Job_Type": df["Job_Type"],
            "_address": df["AddressNum"].astype(str).str.strip()
            + " "
            + df["AddressSt"].astype(str).str.strip(),
            "ClassAInit": pd.to_numeric(df["ClassAInit"], errors="coerce"),
            "ClassAProp": pd.to_numeric(df["ClassAProp"], errors="coerce"),
            "_updated": dates["DateLstUpd"],
        },
        index=df.index,
    )
    complete = jobs[
        df["Job_Status"].isin(COMPLETE_STATUSES)
        & jobs["ClassAInit"].notna()
        & jobs["ClassAProp"].notna()
    ]
    latest_complete = (
        complete.groupby(match_cols)["_updated"].max().rename("_complete_updated").reset_index()
    )
    matched = jobs.merge(latest_complete, on=match_cols, how="left")
    duplicate = incomplete & (matched["_updated"] < matched["_complete_updated"]).to_numpy()

    inactive = withdrawn | stalled | duplicate
    logging.info(
        "Dropping %s inactive jobs of %s (capture date %s): %s withdrawn, %s stalled, %s duplicate",
        int(inactive.sum()),
        len(df),
        capture_date.date(),
        int(withdrawn.sum()),
        int(stalled.sum()),
        int((duplicate & ~stalled).sum()),
    )
    return df[~inactive].copy()
