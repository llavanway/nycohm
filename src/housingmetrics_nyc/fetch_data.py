"""
Download the source datasets that are too large or change too often to commit.

Usage, from the repository root:

    python -m src.housingmetrics_nyc.fetch_data                # download any that are missing
    python -m src.housingmetrics_nyc.fetch_data --force        # re-download all (data refresh)
    python -m src.housingmetrics_nyc.fetch_data housing_database_project_level --force

Files are written to data/downloads/, which is not tracked by git. The ingest
assets call ensure_dataset(), so materializing them downloads a missing file
automatically; a refresh needs --force.
"""

import argparse
import csv
import json
import logging
import shutil
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

DOWNLOAD_DIR = Path(__file__).resolve().parents[2] / "data" / "downloads"
NYC_OPEN_DATA = "https://data.cityofnewyork.us"

# Each dataset is the full CSV export of an NYC Open Data dataset, identified by
# its dataset ID (the last part of the dataset's URL).
DATASETS = {
    "housing_database_project_level": {
        "dataset_id": "br6q-ssj3",
        "filename": "Housing_Database_Project_Level_Files.csv",
        "title": "DCP Housing Database Project Level Files",
    },
    "affordable_housing_production_by_building": {
        "dataset_id": "hg8x-zxpr",
        "filename": "Affordable_Housing_Production_by_Building.csv",
        "title": "HPD Affordable Housing Production by Building",
    },
}


def export_url(dataset_id: str) -> str:
    return f"{NYC_OPEN_DATA}/api/views/{dataset_id}/rows.csv?accessType=DOWNLOAD"


def dataset_page_url(dataset_id: str) -> str:
    return f"{NYC_OPEN_DATA}/d/{dataset_id}"


def _published_row_count(dataset_id: str) -> int:
    url = f"{NYC_OPEN_DATA}/resource/{dataset_id}.json?$select=count(*)%20as%20n"
    with urllib.request.urlopen(url, timeout=120) as response:
        return int(json.load(response)[0]["n"])


def _data_updated_at(dataset_id: str) -> str | None:
    """When NYC Open Data last changed the dataset's rows (not when we downloaded it)."""
    url = f"{NYC_OPEN_DATA}/api/views/{dataset_id}.json"
    with urllib.request.urlopen(url, timeout=120) as response:
        updated = json.load(response).get("rowsUpdatedAt")
    if updated is None:
        return None
    return datetime.fromtimestamp(int(updated), timezone.utc).isoformat(timespec="seconds")


def _csv_row_count(path: Path) -> int:
    csv.field_size_limit(10**9)
    with open(path, newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f)) - 1  # exclude header


def dataset_path(name: str) -> Path:
    return DOWNLOAD_DIR / DATASETS[name]["filename"]


def fetch_dataset(name: str) -> Path:
    """Download a dataset, check it is complete, and record where it came from."""
    source = DATASETS[name]
    target = dataset_path(name)
    partial = target.with_name(target.name + ".part")
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    url = export_url(source["dataset_id"])
    logging.info("Downloading %s from %s", source["title"], url)
    with urllib.request.urlopen(url, timeout=600) as response, open(partial, "wb") as out:
        shutil.copyfileobj(response, out)

    # the export is streamed without a length, so confirm nothing was cut off
    rows = _csv_row_count(partial)
    expected = _published_row_count(source["dataset_id"])
    if rows != expected:
        partial.unlink()
        raise RuntimeError(
            f"{source['title']}: downloaded {rows} rows but NYC Open Data reports {expected}"
        )

    partial.replace(target)
    target.with_name(target.name + ".source.json").write_text(
        json.dumps(
            {
                "title": source["title"],
                "dataset_page": dataset_page_url(source["dataset_id"]),
                "download_url": url,
                "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "data_updated_at": _data_updated_at(source["dataset_id"]),
                "rows": rows,
            },
            indent=2,
        )
        + "\n"
    )
    logging.info("Saved %s rows to %s", rows, target)
    return target


def dataset_source_info(name: str) -> dict:
    """The .source.json written next to a download, or {} if there is none."""
    info_path = dataset_path(name).with_name(dataset_path(name).name + ".source.json")
    return json.loads(info_path.read_text()) if info_path.exists() else {}


def ensure_dataset(name: str) -> Path:
    """Return the local path of a dataset, downloading it first if it is missing."""
    target = dataset_path(name)
    return target if target.exists() else fetch_dataset(name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "names", nargs="*", metavar="name", help=f"datasets to fetch (default: all): {', '.join(DATASETS)}"
    )
    parser.add_argument(
        "--force", action="store_true", help="re-download even if the file exists"
    )
    args = parser.parse_args()
    unknown = [name for name in args.names if name not in DATASETS]
    if unknown:
        parser.error(f"unknown dataset(s): {', '.join(unknown)}")
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    for name in args.names or DATASETS:
        path = fetch_dataset(name) if args.force else ensure_dataset(name)
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()
