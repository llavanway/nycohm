# housingmetrics.nyc
The easiest way to keep track of housing production in all of New York City's neighborhoods.

Access the dashboard: [https://public.tableau.com/app/profile/luke.lavanway5938/viz/NYCOpenHousingMetricsDashboardpre-alphaversion/Sheet1#1](https://public.tableau.com/app/profile/luke.lavanway5938/viz/housingmetrics_nycDashboard/Map)

Questions or issues? Email luke (at) housingmetrics.nyc

## Getting the data

The two housing datasets are large and are republished regularly, so they are not stored in this repo. They are downloaded from NYC Open Data into `data/downloads/` (ignored by git):

| Dataset | NYC Open Data ID | Updated |
|---|---|---|
| [DCP Housing Database Project Level Files](https://data.cityofnewyork.us/d/br6q-ssj3) | `br6q-ssj3` | Twice a year |
| [HPD Affordable Housing Production by Building](https://data.cityofnewyork.us/d/hg8x-zxpr) | `hg8x-zxpr` | Quarterly |

Materializing the ingest assets downloads any file that is missing. To download them yourself, or to refresh to the latest release, run from the repo root:

```bash
python -m src.nycohm.fetch_data          # download any that are missing
python -m src.nycohm.fetch_data --force  # re-download the latest release
```

Each download is checked against the row count NYC Open Data reports, and a `.source.json` file is written next to it recording the source URL and download time. After a refresh, re-materialize all assets. The direct CSV link for any dataset is `https://data.cityofnewyork.us/api/views/<ID>/rows.csv?accessType=DOWNLOAD`.

The smaller reference files (crosswalks, boundaries, census population, transit access) are static and remain in `data/`. All sources are listed in [docs/web_sources.md](docs/web_sources.md).

## How to contribute

1. Clone the repo to your local machine.
2. Install dependencies.
3. Set up your dev database. definitions.py sets the BACKEND variable to 'duckdb' by default, which points Dagster to a DuckDB instance created on your machine. Run dagster dev from the terminal, then use the graphical webserver interface to materialize all assets. This creates a DuckDB instance on your machine where you can query all project assets.
4. Make your changes and submit a pull request. If you are adding a data source, provide instructions on how to access the data source in the PR.
5. For dashboard changes, clone the dashboard and send in the updated Tableau workbook.
