# Refreshing the dashboard

The Tableau workbook reads two files, both written by the pipeline:

| File | Contents |
|---|---|
| `data/exports/main.csv` | Housing metrics, one row per district and dimension |
| `data/exports/nyc_combined.geojson` | Council district, community district and census tract boundaries |

The file names never change, so a refresh does not involve renaming or re-linking anything.

## Refresh steps

1. Download the latest source data:

   ```bash
   python -m src.housingmetrics_nyc.fetch_data --force
   ```

2. Re-materialize all assets (`dagster dev`, then "Materialize all"). The `dashboard_main` asset writes both files to `data/exports/`.
3. Open `dashboard/housingmetrics.nyc Dashboard.twbx` in Tableau Desktop.
4. Data menu > the data source > Extract > Refresh.
5. Check the About tab: the data dates come from the files, so they should show the new release.
6. Publish to Tableau Public, overwriting the existing workbook.

### First time only: point the workbook at the export files

The workbook still remembers the hand-exported files it was first built from (`main_20251114.csv` and `nyc_combined.geojson` in a Downloads folder). Before the first refresh:

1. Open the Data Source tab.
2. For each of the two connections, open its menu, choose Edit Connection, and pick the file in `data/exports/`.
3. Refresh the extract.

After that, step 4 above is all a refresh needs.

## What the workbook expects

`DASHBOARD_COLUMNS` in `src/housingmetrics_nyc/helpers/dashboard_export.py` lists the columns of `main.csv`, in order, with their types. The workbook refers to these names. Add new columns at the end; rename or remove one only together with the workbook.

Columns the dashboard relies on beyond the housing counts:

| Column | Used for |
|---|---|
| `District_Label` | Readable district names: "Council District 33 (Lincoln Restler)", "Brooklyn Community District 1", "Brooklyn Census Tract 555". Unique across all district types. |
| `Council_Member` | The sitting Council Member, for council districts |
| `Neighborhoods` | The neighborhoods a district covers, shown in tooltips, Explore Data and District Trends, and searched by "Find a district" |
| `Is_Placeholder` | 1 on the zero-unit rows that keep a district with no housing activity on the map; 0 on real rows |
| `Rank_Total_Housing_Units`, `Rank_Total_Affordable_Housing_Units`, `District_Count` | "Rank 5 of 51" in the district detail panel |
| `Housing_Data_Version`, `Housing_Data_As_Of` | DCP Housing Database release and the last day it covers, shown on the About tab |
| `Affordable_Data_As_Of` | When HPD last updated its dataset on NYC Open Data, shown on the About tab |
| `Council_Members_As_Of` | When the NYC Council last updated its member list on NYC Open Data, shown on the About tab |

### Districts with no housing activity

The map only draws a district that has rows for the metric being shown. So that a district which produced nothing shows as zero instead of going missing, every residential district gets four zero-unit rows (completed and in progress, new and preserved), marked `Is_Placeholder = 1`. Parks, airports, cemeteries and tracts with no residents get none and stay blank.

These rows have no start or completion year. They appear under "Null" in the year filters; unchecking "Null" hides the districts with no activity again.

### Council Members

After a refresh, check the `council_members_clean` asset's metadata in Dagster for `districts_without_a_listed_member`. Any district listed there shows as "no member listed" on the dashboard until the Council updates its list or you add a row to `data/reference/council_member_overrides.csv`.

## If materializing fails after a column change

The local DuckDB database keeps each table's old column list. When an asset gains or loses columns, delete `dev.duckdb` and materialize all assets again.
