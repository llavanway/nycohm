# New York City Open Housing Metrics (NYCOHM)
Making NYC housing information more accessible. Project status: pre-alpha.

All are welcome to contribute.

Current dashboard location: [https://public.tableau.com/app/profile/luke.lavanway5938/viz/NYCOpenHousingMetricsDashboardpre-alphaversion/Sheet1#1](https://public.tableau.com/app/profile/luke.lavanway5938/viz/NYCOpenHousingMetricsDashboardpre-alphaversion/Sheet1#1)

## Development

Install the project in editable mode so Dagster commands can locate the package:

```bash
pip install -e .
```

Run the Dagster webserver or execute the job directly:

```bash
dagster dev
# or
dagster job execute -m nycohm.dagster_pipeline -j nyc_housing_job
```
