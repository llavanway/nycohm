# New York City Open Housing Metrics (NYCOHM)
Making NYC housing information more accessible. Project status: pre-alpha.

All are welcome to contribute.

Current dashboard location: [https://public.tableau.com/app/profile/luke.lavanway5938/viz/NYCOpenHousingMetricsDashboardpre-alphaversion/Sheet1#1](https://public.tableau.com/app/profile/luke.lavanway5938/viz/NYCOpenHousingMetricsDashboardpre-alphaversion/Sheet1#1)

## Development

Install the project in editable mode so Dagster commands can locate the package:

```bash
pip install -e .
```

Start the Dagster webserver and trigger the job from the UI:

```bash
dagster dev
```

This uses the repository's `workspace.yaml` to load `nycohm.dagster_pipeline:defs`. Open
<http://localhost:3000> in your browser and launch the `nyc_housing_job`.

Alternatively, execute the job directly without the webserver:

```bash
dagster job execute -m nycohm.dagster_pipeline -j nyc_housing_job
```
