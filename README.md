# New York City Open Housing Metrics (NYCOHM)
Making NYC housing information more accessible. Project status: pre-alpha.

Current dashboard location: [https://public.tableau.com/app/profile/luke.lavanway5938/viz/NYCOpenHousingMetricsDashboardpre-alphaversion/Sheet1#1](https://public.tableau.com/app/profile/luke.lavanway5938/viz/NYCOpenHousingMetricsDashboardpre-alphaversion/Sheet1#1)

## How to contribute

1. Clone the repo to your local machine.
2. Install all dependencies.
3. Develop. definitions.py sets the BACKEND variable to 'duckdb' by default. This allows you to explore a database on your local machine. Create the initial database by running dagster dev from the terminal and using the graphical webserver interface to materialize all assets.
4. Submit a pull request. If you are adding a data source, provide instructions on how to connect to the data source in the PR.
5. Project lead will handle updating the dashboard; alternatively, clone the dashboard and send the updated version to the project lead.
