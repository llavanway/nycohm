from dagster import ConfigurableResource
from typing import Any
import pandas as pd

class DuckDBResource(ConfigurableResource):
    database: str = "dev.duckdb"

    def query_df(self, sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
        import duckdb
        with duckdb.connect(self.database) as con:
            return con.execute(sql, params or {}).fetch_df()

    def table(self, name: str) -> str:
        return name  # DuckDB tables are unqualified

class BigQueryResource(ConfigurableResource):
    project: str
    dataset: str

    def query_df(self, sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
        from google.cloud import bigquery
        client = bigquery.Client(project=self.project)
        job_config = None
        if params:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in params.items()
                ]
            )
        return client.query(sql, job_config=job_config).to_dataframe()

    def table(self, name: str) -> str:
        return f"{self.project}.{self.dataset}.{name}"  # Fully-qualified for BQ