import pandas as pd
from dagster import asset, Output, MetadataValue, AssetExecutionContext, AssetIn
import logging
from src.nycohm.helpers.log_config import configure_logging
from itertools import product

configure_logging()


# Create final joined dataset
@asset(
    ins={"housingdb_post2010_clean": AssetIn(key=["housingdb_post2010_clean"]),
         "affordable_housing_production_by_building_clean": AssetIn(key=["affordable_housing_production_by_building_clean"]),
         "agg_council_district": AssetIn(key=["agg_council_district"]),
         "agg_community_district": AssetIn(key=["agg_community_district"]),
         "agg_census_tract": AssetIn(key=["agg_census_tract"]),
         },
    name="main",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="main",
)
def main(housingdb_post2010_clean,affordable_housing_production_by_building_clean, agg_council_district,
         agg_community_district, agg_census_tract) -> Output[pd.DataFrame]:
    # get only needed columns from each dataset
    shared_columns = ['Project_Key','Community_District','Council_District','Census_Tract','source_dataset','Delivery_Status',
                      'Unit_Type','Housing_Units','Project_Start_Year',
                      'Project_Completion_Year','Borough','_ingested_at']
    housingdb_post2010_clean = housingdb_post2010_clean[shared_columns]
    affordable_housing_production_by_building_clean = affordable_housing_production_by_building_clean[shared_columns
    ]

    # union of housing with affordable
    df = pd.concat([housingdb_post2010_clean, affordable_housing_production_by_building_clean], ignore_index=True)
    logging.info('rows after union of housing with affordable: {}'.format(len(df)))

    # # Ensure all dimension values have >=1 row by creating cartesian product data (resolves map display issue)
    # c_cols = shared_columns.remove(['Project_Key','source_dataset','_ingested_at'])
    # # Collect unique non-null values for each column (sorted)
    # uniques = {c: sorted(df[c].dropna().unique().tolist()) for c in c_cols}
    # # Build Cartesian product
    # product_rows = list(product(*(uniques[c] for c in c_cols)))
    # # Create resulting dataframe
    # cartesian_df = pd.DataFrame(product_rows, columns=c_cols)
    # cartesian_df['Project_Key'] = 'placeholder_key'
    # cartesian_df['source_dataset'] = 'placeholder_dataset'
    # cartesian_df['_ingested_at'] = None
    # # Join with main dataset
    # df = pd.concat([df, cartesian_df], ignore_index=True)
    # logging.info('rows after addition of placeholder data: {}'.format(len(df)))

    # join aggregate metrics from council district, community district, census tract
    logging.info('check agg_council_district: \n{}'.format(agg_council_district.head(5).to_string()))
    logging.info('check agg_community_district: \n{}'.format(agg_community_district.head(5).to_string()))
    logging.info('check agg_census_tract: \n{}'.format(agg_census_tract.head(5).to_string()))
    df = df.merge(agg_council_district,left_on='Council_District',
                  right_on='Council_District',how='left')
    df = df.merge(agg_community_district, left_on='Community_District',
                  right_on='Community_District', how='left')
    df = df.merge(agg_census_tract, left_on='Census_Tract',
                  right_on='Census_Tract', how='left')

    logging.info('rows after joining aggregate metrics: {}'.format(len(df)))

    logging.info(df.head(20).to_string())

    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)

assets_main = [main]
