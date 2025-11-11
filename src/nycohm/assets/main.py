import pandas as pd
from dagster import asset, Output, MetadataValue, AssetExecutionContext, AssetIn
import logging
from src.nycohm.helpers.log_config import configure_logging
from itertools import product
from math import prod

configure_logging()


# Create final joined dataset
@asset(
    ins={"agg_housing_metrics_cc": AssetIn(key=["agg_housing_metrics_cc"]),
    "agg_housing_metrics_cd": AssetIn(key=["agg_housing_metrics_cd"]),
    "agg_housing_metrics_ct": AssetIn(key=["agg_housing_metrics_ct"]),
    "agg_council_district": AssetIn(key=["agg_council_district"]),
    "agg_community_district": AssetIn(key=["agg_community_district"]),
    "agg_census_tract": AssetIn(key=["agg_census_tract"]),
     },
    name="main",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="main",
)
def main(agg_housing_metrics_cc,agg_housing_metrics_cd, agg_housing_metrics_ct,
         agg_council_district, agg_community_district, agg_census_tract) -> Output[pd.DataFrame]:

    # council district
    df_cc = agg_housing_metrics_cc
    # merge with district-level metrics
    df_cc = df_cc.merge(agg_council_district, left_on='Council_District',right_on='Council_District', how='left')
    # format
    df_cc = df_cc.rename(columns=lambda c: c[3:] if isinstance(c, str) and c.startswith("CC_") else c)
    df_cc['District_Type'] = 'Council_District'
    df_cc.rename(columns={'Council_District': 'District_Value'}, inplace=True)
    logging.info('check df_cc head: \n{}'.format(df_cc.head(5).to_string()))
    logging.info('check df_cc dtypes: \n{}'.format(df_cc.dtypes.to_string()))

    # community district
    df_cd = agg_housing_metrics_cd
    # merge with district-level metrics
    df_cd = df_cd.merge(agg_community_district, left_on='Community_District', right_on='Community_District', how='left')
    # format
    df_cd = df_cd.rename(columns=lambda c: c[3:] if isinstance(c, str) and c.startswith("CD_") else c)
    df_cd['District_Type'] = 'Community_District'
    df_cd.rename(columns={'Community_District': 'District_Value'}, inplace=True)
    logging.info('check df_cd head: \n{}'.format(df_cd.head(5).to_string()))
    logging.info('check df_cd dtypes: \n{}'.format(df_cd.dtypes.to_string()))

    # census tract
    df_ct = agg_housing_metrics_ct
    # merge with district-level metrics
    df_ct = df_ct.merge(agg_census_tract, left_on='Census_Tract', right_on='Census_Tract', how='left')
    # format
    df_ct = df_ct.rename(columns=lambda c: c[3:] if isinstance(c, str) and c.startswith("CT_") else c)
    df_ct['District_Type'] = 'Census_Tract'
    df_ct.rename(columns={'Census_Tract': 'District_Value'}, inplace=True)
    logging.info('check df_ct head: \n{}'.format(df_ct.head(5).to_string()))
    logging.info('check df_ct dtypes: \n{}'.format(df_ct.dtypes.to_string()))

    # join district and dimension-level aggregations
    df = pd.concat([df_cc, df_cd, df_ct], ignore_index=True)
    logging.info('check initial dimension-level agg merge: \n{}'.format(df.sample(n=20).to_string()))

    # convert geo key types
    df['District_Value'] = df['District_Value'].astype(str)

    # column order
    district_type_column = df.pop('District_Type')
    df.insert(0, 'District_Type', district_type_column)  # Insert 'Name' at index 0

    logging.info('rows after joining district-level metrics: {}'.format(len(df)))

    logging.info(df.head(20).to_string())

    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)

assets_main = [main]
