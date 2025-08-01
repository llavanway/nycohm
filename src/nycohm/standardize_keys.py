import os
import pandas as pd

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '../../data/standardized')
os.makedirs(OUTPUT_DIR, exist_ok=True)

BORO_MAP = {
    'MN': 1,
    'BX': 2,
    'BK': 3,
    'QN': 4,
    'SI': 5
}

def standardize_community_board(val: str | float | int | None):
    if pd.isna(val):
        return pd.NA
    if isinstance(val, str):
        val = val.strip()
        if '-' in val:
            prefix, dist = val.split('-', 1)
            prefix = prefix.strip()
            dist = dist.strip()
            boro = BORO_MAP.get(prefix.upper())
            if boro is None:
                return pd.NA
            try:
                return int(f"{boro}{int(dist):02d}")
            except ValueError:
                return pd.NA
        try:
            val = float(val)
        except ValueError:
            return pd.NA
    return int(val)

def standardize_council(val):
    if pd.isna(val):
        return pd.NA
    try:
        return int(float(val))
    except ValueError:
        return pd.NA

def standardize_school(val):
    return standardize_council(val)

def standardize_census_tract(val):
    if pd.isna(val):
        return pd.NA
    s = str(int(float(val))) if isinstance(val, (int, float)) else str(val)
    s = s.strip()
    if len(s) > 6:
        s = s[-6:]
    return s.zfill(6)

def process_housing(path):
    df = pd.read_csv(path)
    df = df.rename(columns={
        'CommntyDst': 'community_district',
        'CouncilDst': 'council_district',
        'CenTract20': 'census_tract',
    })
    df['community_district'] = df['community_district'].apply(standardize_community_board)
    df['council_district'] = df['council_district'].apply(standardize_council)
    df['census_tract'] = df['census_tract'].apply(standardize_census_tract)
    out = os.path.join(OUTPUT_DIR, 'HousingDB_post2010_standardized.csv')
    df.to_csv(out, index=False)


def process_affordable(path):
    df = pd.read_csv(path)
    df = df.rename(columns={
        'Community Board': 'community_district',
        'Council District': 'council_district',
        'Census Tract': 'census_tract',
    })
    df['community_district'] = df['community_district'].apply(standardize_community_board)
    df['council_district'] = df['council_district'].apply(standardize_council)
    df['census_tract'] = df['census_tract'].apply(standardize_census_tract)
    out = os.path.join(OUTPUT_DIR, 'Affordable_Housing_Production_by_Building_standardized.csv')
    df.to_csv(out, index=False)


def process_school(path):
    df = pd.read_csv(path)
    df = df.rename(columns={'Geo Dist': 'school_district'})
    df['school_district'] = df['school_district'].apply(standardize_school)
    out = os.path.join(OUTPUT_DIR, 'Enrollment_Capacity_And_Utilization_Reports_standardized.csv')
    df.to_csv(out, index=False)


def process_transit(path):
    df = pd.read_csv(path)
    df = df.rename(columns={'Census ID': 'census_tract'})
    df['census_tract'] = df['census_tract'].apply(standardize_census_tract)
    out = os.path.join(OUTPUT_DIR, 'New_York_36_transit_census_tract_standardized.csv')
    df.to_csv(out, index=False)


def process_crosswalk(path, rename_map, output_name):
    df = pd.read_csv(path)
    df = df.rename(columns=rename_map)
    for col in df.columns:
        if 'community_district' in col:
            df[col] = df[col].apply(standardize_community_board)
        if 'council_district' in col:
            df[col] = df[col].apply(standardize_council)
        if 'school_district' in col:
            df[col] = df[col].apply(standardize_school)
        if 'census_tract' in col:
            df[col] = df[col].apply(standardize_census_tract)
    out = os.path.join(OUTPUT_DIR, output_name)
    df.to_csv(out, index=False)


if __name__ == '__main__':
    base = os.path.join(os.path.dirname(__file__), '../../data')
    process_housing(os.path.join(base, 'HousingDB_post2010.csv'))
    process_affordable(os.path.join(base, 'Affordable_Housing_Production_by_Building_20250731.csv'))
    process_school(os.path.join(base, 'Enrollment_Capacity_And_Utilization_Reports_20250731.csv'))
    process_transit(os.path.join(base, 'New York_36_transit_census_tract_2022.csv'))

    process_crosswalk(
        os.path.join(base, 'community_to_council.csv'),
        {'BoroCD': 'community_district', 'CounDist': 'council_district'},
        'community_to_council_standardized.csv'
    )
    process_crosswalk(
        os.path.join(base, 'community_to_school.csv'),
        {'BoroCD': 'community_district', 'SchoolDist': 'school_district'},
        'community_to_school_standardized.csv'
    )
    process_crosswalk(
        os.path.join(base, 'community_to_tract.csv'),
        {'BoroCD': 'community_district', 'CT2020': 'census_tract'},
        'community_to_tract_standardized.csv'
    )
    process_crosswalk(
        os.path.join(base, 'council_to_school.csv'),
        {'CounDist': 'council_district', 'SchoolDist': 'school_district'},
        'council_to_school_standardized.csv'
    )
    process_crosswalk(
        os.path.join(base, 'council_to_tract.csv'),
        {'CounDist': 'council_district', 'CT2020': 'census_tract'},
        'council_to_tract_standardized.csv'
    )