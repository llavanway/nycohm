import pandas as pd
from scipy import stats
from .log_config import configure_logging
import logging


def check_data(data):
    """Perform quality checks on the dataset and log the results. data should be a pandas DataFrame."""
    logging.info(data.dtypes)

    # dataset quality checks
    logging.info('\nDATASET CHECKS')
    check_list = [
        'total rows:', len(data.index),
        'duplicated rows:', sum(i is True for i in data.duplicated(keep=False)),
        'rows with missing values:', data.isnull().any(axis=1).sum()
        # 'rows with duplicate primary key EmployeeNumber:',df.EmployeeNumber.duplicated(keep=False).loc[lambda x: x == True].count()
    ]

    for i in check_list: logging.info(i)

    # # attribute quality checks
    results = pd.DataFrame()
    results.index = data.columns.tolist()
    results['checked column'] = results.index

    results['dtype'] = ''
    results['sample'] = ''
    results['missing'] = ''
    results['outliers'] = ''

    # counts, means, modes, ranges, min/max

    for column in data:
        results.loc[column]['dtype'] = str(data[column].dtype)

        if len(data[column].unique()) <= 10:
            results.loc[column]['sample'] = ','.join(str(i) for i in data[column].unique())
        elif data.dtypes[column] == 'object':
            results.loc[column]['sample'] = 'many str values'
        elif data.dtypes[column] in ['float64', 'int32', 'datetime64[ns]']:
            results.loc[column]['sample'] = 'bounds: ' + str(data[column].min()) + '-' + str(data[column].max())
        else:
            results.loc[column]['sample'] = 'Unknown'

        results.loc[column]['missing'] = str(data[column].isnull().sum())

        if str(data[column].dtype) in ['int32', 'float64']:
            results.loc[column]['outliers'] = sum(i > 3 for i in abs(stats.zscore(data[column])))
        else:
            results.loc[column]['outliers'] = 'NA'

    logging.info(results)