import pandas as pd
import numpy as np

def standardize_null_values(df: pd.DataFrame, null_replacement=pd.NA) -> pd.DataFrame:
    """
    Replace null-like values in a DataFrame with a standardized value.

    Args:
        df (pd.DataFrame): The input DataFrame.
        null_replacement: The value to replace null-like values with (default: pd.NA).

    Returns:
        pd.DataFrame: The DataFrame with standardized null values.
    """
    # Define null-like values to standardize
    null_like_values = ["", "null", "None", "NaN", "nan", "<NA>", np.nan, None, pd.NaT]

    # Replace null-like values with the standardized value
    df = df.replace(null_like_values, null_replacement)

    # Ensure proper handling of native pandas nulls
    df = df.where(df.notna(), null_replacement)

    return df