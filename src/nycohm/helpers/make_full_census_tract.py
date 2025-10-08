import pandas as pd
import numpy as np
import re

BORO_TO_COUNTY = {
    "Bronx": "005",
    "Manhattan": "061",
    "Brooklyn": "047",
    "Queens": "081",
    "Staten Island": "085",
}

def _to_tract6(val):
    """
    Normalize various tract inputs to the 6-digit Census tract code:
    - 239     -> '023900'   (239.00)
    - '24501' -> '024501'   (245.01)
    - '245.1' -> '024510'   (245.10)
    - '245.01'-> '024501'   (245.01)
    Returns pd.NA if input is null/empty/unparseable.
    """
    if pd.isna(val):
        return pd.NA

    s = str(val).strip()
    if s == "" or s.lower() in {"nan", "na", "<na>", "none"}:
        return pd.NA

    # Keep only digits and dot; if nothing useful remains -> NA
    s = re.sub(r"[^0-9.]", "", s)
    if s == "":
        return pd.NA

    # Case A: has a decimal (e.g., '245.01' or '245.1')
    if "." in s:
        int_part, frac = s.split(".", 1)
        if int_part == "" or not int_part.isdigit():
            return pd.NA
        # Use up to 2 decimal digits; pad to 2
        frac = re.sub(r"[^0-9]", "", frac)[:2].ljust(2, "0")
        # 4-digit base + 2-digit suffix
        return int_part.zfill(4) + frac

    # Case B: digits only (e.g., '239', '24501')
    digits = s
    if not digits.isdigit():
        return pd.NA

    if len(digits) <= 4:
        # Pure integer tract -> suffix '00'
        return digits.zfill(4) + "00"
    else:
        # Assume last two digits are the suffix (e.g., 24501 -> 245 + 01)
        base = digits[:-2]
        suffix = digits[-2:]
        return base.zfill(4) + suffix

def make_full_census_tract(df, tract_col="Census_Tract", borough_col="Borough"):
    """
    Adds:
      - Census_Tract6: 6-digit tract code (string)
      - County_FIPS: 3-digit county code from borough
      - Census_Tract_Full: 11-digit GEOID (or <NA> if not derivable)
    Leaves nulls as <NA> instead of erroring.
    """
    # County codes from Borough (null-safe)
    county = df[borough_col].map(BORO_TO_COUNTY)

    # Normalize tract to 6-digit code (null-safe)
    tract6 = df[tract_col].apply(_to_tract6)

    # Build full 11-digit GEOID where both pieces exist
    geoid11 = pd.Series(pd.NA, index=df.index, dtype="object")
    mask = (~county.isna()) & (~tract6.isna())
    geoid11.loc[mask] = "36" + county[mask] + tract6[mask]

    out = df.copy()
    out["County_FIPS"] = county
    out["Census_Tract6"] = tract6
    out["Census_Tract_Full"] = geoid11
    return out