import re
import unicodedata
import pandas as pd
from typing import Tuple, Dict

_BQ_MAX_LEN = 300
_INVALID_CHARS = re.compile(r'[^A-Za-z0-9_]')
_START_OK = re.compile(r'[A-Za-z_]')

def _to_ascii(s: str) -> str:
    # Drop accents / non-ASCII by transliteration; keeps letters/numbers/underscore best.
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")

def _bq_sanitize_one(
    name: str,
    *,
    lowercase: bool,
    ascii_only: bool,
    replacement: str,
    max_len: int,
) -> str:
    if name is None:
        name = ""
    name = str(name).strip()

    if ascii_only:
        name = _to_ascii(name)
    if lowercase:
        name = name.lower()

    # Replace invalid characters with replacement (usually "_")
    name = _INVALID_CHARS.sub(replacement, name)

    # Must start with a letter or underscore
    if not name or not _START_OK.match(name[0]):
        name = f"_{name}"

    # Collapse multiple replacement characters
    if replacement:
        rep_esc = re.escape(replacement)
        name = re.sub(rf"{rep_esc}{{2,}}", replacement, name)

    # Enforce max length
    name = name[:max_len] if len(name) > max_len else name

    # Fallback if everything vanished
    if not name:
        name = "_col"

    return name

def sanitize_bq_columns(
    df: pd.DataFrame,
    *,
    lowercase: bool = False,
    ascii_only: bool = True,
    replacement: str = "_",
    max_len: int = _BQ_MAX_LEN,
    dedupe_suffix_sep: str = "_",
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Rename DataFrame columns so they satisfy BigQuery load rules:
      - Only letters, numbers, and underscores
      - Must start with a letter or underscore
      - Max length 300 chars (configurable)
      - Unique after renaming (adds suffixes if needed)

    Parameters
    ----------
    df : pd.DataFrame
    lowercase : bool
        Lowercase all column names before sanitizing (optional).
    ascii_only : bool
        Strip accents and non-ASCII characters.
    replacement : str
        Replacement character for invalid chars (default "_").
    max_len : int
        Maximum column name length (default 300).
    dedupe_suffix_sep : str
        Separator used before numeric suffix when deduplicating.

    Returns
    -------
    (new_df, mapping) : Tuple[pd.DataFrame, Dict[str, str]]
        A new DataFrame with renamed columns and a mapping {old_name: new_name}.
    """
    old_cols = list(df.columns)
    # First pass: sanitize each name
    sanitized = [
        _bq_sanitize_one(
            col,
            lowercase=lowercase,
            ascii_only=ascii_only,
            replacement=replacement,
            max_len=max_len,
        )
        for col in old_cols
    ]

    # Second pass: ensure uniqueness (account for collisions after sanitize/truncate)
    used = {}
    final_names = []
    for base in sanitized:
        candidate = base
        i = 1
        # If collision, append _2, _3, ... keeping under max_len
        while candidate in used:
            i += 1
            suffix = f"{dedupe_suffix_sep}{i}"
            # Reserve space for suffix if needed
            trim_len = max_len - len(suffix)
            if trim_len < 1:
                # Extreme edge case: max_len too small; fall back to suffix only
                candidate = suffix[-max_len:]
            else:
                candidate = base[:trim_len] + suffix
        used[candidate] = True
        final_names.append(candidate)

    mapping = {old: new for old, new in zip(old_cols, final_names)}
    new_df = df.rename(columns=mapping)
    return new_df, mapping

# -------------------------
# Example usage:
# df2, col_map = sanitize_bq_columns(df, lowercase=False)
# context.log.info(f"Renamed columns for BigQuery: {col_map}")