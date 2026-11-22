"""Pick the sitting Council Member for each council district.

The source is the NYC Council's own list on NYC Open Data, "City Council
Members (1999 to Present)", which gives each member's district and term dates.
The sitting member is the one whose term covers the date of the data refresh.

That list can lag behind a mid-term change (a resignation followed by a
special election). data/reference/council_member_overrides.csv fills such a
gap: a row there is treated as one more term and wins when it starts later.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

OVERRIDES_CSV = (
    Path(__file__).resolve().parents[3] / "data" / "reference" / "council_member_overrides.csv"
)
MEMBER_COLUMNS = ["Council_District", "Council_Member", "Term_Start", "Term_End"]


def load_overrides(path: Path | str = OVERRIDES_CSV) -> pd.DataFrame:
    """Hand-entered terms that the Council's list does not show yet (may be empty)."""
    path = Path(path)
    if not path.exists():
        return pd.DataFrame(columns=MEMBER_COLUMNS)
    return pd.read_csv(path, dtype=str)[MEMBER_COLUMNS]


def sitting_council_members(terms: pd.DataFrame, as_of, overrides: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    One row per council district that has a member in office on ``as_of``.

    terms / overrides: Council_District, Council_Member, Term_Start, Term_End.
    A blank Term_End means the term is still running. A district with no term
    covering the date (a vacancy, or a change the list has not caught up with)
    is left out rather than filled with the previous member.
    """
    def with_dates(frame: pd.DataFrame) -> pd.DataFrame:
        # parsed per source: the Council's list writes 01/31/2026, a hand-entered
        # override may use 2026-01-31
        frame = frame[MEMBER_COLUMNS].copy()
        frame["Term_Start"] = pd.to_datetime(frame["Term_Start"], errors="coerce")
        frame["Term_End"] = pd.to_datetime(frame["Term_End"], errors="coerce")
        return frame

    frames = [with_dates(terms)]
    if overrides is not None and len(overrides):
        frames.append(with_dates(overrides))
    df = pd.concat(frames, ignore_index=True)

    df["Council_District"] = pd.to_numeric(df["Council_District"], errors="coerce").astype("Int64")
    df["Council_Member"] = df["Council_Member"].astype("string").str.strip()
    df = df.dropna(subset=["Council_District", "Council_Member", "Term_Start"])

    as_of = pd.Timestamp(as_of).normalize()
    in_office = (df["Term_Start"] <= as_of) & (df["Term_End"].isna() | (df["Term_End"] >= as_of))
    df = df[in_office].sort_values(["Council_District", "Term_Start"])
    # if two terms overlap, the one that started most recently is the sitting member
    df = df.drop_duplicates(subset="Council_District", keep="last")

    df["Council_District"] = df["Council_District"].astype(str)
    return df[MEMBER_COLUMNS].reset_index(drop=True)
