import pandas as pd

from src.housingmetrics_nyc.helpers.council_members import load_overrides, sitting_council_members

TERMS = pd.DataFrame(
    [
        # district, member, term start, term end (as the Council's list writes them)
        (1.0, "Earlier Member", "01/01/2022", "12/31/2025"),
        (1.0, "Current Member", "01/01/2026", "12/31/2029"),
        (2.0, "Left Midterm", "01/01/2026", "02/03/2026"),  # seat then unfilled in the list
        (3.0, "Open Ended", "01/01/2026", None),
        (4.0, "Predecessor", "01/01/2026", "12/31/2029"),
        (4.0, "Special Election Winner", "05/12/2026", "12/31/2029"),
        (None, "Before Districts Were Recorded", "01/01/1992", "12/31/1997"),
    ],
    columns=["Council_District", "Council_Member", "Term_Start", "Term_End"],
)


def _by_district(df):
    return dict(zip(df["Council_District"], df["Council_Member"]))


def test_picks_the_member_whose_term_covers_the_date():
    sitting = sitting_council_members(TERMS, "2026-11-22")
    assert _by_district(sitting) == {
        "1": "Current Member",
        "3": "Open Ended",
        "4": "Special Election Winner",  # the later of two overlapping terms
    }
    # a district with no current term is left out, not filled with the last member
    assert "2" not in set(sitting["Council_District"])


def test_as_of_date_changes_the_answer():
    assert _by_district(sitting_council_members(TERMS, "2025-06-30")) == {"1": "Earlier Member"}
    assert _by_district(sitting_council_members(TERMS, "2026-01-15"))["2"] == "Left Midterm"


def test_overrides_fill_a_gap_in_the_list():
    overrides = pd.DataFrame(
        [("2", "Entered By Hand", "2026-05-12", None)],
        columns=["Council_District", "Council_Member", "Term_Start", "Term_End"],
    )
    sitting = sitting_council_members(TERMS, "2026-11-22", overrides=overrides)
    assert _by_district(sitting)["2"] == "Entered By Hand"
    # before the override's term starts it has no effect
    assert "2" not in _by_district(sitting_council_members(TERMS, "2026-03-01", overrides=overrides))


def test_overrides_file_in_the_repo_is_readable():
    overrides = load_overrides()
    assert list(overrides.columns) == ["Council_District", "Council_Member", "Term_Start", "Term_End"]
    # whatever is in the file must be usable as terms
    sitting_council_members(TERMS, "2026-11-22", overrides=overrides)
