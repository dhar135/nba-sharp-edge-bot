from utils.utils import normalize_name
from services.injuries import parse_injuries


def test_diacritics_stripped():
    assert normalize_name("Luka Dončić") == "luka doncic"


def test_periods_and_suffix_stripped():
    assert normalize_name("P.J. Washington Jr.") == "pj washington"


def test_apostrophe_stripped():
    assert normalize_name("De'Aaron Fox") == "deaaron fox"


def test_parse_injuries_matches_normalized_diacritic_name():
    payload = {
        "injuries": [
            {
                "displayName": "Dallas Mavericks",
                "injuries": [
                    {"status": "Out", "athlete": {"displayName": "Luka Dončić"}},
                ],
            }
        ]
    }
    blocked = parse_injuries(payload)
    assert normalize_name("Luka Doncic") in blocked
