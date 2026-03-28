from utils.course_utils import get_surface_override, normalize_course


def test_normalize_course_removes_known_sponsor_prefixes():
    assert normalize_course("BET365 Bairnsdale") == "Bairnsdale"
    assert normalize_course("LADBROKES Pioneer Park") == "Pioneer Park"
    assert normalize_course("SOUTHSIDE Pakenham") == "Pakenham"
    assert normalize_course("SPORTSBET Mareeba") == "Mareeba"
    assert normalize_course("SPORTSBET-Port Lincoln") == "Port Lincoln"


def test_normalize_course_removes_prefixes_after_abbr_mapping(monkeypatch):
    monkeypatch.setitem(normalize_course.__globals__["course_abbr_mapping"], "SPB LINCOLN", "SPORTSBET Port Lincoln")

    assert normalize_course("SPB LINCOLN") == "Port Lincoln"


def test_get_surface_override_supports_course_level_dirt_overrides():
    assert get_surface_override("Darwin") == "DIRT"
    assert get_surface_override("DARWIN") == "DIRT"
    assert get_surface_override("Rosehill Gardens") is None
