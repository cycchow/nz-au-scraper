from datetime import date

from loveracing.loveracing import (
    decode_meetings_payload,
    generate_month_starts,
    parse_day_with_context,
    to_fixture_records,
)


def test_decode_d_string_payload():
    payload = {
        "d": '[{"Day":"Sat 28 Feb","DayID":54910,"Club":"Matamata RC","Racecourse":"Matamata"}]'
    }
    meetings = decode_meetings_payload(payload)
    assert isinstance(meetings, list)
    assert meetings[0]["DayID"] == 54910


def test_parse_day_with_context():
    parsed = parse_day_with_context("Sat 28 Feb", date(2026, 2, 1))
    assert parsed.isoformat() == "2026-02-28"


def test_generate_month_starts_desc_inclusive():
    months = generate_month_starts(date(2026, 2, 1), date(2025, 11, 1))
    assert [m.isoformat() for m in months] == ["2026-02-01", "2026-01-01", "2025-12-01", "2025-11-01"]


def test_to_fixture_records_mapping():
    meetings = [
        {
            "Day": "Sat 28 Feb",
            "DayID": 54910,
            "Club": "Matamata RC",
            "Racecourse": "Matamata",
            "ResultDownloadXML": "Race_54910.xml",
        }
    ]
    fixtures = to_fixture_records(meetings, date(2026, 2, 1))

    assert len(fixtures) == 1
    assert fixtures[0]["course"] == "Matamata"
    assert fixtures[0]["raceDate"].isoformat() == "2026-02-28"
    assert fixtures[0]["year"] == 2026
    assert fixtures[0]["meta"]["DayID"] == 54910
    assert fixtures[0]["meta"]["requestMonth"] == "2026-02"
