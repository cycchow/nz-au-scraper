from datetime import date

from loveracing import loveracing
from scrapers.loveracing_provider import LoveracingProvider


def test_fetch_month_meetings_with_calendar_merge_current_month(monkeypatch):
    calls: list[str] = []

    def fake_fetch_calendar_events(month_start, today=None):
        calls.append("calendar")
        return [{"DayID": 200}, {"DayID": 300}]

    def fake_fetch_month_meetings(month_start):
        calls.append("results")
        return [{"DayID": 100}, {"DayID": 200}]

    monkeypatch.setattr(loveracing, "fetch_calendar_events", fake_fetch_calendar_events)
    monkeypatch.setattr(loveracing, "fetch_month_meetings", fake_fetch_month_meetings)

    meetings = loveracing.fetch_month_meetings_with_calendar_merge(
        date(2026, 3, 1),
        today=date(2026, 3, 5),
    )

    assert calls == ["calendar", "results"]
    assert [item["DayID"] for item in meetings] == [100, 200, 300]


def test_fetch_month_meetings_with_calendar_merge_non_current_month(monkeypatch):
    calls: list[str] = []

    def fake_fetch_month_meetings(month_start):
        calls.append("results")
        return [{"DayID": 100}]

    def fail_fetch_calendar_events(month_start, today=None):
        calls.append("calendar")
        raise AssertionError("calendar should not be called for non-current month")

    monkeypatch.setattr(loveracing, "fetch_month_meetings", fake_fetch_month_meetings)
    monkeypatch.setattr(loveracing, "fetch_calendar_events", fail_fetch_calendar_events)

    meetings = loveracing.fetch_month_meetings_with_calendar_merge(
        date(2026, 2, 1),
        today=date(2026, 3, 5),
    )

    assert calls == ["results"]
    assert meetings == [{"DayID": 100}]


def test_fetch_month_meetings_with_calendar_merge_calendar_failure_continues(monkeypatch, caplog):
    def fail_fetch_calendar_events(month_start, today=None):
        raise RuntimeError("calendar failed")

    def fake_fetch_month_meetings(month_start):
        return [{"DayID": 111}]

    monkeypatch.setattr(loveracing, "fetch_calendar_events", fail_fetch_calendar_events)
    monkeypatch.setattr(loveracing, "fetch_month_meetings", fake_fetch_month_meetings)

    meetings = loveracing.fetch_month_meetings_with_calendar_merge(
        date(2026, 3, 1),
        today=date(2026, 3, 5),
    )

    assert meetings == [{"DayID": 111}]
    assert "GetCalendarEvents failed for current month 2026-03" in caplog.text


def test_provider_fetch_fixtures_uses_calendar_merge_helper(monkeypatch):
    provider = LoveracingProvider()
    called_months: list[date] = []

    monkeypatch.setattr(
        loveracing,
        "generate_month_starts",
        lambda from_month, to_month: [date(2026, 3, 1), date(2026, 2, 1)],
    )

    def fake_fetch_month_meetings_with_calendar_merge(month_start):
        called_months.append(month_start)
        return [{"Day": "Sat 01 Mar", "DayID": 1, "Racecourse": "A", "Club": "A"}]

    monkeypatch.setattr(
        loveracing,
        "fetch_month_meetings_with_calendar_merge",
        fake_fetch_month_meetings_with_calendar_merge,
    )
    monkeypatch.setattr(
        loveracing,
        "to_fixture_records",
        lambda meetings, month_start: [{"month": month_start, "count": len(meetings)}],
    )

    fixtures = provider.fetch_fixtures_for_ingestion(date(2026, 3, 1), date(2026, 2, 1))

    assert called_months == [date(2026, 3, 1), date(2026, 2, 1)]
    assert fixtures == [
        {"month": date(2026, 3, 1), "count": 1},
        {"month": date(2026, 2, 1), "count": 1},
    ]
