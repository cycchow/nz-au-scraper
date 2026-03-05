from datetime import timedelta

from scrapers.loveracing_provider import LoveracingProvider
from loveracing import loveracing


def test_accepts_fixture_requires_day_id():
    provider = LoveracingProvider()
    assert provider.accepts_fixture({"src": "loveracing", "meta": {"DayID": 54916}})
    assert not provider.accepts_fixture({"src": "loveracing", "meta": {"ResultDownloadXML": "Race.xml"}})


def test_parse_fixture_skips_past_fixture():
    provider = LoveracingProvider()
    today_nz = loveracing.datetime.now(loveracing.NZ_TZ).date()
    fixture = {
        "fixtureId": 1,
        "raceDate": (today_nz - timedelta(days=1)).isoformat(),
        "course": "Ellerslie",
        "meta": {"DayID": 54916},
    }

    parsed = provider.parse_fixture(fixture)
    assert parsed.races == []
    assert parsed.results == []


def test_parse_fixture_processes_future_fixture(monkeypatch):
    provider = LoveracingProvider()
    today_nz = loveracing.datetime.now(loveracing.NZ_TZ).date()
    fixture = {
        "fixtureId": 2,
        "raceDate": (today_nz + timedelta(days=1)).isoformat(),
        "course": "Ellerslie",
        "meta": {"DayID": 54916},
    }

    calls = {"fetch": 0, "parse": 0}

    def fake_fetch(day_id: int):
        calls["fetch"] += 1
        assert day_id == 54916
        return "<html></html>"

    def fake_parse(html_text: str, fixture_ctx: dict):
        calls["parse"] += 1
        assert html_text == "<html></html>"
        assert fixture_ctx["meta"]["DayID"] == 54916
        return ([{"raceId": 600233545}], [{"raceId": 600233545, "horseNo": 1}])

    monkeypatch.setattr(loveracing, "fetch_meeting_overview_html", fake_fetch)
    monkeypatch.setattr(loveracing, "parse_meeting_overview_html", fake_parse)

    parsed = provider.parse_fixture(fixture)
    assert calls == {"fetch": 1, "parse": 1}
    assert len(parsed.races) == 1
    assert len(parsed.results) == 1


def test_parse_fixture_returns_empty_when_day_id_missing():
    provider = LoveracingProvider()
    today_nz = loveracing.datetime.now(loveracing.NZ_TZ).date()
    fixture = {
        "fixtureId": 3,
        "raceDate": today_nz.isoformat(),
        "course": "Ellerslie",
        "meta": {},
    }

    parsed = provider.parse_fixture(fixture)
    assert parsed.races == []
    assert parsed.results == []
