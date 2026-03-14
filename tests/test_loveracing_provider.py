from datetime import timedelta

import main
from scrapers.loveracing_provider import LoveracingProvider
from loveracing import loveracing


def test_accepts_fixture_requires_day_id():
    provider = LoveracingProvider()
    assert provider.accepts_fixture({"src": "loveracing", "meta": {"DayID": 54916}})
    assert not provider.accepts_fixture({"src": "loveracing", "meta": {"ResultDownloadXML": "Race.xml"}})


def test_parse_fixture_uses_xml_for_past_fixture(monkeypatch):
    provider = LoveracingProvider()
    today_nz = loveracing.datetime.now(loveracing.NZ_TZ).date()
    fixture = {
        "fixtureId": 1,
        "raceDate": (today_nz - timedelta(days=1)).isoformat(),
        "course": "Ellerslie",
        "meta": {"DayID": 54916, "ResultDownloadXML": "Race_54916.xml"},
    }

    calls = {"xml": 0, "parse_xml": 0}

    def fake_fetch_xml(day_id: int, filename: str):
        calls["xml"] += 1
        assert day_id == 54916
        assert filename == "Race_54916.xml"
        return "<meeting></meeting>"

    def fake_parse_xml(xml_text: str, fixture_ctx: dict, sectional_fetcher=None):
        calls["parse_xml"] += 1
        assert xml_text == "<meeting></meeting>"
        assert fixture_ctx["meta"]["DayID"] == 54916
        return ([{"raceId": 123}], [{"raceId": 123, "horseNo": 1}])

    monkeypatch.setattr(loveracing, "fetch_meeting_xml", fake_fetch_xml)
    monkeypatch.setattr(loveracing, "parse_meeting_xml", fake_parse_xml)

    parsed = provider.parse_fixture(fixture)
    assert calls == {"xml": 1, "parse_xml": 1}
    assert len(parsed.races) == 1
    assert len(parsed.results) == 1


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


def test_parse_fixture_recovers_missing_past_result_download_xml(monkeypatch):
    provider = LoveracingProvider()
    today_nz = loveracing.datetime.now(loveracing.NZ_TZ).date()
    fixture = {
        "fixtureId": 5,
        "raceDate": (today_nz - timedelta(days=1)).isoformat(),
        "course": "Ellerslie",
        "meta": {"DayID": 54915},
    }

    calls = {"lookup": 0, "xml": 0, "parse_xml": 0}

    def fake_lookup(day_id: int, meeting_date):
        calls["lookup"] += 1
        assert day_id == 54915
        assert meeting_date == today_nz - timedelta(days=1)
        return {"DayID": 54915, "ResultDownloadXML": "Race_54915.xml"}

    def fake_fetch_xml(day_id: int, filename: str):
        calls["xml"] += 1
        assert day_id == 54915
        assert filename == "Race_54915.xml"
        return "<meeting></meeting>"

    def fake_parse_xml(xml_text: str, fixture_ctx: dict, sectional_fetcher=None):
        calls["parse_xml"] += 1
        assert fixture_ctx["meta"]["ResultDownloadXML"] == "Race_54915.xml"
        return ([{"raceId": 123}], [{"raceId": 123, "horseNo": 1}])

    monkeypatch.setattr(loveracing, "fetch_meeting_result_by_day_id", fake_lookup)
    monkeypatch.setattr(loveracing, "fetch_meeting_xml", fake_fetch_xml)
    monkeypatch.setattr(loveracing, "parse_meeting_xml", fake_parse_xml)

    parsed = provider.parse_fixture(fixture)

    assert calls == {"lookup": 1, "xml": 1, "parse_xml": 1}
    assert parsed.races == [{"raceId": 123}]
    assert parsed.results == [{"raceId": 123, "horseNo": 1}]


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


def test_parse_fixture_merges_same_day_started_races_with_xml(monkeypatch):
    provider = LoveracingProvider()
    today_nz = loveracing.datetime.now(loveracing.NZ_TZ).date()
    fixture = {
        "fixtureId": 4,
        "raceDate": today_nz.isoformat(),
        "course": "Ellerslie",
        "meta": {"DayID": 54916, "ResultDownloadXML": "Race_54916.xml"},
    }

    now_nz = loveracing.datetime.now(loveracing.NZ_TZ).replace(hour=13, minute=0, second=0, microsecond=0)

    class FrozenDateTime(loveracing.datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return now_nz.replace(tzinfo=None)
            return now_nz.astimezone(tz)

    overview_races = [
        {"raceId": 600001, "raceNo": 1, "startTimeZoned": f"{today_nz.isoformat()}T12:30:00+13:00"},
        {"raceId": 600002, "raceNo": 2, "startTimeZoned": f"{today_nz.isoformat()}T14:00:00+13:00"},
    ]
    overview_results = [
        {"raceId": 600001, "horseNo": 1, "startTimeZoned": f"{today_nz.isoformat()}T12:30:00+13:00", "rank": None},
        {"raceId": 600002, "horseNo": 2, "startTimeZoned": f"{today_nz.isoformat()}T14:00:00+13:00", "rank": None},
    ]
    xml_races = [
        {"raceId": 600001, "raceNo": 1, "startTimeZoned": f"{today_nz.isoformat()}T12:30:00+13:00", "going": "Soft"}
    ]
    xml_results = [
        {"raceId": 600001, "horseNo": 1, "startTimeZoned": f"{today_nz.isoformat()}T12:30:00+13:00", "rank": 1}
    ]

    monkeypatch.setattr("scrapers.loveracing_provider.datetime", FrozenDateTime)
    monkeypatch.setattr(loveracing, "fetch_meeting_overview_html", lambda day_id: "<html></html>")
    monkeypatch.setattr(loveracing, "parse_meeting_overview_html", lambda html_text, fixture_ctx: (overview_races, overview_results))
    monkeypatch.setattr(loveracing, "fetch_meeting_xml", lambda day_id, filename: "<meeting></meeting>")
    monkeypatch.setattr(loveracing, "parse_meeting_xml", lambda xml_text, fixture_ctx, sectional_fetcher=None: (xml_races, xml_results))

    parsed = provider.parse_fixture(fixture)

    assert parsed.races == [xml_races[0], overview_races[1]]
    assert parsed.results == [xml_results[0], overview_results[1]]


def test_process_fixture_record_persists_enriched_fixture_meta(monkeypatch):
    provider = LoveracingProvider()
    fixture = {
        "fixtureId": 6000054915,
        "fixtureYear": 2026,
        "raceDate": "2026-03-06",
        "course": "Ellerslie",
        "country": "NZ",
        "src": "loveracing",
        "meta": {"DayID": 54915},
    }

    saved_fixtures = []
    saved_races = []
    saved_results = []

    def fake_parse_fixture(value):
        value["meta"]["ResultDownloadXML"] = "Race_54915.xml"
        return main.FixtureProcessOutput(races=[{"raceId": 1}], results=[{"raceId": 1, "horseNo": 2}])

    monkeypatch.setattr(provider, "parse_fixture", fake_parse_fixture)
    monkeypatch.setattr(main, "save_fixtures", lambda fixtures, country="NZ", provider=None: saved_fixtures.extend(fixtures))
    monkeypatch.setattr(main, "save_races", lambda races: saved_races.extend(races))
    monkeypatch.setattr(main, "save_results", lambda results: saved_results.extend(results))

    main.process_fixture_record(provider, fixture)

    assert saved_fixtures == [fixture]
    assert saved_fixtures[0]["meta"]["ResultDownloadXML"] == "Race_54915.xml"
    assert saved_races == [{"raceId": 1}]
    assert saved_results == [{"raceId": 1, "horseNo": 2}]
