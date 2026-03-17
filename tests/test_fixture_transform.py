import asyncio
from datetime import date, datetime

import main


def test_save_fixtures_nz_defaults(monkeypatch):
    captured = []

    def fake_send_merge_mutation(type_name, input_obj):
        captured.append((type_name, input_obj))
        return {"ok": True}

    monkeypatch.setattr(main, "send_merge_mutation", fake_send_merge_mutation)

    fixtures = [
        {
            "raceDate": date(2026, 2, 28),
            "course": "Matamata",
            "year": 2026,
            "meta": {"DayID": 54910},
        }
    ]

    main.save_fixtures(fixtures, country="NZ")

    assert len(captured) == 1
    type_name, payload = captured[0]
    assert type_name == "com.superstring.globalracing.uk.models.types.Fixture"
    assert payload["country"] == "NZ"
    assert payload["src"] == "loveracing"
    assert payload["fixtureId"] == 6000000000 + 54910
    assert payload["meetingId"] == 54910


def test_save_fixtures_uses_top_level_meeting_id(monkeypatch):
    captured = []

    def fake_send_merge_mutation(type_name, input_obj):
        captured.append((type_name, input_obj))
        return {"ok": True}

    monkeypatch.setattr(main, "send_merge_mutation", fake_send_merge_mutation)

    fixtures = [
        {
            "raceDate": date(2026, 3, 10),
            "course": "Flemington",
            "year": 2026,
            "meetingId": 700123456,
            "meta": {"race_meet_id": 123456},
        }
    ]

    main.save_fixtures(fixtures, country="AUS")

    assert len(captured) == 1
    _, payload = captured[0]
    assert payload["country"] == "AUS"
    assert payload["meetingId"] == 700123456
    assert payload["fixtureId"] == 8000000000 + 700123456


def test_save_fixtures_accepts_graphql_style_fixture(monkeypatch):
    captured = []

    def fake_send_merge_mutation(type_name, input_obj):
        captured.append((type_name, input_obj))
        return {"ok": True}

    monkeypatch.setattr(main, "send_merge_mutation", fake_send_merge_mutation)

    fixtures = [
        {
            "raceDate": "2026-03-06",
            "course": "Ellerslie",
            "fixtureYear": 2026,
            "country": "NZ",
            "meta": {"DayID": 54915, "ResultDownloadXML": "Race_54915.xml"},
        }
    ]

    main.save_fixtures(fixtures, country="NZ")

    _, payload = captured[0]
    assert payload["raceDate"] == "2026-03-06"
    assert payload["fixtureYear"] == 2026
    assert payload["meta"]["ResultDownloadXML"] == "Race_54915.xml"


def test_get_fixtures_from_graphql_includes_optional_src_filter(monkeypatch):
    captured = {}

    async def fake_graphql_subscribe(subscription, variables):
        captured["subscription"] = subscription
        captured["variables"] = variables
        yield {"getFixtures": {"fixtureId": 1, "src": "racingcom"}}
        yield {"getFixtures": {"fixtureId": 2, "src": "loveracing"}}

    monkeypatch.setattr(main, "graphql_subscribe", fake_graphql_subscribe)

    fixtures = asyncio.run(
        main.get_fixtures_from_graphql(
            date(2026, 3, 7),
            date(2026, 3, 7),
            country="AUS",
        )
    )

    assert "$src: String" not in captured["subscription"]
    assert "src: $src" not in captured["subscription"]
    assert captured["variables"] == {
        "from": "2026-03-07",
        "to": "2026-03-07",
        "fetchAll": True,
        "country": "AUS",
    }
    assert fixtures == [
        {"fixtureId": 1, "src": "racingcom"},
        {"fixtureId": 2, "src": "loveracing"},
    ]


def test_get_provider_for_fixture_uses_fixture_src():
    assert main.get_provider_for_fixture({"fixtureId": 1, "src": "racingcom"}).name == "racingcom"
    assert main.get_provider_for_fixture({"fixtureId": 2, "src": "loveracing"}).name == "loveracing"
    assert main.get_provider_for_fixture({"fixtureId": 3, "src": "unknown"}) is None


def test_process_fixtures_from_graphql_routes_by_fixture_src(monkeypatch):
    processed = []

    async def fake_get_fixtures_from_graphql(from_date, to_date, country):
        assert country == "AUS"
        return [
            {"fixtureId": 1, "src": "racingcom"},
            {"fixtureId": 2, "src": "loveracing"},
            {"fixtureId": 3, "src": "unknown"},
        ]

    def fake_process_fixture_record(provider, fixture):
        processed.append((provider.name, fixture["fixtureId"]))

    monkeypatch.setattr(main, "get_fixtures_from_graphql", fake_get_fixtures_from_graphql)
    monkeypatch.setattr(main, "process_fixture_record", fake_process_fixture_record)

    asyncio.run(main.process_fixtures_from_graphql(date(2026, 3, 7), date(2026, 3, 7), "AUS"))

    assert processed == [("racingcom", 1), ("loveracing", 2)]


def test_process_fixtures_from_graphql_applies_optional_source_filter(monkeypatch):
    processed = []

    async def fake_get_fixtures_from_graphql(from_date, to_date, country):
        return [
            {"fixtureId": 1, "src": "racingcom"},
            {"fixtureId": 2, "src": "loveracing"},
        ]

    def fake_process_fixture_record(provider, fixture):
        processed.append((provider.name, fixture["fixtureId"]))

    monkeypatch.setattr(main, "get_fixtures_from_graphql", fake_get_fixtures_from_graphql)
    monkeypatch.setattr(main, "process_fixture_record", fake_process_fixture_record)

    asyncio.run(
        main.process_fixtures_from_graphql(
            date(2026, 3, 7),
            date(2026, 3, 7),
            "AUS",
            source_filter="racingcom",
        )
    )

    assert processed == [("racingcom", 1)]


def test_save_races_serializes_datetime_as_isoformat(monkeypatch):
    captured = []

    def fake_send_merge_mutation(type_name, input_obj):
        captured.append((type_name, input_obj))
        return {"ok": True}

    monkeypatch.setattr(main, "send_merge_mutation", fake_send_merge_mutation)

    main.save_races(
        [
            {
                "raceId": 705449840,
                "raceNo": 6,
                "startTime": datetime(2026, 3, 8, 14, 25, 0),
                "startTimeZoned": datetime.fromisoformat("2026-03-08T14:25:00+11:00"),
                "raceDate": date(2026, 3, 8),
            }
        ]
    )

    assert captured == [
        (
            "com.superstring.globalracing.uk.models.types.Races",
            {
                "raceId": 705449840,
                "raceNo": 6,
                "startTime": "2026-03-08T14:25:00",
                "startTimeZoned": "2026-03-08T14:25:00+11:00",
                "raceDate": "2026-03-08",
            },
        )
    ]
