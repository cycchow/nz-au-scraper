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
            "meetingId": 123456,
            "meta": {"race_meet_id": 123456},
        }
    ]

    main.save_fixtures(fixtures, country="AUS")

    assert len(captured) == 1
    _, payload = captured[0]
    assert payload["country"] == "AUS"
    assert payload["meetingId"] == 123456
    assert payload["fixtureId"] == 7000000000 + 123456


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


def test_get_fixtures_from_graphql_uses_fetch_all_and_course_filters(monkeypatch):
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
            fetch_all=False,
            course="Flemington",
        )
    )

    assert "$course: String" in captured["subscription"]
    assert "course: $course" in captured["subscription"]
    assert captured["variables"] == {
        "from": "2026-03-07",
        "to": "2026-03-07",
        "fetchAll": False,
        "country": "AUS",
        "course": "Flemington",
    }
    assert fixtures == [
        {"fixtureId": 1, "src": "racingcom"},
        {"fixtureId": 2, "src": "loveracing"},
    ]


def test_get_provider_for_fixture_uses_fixture_src():
    assert main.get_provider_for_fixture({"fixtureId": 1, "src": "racingcom"}).name == "racingcom"
    assert main.get_provider_for_fixture({"fixtureId": 2, "src": "loveracing"}).name == "loveracing"
    assert main.get_provider_for_fixture({"fixtureId": 3, "src": "unknown"}) is None


def test_get_provider_for_race_uses_provider_accepts_race():
    assert main.get_provider_for_race({"raceId": 1, "country": "AUS", "meta": {"race_meet_id": 5191184}}).name == "racingcom"
    assert main.get_provider_for_race({"raceId": 11, "country": "AUS", "meta": {"meetingId": 705191184}}).name == "racingcom"
    assert main.get_provider_for_race({"raceId": 2, "country": "NZ", "meta": {"meetingId": 54910}}).name == "loveracing"
    assert main.get_provider_for_race({"raceId": 3, "meta": {"unknown": True}}) is None
    assert main.get_provider_for_race({"raceId": 4, "country": "AUS", "meta": {"meetingId": 54910}}) is None


def test_process_fixtures_for_races_from_graphql_routes_by_fixture_src(monkeypatch):
    processed = []

    async def fake_get_fixtures_from_graphql(from_date, to_date, country, fetch_all=False, course=None):
        assert country == "AUS"
        assert fetch_all is False
        return [
            {"fixtureId": 1, "src": "racingcom"},
            {"fixtureId": 2, "src": "loveracing"},
            {"fixtureId": 3, "src": "unknown"},
        ]

    def fake_process_fixture_for_races_record(provider, fixture):
        processed.append((provider.name, fixture["fixtureId"]))

    monkeypatch.setattr(main, "get_fixtures_from_graphql", fake_get_fixtures_from_graphql)
    monkeypatch.setattr(main, "process_fixture_for_races_record", fake_process_fixture_for_races_record)

    asyncio.run(main.process_fixtures_for_races_from_graphql(date(2026, 3, 7), date(2026, 3, 7), "AUS"))

    assert processed == [("racingcom", 1), ("loveracing", 2)]


def test_process_fixtures_for_races_from_graphql_applies_optional_source_filter(monkeypatch):
    processed = []

    async def fake_get_fixtures_from_graphql(from_date, to_date, country, fetch_all=False, course=None):
        return [
            {"fixtureId": 1, "src": "racingcom"},
            {"fixtureId": 2, "src": "loveracing"},
        ]

    def fake_process_fixture_for_races_record(provider, fixture):
        processed.append((provider.name, fixture["fixtureId"]))

    monkeypatch.setattr(main, "get_fixtures_from_graphql", fake_get_fixtures_from_graphql)
    monkeypatch.setattr(main, "process_fixture_for_races_record", fake_process_fixture_for_races_record)

    asyncio.run(
        main.process_fixtures_for_races_from_graphql(
            date(2026, 3, 7),
            date(2026, 3, 7),
            "AUS",
            source_filter="racingcom",
        )
    )

    assert processed == [("racingcom", 1)]


def test_get_races_from_graphql_uses_fetch_all_and_course_filters(monkeypatch):
    captured = {}

    async def fake_graphql_subscribe(subscription, variables):
        captured["subscription"] = subscription
        captured["variables"] = variables
        yield {"getRaces": {"raceId": 1}}
        yield {"getRaces": {"raceId": 2}}

    monkeypatch.setattr(main, "graphql_subscribe", fake_graphql_subscribe)

    races = asyncio.run(
        main.get_races_from_graphql(
            date(2026, 3, 7),
            date(2026, 3, 7),
            country="AUS",
            fetch_all=False,
            course="Flemington",
        )
    )

    assert "subscription getRaces" in captured["subscription"]
    assert "course: $course" in captured["subscription"]
    assert captured["variables"] == {
        "from": "2026-03-07",
        "to": "2026-03-07",
        "fetchAll": False,
        "country": "AUS",
        "course": "Flemington",
    }
    assert races == [{"raceId": 1}, {"raceId": 2}]


def test_process_races_for_results_from_graphql_routes_by_race_provider(monkeypatch):
    processed = []

    async def fake_get_races_from_graphql(from_date, to_date, country, fetch_all=False, course=None):
        return [
            {"raceId": 1, "country": "AUS", "meta": {"race_meet_id": 5191184}},
            {"raceId": 2, "country": "NZ", "meta": {"meetingId": 54910}},
            {"raceId": 3, "meta": {"unknown": True}},
        ]

    def fake_process_race_for_results_record(provider, race):
        processed.append((provider.name, race["raceId"]))

    monkeypatch.setattr(main, "get_races_from_graphql", fake_get_races_from_graphql)
    monkeypatch.setattr(main, "get_fixtures_from_graphql", lambda *args, **kwargs: asyncio.sleep(0, result=[]))
    monkeypatch.setattr(main, "process_race_for_results_record", fake_process_race_for_results_record)

    asyncio.run(main.process_races_for_results_from_graphql(date(2026, 3, 7), date(2026, 3, 7), "AUS"))

    assert processed == [("racingcom", 1), ("loveracing", 2)]


def test_process_races_for_results_from_graphql_applies_optional_source_filter(monkeypatch):
    processed = []

    async def fake_get_races_from_graphql(from_date, to_date, country, fetch_all=False, course=None):
        return [
            {"raceId": 1, "country": "AUS", "meta": {"race_meet_id": 5191184}},
            {"raceId": 2, "country": "NZ", "meta": {"meetingId": 54910}},
        ]

    def fake_process_race_for_results_record(provider, race):
        processed.append((provider.name, race["raceId"]))

    monkeypatch.setattr(main, "get_races_from_graphql", fake_get_races_from_graphql)
    monkeypatch.setattr(main, "get_fixtures_from_graphql", lambda *args, **kwargs: asyncio.sleep(0, result=[]))
    monkeypatch.setattr(main, "process_race_for_results_record", fake_process_race_for_results_record)

    asyncio.run(
        main.process_races_for_results_from_graphql(
            date(2026, 3, 7),
            date(2026, 3, 7),
            "AUS",
            source_filter="racingcom",
        )
    )

    assert processed == [("racingcom", 1)]


def test_process_races_for_results_from_graphql_enriches_missing_race_meta_from_fixture(monkeypatch):
    processed = []

    async def fake_get_races_from_graphql(from_date, to_date, country, fetch_all=False, course=None):
        return [
            {"raceId": 1, "raceDate": "2026-03-16", "course": "Hawkesbury", "country": "AUS", "meta": None},
        ]

    async def fake_get_fixtures_from_graphql(from_date, to_date, country, fetch_all=False, course=None):
        assert fetch_all is True
        return [
            {"fixtureId": 10, "raceDate": "2026-03-16", "course": "Hawkesbury", "country": "AUS", "src": "racingcom", "meta": {"race_meet_id": 5193255}},
        ]

    def fake_process_race_for_results_record(provider, race):
        processed.append((provider.name, race["raceId"], race["meta"]["race_meet_id"]))

    monkeypatch.setattr(main, "get_races_from_graphql", fake_get_races_from_graphql)
    monkeypatch.setattr(main, "get_fixtures_from_graphql", fake_get_fixtures_from_graphql)
    monkeypatch.setattr(main, "process_race_for_results_record", fake_process_race_for_results_record)

    asyncio.run(main.process_races_for_results_from_graphql(date(2026, 3, 16), date(2026, 3, 16), "AUS"))

    assert processed == [("racingcom", 1, 5193255)]


def test_process_races_for_results_from_graphql_backfills_missing_meta_fields_from_fixture(monkeypatch):
    processed = []

    async def fake_get_races_from_graphql(from_date, to_date, country, fetch_all=False, course=None):
        return [
            {
                "raceId": 1,
                "raceDate": "2026-03-07",
                "course": "Flemington",
                "country": "AUS",
                "meta": {
                    "meetingId": 5191184,
                    "race_meet_id": 5191184,
                    "race": {"id": "5435938"},
                },
            },
        ]

    async def fake_get_fixtures_from_graphql(from_date, to_date, country, fetch_all=False, course=None):
        assert fetch_all is True
        return [
            {
                "fixtureId": 10,
                "raceDate": "2026-03-07",
                "course": "Flemington",
                "country": "AUS",
                "src": "racingcom",
                "meta": {"race_meet_id": 5191184, "meetingId": 5191184, "state": "VIC"},
            },
        ]

    def fake_process_race_for_results_record(provider, race):
        processed.append((provider.name, race["raceId"], race["meta"]["state"], race["meta"]["race"]["id"]))

    monkeypatch.setattr(main, "get_races_from_graphql", fake_get_races_from_graphql)
    monkeypatch.setattr(main, "get_fixtures_from_graphql", fake_get_fixtures_from_graphql)
    monkeypatch.setattr(main, "process_race_for_results_record", fake_process_race_for_results_record)

    asyncio.run(main.process_races_for_results_from_graphql(date(2026, 3, 7), date(2026, 3, 7), "AUS"))

    assert processed == [("racingcom", 1, "VIC", "5435938")]


def test_process_fixture_for_races_record_saves_cards_when_provider_supports_them(monkeypatch):
    provider = main.get_provider("racingcom")
    saved_races = []
    saved_cards = []

    monkeypatch.setattr(provider, "parse_fixture_races", lambda fixture: [{"raceId": 1, "raceNo": 1}])
    monkeypatch.setattr(provider, "parse_fixture_cards", lambda fixture, races: [{"raceId": 1, "horseNo": 1, "rank": None}])
    monkeypatch.setattr(main, "save_races", lambda races: saved_races.extend(races))
    monkeypatch.setattr(main, "save_results", lambda cards: saved_cards.extend(cards))

    main.process_fixture_for_races_record(
        provider,
        {"fixtureId": 7005193246, "raceDate": "2026-03-21", "course": "Rosehill Gardens", "meta": {"race_meet_id": 5193246}},
    )

    assert saved_races == [{"raceId": 1, "raceNo": 1}]
    assert saved_cards == [{"raceId": 1, "horseNo": 1, "rank": None}]


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


def test_save_results_falls_back_to_single_row_merges_when_batch_fails(monkeypatch):
    batched = []
    singles = []

    def fake_send_add_results_mutation(input_objs):
        batched.append(input_objs)
        raise RuntimeError("boom")

    def fake_send_merge_mutation(type_name, input_obj):
        singles.append((type_name, input_obj))
        return {"ok": True}

    monkeypatch.setattr(main, "send_add_results_mutation", fake_send_add_results_mutation)
    monkeypatch.setattr(main, "send_merge_mutation", fake_send_merge_mutation)

    main.save_results(
        [
            {"raceId": 1, "horseNo": 1, "meta": {"horse": {"raceNumber": 1}}},
            {"raceId": 1, "horseNo": 2, "meta": {"horse": {"raceNumber": 1}}},
        ]
    )

    assert len(batched) == 1
    assert len(singles) == 2
    assert [payload["horseNo"] for _, payload in singles] == [1, 2]


def test_save_results_recursively_splits_failed_batches(monkeypatch):
    batched = []
    singles = []

    def fake_send_add_results_mutation(input_objs):
        batched.append(len(input_objs))
        if len(input_objs) > 2:
            raise RuntimeError("too big")
        return {"ok": True}

    def fake_send_merge_mutation(type_name, input_obj):
        singles.append((type_name, input_obj))
        return {"ok": True}

    monkeypatch.setattr(main, "send_add_results_mutation", fake_send_add_results_mutation)
    monkeypatch.setattr(main, "send_merge_mutation", fake_send_merge_mutation)

    main._send_results_batch_with_fallback(
        [
            {"raceId": 1, "horseNo": 1},
            {"raceId": 1, "horseNo": 2},
            {"raceId": 1, "horseNo": 3},
            {"raceId": 1, "horseNo": 4},
        ],
    )

    assert batched == [4, 2, 2]
    assert singles == []


def test_batched_merge_chunks_uses_race_batch_limit():
    payloads = [{"raceId": 1, "horseNo": index} for index in range(1, 6)]

    race_chunks = main._batched_merge_chunks(
        "com.superstring.globalracing.uk.models.types.Races",
        payloads,
        "race",
    )

    assert [len(chunk) for chunk in race_chunks] == [5]


def test_dict_to_graphql_input_escapes_newlines_and_quotes():
    from utils.graphql_client import dict_to_graphql_input

    query_input = dict_to_graphql_input(
        {
            "text": "line 1\nline \"2\"",
            "nested": {"note": "a\r\nb\tc"},
        }
    )

    assert 'text: "line 1\\nline \\"2\\""' in query_input
    assert 'note: "a\\r\\nb\\tc"' in query_input
