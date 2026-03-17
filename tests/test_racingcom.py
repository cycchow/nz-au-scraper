import json
from datetime import date
from pathlib import Path

import main
from racingcom import racingcom
from scrapers.racingcom_provider import RacingComProvider


def test_provider_registered():
    provider = main.get_provider("racingcom")
    assert provider.name == "racingcom"
    assert provider.default_country == "AUS"


def test_iter_month_starts_desc_inclusive():
    months = racingcom.iter_month_starts(date(2026, 3, 1), date(2025, 12, 1))
    assert [m.isoformat() for m in months] == ["2026-03-01", "2026-02-01", "2026-01-01", "2025-12-01"]


def test_extract_custom_site_config_parses_graphql_fields():
    js_text = (
        "en.customSiteConfig = er.env.CUSTOM_SITE_CONFIG || "
        "'{\"environmentName\":\"PRODUCTION\",\"appSyncGraphQLHost\":\"https://graphql.api.racing.com\","
        "\"appSyncGraphQLAPIKey\":\"test-key\"}';"
    )

    config = racingcom.extract_custom_site_config(js_text)

    assert config["appSyncGraphQLHost"] == "https://graphql.api.racing.com"
    assert config["appSyncGraphQLAPIKey"] == "test-key"


def test_runtime_config_from_form_config_parses_calendar_and_race_keys():
    config_text = (
        'window.siteConfig={GraphqlEndpoint:\\"https://graphql.rmdprod.racing.com/\\",'
        'ChampionDataEndpoint:\\"https://graphql.rmdprod.racing.com/\\",'
        'ChampionDataEndpointKey:\\"race-key\\",'
        'DxpExternalDataApiKey:\\"calendar-key\\",'
        'DxpExternalDataUrl:\\"https://graphql.api.racing.com\\"};'
    )

    runtime = racingcom.runtime_config_from_form_config(config_text)

    assert runtime == {
        "appSyncGraphQLHost": "https://graphql.api.racing.com",
        "appSyncGraphQLAPIKey": "calendar-key",
        "raceDetailsGraphQLHost": "https://graphql.rmdprod.racing.com/",
        "raceDetailsGraphQLAPIKey": "race-key",
    }


def test_graphql_api_key_for_host_uses_host_specific_defaults():
    assert racingcom.graphql_api_key_for_host("https://graphql.api.racing.com", "wrong-key") == racingcom.DEFAULT_CALENDAR_API_KEY
    assert (
        racingcom.graphql_api_key_for_host("https://graphql.rmdprod.racing.com/", "wrong-key")
        == racingcom.DEFAULT_RACE_DETAILS_API_KEY
    )
    assert racingcom.graphql_api_key_for_host("https://example.com/graphql", "discovered-key") == "discovered-key"


def test_extract_graphql_clients_parses_host_key_pairs_from_bundle_js():
    js_text = """
          , st = new kc.GraphQLClient("https://graphql.rmdprod.racing.com/",{
            method: "GET",
            headers: {
                "x-api-key": "da2-6nsi4ztsynar3l3frgxf77q5fe",
                "content-type": "application/json"
            }
        })
          , lt = new kc.GraphQLClient("https://graphql.api.racing.com/",{
            method: "GET",
            headers: {
                "x-api-key": "da2-r5s52y73i5c7vi6vxflvfdufsa",
                "content-type": "application/json"
            }
        })
    """

    clients = racingcom.extract_graphql_clients(js_text)

    assert clients == {
        "https://graphql.rmdprod.racing.com/": "da2-6nsi4ztsynar3l3frgxf77q5fe",
        "https://graphql.api.racing.com/": "da2-r5s52y73i5c7vi6vxflvfdufsa",
    }


def test_normalize_jockey_name_uses_url_then_shared_mapping(monkeypatch):
    monkeypatch.setattr(racingcom, "get_jockey_full_name", lambda name: "JAMES MCDONALD" if name == "James Mc Donald" else name.upper())

    assert racingcom.normalize_jockey_name("/jockeys/james-mc-donald-775359", "J.B.McDonald") == "James Mcdonald"
    assert racingcom.normalize_jockey_name("/jockeys/tim-clark-614120", "T.Clark") == "Tim Clark"


def test_normalize_trainer_name_uses_url_when_name_is_abbreviated():
    assert (
        racingcom.normalize_trainer_name("/trainers/chris-waller-451879", "C.Waller")
        == "CHRIS WALLER"
    )
    assert (
        racingcom.normalize_trainer_name("/trainers/gai-waterhouse-adrian-bott-20658331", "Gai Waterhouse & Adrian Bott")
        == "GAI WATERHOUSE & ADRIAN BOTT"
    )


def test_discover_graphql_clients_fetches_app_shell_and_parses_bundle():
    class FakeResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, headers=None, timeout=None):
            self.calls.append(url)
            if url == "https://www.racing.com/form/2026-03-07/flemington/race/1":
                return FakeResponse(
                    """
                    <html>
                      <head></head>
                      <body>
                        <script src="/_next/static/chunks/app-race.js"></script>
                      </body>
                    </html>
                    """
                )
            if url == "https://www.racing.com/_next/static/chunks/app-race.js":
                return FakeResponse(
                    """
                      , st = new kc.GraphQLClient("https://graphql.rmdprod.racing.com/",{
                        method: "GET",
                        headers: {
                            "x-api-key": "da2-6nsi4ztsynar3l3frgxf77q5fe",
                            "content-type": "application/json"
                        }
                    })
                      , lt = new kc.GraphQLClient("https://graphql.api.racing.com/",{
                        method: "GET",
                        headers: {
                            "x-api-key": "da2-r5s52y73i5c7vi6vxflvfdufsa",
                            "content-type": "application/json"
                        }
                    })
                    """
                )
            raise AssertionError(f"Unexpected URL {url}")

    session = FakeSession()

    clients = racingcom.discover_graphql_clients(
        session=session,
        page_url="https://www.racing.com/form/2026-03-07/flemington/race/1",
    )

    assert session.calls == [
        "https://www.racing.com/form/2026-03-07/flemington/race/1",
        "https://www.racing.com/_next/static/chunks/app-race.js",
    ]
    assert clients == {
        "https://graphql.rmdprod.racing.com/": "da2-6nsi4ztsynar3l3frgxf77q5fe",
        "https://graphql.api.racing.com/": "da2-r5s52y73i5c7vi6vxflvfdufsa",
    }


def test_discover_runtime_config_uses_calendar_page_form_config():
    class FakeResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, headers=None, timeout=None):
            self.calls.append(url)
            if url == racingcom.CALENDAR_PAGE_URL:
                return FakeResponse("<html><body>calendar</body></html>")
            if url == racingcom.FORM_CONFIG_URL:
                return FakeResponse(
                    'window.siteConfig={GraphqlEndpoint:\\"https://graphql.rmdprod.racing.com/\\",'
                    'ChampionDataEndpoint:\\"https://graphql.rmdprod.racing.com/\\",'
                    'ChampionDataEndpointKey:\\"race-key\\",'
                    'DxpExternalDataApiKey:\\"calendar-key\\",'
                    'DxpExternalDataUrl:\\"https://graphql.api.racing.com\\"};'
                )
            raise AssertionError(f"Unexpected URL {url}")

    session = FakeSession()

    runtime = racingcom.discover_runtime_config(session=session)

    assert session.calls == [racingcom.CALENDAR_PAGE_URL, racingcom.FORM_CONFIG_URL]
    assert runtime == {
        "appSyncGraphQLHost": "https://graphql.api.racing.com",
        "appSyncGraphQLAPIKey": "calendar-key",
        "raceDetailsGraphQLHost": "https://graphql.rmdprod.racing.com/",
        "raceDetailsGraphQLAPIKey": "race-key",
    }


def test_discover_runtime_config_caches_live_lookup(monkeypatch):
    racingcom._RUNTIME_CONFIG_CACHE = None

    class FakeResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, headers=None, timeout=None):
            self.calls.append(url)
            if url == racingcom.CALENDAR_PAGE_URL:
                return FakeResponse("<html><body>calendar</body></html>")
            if url == racingcom.FORM_CONFIG_URL:
                return FakeResponse(
                    'window.siteConfig={GraphqlEndpoint:\\"https://graphql.rmdprod.racing.com/\\",'
                    'ChampionDataEndpoint:\\"https://graphql.rmdprod.racing.com/\\",'
                    'ChampionDataEndpointKey:\\"race-key\\",'
                    'DxpExternalDataApiKey:\\"calendar-key\\",'
                    'DxpExternalDataUrl:\\"https://graphql.api.racing.com\\"};'
                )
            raise AssertionError(f"Unexpected URL {url}")

        def close(self):
            return None

    fake_session = FakeSession()

    monkeypatch.setattr(racingcom.requests, "Session", lambda: fake_session)

    runtime1 = racingcom.discover_runtime_config()
    runtime2 = racingcom.discover_runtime_config()

    assert fake_session.calls == [racingcom.CALENDAR_PAGE_URL, racingcom.FORM_CONFIG_URL]
    assert runtime1 == runtime2
    assert runtime1["appSyncGraphQLAPIKey"] == "calendar-key"
    racingcom._RUNTIME_CONFIG_CACHE = None


def test_transform_calendar_item_mapping():
    raw_item = {
        "id": "event-1",
        "name": "Flemington Races",
        "race_meet_id": 12345,
        "location_name": "Flemington",
        "club_name": "VRC",
        "event_start_time": "2026-03-18T01:30:00Z",
    }

    out = racingcom.transform_calendar_item(raw_item, 2026, 3)

    assert out is not None
    assert out["raceDate"].isoformat() == "2026-03-18"
    assert out["course"] == "Flemington"
    assert out["meetingId"] == 700012345
    assert out["year"] == 2026
    assert out["meta"]["requestYear"] == 2026
    assert out["meta"]["requestMonth"] == 3
    assert out["meta"]["race_meet_id"] == 12345


def test_provider_fetch_fixtures_iterates_months(monkeypatch):
    provider = RacingComProvider()

    monkeypatch.setattr(
        racingcom,
        "discover_runtime_config",
        lambda: {
            "appSyncGraphQLHost": "https://graphql.api.racing.com",
            "appSyncGraphQLAPIKey": "api-key",
            "raceDetailsGraphQLHost": "https://graphql.rmdprod.racing.com/",
            "raceDetailsGraphQLAPIKey": "race-api-key",
        },
    )

    call_months = []

    def fake_fetch_calendar_items(session, graphql_host, api_key, year, month):
        call_months.append((year, month))
        return [
            {
                "id": f"evt-{year}-{month}",
                "race_meet_id": 1000 + month,
                "event_start_time": f"{year}-{month:02d}-15T10:00:00Z",
                "location_name": f"Course-{month}",
                "name": "Fallback",
            }
        ]

    monkeypatch.setattr(racingcom, "fetch_calendar_items", fake_fetch_calendar_items)

    fixtures = provider.fetch_fixtures_for_ingestion(date(2026, 3, 1), date(2026, 1, 1))

    assert call_months == [(2026, 3), (2026, 2), (2026, 1)]
    assert len(fixtures) == 3
    assert fixtures[0]["meetingId"] == 700001003
    assert fixtures[-1]["meetingId"] == 700001001


def test_fetch_races_for_meet_falls_back_to_race_details_host():
    class FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.calls.append((url, headers["x-api-key"]))
            if len(self.calls) == 1:
                return FakeResponse({"data": None, "errors": [{"message": "Cannot query field"}]})
            return FakeResponse({"data": {"getNoCacheRacesForMeet": [{"id": "5435930"}]}})

    session = FakeSession()

    races = racingcom.fetch_races_for_meet(
        session,
        graphql_host="https://graphql.api.racing.com",
        api_key="api-key",
        meet_code=5191184,
    )

    assert session.calls == [
        ("https://graphql.api.racing.com", racingcom.DEFAULT_CALENDAR_API_KEY),
        (racingcom.DEFAULT_RACE_DETAILS_GRAPHQL_HOST, racingcom.DEFAULT_RACE_DETAILS_API_KEY),
    ]
    assert races == [{"id": "5435930"}]


def test_accepts_fixture_requires_race_meet_id():
    provider = RacingComProvider()
    assert provider.accepts_fixture({"src": "racingcom", "meta": {"race_meet_id": 5191184}})
    assert provider.accepts_fixture({"src": "racingcom", "meta": json.dumps({"race_meet_id": 5191184})})
    assert not provider.accepts_fixture({"src": "racingcom", "meta": {}})


def test_transform_race_items_maps_sample_response():
    sample_path = Path(__file__).resolve().parents[1] / "racingdotcom_sample" / "races.json"
    payload = json.loads(sample_path.read_text())
    items = payload["data"]["getNoCacheRacesForMeet"]

    races = racingcom.transform_race_items(
        items,
        {
            "raceDate": "2026-03-07",
            "course": "Flemington",
            "meetingId": 705191184,
            "race_meet_id": 5191184,
        },
    )

    assert len(races) == len(items)

    race = races[0]
    assert race["raceId"] == racingcom.RACE_ID_BASE_AUS + 5435930
    assert race["raceNo"] == 6
    assert race["distance"] == 1400.0
    assert race["distanceText"] == "1400m"
    assert race["prizeMoney"] == 300000
    assert race["raceClass"] == "Group 2"
    assert race["going"] == "Good"
    assert race["goingText"] == "Good 4"
    assert race["reading"] == 4.0
    assert race["currency"] == "AUD"
    assert race["country"] == "AUS"
    assert race["surface"] == "TURF"
    assert race["startTime"].isoformat() == "2026-03-07T15:05:00"
    assert race["startTimeZoned"].isoformat() == "2026-03-07T15:05:00+11:00"
    assert race["meta"]["race_meet_id"] == 5191184
    assert race["meta"]["race"]["id"] == "5435930"


def test_transform_race_form_results_maps_sample_response():
    race_form_path = Path(__file__).resolve().parents[1] / "racingdotcom_sample" / "raceResult.json"
    race_form = json.loads(race_form_path.read_text())["data"]["getRaceForm"]
    race_payload = {
        "startTime": "2026-03-08T14:25:00",
        "startTimeZoned": "2026-03-08T14:25:00+11:00",
        "course": "Royal Randwick",
        "raceId": racingcom.RACE_ID_BASE_AUS + 5449708,
        "div": 0,
        "raceNo": 3,
    }
    sectionals = [
        {
            "horseNo": 2,
            "horseName": "CHAYAN",
            "finishingTime": 68.88,
            "first400Split": 23.5,
            "first400Time": 23.5,
            "first400Pos": 6,
            "last800Split": 12.4,
            "last800Pos": 5,
            "last600Split": 12.1,
            "last600Pos": 4,
            "last400Split": 11.8,
            "last400Pos": 3,
            "last200Split": 11.5,
            "last200Pos": 2,
        }
    ]

    results = racingcom.transform_race_form_results(
        race_form,
        race_payload,
        {"raceDate": "2026-03-08", "course": "Royal Randwick", "meta": {"state": "NSW"}},
        sectionals=sectionals,
    )

    assert len(results) == len(race_form["formRaceEntries"])

    winner = next(item for item in results if item["horseNo"] == 2)
    runner_up = next(item for item in results if item["horseNo"] == 1)

    assert winner["horseId"] == 5369579
    assert winner["horseName"] == "Chayan"
    assert winner["countryOfOrigin"] == "AUS"
    assert winner["jockey"] == "James Mc Donald"
    assert winner["trainer"] == "ANNABEL & ROB ARCHIBALD"
    assert winner["jockeyId"] == 775359
    assert winner["trainerId"] == 20971980
    assert winner["draw"] == 7
    assert winner["rank"] == 1
    assert winner["finishingTime"] == 68.88
    assert winner["weightCarried"] == 56.5
    assert winner["sp"] == 3.4
    assert winner["first2fSplit"] == 23.5
    assert winner["first2fPos"] == 6
    assert winner["last4fSplit"] == 12.4
    assert winner["last4fPos"] == 5
    assert winner["last3fSplit"] == 12.1
    assert winner["last3fPos"] == 4
    assert winner["last2fSplit"] == 11.8
    assert winner["last2fPos"] == 3
    assert winner["last1fSplit"] == 11.5
    assert winner["last1fPos"] == 2
    assert winner["last4f"] == 47.8
    assert "race" not in winner["meta"]
    assert winner["meta"]["horse"]["horseCode"] == "5369579"
    assert winner["meta"]["horse"]["jockeyUrl"] == "/jockeys/james-mc-donald-775359"
    assert winner["meta"]["horse"]["horse"]["id"] == "5369579"
    assert winner["meta"]["horse"]["race"]["meet"]["meetUrl"] == "https://www.racing.com/form/2026-03-07/royal-randwick"
    assert "SB2" in {item["providerCode"] for item in winner["meta"]["horse"]["odds"]}
    assert runner_up["jockey"] == "Tim Clark"
    assert runner_up["draw"] == 5
    assert runner_up["countryOfOrigin"] == "AUS"
    assert next(item for item in results if item["horseNo"] == 4)["rank"] is None


def test_fetch_sectionals_for_race_only_supports_vic_nsw_qld(monkeypatch):
    calls = []

    def fake_fetch_local_sectionals(endpoint, payload):
        calls.append((endpoint, payload))
        return [{"horseNo": 1}]

    monkeypatch.setattr(racingcom, "fetch_local_sectionals", fake_fetch_local_sectionals)

    vic = racingcom.fetch_sectionals_for_race(
        {"raceNumber": 2, "hasSectionals": True, "meet": {"venue": "Flemington"}},
        {"raceDate": "2026-03-08", "course": "Flemington", "race_meet_id": 5191184, "meta": {"state": "VIC"}},
        api_key="race-key",
    )
    nsw = racingcom.fetch_sectionals_for_race(
        {"raceNumber": 1, "hasSectionals": False, "meet": {"venue": "Rosehill Gardens"}},
        {"raceDate": "2026-03-14", "course": "Rosehill Gardens", "race_meet_id": 5191184, "meta": {"state": "NSW"}},
        api_key="race-key",
    )
    qld = racingcom.fetch_sectionals_for_race(
        {"raceNumber": 1, "hasSectionals": False, "meet": {"venue": "Ladbrokes Cannon Park"}},
        {"raceDate": "2024-01-28", "course": "Ladbrokes Cannon Park", "race_meet_id": 5191184, "meta": {"state": "QLD"}},
        api_key="race-key",
    )
    other = racingcom.fetch_sectionals_for_race(
        {"raceNumber": 1, "hasSectionals": True, "meet": {"venue": "Ascot"}},
        {"raceDate": "2026-03-08", "course": "Ascot", "race_meet_id": 5191184, "meta": {"state": "WA"}},
        api_key="race-key",
    )

    assert vic == [{"horseNo": 1}]
    assert nsw == [{"horseNo": 1}]
    assert qld == [{"horseNo": 1}]
    assert other == []
    assert calls == [
        ("http://localhost:8080/racingdotcom", {"api_key": "race-key", "meet_code": "5191184", "race_no": 2}),
        ("http://localhost:8080/racingnsw", {"course": "Rosehill Gardens", "race_date": "2026-03-14", "race_no": 1}),
        ("http://localhost:8080/racingqld", {"course": "Ladbrokes Cannon Park", "race_date": "2024-01-28", "race_no": 1}),
    ]


def test_parse_fixture_uses_past_fixture_race_details(monkeypatch):
    provider = RacingComProvider()
    fixture = {
        "fixtureId": 8705191184,
        "raceDate": "2026-03-07",
        "course": "Flemington",
        "meetingId": 705191184,
        "meta": json.dumps({"race_meet_id": 5191184}),
    }

    monkeypatch.setattr(
        racingcom,
        "discover_runtime_config",
        lambda: {
            "appSyncGraphQLHost": "https://graphql.api.racing.com",
            "appSyncGraphQLAPIKey": "api-key",
            "raceDetailsGraphQLHost": "https://graphql.rmdprod.racing.com/",
            "raceDetailsGraphQLAPIKey": "race-api-key",
        },
    )

    called = {}

    def fake_fetch_races_for_meet(session, graphql_host, api_key, meet_code):
        called["graphql_host"] = graphql_host
        called["api_key"] = api_key
        called["meet_code"] = meet_code
        return [
            {
                "id": "5435930",
                "meet": {"venue": "Flemington"},
                "raceNumber": 6,
                "distance": "1400m",
                "time": "2026-03-07T04:05:00.000Z",
                "trackCondition": "Good",
                "trackRating": "4",
                "condition": "Track type: Turf.",
                "totalPrizeMoney": "300000.00",
                "rdcClass": "Group 2",
                "hasSectionals": True,
            }
        ]

    def fake_fetch_race_form(session, graphql_host, api_key, meet_code, race_number):
        called["race_form"] = (graphql_host, api_key, meet_code, race_number)
        return {
            "id": "5435930",
            "meetCode": "5191184",
            "raceNumber": 6,
            "venue": {"venueName": "Flemington", "state": "VIC"},
            "formRaceEntries": [
                {
                    "id": "15504626",
                    "horseName": "Agrarian Girl",
                    "horseCode": "5360437",
                    "horseCountry": None,
                    "jockeyName": "T.Clark",
                    "jockeyUrl": "/jockeys/tim-clark-614120",
                    "trainerName": "Gai Waterhouse & Adrian Bott",
                    "trainerCode": "20658331",
                    "jockeyCode": "614120",
                    "raceEntryNumber": 5,
                    "finish": 2,
                    "liveBarrierNumber": 5,
                    "weight": "56.5kg",
                    "apprenticeAllowedClaim": "0",
                    "scratched": False,
                    "winningTime": "6945",
                    "bettingFluctuationsPriceMoveOne": "2.90",
                }
            ],
        }

    def fake_fetch_sectionals_for_race(race_item, fixture_ctx, api_key):
        called["sectionals"] = (fixture_ctx["race_meet_id"], race_item["raceNumber"], api_key)
        return []

    monkeypatch.setattr(racingcom, "fetch_races_for_meet", fake_fetch_races_for_meet)
    monkeypatch.setattr(racingcom, "fetch_race_form", fake_fetch_race_form)
    monkeypatch.setattr(racingcom, "fetch_sectionals_for_race", fake_fetch_sectionals_for_race)

    parsed = provider.parse_fixture(fixture)

    assert called == {
        "graphql_host": "https://graphql.rmdprod.racing.com/",
        "api_key": "race-api-key",
        "meet_code": 5191184,
        "race_form": ("https://graphql.rmdprod.racing.com/", "race-api-key", 5191184, 6),
        "sectionals": (5191184, 6, "race-api-key"),
    }
    assert len(parsed.races) == 1
    assert parsed.races[0]["raceId"] == racingcom.RACE_ID_BASE_AUS + 5435930
    assert len(parsed.results) == 1
    assert parsed.results[0]["horseNo"] == 5


def test_parse_fixture_skips_today_or_future(monkeypatch):
    provider = RacingComProvider()
    fixture = {
        "fixtureId": 8705191184,
        "raceDate": "2099-03-07",
        "course": "Flemington",
        "meetingId": 705191184,
        "meta": {"race_meet_id": 5191184},
    }

    def fail_discover_runtime_config():
        raise AssertionError("runtime config should not be fetched for non-past fixtures")

    monkeypatch.setattr(racingcom, "discover_runtime_config", fail_discover_runtime_config)

    parsed = provider.parse_fixture(fixture)

    assert parsed.races == []
    assert parsed.results == []
