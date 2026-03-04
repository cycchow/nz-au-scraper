from datetime import date

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
