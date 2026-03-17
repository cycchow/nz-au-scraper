from __future__ import annotations

import logging
from datetime import date

import requests

from racingcom import racingcom

from .base import FixtureProcessOutput

logger = logging.getLogger(__name__)


class RacingComProvider:
    name = "racingcom"
    source_code = "racingcom"
    default_country = "AUS"

    def fetch_fixtures_for_ingestion(self, from_month: date, to_month: date) -> list[dict]:
        runtime = racingcom.discover_runtime_config()
        graphql_host = runtime.get("appSyncGraphQLHost", racingcom.DEFAULT_GRAPHQL_HOST)
        api_key = runtime["appSyncGraphQLAPIKey"]
        logger.info(
            "Racing.com runtime config calendarHost=%s calendarApiKey=%s raceDetailsHost=%s raceDetailsApiKey=%s",
            graphql_host,
            api_key,
            runtime.get("raceDetailsGraphQLHost"),
            runtime.get("raceDetailsGraphQLAPIKey"),
        )

        fixtures: list[dict] = []
        with requests.Session() as session:
            for month_start in racingcom.iter_month_starts(from_month, to_month):
                try:
                    items = racingcom.fetch_calendar_items(
                        session,
                        graphql_host=graphql_host,
                        api_key=api_key,
                        year=month_start.year,
                        month=month_start.month,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "Failed fetching racing.com calendar for %s-%02d: %s",
                        month_start.year,
                        month_start.month,
                        exc,
                    )
                    continue

                for item in items:
                    transformed = racingcom.transform_calendar_item(item, month_start.year, month_start.month)
                    if transformed:
                        fixtures.append(transformed)

        logger.info("Fetched racing.com fixtures count=%s", len(fixtures))
        return fixtures

    def accepts_fixture(self, fixture: dict) -> bool:
        src = fixture.get("src")
        if src and src != self.source_code:
            return False

        meta = racingcom.parse_fixture_meta(fixture.get("meta"))
        return bool(meta.get("race_meet_id") or meta.get("race_meet_code"))

    def parse_fixture(self, fixture: dict) -> FixtureProcessOutput:
        fixture_date = racingcom.parse_fixture_date(fixture.get("raceDate"))
        if fixture_date is None:
            logger.warning("Skipping racing.com fixture with invalid raceDate fixtureId=%s", fixture.get("fixtureId"))
            return FixtureProcessOutput(races=[], results=[])

        today_aus = date.today()
        if fixture_date >= today_aus:
            logger.info(
                "Skipping non-past racing.com fixture fixtureId=%s raceDate=%s",
                fixture.get("fixtureId"),
                fixture_date.isoformat(),
            )
            return FixtureProcessOutput(races=[], results=[])

        meta = racingcom.parse_fixture_meta(fixture.get("meta"))
        meet_code = meta.get("race_meet_code") or meta.get("race_meet_id")
        try:
            meet_code = int(meet_code)
        except (TypeError, ValueError):
            logger.warning("Skipping racing.com fixture without race_meet_id fixtureId=%s", fixture.get("fixtureId"))
            return FixtureProcessOutput(races=[], results=[])

        runtime = racingcom.discover_runtime_config()
        graphql_host = runtime.get("raceDetailsGraphQLHost", racingcom.DEFAULT_RACE_DETAILS_GRAPHQL_HOST)
        api_key = runtime.get("raceDetailsGraphQLAPIKey", racingcom.DEFAULT_RACE_DETAILS_API_KEY)
        logger.info(
            "Racing.com runtime config calendarHost=%s calendarApiKey=%s raceDetailsHost=%s raceDetailsApiKey=%s",
            runtime.get("appSyncGraphQLHost"),
            runtime.get("appSyncGraphQLAPIKey"),
            graphql_host,
            api_key,
        )

        fixture_ctx = {
            "raceDate": fixture_date.isoformat(),
            "course": fixture.get("course"),
            "meetingId": fixture.get("meetingId"),
            "race_meet_id": meet_code,
            "meta": meta,
        }

        with requests.Session() as session:
            race_items = racingcom.fetch_races_for_meet(
                session,
                graphql_host=graphql_host,
                api_key=api_key,
                meet_code=meet_code,
            )
            races = racingcom.transform_race_items(race_items, fixture_ctx)
            races_by_no = {race.get("raceNo"): race for race in races}
            results: list[dict] = []

            for race_item in race_items:
                race_no = racingcom.parse_numeric_int(race_item.get("raceNumber"))
                race_payload = races_by_no.get(race_no)
                if race_no is None or race_payload is None:
                    continue

                logger.info(
                    "Processing racing.com race meetCode=%s raceNo=%s raceId=%s course=%s",
                    meet_code,
                    race_no,
                    race_payload.get("raceId"),
                    race_payload.get("course"),
                )
                race_form = racingcom.fetch_race_form(
                    session,
                    graphql_host=graphql_host,
                    api_key=api_key,
                    meet_code=meet_code,
                    race_number=race_no,
                )
                sectionals = racingcom.fetch_sectionals_for_race(race_item, fixture_ctx, api_key)
                results.extend(
                    racingcom.transform_race_form_results(
                        race_form,
                        race_payload,
                        fixture_ctx,
                        sectionals=sectionals,
                    )
                )

        return FixtureProcessOutput(races=races, results=results)
