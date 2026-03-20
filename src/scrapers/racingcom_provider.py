from __future__ import annotations

import logging
from datetime import date, datetime

import requests

from racingcom import racingcom

from .base import FixtureProcessOutput

logger = logging.getLogger(__name__)


class RacingComProvider:
    name = "racingcom"
    source_code = "racingcom"
    default_country = "AUS"

    @staticmethod
    def _extract_meet_code(meta: dict) -> int | None:
        meet_code = racingcom.parse_numeric_int(meta.get("race_meet_id"))
        if meet_code is not None:
            return meet_code

        meeting_id = racingcom.parse_numeric_int(meta.get("meetingId"))
        if meeting_id is None:
            return None

        if meeting_id >= racingcom.RACE_ID_BASE_AUS:
            return meeting_id - racingcom.RACE_ID_BASE_AUS
        if meeting_id >= 1_000_000:
            return meeting_id
        return None

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

    def accepts_race(self, race: dict) -> bool:
        country = race.get("country")
        if country and str(country).upper() != self.default_country:
            return False

        meta = racingcom.parse_fixture_meta(race.get("meta"))
        return self._extract_meet_code(meta) is not None

    def parse_fixture_races(self, fixture: dict) -> list[dict]:
        fixture_date = racingcom.parse_fixture_date(fixture.get("raceDate"))
        if fixture_date is None:
            logger.warning("Skipping racing.com fixture with invalid raceDate fixtureId=%s", fixture.get("fixtureId"))
            return []

        meta = racingcom.parse_fixture_meta(fixture.get("meta"))
        meet_code = meta.get("race_meet_code") or meta.get("race_meet_id")
        try:
            meet_code = int(meet_code)
        except (TypeError, ValueError):
            logger.warning("Skipping racing.com fixture without race_meet_id fixtureId=%s", fixture.get("fixtureId"))
            return []

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
            return racingcom.transform_race_items(race_items, fixture_ctx)

    def parse_fixture_cards(self, fixture: dict, races: list[dict] | None = None) -> list[dict]:
        fixture_date = racingcom.parse_fixture_date(fixture.get("raceDate"))
        if fixture_date is None:
            logger.warning("Skipping racing.com fixture cards with invalid raceDate fixtureId=%s", fixture.get("fixtureId"))
            return []

        today_aus = datetime.now(racingcom.AUS_TZ).date()
        if fixture_date < today_aus:
            return []

        races = races or self.parse_fixture_races(fixture)
        if not races:
            return []

        runtime = racingcom.discover_runtime_config()
        graphql_host = runtime.get("raceDetailsGraphQLHost", racingcom.DEFAULT_RACE_DETAILS_GRAPHQL_HOST)
        api_key = runtime.get("raceDetailsGraphQLAPIKey", racingcom.DEFAULT_RACE_DETAILS_API_KEY)

        cards: list[dict] = []
        with requests.Session() as session:
            for race in races:
                race_meta = racingcom.parse_fixture_meta(race.get("meta"))
                meet_code = self._extract_meet_code(race_meta)
                race_no = racingcom.parse_numeric_int(race.get("raceNo"))
                if meet_code is None or race_no is None:
                    continue

                race_form = racingcom.fetch_race_entries(
                    session,
                    graphql_host=graphql_host,
                    api_key=api_key,
                    meet_code=meet_code,
                    race_number=race_no,
                )
                cards.extend(
                    racingcom.transform_race_form_cards(
                        race_form,
                        race,
                        {
                            "raceDate": race.get("raceDate"),
                            "course": race.get("course"),
                            "meetingId": race_meta.get("meetingId"),
                            "race_meet_id": meet_code,
                            "meta": race_meta,
                        },
                    )
                )
        return cards

    def parse_race_results(self, race: dict) -> list[dict]:
        race_date = racingcom.parse_fixture_date(race.get("raceDate"))
        race_no = racingcom.parse_numeric_int(race.get("raceNo"))
        meta = racingcom.parse_fixture_meta(race.get("meta"))
        meet_code = self._extract_meet_code(meta)

        if race_date is None or race_no is None or meet_code is None:
            logger.warning(
                "Skipping racing.com race without required fields raceId=%s raceDate=%r raceNo=%r meetCode=%r",
                race.get("raceId"),
                race.get("raceDate"),
                race.get("raceNo"),
                meet_code,
            )
            return []

        try:
            meet_code = int(meet_code)
        except (TypeError, ValueError):
            logger.warning("Skipping racing.com race with invalid meetCode raceId=%s meetCode=%r", race.get("raceId"), meet_code)
            return []

        runtime = racingcom.discover_runtime_config()
        graphql_host = runtime.get("raceDetailsGraphQLHost", racingcom.DEFAULT_RACE_DETAILS_GRAPHQL_HOST)
        api_key = runtime.get("raceDetailsGraphQLAPIKey", racingcom.DEFAULT_RACE_DETAILS_API_KEY)

        fixture_ctx = {
            "raceDate": race_date.isoformat(),
            "course": race.get("course"),
            "meetingId": (meta.get("meetingId")),
            "race_meet_id": meet_code,
            "meta": meta,
        }

        logger.info(
            "Processing racing.com race meetCode=%s raceDate=%s course=%s raceNo=%s raceId=%s",
            meet_code,
            race_date.isoformat(),
            race.get("course"),
            race_no,
            race.get("raceId"),
        )

        with requests.Session() as session:
            race_form = racingcom.fetch_race_form(
                session,
                graphql_host=graphql_host,
                api_key=api_key,
                meet_code=meet_code,
                race_number=race_no,
            )
            race_item = meta.get("race") if isinstance(meta.get("race"), dict) else {}
            sectionals = racingcom.fetch_sectionals_for_race(race_item, fixture_ctx, api_key)
            logger.info(
                "Fetched sectionals source=racingcom meetCode=%s raceDate=%s course=%s raceNo=%s raceId=%s sectionals=%s",
                meet_code,
                race_date.isoformat(),
                race.get("course"),
                race_no,
                race.get("raceId"),
                len(sectionals),
            )
            return racingcom.transform_race_form_results(
                race_form,
                race,
                fixture_ctx,
                sectionals=sectionals,
            )

    def parse_fixture(self, fixture: dict) -> FixtureProcessOutput:
        races = self.parse_fixture_races(fixture)
        results: list[dict] = []
        for race in races:
            results.extend(self.parse_race_results(race))
        return FixtureProcessOutput(races=races, results=results)
