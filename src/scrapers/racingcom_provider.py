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
        return not src or src == self.source_code

    def parse_fixture(self, fixture: dict) -> FixtureProcessOutput:
        return FixtureProcessOutput(races=[], results=[])
