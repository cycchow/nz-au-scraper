from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol


@dataclass(frozen=True)
class FixtureProcessOutput:
    races: list[dict[str, Any]]
    results: list[dict[str, Any]]


class ScraperProvider(Protocol):
    name: str
    source_code: str
    default_country: str

    def fetch_fixtures_for_ingestion(self, from_month: date, to_month: date) -> list[dict[str, Any]]:
        """Fetch fixture-like records from source website for fixture ingestion mode."""

    def accepts_fixture(self, fixture: dict[str, Any]) -> bool:
        """Whether a GraphQL fixture row should be processed by this provider."""

    def accepts_race(self, race: dict[str, Any]) -> bool:
        """Whether a GraphQL race row should be processed by this provider."""

    def parse_fixture_races(self, fixture: dict[str, Any]) -> list[dict[str, Any]]:
        """Transform one GraphQL fixture row into Races payloads."""

    def parse_race_results(self, race: dict[str, Any]) -> list[dict[str, Any]]:
        """Transform one GraphQL race row into Results payloads."""

    def parse_fixture(self, fixture: dict[str, Any]) -> FixtureProcessOutput:
        """Transform one GraphQL fixture row into Races + Results payloads."""
