from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any

from loveracing import loveracing

from .base import FixtureProcessOutput

logger = logging.getLogger(__name__)


class LoveracingProvider:
    name = "loveracing"
    source_code = "loveracing"
    default_country = "NZ"

    def fetch_fixtures_for_ingestion(self, from_month: date, to_month: date) -> list[dict[str, Any]]:
        fixtures: list[dict[str, Any]] = []
        for month_start in loveracing.generate_month_starts(from_month, to_month):
            meetings = loveracing.fetch_month_meetings_with_calendar_merge(month_start)
            fixtures.extend(loveracing.to_fixture_records(meetings, month_start))
        return fixtures

    def accepts_fixture(self, fixture: dict[str, Any]) -> bool:
        src = fixture.get("src")
        if src and src != self.source_code:
            return False

        meta = fixture.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except json.JSONDecodeError:
                return False

        return bool(meta.get("DayID"))

    def parse_fixture(self, fixture: dict[str, Any]) -> FixtureProcessOutput:
        meta = fixture.get("meta") or {}
        if isinstance(meta, str):
            meta = json.loads(meta)

        fixture_date = fixture.get("raceDate")
        if fixture_date:
            try:
                fixture_date = date.fromisoformat(str(fixture_date))
            except ValueError:
                try:
                    fixture_date = datetime.fromisoformat(str(fixture_date).replace("Z", "+00:00")).date()
                except ValueError:
                    fixture_date = None

        if fixture_date is None:
            logger.warning("Skipping fixture with invalid raceDate fixtureId=%s", fixture.get("fixtureId"))
            return FixtureProcessOutput(races=[], results=[])

        today_nz = datetime.now(loveracing.NZ_TZ).date()
        if fixture_date < today_nz:
            logger.info(
                "Skipping past loveracing fixture fixtureId=%s raceDate=%s todayNZ=%s",
                fixture.get("fixtureId"),
                fixture_date,
                today_nz,
            )
            return FixtureProcessOutput(races=[], results=[])

        try:
            day_id = int(meta.get("DayID"))
        except (TypeError, ValueError):
            logger.warning("Skipping fixture missing DayID fixtureId=%s", fixture.get("fixtureId"))
            return FixtureProcessOutput(races=[], results=[])

        fixture_ctx = {
            "raceDate": fixture_date.isoformat(),
            "course": fixture.get("course"),
            "meta": meta,
        }

        html_text = loveracing.fetch_meeting_overview_html(day_id)
        races, results = loveracing.parse_meeting_overview_html(html_text, fixture_ctx)
        return FixtureProcessOutput(races=races, results=results)
