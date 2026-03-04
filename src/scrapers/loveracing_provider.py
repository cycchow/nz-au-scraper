from __future__ import annotations

import json
import logging
import random
import time
from datetime import date
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
            meetings = loveracing.fetch_month_meetings(month_start)
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

        return bool(meta.get("DayID") and meta.get("ResultDownloadXML"))

    def parse_fixture(self, fixture: dict[str, Any]) -> FixtureProcessOutput:
        meta = fixture.get("meta") or {}
        if isinstance(meta, str):
            meta = json.loads(meta)

        day_id = int(meta["DayID"])
        filename = str(meta["ResultDownloadXML"])

        sleep_seconds = random.uniform(5, 20)
        logger.info(
            "Throttling XML download DayID=%s by %.1fs to reduce request rate",
            day_id,
            sleep_seconds,
        )
        time.sleep(sleep_seconds)

        xml_text = loveracing.fetch_meeting_xml(day_id, filename)

        fixture_date = fixture.get("raceDate")
        if fixture_date:
            try:
                fixture_date = date.fromisoformat(str(fixture_date))
            except ValueError:
                fixture_date = str(fixture_date)

        fixture_ctx = {
            "raceDate": fixture_date,
            "course": fixture.get("course"),
            "meta": meta,
        }

        races, results = loveracing.parse_meeting_xml(
            xml_text,
            fixture_ctx,
            sectional_fetcher=loveracing.fetch_sectionals,
        )
        return FixtureProcessOutput(races=races, results=results)
