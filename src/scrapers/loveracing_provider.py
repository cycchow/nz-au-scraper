from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any

from loveracing import loveracing

from .base import FixtureProcessOutput

logger = logging.getLogger(__name__)


def _parse_start_time_zoned(value: Any) -> datetime | None:
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=loveracing.NZ_TZ)
    return parsed.astimezone(loveracing.NZ_TZ)


def _merge_same_day_outputs(
    overview: FixtureProcessOutput,
    xml_output: FixtureProcessOutput,
    now_nz: datetime,
) -> FixtureProcessOutput:
    xml_races_by_id = {race.get("raceId"): race for race in xml_output.races}
    xml_results_by_race: dict[Any, list[dict[str, Any]]] = {}
    for result in xml_output.results:
        xml_results_by_race.setdefault(result.get("raceId"), []).append(result)

    merged_races: list[dict[str, Any]] = []
    for race in overview.races:
        race_id = race.get("raceId")
        start_dt = _parse_start_time_zoned(race.get("startTimeZoned"))
        if race_id in xml_races_by_id and start_dt is not None and start_dt <= now_nz:
            merged_races.append(xml_races_by_id[race_id])
        else:
            merged_races.append(race)

    merged_results: list[dict[str, Any]] = []
    seen_started_races: set[Any] = set()
    for result in overview.results:
        race_id = result.get("raceId")
        start_dt = _parse_start_time_zoned(result.get("startTimeZoned"))
        if race_id in xml_results_by_race and start_dt is not None and start_dt <= now_nz:
            if race_id not in seen_started_races:
                merged_results.extend(xml_results_by_race[race_id])
                seen_started_races.add(race_id)
            continue
        merged_results.append(result)

    return FixtureProcessOutput(races=merged_races, results=merged_results)


def _resolve_result_download_xml(meta: dict[str, Any], day_id: int, fixture_date: date) -> str | None:
    filename = meta.get("ResultDownloadXML")
    if filename:
        return str(filename)

    meeting = loveracing.fetch_meeting_result_by_day_id(day_id, fixture_date)
    if not meeting:
        return None

    resolved_filename = meeting.get("ResultDownloadXML")
    if resolved_filename:
        meta["ResultDownloadXML"] = resolved_filename
        return str(resolved_filename)

    return None


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
        now_nz = datetime.now(loveracing.NZ_TZ)
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

        # Future fixtures always use the meeting overview racecard.
        if fixture_date > today_nz:
            html_text = loveracing.fetch_meeting_overview_html(day_id)
            races, results = loveracing.parse_meeting_overview_html(html_text, fixture_ctx)
            return FixtureProcessOutput(races=races, results=results)

        # Same-day fixtures can have a mix of completed races and upcoming racecards.
        if fixture_date == today_nz:
            html_text = loveracing.fetch_meeting_overview_html(day_id)
            overview_races, overview_results = loveracing.parse_meeting_overview_html(html_text, fixture_ctx)
            overview_output = FixtureProcessOutput(races=overview_races, results=overview_results)

            filename = _resolve_result_download_xml(meta, day_id, fixture_date)
            if not filename:
                return overview_output

            try:
                xml_text = loveracing.fetch_meeting_xml(day_id, filename)
                xml_races, xml_results = loveracing.parse_meeting_xml(
                    xml_text,
                    fixture_ctx,
                    sectional_fetcher=loveracing.fetch_sectionals,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed same-day XML overlay fixtureId=%s dayId=%s: %s",
                    fixture.get("fixtureId"),
                    day_id,
                    exc,
                )
                return overview_output

            return _merge_same_day_outputs(
                overview_output,
                FixtureProcessOutput(races=xml_races, results=xml_results),
                now_nz,
            )

        # Past fixtures: fetch XML results payload.
        filename = _resolve_result_download_xml(meta, day_id, fixture_date)
        if not filename:
            logger.warning(
                "Skipping past fixture without ResultDownloadXML fixtureId=%s dayId=%s",
                fixture.get("fixtureId"),
                day_id,
            )
            return FixtureProcessOutput(races=[], results=[])

        xml_text = loveracing.fetch_meeting_xml(day_id, filename)
        races, results = loveracing.parse_meeting_xml(
            xml_text,
            fixture_ctx,
            sectional_fetcher=loveracing.fetch_sectionals,
        )
        return FixtureProcessOutput(races=races, results=results)
