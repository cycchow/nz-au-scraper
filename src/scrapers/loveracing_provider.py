from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime
from typing import Any

from loveracing import loveracing

from .base import FixtureProcessOutput

logger = logging.getLogger(__name__)
DEFAULT_XML_CACHE_TTL_SECONDS = 600


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


def _parse_meta(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return loaded if isinstance(loaded, dict) else {}
    return {}


def _coerce_race_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
        except ValueError:
            return None


def _day_id_from_race_meta(meta: dict[str, Any]) -> int | None:
    meeting = meta.get("meeting") if isinstance(meta.get("meeting"), dict) else {}
    return loveracing._to_int(meta.get("meetingId") or meeting.get("id") or meeting.get("meetingId"))


class LoveracingProvider:
    name = "loveracing"
    source_code = "loveracing"
    default_country = "NZ"

    def __init__(self, xml_cache_ttl_seconds: int = DEFAULT_XML_CACHE_TTL_SECONDS):
        self.xml_cache_ttl_seconds = xml_cache_ttl_seconds
        self._xml_cache: dict[tuple[int, str], tuple[float, str]] = {}

    def _get_cached_meeting_xml(self, day_id: int, filename: str) -> str:
        cache_key = (int(day_id), str(filename))
        now = time.time()
        cached = self._xml_cache.get(cache_key)
        if cached is not None:
            expires_at, xml_text = cached
            if expires_at > now:
                logger.debug("Using cached Loveracing meeting XML dayId=%s file=%s", day_id, filename)
                return xml_text
            self._xml_cache.pop(cache_key, None)

        xml_text = loveracing.fetch_meeting_xml(day_id, filename)
        self._xml_cache[cache_key] = (now + self.xml_cache_ttl_seconds, xml_text)
        return xml_text

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

        meta = _parse_meta(fixture.get("meta"))
        return bool(meta.get("DayID"))

    def accepts_race(self, race: dict[str, Any]) -> bool:
        country = race.get("country")
        if country and str(country).upper() != self.default_country:
            return False

        meta = _parse_meta(race.get("meta"))
        return _day_id_from_race_meta(meta) is not None

    def parse_fixture_races(self, fixture: dict[str, Any]) -> list[dict[str, Any]]:
        meta = _parse_meta(fixture.get("meta"))
        fixture_date = _coerce_race_date(fixture.get("raceDate"))
        if fixture_date is None:
            logger.warning("Skipping fixture with invalid raceDate fixtureId=%s", fixture.get("fixtureId"))
            return []

        today_nz = datetime.now(loveracing.NZ_TZ).date()
        try:
            day_id = int(meta.get("DayID"))
        except (TypeError, ValueError):
            logger.warning("Skipping fixture missing DayID fixtureId=%s", fixture.get("fixtureId"))
            return []

        fixture_ctx = {
            "raceDate": fixture_date.isoformat(),
            "course": fixture.get("course"),
            "meta": meta,
        }

        # Future fixtures always use the meeting overview racecard.
        if fixture_date > today_nz:
            html_text = loveracing.fetch_meeting_overview_html(day_id)
            races, results = loveracing.parse_meeting_overview_html(html_text, fixture_ctx)
            return races

        # Same-day and past fixtures use XML-backed races for the persisted race rows.
        if fixture_date <= today_nz:
            filename = _resolve_result_download_xml(meta, day_id, fixture_date)
            if not filename:
                logger.warning(
                    "Skipping fixture without ResultDownloadXML fixtureId=%s dayId=%s",
                    fixture.get("fixtureId"),
                    day_id,
                )
                return []
            xml_text = self._get_cached_meeting_xml(day_id, filename)
            races, _ = loveracing.parse_meeting_xml(
                xml_text,
                fixture_ctx,
                sectional_fetcher=loveracing.fetch_sectionals,
            )
            return races

        return []

    def parse_race_results(self, race: dict[str, Any]) -> list[dict[str, Any]]:
        race_meta = _parse_meta(race.get("meta"))
        race_date = _coerce_race_date(race.get("raceDate"))
        race_id = race.get("raceId")
        day_id = _day_id_from_race_meta(race_meta)

        if race_date is None or day_id is None or race_id is None:
            logger.warning(
                "Skipping race result fetch missing identifiers raceId=%s raceDate=%r dayId=%r",
                race_id,
                race.get("raceDate"),
                day_id,
            )
            return []

        today_nz = datetime.now(loveracing.NZ_TZ).date()
        if race_date > today_nz:
            return []

        filename = _resolve_result_download_xml(race_meta, day_id, race_date)
        if not filename:
            logger.warning("Skipping race without ResultDownloadXML raceId=%s dayId=%s", race_id, day_id)
            return []

        fixture_ctx = {
            "raceDate": race_date.isoformat(),
            "course": race.get("course"),
            "meta": race_meta,
        }

        xml_text = self._get_cached_meeting_xml(day_id, filename)
        _, results = loveracing.parse_meeting_xml(
            xml_text,
            fixture_ctx,
            sectional_fetcher=loveracing.fetch_sectionals,
        )
        return [result for result in results if result.get("raceId") == race_id]

    def parse_fixture(self, fixture: dict[str, Any]) -> FixtureProcessOutput:
        meta = _parse_meta(fixture.get("meta"))
        fixture_date = _coerce_race_date(fixture.get("raceDate"))

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
                xml_text = self._get_cached_meeting_xml(day_id, filename)
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

        xml_text = self._get_cached_meeting_xml(day_id, filename)
        races, results = loveracing.parse_meeting_xml(
            xml_text,
            fixture_ctx,
            sectional_fetcher=loveracing.fetch_sectionals,
        )
        return FixtureProcessOutput(races=races, results=results)
