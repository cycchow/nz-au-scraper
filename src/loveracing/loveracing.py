import json
import logging
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime
from zoneinfo import ZoneInfo
from typing import Any, Callable

import requests
from utils.course_utils import get_direction, normalize_course
from utils.jockey_name_mapping import get_jockey_full_name

logger = logging.getLogger(__name__)

MEETING_RESULTS_ENDPOINT = "https://loveracing.nz/ServerScript/RaceInfo.aspx/GetMeetingResults"
RESULT_DOWNLOAD_ENDPOINT = "https://loveracing.nz/SystemTemplates/RaceInfo/ResultDownloads.ashx"
SECTIONAL_ENDPOINT = "http://localhost:8080/loveracing"
NZ_TZ = ZoneInfo("Pacific/Auckland")
RACE_ID_BASE_NZ = 600000000


def format_month_payload(month_start: date) -> dict[str, str]:
    return {"start": f"1 {month_start.strftime('%b %Y')}"}


def parse_day_with_context(day_text: str, month_context: date) -> date:
    if not day_text:
        raise ValueError("missing day text")

    match = re.match(r"^\s*[A-Za-z]{3}\s+(\d{1,2})\s+([A-Za-z]{3})\s*$", day_text)
    if not match:
        raise ValueError(f"unexpected day format: {day_text}")

    day_num = int(match.group(1))
    month_abbr = match.group(2)
    parsed = datetime.strptime(f"{day_num} {month_abbr} {month_context.year}", "%d %b %Y")
    return parsed.date()


def decode_meetings_payload(response_json: dict[str, Any]) -> list[dict[str, Any]]:
    raw_d = response_json.get("d")
    if raw_d is None:
        raise ValueError("response payload missing 'd'")

    if isinstance(raw_d, str):
        decoded = json.loads(raw_d)
    elif isinstance(raw_d, list):
        decoded = raw_d
    else:
        raise ValueError("'d' must be a JSON string or array")

    if not isinstance(decoded, list):
        raise ValueError("decoded 'd' payload is not a list")

    return decoded


def fetch_month_meetings(month_start: date) -> list[dict[str, Any]]:
    payload = format_month_payload(month_start)
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }
    resp = requests.post(MEETING_RESULTS_ENDPOINT, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    response_json = resp.json()
    return decode_meetings_payload(response_json)


def generate_month_starts(from_month: date, to_month: date) -> list[date]:
    from_month = from_month.replace(day=1)
    to_month = to_month.replace(day=1)

    if from_month < to_month:
        from_month, to_month = to_month, from_month

    months = []
    current = from_month
    while current >= to_month:
        months.append(current)
        if current.month == 1:
            current = current.replace(year=current.year - 1, month=12)
        else:
            current = current.replace(month=current.month - 1)

    return months


def fetch_meetings_by_month_range(from_month: date, to_month: date) -> list[dict[str, Any]]:
    aggregated: list[dict[str, Any]] = []
    for month_start in generate_month_starts(from_month, to_month):
        try:
            meetings = fetch_month_meetings(month_start)
            logger.info("Fetched %s meetings for %s", len(meetings), month_start.strftime("%Y-%m"))
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed fetching month %s: %s", month_start.strftime("%Y-%m"), exc)
            continue

        for meeting in meetings:
            aggregated.append({"month_start": month_start, "meeting": meeting})

    return aggregated


def to_fixture_records(meetings: list[dict[str, Any]], month_context: date) -> list[dict[str, Any]]:
    fixtures: list[dict[str, Any]] = []
    month_label = month_context.strftime("%Y-%m")

    for meeting in meetings:
        day_text = meeting.get("Day")
        day_id = meeting.get("DayID")
        try:
            race_date = parse_day_with_context(day_text, month_context)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping DayID=%s, invalid Day=%r: %s", day_id, day_text, exc)
            continue

        course = normalize_course(meeting.get("Racecourse") or meeting.get("Club"))
        fixtures.append(
            {
                "course": course,
                "raceDate": race_date,
                "year": race_date.year,
                "meta": {
                    **meeting,
                    "requestMonth": month_label,
                },
            }
        )

    return fixtures


def build_result_download_url(day_id: int, filename: str) -> str:
    return f"{RESULT_DOWNLOAD_ENDPOINT}?DayID={int(day_id)}&FileName={filename}"


def fetch_meeting_xml(day_id: int, filename: str) -> str:
    url = build_result_download_url(day_id, filename)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.text


def fetch_sectionals(meeting_id: int, race_no: int) -> list[dict[str, Any]]:
    payload = {"meeting_id": meeting_id, "race_no": race_no}
    response = requests.post(SECTIONAL_ENDPOINT, json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, list) else []


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return None


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return None


def _parse_actual_time_to_seconds(actual_time: str | None) -> float | None:
    if not actual_time:
        return None
    text = str(actual_time).strip()
    parts = text.split(".")
    if len(parts) != 3:
        return _to_float(text)
    try:
        minutes = int(parts[0])
        seconds = int(parts[1])
        hundredths = int(parts[2])
        return minutes * 60 + seconds + (hundredths / 100)
    except ValueError:
        return None


def _extract_horse_name_and_origin(raw_name: str | None) -> tuple[str, str]:
    if not raw_name:
        return "", "NZ"
    name = raw_name.strip()
    match = re.match(r"^(.*?)\s*\(([A-Za-z]{2,3})\)\s*$", name)
    if match:
        return match.group(1).strip().upper(), match.group(2).upper()
    return name.upper(), "NZ"


def _combine_race_times(meeting_date: str, race_time: str) -> tuple[str, str]:
    local_dt = datetime.fromisoformat(f"{meeting_date}T{race_time}").replace(tzinfo=NZ_TZ)
    start_time = local_dt.replace(tzinfo=None).isoformat(timespec="seconds")
    start_time_zoned = local_dt.isoformat(timespec="seconds")
    return start_time, start_time_zoned


def _normalize_runner_name(name: str | None) -> str:
    base, _ = _extract_horse_name_and_origin(name)
    return base


def _sectional_value(candidate: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in candidate and candidate[key] not in (None, ""):
            return candidate[key]
    return None


def _map_sectionals(sectional: dict[str, Any] | None) -> dict[str, Any]:
    mapped = {
        "first2fTime": None,
        "first2fSplit": None,
        "first2fPos": None,
        "first2f": None,
        "last4fSplit": None,
        "last3fSplit": None,
        "last1fSplit": None,
        "sectionalMeta": sectional or {},
    }

    if not sectional:
        return mapped

    # Primary shape returned by localhost:8080/loveracing:
    # {
    #   cloth_number, horse_name, ...,
    #   sectionals: [{sector_distance, sector_time, cumulative_sector_time, sector_position}, ...]
    # }
    entries = sectional.get("sectionals") if isinstance(sectional, dict) else None
    if isinstance(entries, list) and entries:
        by_distance: dict[int, dict[str, Any]] = {}
        by_sector_number: dict[int, dict[str, Any]] = {}
        for entry in entries:
            sector_no = _to_int(entry.get("sector_number"))
            dist = _to_int(entry.get("sector_distance"))
            if sector_no is not None:
                by_sector_number[sector_no] = entry
            # keep first seen for a distance to avoid overwriting First 400m with Last 400m
            if dist is not None and dist not in by_distance:
                by_distance[dist] = entry

        # First 400m is always sector_number=0 in the loveracing response.
        first400 = by_sector_number.get(0) or by_distance.get(400)
        last800 = by_distance.get(800)
        last600 = by_distance.get(600)
        last200 = by_distance.get(200)

        if first400:
            first400_split = _to_float(first400.get("sector_time"))
            first400_cum = _to_float(first400.get("cumulative_sector_time"))
            mapped["first2fSplit"] = first400_split
            mapped["first2fTime"] = first400_cum if first400_cum is not None else first400_split
            mapped["first2fPos"] = _to_int(first400.get("sector_position"))
            mapped["first2f"] = first400_split

        mapped["last4fSplit"] = _to_float(last800.get("sector_time")) if last800 else None
        mapped["last3fSplit"] = _to_float(last600.get("sector_time")) if last600 else None
        mapped["last1fSplit"] = _to_float(last200.get("sector_time")) if last200 else None

        return mapped

    # Backward-compatible fallback for flat sectional keys.
    first400_split = _to_float(
        _sectional_value(sectional, ["first400Split", "first_400_split", "first400", "first_400"])
    )
    first400_time = _to_float(
        _sectional_value(sectional, ["first400Time", "first_400_time", "first400TimeSec"])
    )
    first400_pos = _to_int(_sectional_value(sectional, ["first400Pos", "first_400_pos", "first400Rank"]))

    mapped["first2fSplit"] = first400_split
    mapped["first2fTime"] = first400_time if first400_time is not None else first400_split
    mapped["first2fPos"] = first400_pos
    mapped["first2f"] = mapped["first2fSplit"]

    mapped["last4fSplit"] = _to_float(
        _sectional_value(sectional, ["last800Split", "last_800_split", "last800", "last_800"])
    )
    mapped["last3fSplit"] = _to_float(
        _sectional_value(sectional, ["last600Split", "last_600_split", "last600", "last_600"])
    )
    mapped["last1fSplit"] = _to_float(
        _sectional_value(sectional, ["last200Split", "last_200_split", "last200", "last_200"])
    )

    return mapped
def _index_sectionals(sectionals: list[dict[str, Any]]) -> tuple[dict[int, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_no: dict[int, dict[str, Any]] = {}
    by_name: dict[str, dict[str, Any]] = {}

    for item in sectionals or []:
        horse_no = _to_int(
            _sectional_value(item, ["horse_no", "horseNo", "cloth_number", "clothNumber", "toteNumber", "tote_number"])
        )
        horse_name = _normalize_runner_name(
            _sectional_value(item, ["horse_name", "horseName", "horse", "name"])
        )
        if horse_no is not None:
            by_no[horse_no] = item
        if horse_name:
            by_name[horse_name] = item

    return by_no, by_name


def parse_races_from_meeting(meeting_elem: ET.Element, fixture_ctx: dict[str, Any]) -> list[dict[str, Any]]:
    meeting_attrs = dict(meeting_elem.attrib)
    meeting_date = meeting_attrs.get("date") or fixture_ctx.get("raceDate")
    if hasattr(meeting_date, "isoformat"):
        meeting_date = meeting_date.isoformat()

    course = normalize_course(fixture_ctx.get("course") or meeting_attrs.get("track"))
    track_condition = meeting_attrs.get("trackCondition")

    races_payload = []
    for race_elem in meeting_elem.findall("./races/race"):
        race_attrs = dict(race_elem.attrib)
        race_no = _to_int(race_attrs.get("number"))
        race_xml_id = _to_int(race_attrs.get("id"))
        if race_no is None or race_xml_id is None:
            continue

        start_time, start_time_zoned = _combine_race_times(meeting_date, race_attrs.get("time", "00:00:00"))
        distance = _to_float(race_attrs.get("distance"))
        stake = _to_int(_to_float(race_attrs.get("stake")))

        # Loveracing synthetic tracks should map to DIRT for downstream compatibility.
        if course and "synthetic" in course.lower():
            surface = "DIRT"
        else:
            surface = "TURF" if (race_attrs.get("type") or "").lower() == "flat" else None
        direction = get_direction(course, str(int(distance)), surface) if course and distance is not None else None

        races_payload.append(
            {
                "raceDate": meeting_date,
                "course": course,
                "distance": distance,
                "distanceText": f"{int(distance)}m" if distance is not None else None,
                "prizeMoney": stake,
                "raceType": (race_attrs.get("type") or "FLAT").upper(),
                "raceClass": race_attrs.get("class"),
                "raceId": RACE_ID_BASE_NZ + race_xml_id,
                "div": 0,
                "startTime": start_time,
                "startTimeZoned": start_time_zoned,
                "raceNo": race_no,
                "country": "NZ",
                "currency": "NZD",
                "goingText": track_condition,
                "going": track_condition,
                "surface": surface,
                "direction": direction,
                "meta": {
                    "meeting": meeting_attrs,
                    "race": race_attrs,
                },
            }
        )

    return races_payload


def parse_results_from_meeting(
    meeting_elem: ET.Element,
    races_payload: list[dict[str, Any]],
    fixture_ctx: dict[str, Any],
    sectional_fetcher: Callable[[int, int], list[dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    meeting_id = _to_int((fixture_ctx.get("meta") or {}).get("DayID") or meeting_elem.attrib.get("id"))
    race_by_no = {r["raceNo"]: r for r in races_payload}
    results_payload: list[dict[str, Any]] = []

    for race_elem in meeting_elem.findall("./races/race"):
        race_no = _to_int(race_elem.attrib.get("number"))
        if race_no is None or race_no not in race_by_no:
            continue

        race_payload = race_by_no[race_no]
        sectionals = []
        if sectional_fetcher and meeting_id is not None:
            try:
                sectionals = sectional_fetcher(meeting_id, race_no) or []
            except Exception as exc:  # noqa: BLE001
                logger.warning("Sectional fetch failed for meeting=%s raceNo=%s: %s", meeting_id, race_no, exc)
                sectionals = []

        sectionals_by_no, sectionals_by_name = _index_sectionals(sectionals)

        for runner in race_elem.findall("./runners/runner"):
            runner_attrs = dict(runner.attrib)
            horse_no = _to_int(runner_attrs.get("toteNumber"))
            if horse_no is None:
                continue

            horse_name, country_origin = _extract_horse_name_and_origin(runner_attrs.get("name"))
            sectional = sectionals_by_no.get(horse_no) or sectionals_by_name.get(horse_name)
            sec_map = _map_sectionals(sectional)

            jockey_elem = runner.find("./jockey")
            jockey_name = jockey_elem.attrib.get("name") if jockey_elem is not None else None
            jockey_carried = jockey_elem.attrib.get("carried") if jockey_elem is not None else None
            jockey_mapped = get_jockey_full_name(jockey_name) if jockey_name else None

            results_payload.append(
                {
                    "startTime": race_payload["startTime"],
                    "startTimeZoned": race_payload["startTimeZoned"],
                    "course": race_payload["course"],
                    "raceId": race_payload["raceId"],
                    "div": race_payload["div"],
                    "horseNo": horse_no,
                    "horseId": _to_int(runner_attrs.get("id")) or -999,
                    "horseName": horse_name,
                    "countryOfOrigin": country_origin,
                    "draw": _to_int(runner_attrs.get("barrier")),
                    "jockey": jockey_mapped,
                    "trainer": (runner_attrs.get("trainer") or "").upper() or None,
                    "jockeyId": -999,
                    "trainerId": -999,
                    "rank": _to_int(runner_attrs.get("finishingposition")),
                    "finishingTime": _parse_actual_time_to_seconds(runner_attrs.get("actualtime")),
                    "weightCarried": _to_float(jockey_carried or runner_attrs.get("weight")),
                    "last1fTime": None,
                    "last1fSplit": sec_map["last1fSplit"],
                    "last1fPos": None,
                    "last1f": None,
                    "last2fTime": None,
                    "last2fSplit": None,
                    "last2fPos": None,
                    "last2f": None,
                    "last3fTime": None,
                    "last3fSplit": sec_map["last3fSplit"],
                    "last3fPos": None,
                    "last3f": None,
                    "last4fTime": None,
                    "last4fSplit": sec_map["last4fSplit"],
                    "last4fPos": None,
                    "last4f": None,
                    "last5fTime": None,
                    "last5fSplit": None,
                    "last5fPos": None,
                    "last5f": None,
                    "first1fTime": None,
                    "first1fSplit": None,
                    "first1fPos": None,
                    "first1f": None,
                    "first2fTime": sec_map["first2fTime"],
                    "first2fSplit": sec_map["first2fSplit"],
                    "first2fPos": sec_map["first2fPos"],
                    "first2f": sec_map["first2f"],
                    "sp": _to_float(runner_attrs.get("startingPriceWin")),
                    "meta": {
                        "runner": runner_attrs,
                        "sectional": sec_map["sectionalMeta"],
                    },
                }
            )

    return results_payload


def parse_meeting_xml(
    xml_text: str,
    fixture_ctx: dict[str, Any],
    sectional_fetcher: Callable[[int, int], list[dict[str, Any]]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    clean_xml = xml_text.lstrip("\ufeff")
    meeting_elem = ET.fromstring(clean_xml)
    races_payload = parse_races_from_meeting(meeting_elem, fixture_ctx)
    results_payload = parse_results_from_meeting(meeting_elem, races_payload, fixture_ctx, sectional_fetcher)
    return races_payload, results_payload
