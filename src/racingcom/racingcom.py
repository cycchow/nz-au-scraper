from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from typing import Any
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

RACING_BASE_URL = "https://www.racing.com"
DEFAULT_GRAPHQL_HOST = "https://graphql.api.racing.com"

MEET_TYPES = ["Metro", "Provincial", "Country", "Picnic"]
EVENT_TYPES = ["Racing"]
STATES = ["VIC", "SA", "NSW", "QLD", "WA", "ACT", "NT", "TAS"]

SITE_CONFIG_PATTERN = re.compile(r"CUSTOM_SITE_CONFIG\s*\|\|\s*'(?P<json>\{.*?\})'", re.DOTALL)
SCRIPT_SRC_PATTERN = re.compile(
    r"<script[^>]+src=(?:\"(?P<double>[^\"]+)\"|'(?P<single>[^']+)')",
    re.IGNORECASE,
)
NEXT_DATA_PATTERN = re.compile(r'<script id="__NEXT_DATA__" type="application/json">(?P<json>.*?)</script>', re.DOTALL)
BUILD_MANIFEST_CHUNK_PATTERN = re.compile(r'"(?P<path>/_next/static/chunks/[^"]+\.js)"')
NEXT_STATIC_PATH_PATTERN = re.compile(r"(?P<path>/_next/static/chunks/[A-Za-z0-9_./-]+\.js(?:\?[^\"'\\s<]*)?)")


class RuntimeConfigError(RuntimeError):
    """Raised when runtime config cannot be extracted from racing.com assets."""


def iter_month_starts(from_month: date, to_month: date) -> list[date]:
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


def _discover_chunk_urls(session: requests.Session, landing_html: str) -> list[str]:
    urls: set[str] = set()
    for match in SCRIPT_SRC_PATTERN.finditer(landing_html):
        src = match.group("double") or match.group("single")
        if not src:
            continue
        if "/_next/static/" not in src or ".js" not in src:
            continue
        urls.add(urljoin(RACING_BASE_URL, src))

    next_data_match = NEXT_DATA_PATTERN.search(landing_html)
    if next_data_match:
        try:
            next_data = json.loads(next_data_match.group("json"))
            build_id = next_data.get("buildId")
            if build_id:
                manifest_url = urljoin(RACING_BASE_URL, f"/_next/static/{build_id}/_buildManifest.js")
                manifest_resp = session.get(manifest_url, timeout=30)
                manifest_resp.raise_for_status()
                for match in BUILD_MANIFEST_CHUNK_PATTERN.finditer(manifest_resp.text):
                    urls.add(urljoin(RACING_BASE_URL, match.group("path")))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Unable to load _buildManifest.js: %s", exc)

    if not urls:
        for match in NEXT_STATIC_PATH_PATTERN.finditer(landing_html):
            urls.add(urljoin(RACING_BASE_URL, match.group("path")))

    return sorted(urls)


def extract_custom_site_config(js_text: str) -> dict[str, Any]:
    match = SITE_CONFIG_PATTERN.search(js_text)
    if not match:
        raise RuntimeConfigError("CUSTOM_SITE_CONFIG payload not found in script")

    raw_json = match.group("json")
    try:
        return json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise RuntimeConfigError(f"CUSTOM_SITE_CONFIG JSON parse failed: {exc}") from exc


def discover_runtime_config(session: requests.Session | None = None) -> dict[str, str]:
    own_session = session is None
    session = session or requests.Session()

    try:
        landing_resp = session.get(
            RACING_BASE_URL,
            headers={
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "user-agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
                ),
            },
            timeout=30,
        )
        landing_resp.raise_for_status()
        chunk_urls = _discover_chunk_urls(session, landing_resp.text)

        if not chunk_urls:
            html_sample = landing_resp.text[:300].replace("\n", " ")
            raise RuntimeConfigError(f"No Next.js chunk URLs discovered from landing page. html_sample={html_sample!r}")

        errors: list[str] = []
        for chunk_url in chunk_urls:
            try:
                chunk_resp = session.get(chunk_url, timeout=30)
                chunk_resp.raise_for_status()
                site_config = extract_custom_site_config(chunk_resp.text)

                graphql_host = site_config.get("appSyncGraphQLHost")
                graphql_api_key = site_config.get("appSyncGraphQLAPIKey")
                if graphql_host and graphql_api_key:
                    return {
                        "appSyncGraphQLHost": str(graphql_host),
                        "appSyncGraphQLAPIKey": str(graphql_api_key),
                    }
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{chunk_url}: {exc}")

        detail = " | ".join(errors[:5])
        raise RuntimeConfigError(f"Unable to extract GraphQL host/API key from chunks. Attempts={len(chunk_urls)} {detail}")
    finally:
        if own_session:
            session.close()


def build_calendar_query(year: int, month: int) -> str:
    meet_types = ", ".join(f'"{item}"' for item in MEET_TYPES)
    event_types = ", ".join(f'"{item}"' for item in EVENT_TYPES)
    states = ", ".join(f'"{item}"' for item in STATES)

    return f"""
query GetCalendarEvents {{
  getCalendarItems(
    meetTypes: [{meet_types}],
    eventTypes: [{event_types}],
    states: [{states}],
    year: {int(year)},
    month: {int(month)}
  ) {{
    id
    name
    environment
    race_meet_id
    club_name
    club_id
    race_meet_type
    race_meet_status
    location_name
    location_address
    location_public_transport
    location_driving_time
    event_gates_open
    event_start_time
    event_results_url
    event_next_race_url
    event_page_url
    event_status
    event_type
    type
    image_url
    thumbnail_image
    night_event
    state
  }}
}}
""".strip()


def fetch_calendar_items(
    session: requests.Session,
    graphql_host: str,
    api_key: str,
    year: int,
    month: int,
) -> list[dict[str, Any]]:
    query = build_calendar_query(year, month)

    resp = session.get(
        graphql_host,
        params={"query": query},
        headers={
            "accept": "*/*",
            "origin": RACING_BASE_URL,
            "referer": f"{RACING_BASE_URL}/",
            "content-type": "application/json;charset=UTF-8",
            "x-api-key": api_key,
        },
        timeout=30,
    )
    resp.raise_for_status()

    payload = resp.json()
    data = payload.get("data") or {}
    items = data.get("getCalendarItems")
    if isinstance(items, list):
        return items

    raise RuntimeConfigError(f"Unexpected calendar response shape: keys={list(payload.keys())}")


def transform_calendar_item(item: dict[str, Any], request_year: int, request_month: int) -> dict[str, Any] | None:
    race_meet_id_raw = item.get("race_meet_id")
    try:
        race_meet_id = int(race_meet_id_raw)
    except (TypeError, ValueError):
        logger.warning("Skipping calendar item with invalid race_meet_id=%r id=%r", race_meet_id_raw, item.get("id"))
        return None

    event_start_time = item.get("event_start_time")
    if not event_start_time:
        logger.warning("Skipping calendar item with missing event_start_time race_meet_id=%s", race_meet_id)
        return None

    try:
        normalized_dt = str(event_start_time).replace("Z", "+00:00")
        race_date = datetime.fromisoformat(normalized_dt).date()
    except ValueError:
        logger.warning(
            "Skipping calendar item with invalid event_start_time=%r race_meet_id=%s",
            event_start_time,
            race_meet_id,
        )
        return None

    course = item.get("location_name") or item.get("club_name") or item.get("name")
    if not course:
        logger.warning("Skipping calendar item with missing course fields race_meet_id=%s", race_meet_id)
        return None

    return {
        "course": str(course),
        "raceDate": race_date,
        "year": race_date.year,
        "meetingId": 700000000 + race_meet_id,
        "meta": {
            **item,
            "requestYear": int(request_year),
            "requestMonth": int(request_month),
            "race_meet_id": race_meet_id,
        },
    }
