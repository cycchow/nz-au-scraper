from __future__ import annotations

import json
import logging
import os
import re
from copy import deepcopy
from datetime import date, datetime
from zoneinfo import ZoneInfo
from typing import Any
from urllib.parse import urljoin

import requests

from utils.course_utils import get_direction, normalize_course
from utils.jockey_name_mapping import get_jockey_full_name

logger = logging.getLogger(__name__)

RACING_BASE_URL = "https://www.racing.com"
CALENDAR_PAGE_URL = f"{RACING_BASE_URL}/calendar"
FORM_CONFIG_URL = f"{RACING_BASE_URL}/form/config.js"
DEFAULT_GRAPHQL_HOST = "https://graphql.api.racing.com"
DEFAULT_CALENDAR_API_KEY = "da2-r5s52y73i5c7vi6vxflvfdufsa"
DEFAULT_RACE_DETAILS_GRAPHQL_HOST = "https://graphql.rmdprod.racing.com/"
DEFAULT_RACE_DETAILS_API_KEY = "da2-6nsi4ztsynar3l3frgxf77q5fe"
SECTIONAL_API_TIMEOUT_SECONDS = 600
AUS_TZ = ZoneInfo("Australia/Melbourne")
STATE_TIMEZONES: dict[str, ZoneInfo] = {
    "NSW": ZoneInfo("Australia/Sydney"),
    "ACT": ZoneInfo("Australia/Sydney"),
    "VIC": ZoneInfo("Australia/Sydney"),
    "TAS": ZoneInfo("Australia/Sydney"),
    "QLD": ZoneInfo("Australia/Brisbane"),
    "SA": ZoneInfo("Australia/Adelaide"),
    "WA": ZoneInfo("Australia/Perth"),
    "NT": ZoneInfo("Australia/Darwin"),
}
RACE_ID_BASE_AUS = 700000000

MEET_TYPES = ["Metro", "Provincial", "Country", "Picnic"]
EVENT_TYPES = ["Racing"]
STATES = ["VIC", "SA", "NSW", "QLD", "WA", "ACT", "NT", "TAS"]

SITE_CONFIG_PATTERN = re.compile(r"CUSTOM_SITE_CONFIG\s*\|\|\s*'(?P<json>\{.*?\})'", re.DOTALL)
GRAPHQL_CLIENT_PATTERN = re.compile(
    r'GraphQLClient\("(?P<host>https://graphql\.[^"]+)",\s*\{.*?headers:\s*\{.*?"x-api-key":\s*"(?P<api_key>[^"]+)"',
    re.DOTALL,
)
CONFIG_STRING_FIELD_PATTERN = re.compile(r'(?P<key>[A-Za-z0-9_]+):\\"(?P<value>[^"]+)\\"')
SCRIPT_SRC_PATTERN = re.compile(
    r"<script[^>]+src=(?:\"(?P<double>[^\"]+)\"|'(?P<single>[^']+)')",
    re.IGNORECASE,
)
NEXT_DATA_PATTERN = re.compile(r'<script id="__NEXT_DATA__" type="application/json">(?P<json>.*?)</script>', re.DOTALL)
BUILD_MANIFEST_CHUNK_PATTERN = re.compile(r'"(?P<path>/_next/static/chunks/[^"]+\.js)"')
NEXT_STATIC_PATH_PATTERN = re.compile(r"(?P<path>/_next/static/chunks/[A-Za-z0-9_./-]+\.js(?:\?[^\"'\\s<]*)?)")
WEIGHT_PATTERN = re.compile(r"(-?\d+(?:\.\d+)?)")


class RuntimeConfigError(RuntimeError):
    """Raised when runtime config cannot be extracted from racing.com assets."""


_RUNTIME_CONFIG_CACHE: dict[str, str] | None = None


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


def extract_graphql_clients(js_text: str) -> dict[str, str]:
    clients: dict[str, str] = {}
    for match in GRAPHQL_CLIENT_PATTERN.finditer(js_text):
        clients[match.group("host")] = match.group("api_key")
    return clients


def extract_form_config(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for match in CONFIG_STRING_FIELD_PATTERN.finditer(text):
        values[match.group("key")] = match.group("value")
    return values


def runtime_config_from_form_config(config_text: str) -> dict[str, str]:
    config = extract_form_config(config_text)
    calendar_host = config.get("DxpExternalDataUrl") or DEFAULT_GRAPHQL_HOST
    calendar_key = config.get("DxpExternalDataApiKey") or DEFAULT_CALENDAR_API_KEY
    race_host = config.get("ChampionDataEndpoint") or config.get("GraphqlEndpoint") or DEFAULT_RACE_DETAILS_GRAPHQL_HOST
    race_key = config.get("ChampionDataEndpointKey") or DEFAULT_RACE_DETAILS_API_KEY
    return {
        "appSyncGraphQLHost": calendar_host,
        "appSyncGraphQLAPIKey": calendar_key,
        "raceDetailsGraphQLHost": race_host,
        "raceDetailsGraphQLAPIKey": race_key,
    }


def discover_graphql_clients(
    session: requests.Session | None = None,
    page_url: str = RACING_BASE_URL,
) -> dict[str, str]:
    own_session = session is None
    session = session or requests.Session()

    try:
        landing_resp = session.get(
            page_url,
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
            raise RuntimeConfigError("No Next.js chunk URLs discovered for GraphQL client extraction")

        clients: dict[str, str] = {}
        for chunk_url in chunk_urls:
            try:
                chunk_resp = session.get(chunk_url, timeout=30)
                chunk_resp.raise_for_status()
                clients.update(extract_graphql_clients(chunk_resp.text))
            except Exception as exc:  # noqa: BLE001
                logger.debug("Skipping chunk during GraphQL client extraction url=%s err=%s", chunk_url, exc)

        if not clients:
            raise RuntimeConfigError("No GraphQL clients discovered from Next.js chunks")
        return clients
    finally:
        if own_session:
            session.close()


def discover_runtime_config(session: requests.Session | None = None) -> dict[str, str]:
    global _RUNTIME_CONFIG_CACHE

    if session is None and _RUNTIME_CONFIG_CACHE is not None:
        return deepcopy(_RUNTIME_CONFIG_CACHE)

    own_session = session is None
    session = session or requests.Session()

    try:
        landing_resp = session.get(
            CALENDAR_PAGE_URL,
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

        try:
            config_resp = session.get(FORM_CONFIG_URL, timeout=30)
            config_resp.raise_for_status()
            runtime = runtime_config_from_form_config(config_resp.text)
            if runtime.get("appSyncGraphQLHost") and runtime.get("appSyncGraphQLAPIKey"):
                if own_session:
                    _RUNTIME_CONFIG_CACHE = deepcopy(runtime)
                return runtime
        except Exception as exc:  # noqa: BLE001
            logger.warning("Unable to load /form/config.js runtime config: %s", exc)

        chunk_urls = _discover_chunk_urls(session, landing_resp.text)
        if not chunk_urls:
            html_sample = landing_resp.text[:300].replace("\n", " ")
            raise RuntimeConfigError(f"No Next.js chunk URLs discovered from landing page. html_sample={html_sample!r}")

        errors: list[str] = []
        clients: dict[str, str] = {}
        for chunk_url in chunk_urls:
            try:
                chunk_resp = session.get(chunk_url, timeout=30)
                chunk_resp.raise_for_status()
                chunk_text = chunk_resp.text
                clients.update(extract_graphql_clients(chunk_text))
                site_config = extract_custom_site_config(chunk_text)

                graphql_host = site_config.get("appSyncGraphQLHost")
                graphql_api_key = site_config.get("appSyncGraphQLAPIKey")
                if graphql_host and graphql_api_key:
                    runtime = {
                        "appSyncGraphQLHost": str(graphql_host),
                        "appSyncGraphQLAPIKey": str(graphql_api_key),
                        "raceDetailsGraphQLHost": DEFAULT_RACE_DETAILS_GRAPHQL_HOST,
                        "raceDetailsGraphQLAPIKey": clients.get(DEFAULT_RACE_DETAILS_GRAPHQL_HOST, DEFAULT_RACE_DETAILS_API_KEY),
                    }
                    if own_session:
                        _RUNTIME_CONFIG_CACHE = deepcopy(runtime)
                    return runtime
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{chunk_url}: {exc}")

        if clients:
            runtime = {
                "appSyncGraphQLHost": DEFAULT_GRAPHQL_HOST,
                "appSyncGraphQLAPIKey": clients.get(DEFAULT_GRAPHQL_HOST, DEFAULT_CALENDAR_API_KEY),
                "raceDetailsGraphQLHost": DEFAULT_RACE_DETAILS_GRAPHQL_HOST,
                "raceDetailsGraphQLAPIKey": clients.get(DEFAULT_RACE_DETAILS_GRAPHQL_HOST, DEFAULT_RACE_DETAILS_API_KEY),
            }
            if own_session:
                _RUNTIME_CONFIG_CACHE = deepcopy(runtime)
            return runtime

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


def build_race_list_query() -> str:
    return """
query getRaceNumberList_CD($meetCode: ID!) {
  getNoCacheRacesForMeet(meetCode: $meetCode) {
    id
    meet {
      venue
      meetUrl
      meetUrlSegment
      trackMap
    }
    raceNumber
    raceStatus
    distance
    time
    name
    nameForm
    trackCondition
    isTrial
    isJumpOut
    trackRating
    trackRecordTime
    trackRecordHorseCode
    trackRecordHorseName
    trackRecordRace {
      date
    }
    condition
    prizeMoney
    totalPrizeMoney
    hasSectionals
    hasTips
    hasSpeedMap
    hasResults
    hasStewards
    hasHistory
    hasSectionals
    hasField
    hasFullForm
    trackCode
    rdcClass
    formRaceEntries {
      horseName
      position
      winningTime
      standardTimeDifference
    }
    toEightHundredMetresSeconds
    standardTimeTo800Difference
    eightHundredToFourHundredMetresSeconds
    standardTime800To400Difference
    fourHundredToFinishMetresSeconds
    standardTime400ToFinishDifference
    raceTime
    standardTimeDifference
    standardTimeId
    stewardsReportUrl
  }
}
""".strip()


def build_race_results_query() -> str:
    return """
query getRaceResults_CD($meetCode: ID!, $raceNumber: Int!) {
  getRaceForm(meetCode: $meetCode, raceNumber: $raceNumber) {
    id
    meetCode
    venue {
      venueName
      state
    }
    raceNumber
    photoFinish
    raceStatus
    rdcClass
    isTrial
    isJumpOut
    videoItems {
      id
      contenttype
      poster
    }
    formRaceEntries {
      id
      meetCode
      raceNumber
      position
      barrierNumber
      liveBarrierNumber
      prizeMoney
      scratched
      startingPrice
      odds {
        id
        providerCode
        oddsPlace
        oddsWin
        oddsIsFavouriteWin
        oddsIsMarketMover
        deepLinkWin
        deepLinkPlace
        deepLinkRace
        flucsWin {
          updateTime
          amount
        }
      }
      comment
      commentShort
      commentStewards
      raceEntryNumber
      apprenticeCanClaim
      apprenticeAllowedClaim
      weight
      margin
      winningTime
      finish
      finishAbv
      gearHasChanges
      gearChanges
      lastGear
      lastGearDate
      finish
      horseName
      horseCode
      horseCountry
      horseUrl
      silkUrl
      race {
        meet {
          meetUrl
          meetTips {
            longComment
            shortComment
          }
        }
      }
      horse {
        id
        lastFive
        silkUrl
        stats {
          key
          firsts
          starts
          thirds
          seconds
        }
        lastProfessionalRaceEntryItem {
          raceCode
          position
          race {
            runnersCount
            distance
            date
            venueAbbr
          }
        }
      }
      standardTimeDifference
      jockeyUrl
      jockeyName
      trainerUrl
      trainerCode
      jockeyCode
      trainerName
      positionAt400
      positionAt400Abv
      positionAt800
      positionAt800Abv
      bettingFluctuationsPriceOpen
      bettingFluctuationsPriceMoveOne
      bettingFluctuationsPriceMoveTwo
      bonusMoney
    }
  }
}
""".strip()


def build_race_entries_query() -> str:
    return """
query getRaceEntriesForField_CD($meetCode: ID!, $raceNumber: Int!) {
  getRaceForm(meetCode: $meetCode, raceNumber: $raceNumber) {
    id
    status
    tempo
    isTrial
    isJumpOut
    location
    venueCode
    rdcClass
    venueState
    venue {
      venueName
      state
    }
    distance
    trackCondition
    trackRating
    class
    group
    nameForm
    videoItems {
      id
      contenttype
      url
      poster
    }
    bestBets {
      overview
      suggestedBet
      tipCondition
      selectionTipper
    }
    raceTips {
      raceCode
      condition
      comment
      tipType
      tipster {
        tipsterId
        profileImageUrl
        tipsterName
        isLead
      }
      tips {
        position
        tipBetType
        comment
        raceEntryItem {
          raceEntryNumber
          horseName
          horseCode
          jockeyCode
          trainerCode
          horseCountry
          silkUrl
          barrierNumber
          emergency
          emergencyNumber
          scratched
          trainerName
          jockeyName
          apprenticeCanClaim
          apprenticeAllowedClaim
          speedValue
          faisHighlight {
            key
            positive
          }
          odds {
            id
            providerCode
            oddsPlace
            oddsWin
            oddsIsFavouriteWin
            oddsIsMarketMover
            deepLinkWin
            deepLinkPlace
            deepLinkRace
          }
        }
      }
    }
    formRaceEntries {
      id
      meetCode
      raceCode
      raceNumber
      weight
      weightPrevious
      position
      barrierNumber
      liveBarrierNumber
      prizeMoney
      scratched
      startingPrice
      odds {
        id
        providerCode
        oddsPlace
        oddsWin
        oddsIsFavouriteWin
        oddsIsMarketMover
        deepLinkWin
        deepLinkPlace
        deepLinkRace
        flucsWin {
          updateTime
          amount
        }
      }
      comment
      commentShort
      commentStewards
      raceEntryNumber
      apprenticeCanClaim
      apprenticeAllowedClaim
      weight
      margin
      winningTime
      finish
      finishAbv
      gearHasChanges
      gearChanges
      lastGear
      lastGearDate
      horseUrl
      silkUrl
      jockeyUrl
      jockeyCode
      jockeyName
      trainerUrl
      trainerCode
      trainerName
      horseName
      horseCode
      handicapRating
      handicapRatingProgression
      isBlackbookSelected
      isGetOnSelected
      speedValue
      trackDistanceStats
      trackStats
      distanceStats
      jockeyStats
      atThisClassStats
      lastRaceDate
      horseCountry
      emergency
      emergencyNumber
      bestBets {
        overview
      }
      faisHighlight {
        key
        positive
      }
      race {
        meet {
          meetUrl
          meetTips {
            longComment
            shortComment
          }
        }
      }
      horse {
        id
        age
        sex
        colour
        owners
        sireHorseName
        damHorseName
        rating
        ratingProgression
        lastFive
        country
        careerWinPercent
        careerPlacePercent
        lastTenStats
        lastTwelveMonthsStats
        firstUpStats
        secondUpStats
        thirdUpStats
        winningRange
        maxDistanceWon
        minDistanceWon
        firmStats
        goodStats
        softStats
        heavyStats
        syntheticStats
        jumpsRecordStats
        groupListedStats
        lastRaceDate
        lastWinDate
        careerPrizeMoney
        silkUrl
        stats {
          starts
          firsts
          seconds
          thirds
        }
        lastProfessionalRaceEntryItem {
          raceCode
          position
          positionAbbreviation
          race {
            runnersCount
            distance
            date
            venueAbbr
            videoItems {
              videoId
              contenttype
            }
          }
        }
        blackBook {
          timeStamp
          meetCode
          comment
          raceEntryItemCode
          tipsterName
          raceDate
          isTrial
          isJumpOut
          venueCode
          venueName
        }
      }
    }
  }
}
""".strip()


def graphql_api_key_for_host(host: str, discovered_api_key: str | None = None) -> str:
    normalized_host = (host or "").rstrip("/")
    if normalized_host == DEFAULT_RACE_DETAILS_GRAPHQL_HOST.rstrip("/"):
        return os.getenv("RACINGCOM_RACE_DETAILS_API_KEY", DEFAULT_RACE_DETAILS_API_KEY)
    if normalized_host == DEFAULT_GRAPHQL_HOST.rstrip("/"):
        return os.getenv("RACINGCOM_CALENDAR_API_KEY", DEFAULT_CALENDAR_API_KEY)
    return discovered_api_key or ""


def fetch_calendar_items(
    session: requests.Session,
    graphql_host: str,
    api_key: str,
    year: int,
    month: int,
) -> list[dict[str, Any]]:
    query = build_calendar_query(year, month)
    host_api_key = graphql_api_key_for_host(graphql_host, discovered_api_key=api_key)

    resp = session.get(
        graphql_host,
        params={"query": query},
        headers={
            "accept": "*/*",
            "origin": RACING_BASE_URL,
            "referer": f"{RACING_BASE_URL}/",
            "content-type": "application/json;charset=UTF-8",
            "x-api-key": host_api_key,
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


def fetch_races_for_meet(
    session: requests.Session,
    graphql_host: str,
    api_key: str,
    meet_code: int | str,
) -> list[dict[str, Any]]:
    hosts_to_try: list[str] = []
    for host in [graphql_host, DEFAULT_RACE_DETAILS_GRAPHQL_HOST]:
        if host and host not in hosts_to_try:
            hosts_to_try.append(host)

    errors: list[str] = []
    for host in hosts_to_try:
        host_api_key = graphql_api_key_for_host(host, discovered_api_key=api_key)

        resp = session.get(
            host,
            params={
                "query": build_race_list_query(),
                "variables": json.dumps({"meetCode": str(meet_code)}),
            },
            headers={
                "accept": "*/*",
                "origin": RACING_BASE_URL,
                "referer": f"{RACING_BASE_URL}/",
                "content-type": "application/json;charset=UTF-8",
                "x-api-key": host_api_key,
                "user-agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
                ),
            },
            timeout=30,
        )
        resp.raise_for_status()

        payload = resp.json()
        data = payload.get("data") or {}
        races = data.get("getNoCacheRacesForMeet")
        if isinstance(races, list):
            return races

        payload_errors = payload.get("errors") or []
        if payload_errors:
            messages = ", ".join(str(error.get("message") or error) for error in payload_errors)
            errors.append(f"{host}: {messages}")
            continue

        errors.append(f"{host}: unexpected response keys={list(payload.keys())}")

    raise RuntimeConfigError(f"Unable to fetch races for meetCode={meet_code}. Attempts={' | '.join(errors)}")


def fetch_race_form(
    session: requests.Session,
    graphql_host: str,
    api_key: str,
    meet_code: int | str,
    race_number: int,
) -> dict[str, Any]:
    resp = session.get(
        graphql_host,
        params={
            "query": build_race_results_query(),
            "variables": json.dumps({"meetCode": str(meet_code), "raceNumber": int(race_number)}),
        },
        headers={
            "accept": "*/*",
            "origin": RACING_BASE_URL,
            "referer": f"{RACING_BASE_URL}/",
            "content-type": "application/json;charset=UTF-8",
            "x-api-key": api_key,
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
            ),
        },
        timeout=30,
    )
    resp.raise_for_status()

    payload = resp.json()
    race_form = (payload.get("data") or {}).get("getRaceForm")
    if isinstance(race_form, dict):
        return race_form

    errors = payload.get("errors") or []
    if errors:
        messages = ", ".join(str(error.get("message") or error) for error in errors)
        raise RuntimeConfigError(f"Unable to fetch race form meetCode={meet_code} raceNumber={race_number}: {messages}")
    raise RuntimeConfigError(f"Unexpected race form response shape: keys={list(payload.keys())}")


def fetch_race_entries(
    session: requests.Session,
    graphql_host: str,
    api_key: str,
    meet_code: int | str,
    race_number: int,
) -> dict[str, Any]:
    host_api_key = graphql_api_key_for_host(graphql_host, discovered_api_key=api_key)
    resp = session.get(
        graphql_host,
        params={
            "query": build_race_entries_query(),
            "variables": json.dumps({"meetCode": str(meet_code), "raceNumber": int(race_number)}),
        },
        headers={
            "accept": "*/*",
            "origin": RACING_BASE_URL,
            "referer": f"{RACING_BASE_URL}/",
            "content-type": "application/json",
            "x-api-key": host_api_key,
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
            ),
        },
        timeout=30,
    )
    resp.raise_for_status()

    payload = resp.json()
    race_form = (payload.get("data") or {}).get("getRaceForm")
    if isinstance(race_form, dict):
        return race_form

    errors = payload.get("errors") or []
    if errors:
        messages = ", ".join(str(error.get("message") or error) for error in errors)
        raise RuntimeConfigError(f"Unable to fetch race entries meetCode={meet_code} raceNumber={race_number}: {messages}")
    raise RuntimeConfigError(f"Unexpected race entries response shape: keys={list(payload.keys())}")


def parse_fixture_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if not value:
        return None

    text = str(value).strip()
    try:
        return date.fromisoformat(text)
    except ValueError:
        pass

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def parse_fixture_meta(meta: Any) -> dict[str, Any]:
    if isinstance(meta, dict):
        return meta
    if not meta:
        return {}
    if isinstance(meta, str):
        try:
            loaded = json.loads(meta)
            if isinstance(loaded, dict):
                return loaded
        except json.JSONDecodeError:
            logger.warning("Unable to decode fixture meta JSON: %r", meta)
    return {}


def parse_distance_text(value: Any) -> float | None:
    if value is None:
        return None

    match = re.search(r"(\d+(?:\.\d+)?)", str(value))
    if not match:
        return None
    return float(match.group(1))


def parse_numeric_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_numeric_int(value: Any) -> int | None:
    parsed = parse_numeric_float(value)
    if parsed is None:
        return None
    return int(parsed)


def timezone_for_state(state: Any) -> ZoneInfo:
    return STATE_TIMEZONES.get(str(state or "").upper(), AUS_TZ)


def parse_start_times(value: Any, tz: ZoneInfo = AUS_TZ) -> tuple[datetime | None, datetime | None]:
    if not value:
        return None, None

    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None, None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    local_dt = parsed.astimezone(tz)
    return local_dt.replace(tzinfo=None), local_dt


def infer_surface(condition_text: Any, course: Any = None) -> str | None:
    text = str(condition_text or "").lower()
    course_text = str(course or "").upper()
    if (
        "synthetic" in text
        or "polytrack" in text
        or "dirt" in text
        or "POLY" in course_text
        or "SYNTHETIC" in course_text
        or "TAPETA" in course_text
    ):
        return "DIRT"
    if "turf" in text:
        return "TURF"
    return "TURF"


def build_going_text(track_condition: Any, track_rating: Any) -> str | None:
    condition = str(track_condition).strip() if track_condition is not None else ""
    rating = str(track_rating).strip() if track_rating is not None else ""
    if condition and rating:
        return f"{condition} {rating}"
    if condition:
        return condition
    if rating:
        return rating
    return None


def parse_centiseconds(value: Any) -> float | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if re.fullmatch(r"\d+", text):
        return int(text) / 100
    return parse_numeric_float(text)


def normalize_finish_rank(value: Any) -> int | None:
    rank = parse_numeric_int(value)
    if rank is None:
        return None
    if rank > 100:
        return None
    return rank


def parse_price(value: Any) -> float | None:
    if value in (None, ""):
        return None
    match = WEIGHT_PATTERN.search(str(value))
    if not match:
        return None
    return parse_numeric_float(match.group(1))


def parse_weight_carried(weight_text: Any, apprentice_claim_text: Any) -> float | None:
    weight = parse_price(weight_text)
    claim = parse_price(apprentice_claim_text) or 0.0
    if weight is None:
        return None
    return weight - claim


def is_abandoned_race_status(value: Any) -> bool:
    return str(value or "").strip().lower() == "abandoned"


def parse_card_price(entry: dict[str, Any]) -> float | None:
    direct_price = parse_price(entry.get("bettingFluctuationsPriceMoveOne"))
    if direct_price is not None:
        return direct_price
    for odds_item in entry.get("odds") or []:
        if not isinstance(odds_item, dict):
            continue
        odds_price = parse_price(odds_item.get("oddsWin"))
        if odds_price is not None:
            return odds_price
    return None


def full_name_from_profile_url(url: Any, fallback: Any = None) -> str | None:
    if not url:
        return fallback
    slug = str(url).rstrip("/").split("/")[-1]
    slug = re.sub(r"-\d+$", "", slug)
    parts = [part for part in slug.split("-") if part]
    if not parts:
        return fallback
    return " ".join(part.capitalize() for part in parts)


def normalize_jockey_name(url: Any, fallback_name: Any = None) -> str | None:
    candidate = full_name_from_profile_url(url, fallback_name)
    if not candidate:
        return None
    candidate = re.sub(r"\s+(GB|HK|GER|FR|NZ|JPN|IRE|JNR)\s*$", "", str(candidate).strip(), flags=re.IGNORECASE)
    if candidate == "-":
        return None
    mapped = get_jockey_full_name(candidate)
    if mapped and mapped != candidate.upper():
        return mapped.title()
    return candidate


def normalize_trainer_name(url: Any, fallback_name: Any = None) -> str | None:
    fallback = str(fallback_name).strip() if fallback_name is not None else ""
    if "." in fallback:
        candidate = full_name_from_profile_url(url, fallback)
        if candidate:
            return candidate.upper()
    return fallback.upper() or None


def compact_odds(odds_items: Any) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for item in odds_items or []:
        if not isinstance(item, dict):
            continue
        compact.append(
            {
                "providerCode": item.get("providerCode"),
                "oddsWin": item.get("oddsWin"),
                "oddsPlace": item.get("oddsPlace"),
                "oddsIsFavouriteWin": item.get("oddsIsFavouriteWin"),
                "oddsIsMarketMover": item.get("oddsIsMarketMover"),
            }
        )
    return compact


def full_entry_meta(entry: dict[str, Any]) -> dict[str, Any]:
    return deepcopy(entry)


def normalize_runner_name(name: Any) -> str:
    text = str(name or "").strip().upper()
    text = re.sub(r"\s*\([^)]*\)", "", text)
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return " ".join(text.split())


def sectional_value(candidate: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in candidate and candidate[key] not in (None, ""):
            return candidate[key]
    return None


def map_sectionals(sectional: dict[str, Any] | None) -> dict[str, Any]:
    mapped = {
        "first1fTime": None,
        "first1fSplit": None,
        "first1fPos": None,
        "first1f": None,
        "first2fTime": None,
        "first2fSplit": None,
        "first2fPos": None,
        "first2f": None,
        "last5fTime": None,
        "last5fSplit": None,
        "last5fPos": None,
        "last5f": None,
        "last4fTime": None,
        "last4fSplit": None,
        "last4fPos": None,
        "last4f": None,
        "last3fTime": None,
        "last3fSplit": None,
        "last3fPos": None,
        "last3f": None,
        "last2fTime": None,
        "last2fSplit": None,
        "last2fPos": None,
        "last2f": None,
        "last1fTime": None,
        "last1fSplit": None,
        "last1fPos": None,
        "last1f": None,
        "finishingTime": None,
        "sectionalMeta": sectional or {},
    }
    if not sectional:
        return mapped

    entries = sectional.get("sectionals") if isinstance(sectional, dict) else None
    if isinstance(entries, list) and entries:
        ordered = []
        for idx, entry in enumerate(entries):
            if not isinstance(entry, dict):
                continue
            sector_no = parse_numeric_int(entry.get("sector_number"))
            ordered.append((sector_no if sector_no is not None else idx, entry))
        ordered = [entry for _, entry in sorted(ordered, key=lambda item: item[0])]

        if ordered:
            first = ordered[0]
            mapped["first1fSplit"] = parse_numeric_float(first.get("sector_time"))
            mapped["first1fTime"] = mapped["first1fSplit"]
            mapped["first1fPos"] = parse_numeric_int(first.get("sector_position"))
            mapped["first1f"] = mapped["first1fSplit"]
        if len(ordered) >= 2:
            second = ordered[1]
            first_split = parse_numeric_float(ordered[0].get("sector_time")) or 0.0
            second_split = parse_numeric_float(second.get("sector_time"))
            mapped["first2fSplit"] = (first_split + second_split) if second_split is not None else None
            mapped["first2fTime"] = mapped["first2fSplit"]
            mapped["first2fPos"] = parse_numeric_int(second.get("sector_position"))
            mapped["first2f"] = mapped["first2fSplit"]

        if len(ordered) >= 1:
            last1 = ordered[-1]
            mapped["last1fSplit"] = parse_numeric_float(last1.get("sector_time"))
            mapped["last1fPos"] = parse_numeric_int(last1.get("sector_position"))
        if len(ordered) >= 2:
            last2 = ordered[-2]
            mapped["last2fSplit"] = parse_numeric_float(last2.get("sector_time"))
            mapped["last2fPos"] = parse_numeric_int(last2.get("sector_position"))
        if len(ordered) >= 3:
            last3 = ordered[-3]
            mapped["last3fSplit"] = parse_numeric_float(last3.get("sector_time"))
            mapped["last3fPos"] = parse_numeric_int(last3.get("sector_position"))
        if len(ordered) >= 4:
            last4 = ordered[-4]
            mapped["last4fSplit"] = parse_numeric_float(last4.get("sector_time"))
            mapped["last4fPos"] = parse_numeric_int(last4.get("sector_position"))
        if len(ordered) >= 5:
            last5 = ordered[-5]
            mapped["last5fSplit"] = parse_numeric_float(last5.get("sector_time"))
            mapped["last5fPos"] = parse_numeric_int(last5.get("sector_position"))
    else:
        mapped["first1fSplit"] = parse_numeric_float(sectional_value(sectional, ["first200Split", "first_200_split", "first200"]))
        mapped["first1fTime"] = parse_numeric_float(sectional_value(sectional, ["first200Time", "first_200_time"]))
        mapped["first1fPos"] = parse_numeric_int(sectional_value(sectional, ["first200Pos", "first_200_pos"]))
        mapped["first1f"] = mapped["first1fSplit"]
        mapped["first2fSplit"] = parse_numeric_float(sectional_value(sectional, ["first400Split", "first_400_split", "first400"]))
        mapped["first2fTime"] = parse_numeric_float(sectional_value(sectional, ["first400Time", "first_400_time"])) or mapped["first2fSplit"]
        mapped["first2fPos"] = parse_numeric_int(sectional_value(sectional, ["first400Pos", "first_400_pos", "first400Rank"]))
        mapped["first2f"] = mapped["first2fSplit"]
        mapped["last5fSplit"] = parse_numeric_float(sectional_value(sectional, ["last1000Split", "last_1000_split", "last1000"]))
        mapped["last5fPos"] = parse_numeric_int(sectional_value(sectional, ["last1000Pos", "last_1000_pos", "last1000Rank"]))
        mapped["last4fSplit"] = parse_numeric_float(sectional_value(sectional, ["last800Split", "last_800_split", "last800"]))
        mapped["last4fPos"] = parse_numeric_int(sectional_value(sectional, ["last800Pos", "last_800_pos", "last800Rank"]))
        mapped["last3fSplit"] = parse_numeric_float(sectional_value(sectional, ["last600Split", "last_600_split", "last600"]))
        mapped["last3fPos"] = parse_numeric_int(sectional_value(sectional, ["last600Pos", "last_600_pos", "last600Rank"]))
        mapped["last2fSplit"] = parse_numeric_float(sectional_value(sectional, ["last400Split", "last_400_split", "last400"]))
        mapped["last2fPos"] = parse_numeric_int(sectional_value(sectional, ["last400Pos", "last_400_pos", "last400Rank"]))
        mapped["last1fSplit"] = parse_numeric_float(sectional_value(sectional, ["last200Split", "last_200_split", "last200"]))
        mapped["last1fPos"] = parse_numeric_int(sectional_value(sectional, ["last200Pos", "last_200_pos", "last200Rank"]))

    mapped["last1f"] = mapped["last1fSplit"]
    mapped["last1fTime"] = mapped["last1fSplit"]
    if mapped["last1fSplit"] is not None and mapped["last2fSplit"] is not None:
        mapped["last2f"] = mapped["last1fSplit"] + mapped["last2fSplit"]
        mapped["last2fTime"] = mapped["last2f"]
    if mapped["last2f"] is not None and mapped["last3fSplit"] is not None:
        mapped["last3f"] = mapped["last2f"] + mapped["last3fSplit"]
        mapped["last3fTime"] = mapped["last3f"]
    if mapped["last3f"] is not None and mapped["last4fSplit"] is not None:
        mapped["last4f"] = mapped["last3f"] + mapped["last4fSplit"]
        mapped["last4fTime"] = mapped["last4f"]
    if mapped["last4f"] is not None and mapped["last5fSplit"] is not None:
        mapped["last5f"] = mapped["last4f"] + mapped["last5fSplit"]
        mapped["last5fTime"] = mapped["last5f"]

    mapped["finishingTime"] = parse_numeric_float(sectional_value(sectional, ["finishingTime", "finishing_time", "time"]))
    return mapped


def index_sectionals(sectionals: list[dict[str, Any]]) -> tuple[dict[int, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_no: dict[int, dict[str, Any]] = {}
    by_name: dict[str, dict[str, Any]] = {}
    for item in sectionals or []:
        horse_no = parse_numeric_int(sectional_value(item, ["horse_no", "horseNo", "cloth_number", "clothNumber", "raceEntryNumber"]))
        horse_name = normalize_runner_name(sectional_value(item, ["horse_name", "horseName", "horse", "name"]))
        if horse_no is not None:
            by_no[horse_no] = item
        if horse_name:
            by_name[horse_name] = item
    return by_no, by_name


def extract_sectional_entries(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ["sectionals", "data", "results", "runners", "entries"]:
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return [payload]


def fetch_local_sectionals(endpoint: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    resp = requests.post(endpoint, json=payload, timeout=SECTIONAL_API_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return extract_sectional_entries(resp.json())


def fetch_sectionals_for_race(
    race_item: dict[str, Any],
    fixture_ctx: dict[str, Any],
    api_key: str,
) -> list[dict[str, Any]]:
    race_no = parse_numeric_int(race_item.get("raceNumber"))
    meet_code = fixture_ctx.get("race_meet_id")
    if race_no is None:
        return []

    meta = fixture_ctx.get("meta") or {}
    state = (
        meta.get("state")
        or ((meta.get("race") or {}).get("meet") or {}).get("state")
        or race_item.get("meet", {}).get("state")
    )
    course = race_item.get("meet", {}).get("venue") or fixture_ctx.get("course")
    race_date = fixture_ctx.get("raceDate")
    has_sectionals = bool(race_item.get("hasSectionals"))

    logger.info(
        "Sectional routing raceDate=%s course=%s raceNo=%s state=%s meetCode=%s hasSectionals=%s",
        race_date,
        course,
        race_no,
        state,
        meet_code,
        has_sectionals,
    )

    try:
        if state == "VIC" and has_sectionals:
            return fetch_local_sectionals(
                "http://localhost:8080/racingdotcom",
                {"api_key": api_key, "meet_code": str(meet_code), "race_no": int(race_no)},
            )
        if state == "NSW":
            return fetch_local_sectionals(
                "http://localhost:8080/racingnsw",
                {"course": course, "race_date": race_date, "race_no": int(race_no)},
            )
        if state == "QLD":
            return fetch_local_sectionals(
                "http://localhost:8080/racingqld",
                {"course": course, "race_date": race_date, "race_no": int(race_no)},
            )
        # Leave room for future state-specific sectional providers.
        logger.debug("Skipping sectional fetch for unsupported state=%s meetCode=%s raceNo=%s", state, meet_code, race_no)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Sectional fetch failed meetCode=%s raceNo=%s: %s", meet_code, race_no, exc)
    return []


def transform_race_form_results(
    race_form: dict[str, Any],
    race_payload: dict[str, Any],
    fixture_ctx: dict[str, Any],
    sectionals: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    if is_abandoned_race_status(race_form.get("raceStatus")):
        logger.info(
            "Skipping abandoned racing.com results raceId=%s raceNo=%s raceDate=%s course=%s",
            race_payload.get("raceId"),
            race_payload.get("raceNo"),
            fixture_ctx.get("raceDate"),
            fixture_ctx.get("course"),
        )
        return []

    sectionals_by_no, sectionals_by_name = index_sectionals(sectionals or [])
    results: list[dict[str, Any]] = []
    matched_sectionals = 0
    unmatched_horses: list[str] = []

    if sectionals:
        logger.info(
            "Prepared sectional indexes raceId=%s raceNo=%s byNo=%s byName=%s",
            race_payload.get("raceId"),
            race_payload.get("raceNo"),
            sorted(sectionals_by_no.keys())[:10],
            sorted(sectionals_by_name.keys())[:5],
        )

    for entry in race_form.get("formRaceEntries") or []:
        horse_no = parse_numeric_int(entry.get("raceEntryNumber"))
        if horse_no is None:
            continue

        horse_name = str(entry.get("horseName") or "").strip()
        normalized_horse_name = normalize_runner_name(horse_name)
        sectional = sectionals_by_no.get(horse_no) or sectionals_by_name.get(normalized_horse_name)
        if sectional is not None:
            matched_sectionals += 1
        else:
            unmatched_horses.append(f"{horse_no}:{horse_name}")
        sec_map = map_sectionals(sectional)
        logger.info(
            "Sectional match raceId=%s raceNo=%s horseNo=%s horseName=%s normalizedHorse=%s matched=%s sectionalKeys=%s first2f=%s last4f=%s last1f=%s",
            race_payload.get("raceId"),
            race_payload.get("raceNo"),
            horse_no,
            horse_name,
            normalized_horse_name,
            sectional is not None,
            sorted(sectional.keys()) if isinstance(sectional, dict) else None,
            sec_map["first2fSplit"],
            sec_map["last4fSplit"],
            sec_map["last1fSplit"],
        )
        scratched = bool(entry.get("scratched"))
        rank = normalize_finish_rank(entry.get("finish"))
        winning_time = parse_centiseconds(entry.get("winningTime"))
        draw = None if scratched else parse_numeric_int(entry.get("liveBarrierNumber") or entry.get("barrierNumber"))
        jockey_name = None if scratched else normalize_jockey_name(entry.get("jockeyUrl"), entry.get("jockeyName"))

        results.append(
            {
                "raceDate": fixture_ctx.get("raceDate"),
                "startTime": race_payload["startTime"],
                "startTimeZoned": race_payload["startTimeZoned"],
                "course": race_payload["course"],
                "raceId": race_payload["raceId"],
                "div": race_payload["div"],
                "horseNo": horse_no,
                "horseId": parse_numeric_int(entry.get("horseCode")) or parse_numeric_int((entry.get("horse") or {}).get("id")) or -999,
                "horseName": horse_name,
                "countryOfOrigin": str(entry.get("horseCountry") or "AUS").upper(),
                "jockey": jockey_name,
                "trainer": normalize_trainer_name(entry.get("trainerUrl"), entry.get("trainerName")),
                "jockeyId": None if scratched else parse_numeric_int(entry.get("jockeyCode")),
                "trainerId": parse_numeric_int(entry.get("trainerCode")),
                "draw": draw,
                "rank": rank,
                "finishingTime": sec_map["finishingTime"] if sec_map["finishingTime"] is not None else winning_time,
                "weightCarried": parse_weight_carried(entry.get("weight"), entry.get("apprenticeAllowedClaim")),
                "last1fTime": sec_map["last1fTime"],
                "last1fSplit": sec_map["last1fSplit"],
                "last1fPos": sec_map["last1fPos"],
                "last1f": sec_map["last1f"],
                "last2fTime": sec_map["last2fTime"],
                "last2fSplit": sec_map["last2fSplit"],
                "last2fPos": sec_map["last2fPos"],
                "last2f": sec_map["last2f"],
                "last3fTime": sec_map["last3fTime"],
                "last3fSplit": sec_map["last3fSplit"],
                "last3fPos": sec_map["last3fPos"],
                "last3f": sec_map["last3f"],
                "last4fTime": sec_map["last4fTime"],
                "last4fSplit": sec_map["last4fSplit"],
                "last4fPos": sec_map["last4fPos"],
                "last4f": sec_map["last4f"],
                "last5fTime": sec_map["last5fTime"],
                "last5fSplit": sec_map["last5fSplit"],
                "last5fPos": sec_map["last5fPos"],
                "last5f": sec_map["last5f"],
                "first1fTime": sec_map["first1fTime"],
                "first1fSplit": sec_map["first1fSplit"],
                "first1fPos": sec_map["first1fPos"],
                "first1f": sec_map["first1f"],
                "first2fTime": sec_map["first2fTime"],
                "first2fSplit": sec_map["first2fSplit"],
                "first2fPos": sec_map["first2fPos"],
                "first2f": sec_map["first2f"],
                "sp": parse_card_price(entry),
                "meta": {
                    "horse": full_entry_meta(entry),
                    "sectional": sec_map["sectionalMeta"],
                },
            }
        )

    if sectionals:
        logger.info(
            "Mapped sectionals raceId=%s raceNo=%s fetched=%s matched=%s unmatched=%s",
            race_payload.get("raceId"),
            race_payload.get("raceNo"),
            len(sectionals),
            matched_sectionals,
            unmatched_horses[:5],
        )

    return results


def transform_race_form_cards(
    race_form: dict[str, Any],
    race_payload: dict[str, Any],
    fixture_ctx: dict[str, Any],
) -> list[dict[str, Any]]:
    if is_abandoned_race_status(race_form.get("raceStatus")):
        logger.info(
            "Skipping abandoned racing.com racecard raceId=%s raceNo=%s raceDate=%s course=%s",
            race_payload.get("raceId"),
            race_payload.get("raceNo"),
            fixture_ctx.get("raceDate"),
            fixture_ctx.get("course"),
        )
        return []

    cards: list[dict[str, Any]] = []
    for entry in race_form.get("formRaceEntries") or []:
        horse_no = parse_numeric_int(entry.get("raceEntryNumber"))
        if horse_no is None or horse_no <= 0:
            continue

        horse_name = str(entry.get("horseName") or "").strip()
        scratched = bool(entry.get("scratched"))

        cards.append(
            {
                "raceDate": fixture_ctx.get("raceDate"),
                "startTime": race_payload["startTime"],
                "startTimeZoned": race_payload["startTimeZoned"],
                "course": race_payload["course"],
                "raceId": race_payload["raceId"],
                "div": race_payload["div"],
                "horseNo": horse_no,
                "horseId": parse_numeric_int(entry.get("horseCode")) or parse_numeric_int((entry.get("horse") or {}).get("id")) or -999,
                "horseName": horse_name,
                "countryOfOrigin": str(entry.get("horseCountry") or "AUS").upper(),
                "jockey": None if scratched else normalize_jockey_name(entry.get("jockeyUrl"), entry.get("jockeyName")),
                "trainer": normalize_trainer_name(entry.get("trainerUrl"), entry.get("trainerName")),
                "jockeyId": None if scratched else parse_numeric_int(entry.get("jockeyCode")),
                "trainerId": parse_numeric_int(entry.get("trainerCode")),
                "draw": None if scratched else parse_numeric_int(entry.get("liveBarrierNumber") or entry.get("barrierNumber")),
                "rank": None,
                "finishingTime": None,
                "weightCarried": parse_weight_carried(entry.get("weight"), entry.get("apprenticeAllowedClaim")),
                "last1fTime": None,
                "last1fSplit": None,
                "last1fPos": None,
                "last1f": None,
                "last2fTime": None,
                "last2fSplit": None,
                "last2fPos": None,
                "last2f": None,
                "last3fTime": None,
                "last3fSplit": None,
                "last3fPos": None,
                "last3f": None,
                "last4fTime": None,
                "last4fSplit": None,
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
                "first2fTime": None,
                "first2fSplit": None,
                "first2fPos": None,
                "first2f": None,
                "sp": parse_card_price(entry),
                "meta": {
                    "horse": full_entry_meta(entry),
                    "cardRace": {
                        "id": race_form.get("id"),
                        "status": race_form.get("status"),
                        "tempo": race_form.get("tempo"),
                        "rdcClass": race_form.get("rdcClass"),
                        "location": race_form.get("location"),
                        "venueCode": race_form.get("venueCode"),
                        "venueState": race_form.get("venueState"),
                        "distance": race_form.get("distance"),
                        "trackCondition": race_form.get("trackCondition"),
                        "trackRating": race_form.get("trackRating"),
                        "class": race_form.get("class"),
                        "group": race_form.get("group"),
                        "nameForm": race_form.get("nameForm"),
                        "bestBets": deepcopy(race_form.get("bestBets")),
                        "raceTips": deepcopy(race_form.get("raceTips")),
                    },
                },
            }
        )

    return cards


def transform_race_item(item: dict[str, Any], fixture_ctx: dict[str, Any]) -> dict[str, Any] | None:
    fixture_date = parse_fixture_date(fixture_ctx.get("raceDate"))
    if fixture_date is None:
        return None

    if is_abandoned_race_status(item.get("raceStatus")):
        logger.info(
            "Skipping abandoned racing.com race raceDate=%s course=%s raceNo=%s raceId=%s",
            fixture_ctx.get("raceDate"),
            fixture_ctx.get("course"),
            item.get("raceNumber"),
            item.get("id"),
        )
        return None

    race_id_raw = parse_numeric_int(item.get("id"))
    race_no = parse_numeric_int(item.get("raceNumber"))
    if race_id_raw is None or race_no is None:
        logger.warning("Skipping race item with invalid identifiers id=%r raceNumber=%r", item.get("id"), item.get("raceNumber"))
        return None

    # Keep the fixture course as the canonical downstream key so getFixtures/getRaces
    # correlate on the same meeting course/date pair.
    course = normalize_course(
        fixture_ctx.get("course")
        or item.get("meet", {}).get("venue")
    )
    distance = parse_distance_text(item.get("distance"))
    surface = infer_surface(item.get("condition"), course)
    direction = get_direction(course, str(int(distance)), surface) if course and distance is not None else None
    rdc_class = (item.get("rdcClass") or "").strip()
    rdc_class_upper = rdc_class.upper()
    if "STEEPLECHASE" in rdc_class_upper:
        race_type = "Steeplechase"
    elif "HURDLE" in rdc_class_upper:
        race_type = "Hurdle"
    else:
        race_type = "FLAT"
    state = (fixture_ctx.get("meta") or {}).get("state") or item.get("meet", {}).get("state")
    start_time, start_time_zoned = parse_start_times(item.get("time"), timezone_for_state(state))
    if start_time is None:
        logger.warning("Skipping race item with invalid time id=%s time=%r", race_id_raw, item.get("time"))
        return None

    return {
        "raceDate": fixture_date.isoformat(),
        "course": course,
        "raceNo": race_no,
        "distance": distance,
        "distanceText": item.get("distance"),
        "prizeMoney": parse_numeric_int(item.get("totalPrizeMoney")),
        "raceType": race_type,
        "going": item.get("trackCondition"),
        "goingText": build_going_text(item.get("trackCondition"), item.get("trackRating")),
        "reading": parse_numeric_float(item.get("trackRating")),
        "raceClass": item.get("rdcClass"),
        "direction": direction,
        "raceId": RACE_ID_BASE_AUS + race_id_raw,
        "div": 0,
        "startTime": start_time,
        "startTimeZoned": start_time_zoned,
        "ratingRange": None,
        "currency": "AUD",
        "surface": surface,
        "country": "AUS",
        "meta": {
            "meetingId": fixture_ctx.get("meetingId"),
            "race_meet_id": fixture_ctx.get("race_meet_id"),
            "state": (fixture_ctx.get("meta") or {}).get("state") or item.get("meet", {}).get("state"),
            "race": item,
        },
    }


def transform_race_items(items: list[dict[str, Any]], fixture_ctx: dict[str, Any]) -> list[dict[str, Any]]:
    races: list[dict[str, Any]] = []
    for item in items:
        transformed = transform_race_item(item, fixture_ctx)
        if transformed:
            races.append(transformed)
    return races


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
        event_dt = datetime.fromisoformat(normalized_dt)
    except ValueError:
        logger.warning(
            "Skipping calendar item with invalid event_start_time=%r race_meet_id=%s",
            event_start_time,
            race_meet_id,
        )
        return None

    state = item.get("state")
    event_tz = timezone_for_state(state)
    if event_dt.tzinfo is None:
        event_dt = event_dt.replace(tzinfo=event_tz)
    race_date = event_dt.astimezone(event_tz).date()

    course = normalize_course(item.get("location_name") or item.get("club_name") or item.get("name"))
    if not course:
        logger.warning("Skipping calendar item with missing course fields race_meet_id=%s", race_meet_id)
        return None

    return {
        "course": str(course),
        "raceDate": race_date,
        "year": race_date.year,
        "meetingId": race_meet_id,
        "meta": {
            **item,
            "requestYear": int(request_year),
            "requestMonth": int(request_month),
            "race_meet_id": race_meet_id,
        },
    }
