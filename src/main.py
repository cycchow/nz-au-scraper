import argparse
import asyncio
import json
import logging
import logging.config
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# Load .env before importing modules that read env vars at import time.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

from scrapers import LoveracingProvider, RacingComProvider, ScraperProvider
from scrapers.base import FixtureProcessOutput
from utils.graphql_client import graphql_subscribe, send_merge_mutation

LOG_LEVEL = "INFO"
LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {"format": "%(asctime)s %(levelname)s %(name)s: %(message)s"},
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "stream": "ext://sys.stdout",
        },
    },
    "root": {"level": LOG_LEVEL, "handlers": ["console"]},
}

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(__name__)


PROVIDERS: dict[str, ScraperProvider] = {
    "loveracing": LoveracingProvider(),
    "racingcom": RacingComProvider(),
}


def get_provider(source: str) -> ScraperProvider:
    provider = PROVIDERS.get(source)
    if not provider:
        raise ValueError(f"Unknown source '{source}'. Available: {', '.join(sorted(PROVIDERS))}")
    return provider


def get_provider_for_fixture(fixture: dict[str, Any]) -> ScraperProvider | None:
    src = fixture.get("src")
    if not src:
        logger.warning("Skipping fixture without src fixtureId=%s", fixture.get("fixtureId"))
        return None

    provider = PROVIDERS.get(str(src))
    if provider is None:
        logger.warning("Skipping fixture with unknown src=%r fixtureId=%s", src, fixture.get("fixtureId"))
        return None

    return provider


def get_provider_for_race(race: dict[str, Any]) -> ScraperProvider | None:
    for provider in PROVIDERS.values():
        try:
            if provider.accepts_race(race):
                return provider
        except Exception as exc:  # noqa: BLE001
            logger.warning("Provider accepts_race failed provider=%s raceId=%s: %s", provider.name, race.get("raceId"), exc)
    meta = race.get("meta")
    if isinstance(meta, dict):
        meta_summary = f"dict_keys={sorted(meta.keys())}"
    elif isinstance(meta, str):
        meta_summary = f"str={meta[:300]!r}"
    else:
        meta_summary = f"type={type(meta).__name__} value={meta!r}"
    logger.warning(
        "Skipping race with no matching provider raceId=%s raceDate=%s course=%s country=%r meta=%s",
        race.get("raceId"),
        race.get("raceDate"),
        race.get("course"),
        race.get("country"),
        meta_summary,
    )
    return None


def fixture_id_base_for_country(country: str) -> int:
    if country == "NZ":
        return 6000000000
    if country == "AUS":
        return 7000000000
    if country == "KSA":
        return 9000000000
    return 8000000000


def src_for_country(country: str, provider: ScraperProvider | None = None) -> str:
    if provider:
        return provider.source_code
    if country == "NZ":
        return "loveracing"
    return "era"


def normalize_fixture_race_date(value: Any) -> date | None:
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
        return date.fromisoformat(text.replace("Z", "+00:00").split("T")[0])
    except ValueError:
        return None


def save_fixtures(fixtures, country="NZ", provider: ScraperProvider | None = None):
    fixture_id_base = fixture_id_base_for_country(country)
    src = src_for_country(country, provider=provider)

    for fixture in fixtures:
        race_date = normalize_fixture_race_date(fixture.get("raceDate"))
        if race_date is None:
            logger.warning("Skipping fixture with invalid raceDate=%r", fixture.get("raceDate"))
            continue

        meta = fixture.get("meta", {}) or {}
        meeting_id = fixture.get("meetingId")
        if meeting_id is None:
            meeting_id = meta.get("DayID")
        if meeting_id is None:
            meeting_id = meta.get("race_meet_id")

        if meeting_id is None:
            logger.warning("Skipping fixture without meeting identifier: raceDate=%s", fixture.get("raceDate"))
            continue

        meeting_id = int(meeting_id)
        input_obj = {
            "raceDate": race_date.isoformat(),
            "course": fixture.get("course"),
            "raceType": "FLAT",
            "surface": None,
            "going": None,
            "weather": None,
            "reading": None,
            "raceClass": None,
            "fixtureId": fixture_id_base + meeting_id,
            "fixtureYear": fixture.get("year") or fixture.get("fixtureYear") or race_date.year,
            "meetingId": meeting_id,
            "noOfRaces": None,
            "stalls": None,
            "src": src,
            "country": country,
            "meta": json.loads(json.dumps(fixture.get("meta", fixture), default=json_compatible_default)),
        }

        try:
            logger.info("Merging fixture raceDate=%s course=%s", input_obj["raceDate"], input_obj["course"])
            send_merge_mutation("com.superstring.globalracing.uk.models.types.Fixture", input_obj)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed fixture raceDate=%s: %s", input_obj["raceDate"], exc)


def save_races(races: list[dict[str, Any]]):
    for race in races:
        payload = json.loads(json.dumps(race, default=json_compatible_default))
        try:
            logger.info(
                "Merging race raceDate=%s course=%s raceNo=%s raceId=%s",
                payload.get("raceDate"),
                payload.get("course"),
                payload.get("raceNo"),
                payload.get("raceId"),
            )
            send_merge_mutation("com.superstring.globalracing.uk.models.types.Races", payload)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed race merge raceId=%s raceNo=%s: %s", payload.get("raceId"), payload.get("raceNo"), exc)


def save_results(results: list[dict[str, Any]]):
    for result in results:
        payload = json.loads(json.dumps(result, default=json_compatible_default))
        try:
            logger.info(
                "Merging result raceDate=%s course=%s raceNo=%s raceId=%s horseNo=%s horseName=%s "
                "first2fTime=%s first2fSplit=%s first2fPos=%s first2f=%s "
                "last4fSplit=%s last3fSplit=%s last1fSplit=%s",
                payload.get("raceDate"),
                payload.get("course"),
                payload.get("meta", {}).get("horse", {}).get("raceNumber"),
                payload.get("raceId"),
                payload.get("horseNo"),
                payload.get("horseName"),
                payload.get("first2fTime"),
                payload.get("first2fSplit"),
                payload.get("first2fPos"),
                payload.get("first2f"),
                payload.get("last4fSplit"),
                payload.get("last3fSplit"),
                payload.get("last1fSplit"),
            )
            logger.debug("Results merge payload: %s", json.dumps(payload, ensure_ascii=False, default=json_compatible_default))
            send_merge_mutation("com.superstring.globalracing.uk.models.types.Results", payload)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Failed result merge raceId=%s horseNo=%s: %s",
                payload.get("raceId"),
                payload.get("horseNo"),
                exc,
            )


def json_compatible_default(value: Any):
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def parse_date_or_month_arg(value: str) -> date:
    text = (value or "").strip()
    if not text:
        raise argparse.ArgumentTypeError("Expected YYYY-MM or YYYY-MM-DD format")

    if len(text) == 7:
        try:
            return date.fromisoformat(f"{text}-01")
        except ValueError as exc:
            raise argparse.ArgumentTypeError("Expected YYYY-MM or YYYY-MM-DD format") from exc

    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Expected YYYY-MM or YYYY-MM-DD format") from exc


def month_window(from_month: date, to_month: date) -> tuple[date, date]:
    start_month = min(from_month, to_month).replace(day=1)
    end_month = max(from_month, to_month).replace(day=1)

    if end_month.month == 12:
        next_month = date(end_month.year + 1, 1, 1)
    else:
        next_month = date(end_month.year, end_month.month + 1, 1)

    end_date = next_month - timedelta(days=1)
    return start_month, end_date


def date_window(from_date: date, to_date: date) -> tuple[date, date]:
    return min(from_date, to_date), max(from_date, to_date)


def run_fixture_ingestion(provider: ScraperProvider, from_month: date, to_month: date, country: str):
    # Fixture fetchers operate by month buckets, so truncate any explicit day input.
    total_fixtures = provider.fetch_fixtures_for_ingestion(from_month.replace(day=1), to_month.replace(day=1))
    logger.info("Total fixtures transformed=%s source=%s", len(total_fixtures), provider.name)
    save_fixtures(total_fixtures, country=country, provider=provider)


def build_get_fixtures_subscription() -> str:
    return """
    subscription getFixtures($from: Date!, $to: Date!, $fetchAll: Boolean, $country: String, $course: String) {
      getFixtures(from: $from, to: $to, fetchAll: $fetchAll, country: $country, course: $course) {
        fixtureId
        fixtureYear
        raceDate
        course
        src
        country
        meta
      }
    }
    """


def build_get_races_subscription() -> str:
    return """
    subscription getRaces($from: Date!, $to: Date!, $fetchAll: Boolean, $country: String, $course: String) {
      getRaces(from: $from, to: $to, fetchAll: $fetchAll, country: $country, course: $course) {
        raceDate
        course
        raceNo
        startTime
        startTimeZoned
        raceId
        div
        country
        meta
      }
    }
    """


async def get_fixtures_from_graphql(
    from_date: date,
    to_date: date,
    country: str = "NZ",
    fetch_all: bool = False,
    course: str | None = None,
) -> list[dict[str, Any]]:
    subscription = build_get_fixtures_subscription()
    variables = {
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "fetchAll": fetch_all,
        "country": country,
        "course": course,
    }

    fixtures: list[dict[str, Any]] = []
    async for data in graphql_subscribe(subscription, variables):
        item = data.get("getFixtures")
        if item:
            fixtures.append(item)

    return fixtures


async def get_races_from_graphql(
    from_date: date,
    to_date: date,
    country: str = "NZ",
    fetch_all: bool = False,
    course: str | None = None,
) -> list[dict[str, Any]]:
    subscription = build_get_races_subscription()
    variables = {
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "fetchAll": fetch_all,
        "country": country,
        "course": course,
    }

    races: list[dict[str, Any]] = []
    async for data in graphql_subscribe(subscription, variables):
        item = data.get("getRaces")
        if item:
            races.append(item)

    return races


def process_fixture_for_races_record(provider: ScraperProvider, fixture: dict[str, Any]):
    if not provider.accepts_fixture(fixture):
        logger.debug("Skipping fixture not accepted by provider=%s fixtureId=%s", provider.name, fixture.get("fixtureId"))
        return

    meta_before = json.dumps(fixture.get("meta", {}), sort_keys=True, default=str)
    try:
        races = provider.parse_fixture_races(fixture)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Failed race parse source=%s fixtureId=%s: %s",
            provider.name,
            fixture.get("fixtureId"),
            exc,
        )
        return

    meta_after = json.dumps(fixture.get("meta", {}), sort_keys=True, default=str)
    if meta_after != meta_before:
        save_fixtures([fixture], country=fixture.get("country") or provider.default_country, provider=provider)

    logger.info(
        "Parsed fixture races source=%s fixtureId=%s raceDate=%s course=%s races=%s",
        provider.name,
        fixture.get("fixtureId"),
        fixture.get("raceDate"),
        fixture.get("course"),
        len(races),
    )
    save_races(races)


def process_race_for_results_record(provider: ScraperProvider, race: dict[str, Any]):
    if not provider.accepts_race(race):
        logger.debug("Skipping race not accepted by provider=%s raceId=%s", provider.name, race.get("raceId"))
        return

    try:
        results = provider.parse_race_results(race)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Failed result parse source=%s raceDate=%s course=%s raceNo=%s raceId=%s: %s",
            provider.name,
            race.get("raceDate"),
            race.get("course"),
            race.get("raceNo"),
            race.get("raceId"),
            exc,
        )
        return

    logger.info(
        "Parsed race results source=%s raceDate=%s course=%s raceNo=%s raceId=%s results=%s",
        provider.name,
        race.get("raceDate"),
        race.get("course"),
        race.get("raceNo"),
        race.get("raceId"),
        len(results),
    )
    save_results(results)


def process_fixture_record(provider: ScraperProvider, fixture: dict[str, Any]):
    if not provider.accepts_fixture(fixture):
        logger.debug("Skipping fixture not accepted by provider=%s fixtureId=%s", provider.name, fixture.get("fixtureId"))
        return

    meta_before = json.dumps(fixture.get("meta", {}), sort_keys=True, default=str)
    try:
        parsed: FixtureProcessOutput = provider.parse_fixture(fixture)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Failed provider parse source=%s fixtureId=%s: %s",
            provider.name,
            fixture.get("fixtureId"),
            exc,
        )
        return

    meta_after = json.dumps(fixture.get("meta", {}), sort_keys=True, default=str)
    if meta_after != meta_before:
        save_fixtures([fixture], country=fixture.get("country") or provider.default_country, provider=provider)

    logger.info(
        "Parsed fixture source=%s fixtureId=%s races=%s results=%s",
        provider.name,
        fixture.get("fixtureId"),
        len(parsed.races),
        len(parsed.results),
    )
    save_races(parsed.races)
    save_results(parsed.results)


async def process_fixtures_for_races_from_graphql(
    from_date: date,
    to_date: date,
    country: str,
    source_filter: str | None = None,
    fetch_all: bool = False,
    course: str | None = None,
):
    fixtures = await get_fixtures_from_graphql(from_date, to_date, country, fetch_all=fetch_all, course=course)
    logger.info(
        "Fetched fixtures from graphql count=%s country=%s course=%s sourceFilter=%s fetchAll=%s",
        len(fixtures),
        country,
        course,
        source_filter,
        fetch_all,
    )

    for fixture in fixtures:
        fixture_src = fixture.get("src")
        if source_filter and fixture_src != source_filter:
            continue

        provider = get_provider_for_fixture(fixture)
        if provider is None:
            continue
        process_fixture_for_races_record(provider, fixture)


async def process_races_for_results_from_graphql(
    from_date: date,
    to_date: date,
    country: str,
    source_filter: str | None = None,
    fetch_all: bool = False,
    course: str | None = None,
):
    races = await get_races_from_graphql(from_date, to_date, country, fetch_all=fetch_all, course=course)
    fixtures = await get_fixtures_from_graphql(from_date, to_date, country, fetch_all=True, course=course)
    fixture_lookup = {
        (
            str(item.get("raceDate") or ""),
            str(item.get("course") or ""),
            str(item.get("country") or country or ""),
        ): item
        for item in fixtures
    }
    logger.info(
        "Fetched races from graphql count=%s country=%s course=%s sourceFilter=%s fetchAll=%s fixtureContextCount=%s",
        len(races),
        country,
        course,
        source_filter,
        fetch_all,
        len(fixtures),
    )

    for race in races:
        if race.get("meta") in (None, "", {}):
            fixture_key = (
                str(race.get("raceDate") or ""),
                str(race.get("course") or ""),
                str(race.get("country") or country or ""),
            )
            fixture = fixture_lookup.get(fixture_key)
            if fixture is not None:
                race = dict(race)
                race["meta"] = fixture.get("meta")

        provider = get_provider_for_race(race)
        if provider is None:
            continue
        if source_filter and provider.source_code != source_filter:
            continue
        process_race_for_results_record(provider, race)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Global racing scraper orchestrator")
    parser.add_argument("--source", choices=sorted(PROVIDERS.keys()))

    args_preview, _ = parser.parse_known_args(argv)
    preview_provider = get_provider(args_preview.source) if args_preview.source else None

    today_month = date.today().replace(day=1)
    parser.add_argument("--from", dest="from_month", type=parse_date_or_month_arg, default=today_month)
    parser.add_argument("--to", dest="to_month", type=parse_date_or_month_arg, default=date(2023, 1, 1))
    parser.add_argument("--country", default=preview_provider.default_country if preview_provider else "NZ")
    parser.add_argument("--course")
    parser.add_argument("--mode", choices=["fixtures", "races-results"], default="fixtures")
    parser.add_argument("--fetch-all", action="store_true")
    args = parser.parse_args(argv)

    if args.mode == "fixtures":
        provider = get_provider(args.source or "loveracing")
        run_fixture_ingestion(provider, args.from_month, args.to_month, args.country)
        return

    from_date, to_date = date_window(args.from_month, args.to_month)
    logger.info(
        "Races-results mode sourceFilter=%s fetchAll=%s date window from=%s to=%s",
        args.source,
        args.fetch_all,
        from_date.isoformat(),
        to_date.isoformat(),
    )
    logger.info("Starting race generation phase")
    asyncio.run(
        process_fixtures_for_races_from_graphql(
            from_date,
            to_date,
            args.country,
            source_filter=args.source,
            fetch_all=args.fetch_all,
            course=args.course,
        )
    )
    logger.info("Completed race generation phase")
    logger.info("Starting result rerun phase")
    asyncio.run(
        process_races_for_results_from_graphql(
            from_date,
            to_date,
            args.country,
            source_filter=args.source,
            fetch_all=args.fetch_all,
            course=args.course,
        )
    )
    logger.info("Completed result rerun phase")


if __name__ == "__main__":
    main()
