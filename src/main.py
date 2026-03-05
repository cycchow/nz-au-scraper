import argparse
import asyncio
import json
import logging
import logging.config
from datetime import date, timedelta
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


def fixture_id_base_for_country(country: str) -> int:
    if country == "NZ":
        return 6000000000
    if country == "KSA":
        return 9000000000
    return 8000000000


def src_for_country(country: str, provider: ScraperProvider | None = None) -> str:
    if provider:
        return provider.source_code
    if country == "NZ":
        return "loveracing"
    return "era"


def save_fixtures(fixtures, country="NZ", provider: ScraperProvider | None = None):
    fixture_id_base = fixture_id_base_for_country(country)
    src = src_for_country(country, provider=provider)

    for fixture in fixtures:
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
            "raceDate": fixture.get("raceDate").isoformat(),
            "course": fixture.get("course"),
            "raceType": "FLAT",
            "surface": None,
            "going": None,
            "weather": None,
            "reading": None,
            "raceClass": None,
            "fixtureId": fixture_id_base + meeting_id,
            "fixtureYear": fixture.get("year"),
            "meetingId": meeting_id,
            "noOfRaces": None,
            "stalls": None,
            "src": src,
            "country": country,
            "meta": json.loads(json.dumps(fixture.get("meta", fixture), default=str)),
        }

        try:
            logger.info("Merging fixture raceDate=%s course=%s", input_obj["raceDate"], input_obj["course"])
            send_merge_mutation("com.superstring.globalracing.uk.models.types.Fixture", input_obj)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed fixture raceDate=%s: %s", input_obj["raceDate"], exc)


def save_races(races: list[dict[str, Any]]):
    for race in races:
        payload = json.loads(json.dumps(race, default=str))
        try:
            send_merge_mutation("com.superstring.globalracing.uk.models.types.Races", payload)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed race merge raceId=%s raceNo=%s: %s", payload.get("raceId"), payload.get("raceNo"), exc)


def save_results(results: list[dict[str, Any]]):
    for result in results:
        payload = json.loads(json.dumps(result, default=str))
        try:
            logger.info(
                "Merging result raceId=%s horseNo=%s horseName=%s "
                "first2fTime=%s first2fSplit=%s first2fPos=%s first2f=%s "
                "last4fSplit=%s last3fSplit=%s last1fSplit=%s",
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
            logger.debug("Results merge payload: %s", json.dumps(payload, ensure_ascii=False, default=str))
            send_merge_mutation("com.superstring.globalracing.uk.models.types.Results", payload)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Failed result merge raceId=%s horseNo=%s: %s",
                payload.get("raceId"),
                payload.get("horseNo"),
                exc,
            )


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


async def get_fixtures_from_graphql(from_date: date, to_date: date, country: str = "NZ") -> list[dict[str, Any]]:
    subscription = """
    subscription getFixtures($from: Date!, $to: Date!, $fetchAll: Boolean, $country: String) {
      getFixtures(from: $from, to: $to, fetchAll: $fetchAll, country: $country) {
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
    variables = {
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "fetchAll": True,
        "country": country,
    }

    fixtures: list[dict[str, Any]] = []
    async for data in graphql_subscribe(subscription, variables):
        item = data.get("getFixtures")
        if item:
            fixtures.append(item)

    return fixtures


def process_fixture_record(provider: ScraperProvider, fixture: dict[str, Any]):
    if not provider.accepts_fixture(fixture):
        logger.debug("Skipping fixture not accepted by provider=%s fixtureId=%s", provider.name, fixture.get("fixtureId"))
        return

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

    logger.info(
        "Parsed fixture source=%s fixtureId=%s races=%s results=%s",
        provider.name,
        fixture.get("fixtureId"),
        len(parsed.races),
        len(parsed.results),
    )
    save_races(parsed.races)
    save_results(parsed.results)


async def process_fixtures_from_graphql(provider: ScraperProvider, from_date: date, to_date: date, country: str):
    fixtures = await get_fixtures_from_graphql(from_date, to_date, country)
    logger.info("Fetched fixtures from graphql count=%s source=%s", len(fixtures), provider.name)

    for fixture in fixtures:
        process_fixture_record(provider, fixture)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Global racing scraper orchestrator")
    parser.add_argument("--source", choices=sorted(PROVIDERS.keys()), default="loveracing")

    args_preview, _ = parser.parse_known_args(argv)
    provider = get_provider(args_preview.source)

    today_month = date.today().replace(day=1)
    parser.add_argument("--from", dest="from_month", type=parse_date_or_month_arg, default=today_month)
    parser.add_argument("--to", dest="to_month", type=parse_date_or_month_arg, default=date(2023, 1, 1))
    parser.add_argument("--country", default=provider.default_country)
    parser.add_argument("--mode", choices=["fixtures", "races-results"], default="fixtures")
    args = parser.parse_args(argv)

    provider = get_provider(args.source)

    if args.mode == "fixtures":
        run_fixture_ingestion(provider, args.from_month, args.to_month, args.country)
        return

    from_date, to_date = date_window(args.from_month, args.to_month)
    logger.info(
        "Races-results mode source=%s date window from=%s to=%s",
        provider.name,
        from_date.isoformat(),
        to_date.isoformat(),
    )
    asyncio.run(process_fixtures_from_graphql(provider, from_date, to_date, args.country))


if __name__ == "__main__":
    main()
