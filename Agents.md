# nz-au-scraper Agent Notes

Purpose: quick project context for future coding sessions without re-reading the repo.

## What this project does

This is a Python scraper/orchestrator for NZ and AU racing data.

Primary flow:
1. Fetch fixture data from a provider site.
2. Transform fixtures into a normalized payload.
3. Merge fixtures, races, and results into a downstream GraphQL service.

Current providers:
- `loveracing`: NZ, fully wired for fixture ingestion and race/result parsing.
- `racingcom`: AUS, fixture ingestion plus race/result parsing are now wired.

Current repo shape:
- small Python project, no package build system yet
- dependencies installed from `requirements.txt`
- tests run directly with `pytest`
- `tests/conftest.py` injects `src/` into `sys.path`

## Entry point

- `src/main.py`

Main CLI:
- `python src/main.py`
- `python src/main.py --source loveracing --mode fixtures --from 2026-03 --to 2026-01`
- `python src/main.py --source loveracing --mode races-results --from 2026-03-01 --to 2026-03-06`
- `python src/main.py --source racingcom --mode fixtures --from 2026-03 --to 2026-01`

Useful local setup:
- `python -m pip install -r requirements.txt`
- `pytest`

CLI behavior:
- `--source`: `loveracing` or `racingcom`
- `--mode`:
  - `fixtures`: fetch provider fixtures by month and merge them to GraphQL
  - `races-results`: read fixtures back from GraphQL, parse them via provider, then merge races/results
- `--from` / `--to`: accepts `YYYY-MM` or `YYYY-MM-DD`
- if `--source` is omitted, `main.py` still defaults to `loveracing`
- `.env` is loaded before provider/config imports

## Important modules

- `src/main.py`: orchestration, CLI, GraphQL save operations
- `src/scrapers/base.py`: provider protocol and `FixtureProcessOutput`
- `src/scrapers/loveracing_provider.py`: NZ provider implementation
- `src/scrapers/racingcom_provider.py`: AUS provider implementation for fixture->races and race->results routing
- `src/loveracing/loveracing.py`: main Loveracing scraping/parsing logic
- `src/racingcom/racingcom.py`: Racing.com runtime discovery, calendar fetch, race list fetch, race form fetch, sectional mapping, and AU transforms
- `src/utils/graphql_client.py`: GraphQL mutation/subscription transport
- `src/utils/config.py`: GraphQL endpoint and TLS env config
- `src/utils/course_utils.py`, `src/utils/jockey_name_mapping.py`: transformation helpers

## Runtime assumptions

Environment variables in use:
- `GRAPHQL_ENDPOINT`
- `GRAPHQL_WS_ENDPOINT`
- `GRAPHQL_VERIFY_TLS`

Defaults point at local endpoints:
- HTTP: `https://localhost:8888/uk-data/graphql`
- WS: `wss://localhost:8888/uk-data/graphql`

Observed repo-local `.env` exists at project root and is loaded by `src/main.py` before imports.

Notes:
- `GRAPHQL_VERIFY_TLS=false` is intended for local/self-signed environments.
- `send_merge_mutation()` builds GraphQL input literals manually, so payload shape changes should be reviewed carefully.
- `save_results()` now prefers backend `addResults(input: [JSON!]!)` with GraphQL variables for bulk result writes.
- `graphql_subscribe()` uses websocket transport with schema fetch enabled, so connection issues can come from endpoint, TLS, or schema negotiation.

Minimal `.env` expectation:
- `GRAPHQL_ENDPOINT=https://localhost:8888/uk-data/graphql`
- `GRAPHQL_WS_ENDPOINT=wss://localhost:8888/uk-data/graphql`
- `GRAPHQL_VERIFY_TLS=false` for self-signed local dev only

## Data flow

### Fixture ingestion mode

`main.py` -> provider `fetch_fixtures_for_ingestion()` -> `save_fixtures()` -> GraphQL `merge()`

Fixture IDs:
- NZ base: `6000000000`
- AUS base: `7000000000`
- default fallback base: `8000000000`
- KSA base: `9000000000`

Source codes:
- `loveracing` provider saves `src="loveracing"`
- `racingcom` provider saves `src="racingcom"`
- fallback non-provider country logic uses `src="era"`

Expected fixture payload shape sent to GraphQL:
- required in practice: `raceDate`, `course`, `fixtureYear`, `meetingId`, `fixtureId`, `src`, `country`, `meta`
- fields always included by `save_fixtures()`: `raceType`, `surface`, `going`, `weather`, `reading`, `raceClass`, `noOfRaces`, `stalls`
- `save_fixtures()` normalizes `raceDate`; ISO datetime strings like `2026-03-18T01:30:00Z` are accepted and truncated to date
- meeting identifier resolution order:
  1. top-level `meetingId`
  2. `meta.DayID`
  3. `meta.race_meet_id`

Racing.com fixture specifics:
- default provider country is `AUS`
- `transform_calendar_item()` requires numeric `race_meet_id`
- racing.com top-level `meetingId` is now the raw `race_meet_id`
- persisted downstream `fixtureId` is `7000000000 + meetingId`
- this now aligns with the NZ convention:
  - keep `meetingId` as the raw provider meeting code
  - derive `fixtureId` as `country base + meetingId`
- calendar query currently uses:
  - `meetTypes`: `Metro`, `Provincial`, `Country`, `Picnic`
  - `eventTypes`: `Racing`
  - `states`: `VIC`, `SA`, `NSW`, `QLD`, `WA`, `ACT`, `NT`, `TAS`
- runtime key discovery is HTTP-first:
  - fetch `https://www.racing.com`
  - discover Next.js chunk URLs from script tags / `__NEXT_DATA__` / `_buildManifest.js`
  - scan chunks for `CUSTOM_SITE_CONFIG`
  - extract `appSyncGraphQLHost` and `appSyncGraphQLAPIKey`
- fixture `raceDate` must be treated as the meeting's local state date, not raw UTC `.date()`
- course normalization matters before save:
  - Racing.com fixtures and races now run through `normalize_course()`
  - `course_abbr_mapping` is applied first
  - then common sponsor prefixes are stripped case-insensitively:
    - `BET365`
    - `LADBROKES`
    - `SOUTHSIDE`
    - `SPORTSBET`
    - `SPORTSBET-`
- state timezone mapping now matters for Racing.com:
  - `NSW`, `ACT`, `VIC`, `TAS` -> `Australia/Sydney`
  - `QLD` -> `Australia/Brisbane`
  - `SA` -> `Australia/Adelaide`
  - `WA` -> `Australia/Perth`
  - `NT` -> `Australia/Darwin`
- race times now use state-local time:
  - `startTime`: naive local wall time in the meeting's state timezone
  - `startTimeZoned`: same instant with timezone attached
  - source field for race start time is the race item `time` property, not fixture `raceDate`

### Races/results mode

Current orchestration in `src/main.py` is two-phase for `--mode=races-results`:
1. race generation phase:
   - subscribe `getFixtures()` from GraphQL
   - provider `accepts_fixture()`
   - provider `parse_fixture_races()`
   - `save_races()`
   - Racing.com also saves racecards in this phase for today/future fixtures
2. result rerun phase:
   - subscribe `getRaces()` from GraphQL
   - provider `accepts_race()`
   - provider `parse_race_results()`
   - `save_results()`

Older `parse_fixture()` still exists for compatibility but is no longer the primary orchestration path.

Loveracing parsing split:
- future/today fixtures: meeting overview HTML
- past fixtures: XML results + sectional fetch
- same-day NZ fixtures now follow the split pipeline correctly:
  - `parse_fixture_races()` uses overview HTML for same-day/future fixtures
  - same-day XML is only an overlay for already-started races when `ResultDownloadXML` exists
  - `parse_fixture_cards()` now exists for Loveracing so today/future NZ racecards are saved in `races-results` mode

Expected race/result behavior:
- provider returns `FixtureProcessOutput(races=[...], results=[...])`
- `save_races()` and `save_results()` JSON-normalize payloads before merge
- failures are logged per record and do not stop the whole run

GraphQL write behavior:
- fixtures still use single-row `merge(...)`
- races use client-side batched `merge(...)`
- results now prefer backend bulk `addResults(input: [JSON!]!)`
- `addResults(...)` is used with GraphQL variables so large `meta` payloads are not embedded into the mutation text
- this avoids the Spring GraphQL / GraphQL Java parser-token failure seen with large inline aliased `merge(...)` requests:
  - `More than 15,000 'grammar' tokens have been presented`
- if a bulk `addResults(...)` call fails, the client recursively splits the chunk and eventually falls back to single-row `merge(...)`

Racing.com race/result specifics:
- `parse_fixture_races()` uses `getNoCacheRacesForMeet`
- `parse_race_results()` uses `getRaceForm` / `getRaceResults_CD`
- Racing.com racecards use the richer `getRaceEntriesForField_CD` / `getRaceForm` payload
- future/today Racing.com fixtures now persist:
  - race rows from `getNoCacheRacesForMeet`
  - card rows in the downstream `Results` model from `getRaceEntriesForField_CD`
- card rows follow the Loveracing pattern:
  - `rank=None`
  - `finishingTime=None`
  - no sectional fields
  - `meta.horse` keeps the raw entry payload
  - `meta.cardRace` keeps selected racecard-level context such as `status`, `tempo`, `bestBets`, and `raceTips`
- race provider acceptance uses `meta.race_meet_id` first
- if race `meta.meetingId` is present instead, Racing.com provider only accepts it when:
  - `meetingId >= RACE_ID_BASE_AUS` and can be converted back to meet code, or
  - `meetingId` is a large raw Racing.com meet code (`>= 1_000_000`)
- race `meta` must include `state` for sectional reruns to work correctly
- saved race `meta` now includes:
  - `meetingId`
  - `race_meet_id`
  - `state`
  - `race`
- abandoned races are skipped:
  - if a race list item has `raceStatus="Abandoned"`, no race row is saved
  - if `getRaceForm` returns `raceStatus="Abandoned"`, no result rows are saved
  - if a racecard `getRaceForm` returns `raceStatus="Abandoned"`, no card rows are saved
- surface inference:
  - Racing.com defaults to `DIRT` when condition text includes `synthetic`, `polytrack`, or `dirt`
  - Racing.com also defaults to `DIRT` when the course name contains `POLY`, `SYNTHETIC`, or `TAPETA` (case-insensitive)
- jockey cleanup in Racing.com:
  - `"-"` is normalized to `None`
  - trailing suffixes are stripped case-insensitively before name mapping:
    - `GB`
    - `HK`
    - `GER`
    - `FR`
    - `NZ`
    - `JPN`
    - `IRE`
    - `JNR`

Sectionals:
- sectionals are not fetched directly from Racing.com
- the scraper posts to local bridge endpoints on `localhost:8080`
- routing rules:
  - `VIC` with `hasSectionals=true` -> `http://localhost:8080/racingdotcom`
  - `NSW` -> `http://localhost:8080/racingnsw`
  - `QLD` -> `http://localhost:8080/racingqld`
  - other states currently skip sectionals
- important regression found and fixed:
  - results could merge without any NSW sectional call if race-level `meta.state` was not persisted
  - Grafton exposed this
- another important reality discovered during live debugging:
  - if races were saved before `meta.state` existed at race level, sectional routing can fail later even though fixtures are correct
  - symptom:
    - logs show `state=None`
    - sectional endpoint selection fails or returns zero sectionals
    - result rows merge with all sectional fields `None`
  - practical fix:
    - regenerate/save races again for the affected date window
    - then rerun results
  - code-side mitigation:
    - result rerun now backfills missing race `meta` fields like `state`, `meetingId`, and `race_meet_id` from matching fixture rows when possible

Known Loveracing payload expectations from tests:
- race payloads include `raceId`, `country="NZ"`, `currency="NZD"`
- result payloads can include sectional fields such as `first2fSplit`, `first2fPos`, `last4fSplit`, `last3fSplit`, `last2fSplit`, `last1fSplit`, `last1f`, `last2f`, `last3f`, `last4f`
- country-of-origin normalization is expected, for example `"Silent Spy (AUS)" -> countryOfOrigin="AUS"`

## Tests

Use:
- `.venv/bin/python -m pytest`
- `.venv/bin/python -m pytest tests/test_loveracing_provider_flow.py`
- `.venv/bin/python -m pytest tests/test_racingcom.py`

Test setup:
- `tests/conftest.py` inserts `src/` onto `sys.path`

Current test coverage appears focused on:
- loveracing parsing and provider flow
- racing.com fixture/calendar transforms
- racing.com race/result transforms
- XML ingestion and fixture transform behavior

Useful targeted test commands:
- `pytest tests/test_course_utils.py`
- `pytest tests/test_fixture_transform.py`
- `pytest tests/test_xml_ingestion.py`
- `pytest tests/test_loveracing_provider.py`
- `pytest tests/test_loveracing_provider_flow.py`
- `pytest tests/test_racingcom.py`
- `pytest tests/test_racingcom.py tests/test_fixture_transform.py`

If the system `python` / `pytest` is missing, use the repo venv:
- `.venv/bin/python -m pytest -q`

What each test file is for:
- `tests/test_fixture_transform.py`: `save_fixtures()` payload construction and fixture ID behavior
- `tests/test_xml_ingestion.py`: Loveracing XML parsing and sectional mapping
- `tests/test_loveracing_provider.py`: provider routing for past vs future fixtures, same-day overview handling, and XML fallback/caching
- `tests/test_loveracing_provider_flow.py`: month-calendar merge behavior
- `tests/test_racingcom.py`: provider registration, month iteration, runtime config parsing, racing.com calendar query behavior, AUS fixture transforms
- `tests/test_racingcom.py` also covers:
  - state-local fixture dates
  - state-local race start times
  - racecard transform behavior
  - sectional endpoint routing
  - race/result transform behavior
- `tests/test_course_utils.py`: shared course normalization behavior including sponsor-prefix stripping

## Known project realities

- `src/loveracing/loveracing.py` is currently modified in the worktree; do not overwrite user changes blindly.
- `racingcom` now supports fixture ingestion plus race/result parsing, but there are still important behavior assumptions around GraphQL payload shape and state/timezone handling.
- README is minimal and does not fully describe the race/result mode.
- `AGENTS.md` is a shortcut, not a source of truth when behavior is subtle.
- The downstream GraphQL API matters for result reruns:
  - live debugging showed `getRaces()` rows can arrive with `meta=None` even when DB rows contain `meta`
  - this was confirmed by inspecting `/Users/cycchow/Documents/workspace/uk-racing/uk-racing-data-api`
  - `RaceController.getFixtures()` manually reparses `meta` from the row CLOB
  - `RaceController.getRaces()` used only the entity converter and did not mirror that behavior
  - scraper-side workaround: result rerun backfills missing race `meta` from matching fixture rows by `(raceDate, course, country)`
- another subtle AUS reality:
  - even when race `meta` is present, it may still be stale or partial
  - older stored race rows were observed with:
    - `race_meet_id` present
    - `race` present
    - but no `state`
  - that was enough to break sectional routing for VIC/NSW/QLD until races were regenerated
- backend bulk results reality:
  - sibling backend project: `/Users/cycchow/Documents/workspace/uk-racing/uk-racing-data-api`
  - schema already exposed `addResults(input: [JSON!]!): JSON`
  - `RaceController.addResults(...)` was fixed to match that schema and bulk-merge `Results`
  - `merge(type, input)` was intentionally left untouched for compatibility with other projects

## File risk guide

Safest files to edit:
- tests under `tests/`
- `README.md`
- `AGENTS.md`
- provider files when behavior change is isolated

Higher-risk files:
- `src/main.py`: central orchestration, GraphQL writes, CLI behavior
- `src/utils/graphql_client.py`: fragile because it manually builds GraphQL input strings
- `src/loveracing/loveracing.py`: dense parsing logic and currently has uncommitted user changes

When changing these higher-risk files:
- read the matching tests first
- avoid mixing refactors with behavior changes
- run the smallest relevant `pytest` target before broader test runs

## Safe working conventions for future edits

- Read `src/main.py` plus the relevant provider before changing behavior.
- Preserve provider separation; do not put site-specific parsing into `main.py`.
- If changing GraphQL payload fields, update or add focused tests.
- Prefer `pytest` over ad hoc scripts for validation.
- Be careful with date semantics:
  - fixture ingestion is month-based
  - race/result processing is date-range based
  - Racing.com fixture `raceDate` must be local-state date, not UTC date
  - Racing.com `startTime` must be local-state wall time, not a single Australia/Melbourne fallback

## Fast orientation checklist

When returning to this repo, usually read only:
1. `AGENTS.md`
2. `src/main.py`
3. the provider file being changed
4. the matching tests

For common tasks:
- add/adjust fixture transform: read `src/main.py` and `tests/test_fixture_transform.py`
- change NZ parsing: read `src/scrapers/loveracing_provider.py`, `src/loveracing/loveracing.py`, and XML/provider tests
- extend AU support: read `src/scrapers/racingcom_provider.py`, `src/racingcom/racingcom.py`, and `tests/test_racingcom.py`
