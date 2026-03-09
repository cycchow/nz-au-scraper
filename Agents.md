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
- `racingcom`: AU, fixture ingestion exists; race/result parsing is currently a stub.

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
- `.env` is loaded before provider/config imports

## Important modules

- `src/main.py`: orchestration, CLI, GraphQL save operations
- `src/scrapers/base.py`: provider protocol and `FixtureProcessOutput`
- `src/scrapers/loveracing_provider.py`: NZ provider implementation
- `src/scrapers/racingcom_provider.py`: AU provider implementation; `parse_fixture()` returns empty output today
- `src/loveracing/loveracing.py`: main Loveracing scraping/parsing logic
- `src/racingcom/racingcom.py`: Racing.com calendar/runtime config logic
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
- default/AU base: `8000000000`
- KSA base: `9000000000`

Source codes:
- `loveracing` provider saves `src="loveracing"`
- `racingcom` provider saves `src="racingcom"`
- fallback non-provider country logic uses `src="era"`

Expected fixture payload shape sent to GraphQL:
- required in practice: `raceDate`, `course`, `fixtureYear`, `meetingId`, `fixtureId`, `src`, `country`, `meta`
- fields always included by `save_fixtures()`: `raceType`, `surface`, `going`, `weather`, `reading`, `raceClass`, `noOfRaces`, `stalls`
- meeting identifier resolution order:
  1. top-level `meetingId`
  2. `meta.DayID`
  3. `meta.race_meet_id`

### Races/results mode

`main.py` subscribes to GraphQL `getFixtures()` -> provider `accepts_fixture()` -> provider `parse_fixture()` -> `save_races()` / `save_results()`

Loveracing parsing split:
- future/today fixtures: meeting overview HTML
- past fixtures: XML results + sectional fetch

Expected race/result behavior:
- provider returns `FixtureProcessOutput(races=[...], results=[...])`
- `save_races()` and `save_results()` JSON-normalize payloads before merge
- failures are logged per record and do not stop the whole run

Known Loveracing payload expectations from tests:
- race payloads include `raceId`, `country="NZ"`, `currency="NZD"`
- result payloads can include sectional fields such as `first2fSplit`, `first2fPos`, `last4fSplit`, `last3fSplit`, `last2fSplit`, `last1fSplit`, `last1f`, `last2f`, `last3f`, `last4f`
- country-of-origin normalization is expected, for example `"Silent Spy (AUS)" -> countryOfOrigin="AUS"`

## Tests

Use:
- `pytest`
- `pytest tests/test_loveracing_provider_flow.py`
- `pytest tests/test_racingcom.py`

Test setup:
- `tests/conftest.py` inserts `src/` onto `sys.path`

Current test coverage appears focused on:
- loveracing parsing and provider flow
- racing.com fixture/calendar transforms
- XML ingestion and fixture transform behavior

Useful targeted test commands:
- `pytest tests/test_fixture_transform.py`
- `pytest tests/test_xml_ingestion.py`
- `pytest tests/test_loveracing_provider.py`
- `pytest tests/test_loveracing_provider_flow.py`
- `pytest tests/test_racingcom.py`
- `pytest tests/test_racingcom.py tests/test_fixture_transform.py`

What each test file is for:
- `tests/test_fixture_transform.py`: `save_fixtures()` payload construction and fixture ID behavior
- `tests/test_xml_ingestion.py`: Loveracing XML parsing and sectional mapping
- `tests/test_loveracing_provider.py`: provider routing for past vs future fixtures
- `tests/test_loveracing_provider_flow.py`: month-calendar merge behavior
- `tests/test_racingcom.py`: provider registration, month iteration, runtime config parsing, AU fixture transforms

## Known project realities

- `src/loveracing/loveracing.py` is currently modified in the worktree; do not overwrite user changes blindly.
- `racingcom` supports fixture ingestion only. If asked to add AU race/result parsing, start in `src/scrapers/racingcom_provider.py` and `src/racingcom/racingcom.py`.
- README is minimal and does not fully describe the race/result mode.
- `Agents.md` is a shortcut, not a source of truth when behavior is subtle.

## File risk guide

Safest files to edit:
- tests under `tests/`
- `README.md`
- `Agents.md`
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

## Fast orientation checklist

When returning to this repo, usually read only:
1. `Agents.md`
2. `src/main.py`
3. the provider file being changed
4. the matching tests

For common tasks:
- add/adjust fixture transform: read `src/main.py` and `tests/test_fixture_transform.py`
- change NZ parsing: read `src/scrapers/loveracing_provider.py`, `src/loveracing/loveracing.py`, and XML/provider tests
- extend AU support: read `src/scrapers/racingcom_provider.py`, `src/racingcom/racingcom.py`, and `tests/test_racingcom.py`
