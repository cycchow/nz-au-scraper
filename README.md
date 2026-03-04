# nz-au-scraper

NZ/AU scraper bootstrap focused on NZ fixture ingestion from loveracing.nz.

## Install

```bash
python -m pip install -r requirements.txt
```

## Usage

```bash
python src/main.py
python src/main.py --from 2026-02 --to 2023-01 --country NZ
```

Defaults:
- `--from`: current month (`YYYY-MM`)
- `--to`: `2023-01`
- `--country`: `NZ`
