# Real Estate Data Platform

End-to-end data platform for Canadian rental listings. Scrapes property data from online portals, stores it in an immutable Bronze layer, processes it through cleaning, normalisation, and deduplication into an analytical Silver layer, and models it into a dimensional Gold layer using dbt with SCD2 change tracking.

Built around immutability, idempotency, data quality, and clear separation of concerns across data layers.

---

## Architecture

```
Scraper (requests + BeautifulSoup)
  ↓
MinIO — Bronze (raw Parquet, append-only)
  ↓
Polars (transform + clean)
  ↓
PostgreSQL — Silver (deduplicated, typed, upserted)
  ↓
dbt (snapshot SCD2 + incremental)
  ↓
PostgreSQL — Gold (Kimball star schema)
```

Orchestrated with **Prefect** flows and tasks. dbt runs in-process via **prefect-dbt**. Infrastructure runs via **Docker Compose**.

---

## Technology Stack

| Layer | Technology |
|---|---|
| Scraping | `requests` + `BeautifulSoup4` |
| Bronze storage | MinIO (S3-compatible, Parquet) |
| Processing | Polars |
| Silver storage | PostgreSQL (`psycopg`) |
| Gold modelling | dbt-postgres (snapshots + incremental) |
| Orchestration | Prefect + prefect-dbt |
| Config | Pydantic Settings (`.env`) |
| Dev tooling | Poetry, Ruff, pre-commit, pytest |
| Infrastructure | Docker Compose |

---

## Project Structure

```
src/real_estate_data_platform/
├── config/
│   └── settings.py              # Pydantic Settings (MinIO, Postgres, Scraper, dbt)
├── connectors/
│   ├── minio.py                 # S3-compatible storage client
│   └── postgres.py              # Generic, schema-agnostic PostgreSQL connector
├── scrapers/
│   ├── base_scraper.py          # ABC — Template Method pattern with date filtering
│   ├── kijiji_scraper.py        # Kijiji.ca rental scraper
│   └── scraper_type.py          # ScraperType enum + factory
├── models/
│   ├── enums.py                 # City, DataSource, FlowStatus, DateMode
│   ├── listings.py              # RentalsListing Pydantic model (~50 fields)
│   ├── responses.py             # Result models for each flow
│   └── silver_schema.py         # Column registry, SQL generation, validation rules
├── tasks/
│   ├── scraping.py              # fetch_and_parse_page, aggregate_results
│   ├── load_bronze.py           # listings_to_dataframe, save_listings_to_minio
│   ├── read_bronze.py           # read_bronze_listings (MinIO → Polars)
│   ├── transform_silver.py      # transform_to_silver (clean, normalise, dedup)
│   ├── load_silver.py           # write_silver_listings (Polars → PostgreSQL upsert)
│   └── run_dbt.py               # run_dbt (PrefectDbtRunner, in-process invocation)
├── flows/
│   ├── scrape_to_bronze_flow.py # Scrape → Parquet + metadata → MinIO
│   ├── bronze_to_silver_flow.py # MinIO → transform → PostgreSQL (per-partition subflows)
│   └── silver_to_gold_flow.py   # dbt snapshot (SCD2) + model refresh → Gold
├── dbt/
│   ├── dbt_project.yml          # dbt project configuration
│   ├── profiles.yml             # PostgreSQL connection (env_var-based)
│   ├── snapshots/
│   │   └── _snap_fact_rentals_listings.sql   # SCD2 snapshot (check strategy on row_hash)
│   └── models/
│       ├── sources.yml          # Silver sources (rentals_listings, neighbourhoods)
│       └── gold/
│           ├── fact_rentals_listings.sql      # Fact view over snapshot (valid_from/to, is_current)
│           └── dim_neighbourhood.sql          # Neighbourhood dimension (incremental, INSERT-only)
├── utils/
│   ├── dates.py                 # parse_iso_datetime, format_date, date_range
│   ├── hashing.py               # row_hash computation (md5)
│   └── parsers.py               # parse_float, parse_int
└── deployments/                 # (empty — Prefect deployments TBD)
```

---

## Data Layers

### Bronze (Raw / Immutable)

- **Storage:** MinIO
- **Format:** Parquet (append-only, never updated or deleted)
- **Partitioning:** `listings/source={source}/city={city}/dt={YYYY-MM-DD}/`
- **Metadata:** JSON sidecar with scrape parameters and record count

```
raw/
└── listings/
    └── source=kijiji/
        └── city=toronto/
            └── dt=2026-02-27/
                ├── listings.parquet
                └── metadata.json
```

### Silver (Clean & Deduplicated)

- **Storage:** PostgreSQL (`silver.rentals_listings`, `silver.neighbourhoods`)
- **Processing:** Polars
- **Schema:** 48 columns defined in a centralised registry (`silver_schema.py`)
- **Upsert:** `INSERT ON CONFLICT DO UPDATE` with `scraped_at` guard (idempotent — reprocessing older data doesn't regress values)
- **Change detection:** `row_hash` computed via MD5 over 10 key fields (`rent`, `move_in_date`, `bedrooms`, `bathrooms`, `furnished`, `heat`, `water`, `hydro`, `parking_included`, `laundry_in_unit`)
- **Transforms:** strip/lowercase strings, boolean conversion, numeric range validation, PK enforcement, deduplication

### Gold (Dimensional / Star Schema)

- **Storage:** PostgreSQL (`gold` schema)
- **Modelling:** dbt-postgres, invoked in-process via `PrefectDbtRunner` (prefect-dbt ≥ 0.7)
- **SCD2:** `dbt snapshot` with `check` strategy on `row_hash` — when any of the 10 hashed fields changes, a new version is created with `valid_from`/`valid_to` timestamps

| Table | Type | Description |
|---|---|---|
| `_snap_fact_rentals_listings` | Snapshot (internal) | Raw SCD2 rows with `dbt_valid_from`, `dbt_valid_to` |
| `fact_rentals_listings` | View | Public interface — renames temporal columns, adds `is_current` |
| `dim_neighbourhood` | Incremental | Neighbourhood dimension with surrogate key (`neighbourhood_sk`), INSERT-only |

**Snapshot unique key:** `listing_id || '~' || website` (composite business key — same listing ID can exist across different websites).

**SCD2 example** — a listing whose rent changed:

| listing_id | website | rent | valid_from | valid_to | is_current |
|---|---|---|---|---|---|
| 123 | kijiji | 1800 | 2026-01-15 | 2026-02-20 | false |
| 123 | kijiji | 2000 | 2026-02-20 | NULL | true |

---

## Scrapers

| Source | Scraper | Cities |
|---|---|---|
| Kijiji.ca | `KijijiScraper` | Toronto, Vancouver, London |

The `BaseScraper` ABC provides a Template Method pattern with built-in date filtering modes:
- `LAST_X_DAYS` — scrape listings from the last N days
- `SPECIFIC_DATE` — scrape listings from a single date

New scrapers only need to implement `_parse_page_impl` and `_parse_listing_detail`.

---

## Flows

### `scrape-to-bronze`

Instantiates a scraper → fetches N pages sequentially → aggregates listings → saves Parquet + JSON metadata to MinIO.

```python
from real_estate_data_platform.flows.scrape_to_bronze_flow import scrape_to_bronze

result = scrape_to_bronze(
    scraper_type=ScraperType.KIJIJI,
    city=City.TORONTO,
    max_pages=10,
    mode=DateMode.LAST_X_DAYS,
    days=7,
)
```

### `bronze-to-silver`

Iterates over `(source, city, date)` partitions. Each partition is a separate Prefect subflow (isolated, independently retriable):

```python
from real_estate_data_platform.flows.bronze_to_silver_flow import bronze_to_silver

result = bronze_to_silver(
    source=DataSource.KIJIJI,
    city=City.TORONTO,
    mode=DateMode.SPECIFIC_DATE,
    specific_date=date(2026, 2, 27),
)
```

### `silver-to-gold`

Runs dbt in-process via `PrefectDbtRunner`. Executes two steps sequentially:

1. `dbt snapshot` — applies SCD2 on `silver.rentals_listings` → `gold._snap_fact_rentals_listings`
2. `dbt run --select fact_rentals_listings dim_neighbourhood` — refreshes the fact view and the neighbourhood dimension

```python
from real_estate_data_platform.flows.silver_to_gold_flow import silver_to_gold

result = silver_to_gold()
```

---

## Infrastructure

```bash
docker compose up -d    # PostgreSQL + MinIO + bucket init
```

| Service | Port | Purpose |
|---|---|---|
| PostgreSQL | 5432 | Silver + Gold layers |
| MinIO API | 9000 | Bronze storage |
| MinIO Console | 9001 | Web UI |

---

## Configuration

Settings are loaded from environment variables via Pydantic Settings. Nested delimiter is `__`. For local development, create a `.env` file:

```env
ENVIRONMENT=dev

MINIO__ENDPOINT=localhost:9000
MINIO__ACCESS_KEY=minioadmin
MINIO__SECRET_KEY=minioadmin
MINIO__BUCKET_NAME=raw

POSTGRES__HOST=localhost
POSTGRES__PORT=5432
POSTGRES__USER=etl_user
POSTGRES__PASSWORD=etl_pass
POSTGRES__DB=etl_db
POSTGRES__SILVER_SCHEMA=silver
POSTGRES__SILVER_LISTINGS_TABLE=rentals_listings
POSTGRES__SILVER_NEIGHBOURHOODS_TABLE=neighbourhoods

SCRAPER__USER_AGENT=Mozilla/5.0 ...
SCRAPER__DOWNLOAD_DELAY=5.0
```

dbt reads the same Postgres env vars via `env_var()` in `profiles.yml`. No additional configuration needed.

In DEV mode, the Silver schema and table are auto-created. In PROD, they must exist (migrations).

---

## Idempotency

The platform is designed to be fully idempotent:
- Bronze files use deterministic partition paths — re-running overwrites the same object
- Silver upserts deduplicate on `(listing_id, website)` with a `scraped_at` guard — only newer data updates existing rows
- Reprocessing the same partition produces the same result without duplicates

---

## Testing

```bash
poetry run pytest tests/ -v
```

116 tests covering:

| Area | Tests | Scope |
|---|---|---|
| Silver schema | 42 | Column registry, SQL generation, hash columns, upsert strategies |
| Kijiji scraper | 36 | JSON-LD extraction, attribute mapping, neighbourhood parsing, listing detail, HTTP handling |
| Base scraper | 17 | Date filtering logic, template method, init |
| Postgres connector | 13 | Upsert SQL, schema creation, context manager |
| Silver → Gold flow | 5 | dbt snapshot + run success/error paths, result model |
| dbt task | 3 | PrefectDbtRunner invocation, error propagation, settings |

Test fixtures use saved HTML pages from Kijiji for deterministic, offline testing.

---

## Development

```bash
# Install dependencies
poetry install

# Start infrastructure
docker compose up -d

# Run linters
poetry run pre-commit run --all-files

# Run tests
poetry run pytest tests/ -v

# Execute flows
poetry run python src/real_estate_data_platform/flows/scrape_to_bronze_flow.py
poetry run python src/real_estate_data_platform/flows/bronze_to_silver_flow.py
poetry run python src/real_estate_data_platform/flows/silver_to_gold_flow.py
```
