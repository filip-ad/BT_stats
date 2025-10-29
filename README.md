# BT_stats Project Overview 

## Mission and Scope 
BT_stats is a data engineering pipeline for Swedish table tennis information that curates club, player, and tournament data from public sources (primarily OnData/Stupa) into a structured SQLite warehouse. The project focuses on scraping upstream systems, normalising the results through resolver logic, and maintaining a denormalised analytics-ready database. 

## Execution Entry Points 
- `src/main.py` is the orchestration script. Each run creates a UUID, establishes a database connection, and wires an `OperationLogger` before rebuilding core tables and indexes.【F:src/main.py†L32-L145】 
- The "new workflow" section in `main.py` is staged for modular updates. Club and player updates are currently commented out, while the tournament pipeline runs with tournament-class group match scraping enabled and other collectors disabled. Each run exports log metadata to Excel for auditing.【F:src/main.py†L149-L176】 

## Database Management 
- `src/db.py` centralises database access. `get_conn` applies WAL journaling, normal synchronous writes, in-memory temp storage, and foreign-key enforcement for the SQLite database defined in `config.py`. Adapter/convertor registration preserves Python `date`/`datetime` fidelity.【F:src/db.py†L12-L61】 
- Schema provisioning helpers (`create_raw_tables`, `create_tables`, `create_indexes`, etc.) are invoked from the entry point. Raw-layer DDL includes tournament, player license, and ranking staging tables with uniqueness constraints and metadata such as content hashes and `last_seen_at` timestamps.【F:src/db.py†L84-L199】 
- Maintenance helpers provide table drop logic, optional custom SQL execution, and database compaction through WAL checkpointing and vacuuming.【F:src/main.py†L55-L140】【F:src/db.py†L36-L45】 

## Configuration and Environment 
- `src/config.py` houses runtime configuration: log destination/level, database path, PDF cache root, and fine-grained scraping limits (cut-off dates, ordering, entity whitelists). These settings gate the batch size and temporal scope for each scraper.【F:src/config.py†L3-L32】 
- Dependencies for scraping, parsing, automation, and analytics live in `requirements.txt`, covering Selenium, Requests/BeautifulSoup, PDF parsing, and pandas/openpyxl for Excel exports.【F:requirements.txt†L1-L34】 

## Data Ingestion Pipelines 

### Clubs 
`src/upd_clubs.py` seeds canonical club definitions, name aliases, and external ID mappings. Inserts are idempotent through `INSERT OR IGNORE`, and per-category metrics feed into a consolidated database summary report.【F:src/upd_clubs.py†L1-L123】 

### Players 
`src/upd_player_data.py` orchestrates optional scrapers for licenses, rankings, and transitions, wrapping each call in guarded try/except blocks. After scraping, it runs a resolver stack (`upd_players_verified`, ranking groups, licenses, transitions) before committing the transaction.【F:src/upd_player_data.py†L1-L59】 
`src/upd_players_verified.py` merges duplicate external IDs, inserts non-duplicate players, repoints dependents, and cleans orphaned unverified players while emitting detailed metrics through `OperationLogger`.【F:src/upd_players_verified.py†L1-L146】 

### Tournaments 
`src/upd_tournament_data.py` toggles a family of OnData scrapers (tournaments, classes, entries, group/knockout matches) and is prepared for downstream resolver invocations. Current configuration only enables group-stage match scraping while other scraping/resolution calls are commented for later activation.【F:src/upd_tournament_data.py†L1-L93】 
The listed-tournament scraper requests `https://resultat.ondata.se`, applies retry/backoff logic, filters by configurable cutoff date, and constructs raw tournament records with URL enrichment and logging.【F:src/scrapers/scrape_tournaments_ondata_listed.py†L1-L160】 

## Resolver Layer 
`src/resolve_data.py` exposes switches for targeted resolve passes (tournaments, classes, player artifacts, participants), providing an alternate entry point when only transformation logic must run.【F:src/resolve_data.py†L1-L33】 

## Utilities and Shared Infrastructure 
- `src/utils.py` configures project-wide logging, Selenium driver bootstrap, PDF caching/downloading helpers, and hashing utilities. The `OperationLogger` class aggregates successes/failures/warnings with optional DB persistence and contextual enrichment for diagnostics.【F:src/utils.py†L1-L386】 
- PDF caching utilities store competition documents under a structured cache directory, validating signatures before reuse and falling back to live downloads when necessary.【F:src/utils.py†L279-L335】 

## Domain Models 
`src/models/` contains dataclass-based representations for core entities (players, tournaments, matches, etc.). For example, `models/player.py` defines sanitisation, caching helpers, and persistence helpers for verified/unverified players, reusing shared normalisation utilities.【F:src/models/player.py†L1-L160】 

## Supporting Assets 
The repository includes Excel exports such as `BTstats_DB_Dictionary.xlsx` and log workbooks that are written by the utilities for documentation and run auditing. (See `/src` root listings.)【F:src/main.py†L9-L16】 

## Current Status and Open Work 
- Primary focus right now is tournament-class group match scraping; other update steps are staged but disabled pending verification.【F:src/main.py†L153-L172】 
- The `_to_do.txt` backlog prioritises participant pipeline build-out (with PDF caching), final position resolution, group-stage game ingestion, and performance improvements for player-license scraping. It also captures cleanup tasks (logger usage harmonisation, transition content hashes) and longer-term scheduling/logging automation ideas.【F:_to_do.txt†L1-L23】 
Use this document to provide high-level context for future prompts or collaborators. It summarises the architecture, operational toggles, and outstanding roadmap items without requiring a deep dive into individual modules.