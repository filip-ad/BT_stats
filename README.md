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

## Setup (Virtual Environment)
- Create and activate the project venv, then install dependencies:

	```bash
	python3 -m venv .venv
	source .venv/bin/activate
	pip install -r requirements.txt
	```

- Always run project commands inside the activated venv. Example:

	```bash
	python src/main.py
	```

- If `python` isn’t available, use `python3`. Ensure `.venv/bin` is on your `PATH` by activating the environment (`source .venv/bin/activate`).

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
`src/resolvers/` contains transformation logic that converts raw scraped data into normalized domain entities:
- `resolve_tournaments.py` - Tournament raw → tournament
- `resolve_tournament_classes.py` - Tournament class raw → tournament_class (includes parent-child detection for B-playoffs)
- `resolve_tournament_class_entries.py` - Entry raw → tournament_class_entry
- `resolve_tournament_class_matches.py` - Match raw → match/game/match_side/match_player (includes sibling resolution)
- `resolve_player_licenses.py`, `resolve_player_rankings.py`, `resolve_player_transitions.py` - Player data normalization

`src/resolve_data.py` exposes switches for targeted resolve passes, providing an alternate entry point when only transformation logic must run.【F:src/resolve_data.py†L1-L33】 

## Scraper Layer
`src/scrapers/` contains modules that fetch data from external sources and store it in raw tables:

### Tournament Scrapers (OnData)
- `scrape_tournaments_ondata_listed.py` - Scrapes tournament list from `resultat.ondata.se`
- `scrape_tournaments_ondata_unlisted.py` - Discovers unlisted tournaments by ID probing
- `scrape_tournament_classes_ondata.py` - Scrapes class definitions for each tournament
- `scrape_tournament_class_entries_ondata.py` - Scrapes participant entries (PDF parsing)
- `scrape_tournament_class_group_matches_ondata.py` - Scrapes group stage matches from PDFs
- `scrape_tournament_class_knockout_matches_ondata.py` - Scrapes KO bracket matches from PDFs

### Player Data Scrapers
- `scrape_player_licenses.py` - Scrapes player license data from SBTF
- `scrape_player_rankings.py` - Scrapes player ranking lists
- `scrape_player_transitions.py` - Scrapes player club transitions

### League Scrapers
- `scrape_leagues_profixio.py` - Scrapes league fixtures from Profixio

All scrapers follow a pattern of: fetch → parse → upsert to `*_raw` tables with content hashing and `last_seen_at` timestamps.

## Utilities and Shared Infrastructure 
- `src/utils.py` configures project-wide logging, Selenium driver bootstrap, PDF caching/downloading helpers, and hashing utilities. The `OperationLogger` class aggregates successes/failures/warnings with optional DB persistence and contextual enrichment for diagnostics.【F:src/utils.py†L1-L386】 
- PDF caching utilities store competition documents under a structured cache directory, validating signatures before reuse and falling back to live downloads when necessary.【F:src/utils.py†L279-L335】 

## Domain Models 
`src/models/` contains dataclass-based representations for core entities (players, tournaments, matches, etc.). For example, `models/player.py` defines sanitisation, caching helpers, and persistence helpers for verified/unverified players, reusing shared normalisation utilities.【F:src/models/player.py†L1-L160】 

### Verified vs Unverified Players
Players exist in two states:
- **Verified** (`is_verified = 1`): Players with license data from SBTF. These have authoritative `player_id_ext`, club affiliations, and can be reliably linked across tournaments.
- **Unverified** (`is_verified = 0`): Players created during entry/match resolution when no verified match is found. These are essentially name containers—a `player_id` plus `fullname_raw` for display purposes.

**Current behavior:** When resolving entries, the resolver first attempts to match against verified players (by name + club). If no match is found, a new unverified player record is created. No deduplication is performed on unverified players—slight name variations (e.g., "GAMBORG NILSEN Carine" vs "GAMBORG-NILSEN Carine") create separate records.

**Design decision:** Unverified players are display-only on the frontend (not searchable or clickable). This means duplicates are acceptable—each match appearance simply shows the scraped name as text. Future work could add deduplication or promotion workflows if unverified players need to become first-class entities.

### Parent-Child Class Relationships
Tournament classes can have parent-child relationships via `tournament_class_id_parent` (added 2025-11-28):
- B-playoff classes (e.g., "P12~B") contain players who didn't advance from the main class ("P12") group stage.
- Detection: Classes with "~B" suffix in shortname are automatically linked to their parent during resolution.
- Sibling resolution: When resolving match players, if a player isn't found in B-class entries, the resolver searches the parent class and creates a "synthetic" entry in the B-class.
- Key files: `resolve_tournament_classes.py` (parent detection), `resolve_tournament_class_matches.py` (sibling resolution).

## Schema and Database Inspection
To explore the current database schema and structure:

```bash
# List all tables
sqlite3 data/table_tennis.db ".tables"

# Show schema for a specific table
sqlite3 data/table_tennis.db ".schema tournament_class"

# Show all table schemas
sqlite3 data/table_tennis.db ".schema"

# Quick row counts
sqlite3 data/table_tennis.db "SELECT name, (SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=m.name) FROM sqlite_master m WHERE type='table';"
```

Alternatively, use Python:
```python
import sqlite3
conn = sqlite3.connect('data/table_tennis.db')
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
for row in cursor.fetchall():
    print(row[0])
```

The project also includes `BTstats_DB_Dictionary.xlsx` in `src/` which documents table structures and field semantics.