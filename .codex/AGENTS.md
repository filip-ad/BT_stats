# Repository Guidelines

## Project Overview
- Pingiskollen / BT_stats is a Python data pipeline for Swedish table-tennis data.
- It scrapes public sources (mainly OnData/Stupa), normalises clubs/players/tournaments, and loads them into a local SQLite warehouse for analysis.
- For a more narrative description and domain background, see the project metadata folder and its `overview.txt`, together with the high-level `README.md` in this repo.

## Project Structure & Modules
- Core Python code lives in `src/`. Entry point for the full pipeline is `src/main.py`.
- Domain models are in `src/models/`, scrapers in `src/scrapers/`, and data resolution/transform steps in `src/resolvers/`.
- Helper scripts live in `src/utils.py` and `src/utils_scripts/`.
- Local data, logs, and databases should stay under `data/` or `src/data/` and remain untracked unless explicitly needed.

## Setup, Run, and Development
- Create a virtualenv and install dependencies:  
  `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
- Run the full pipeline (main DB + tournament update):  
  `python src/main.py`
- For focused work, call individual update or scraper scripts directly (e.g. `python src/upd_tournament_data.py`, `python src/scrapers/scrape_player_rankings.py`).

## Coding Style & Naming
- Use Python 3 with 4-space indentation and type hints where practical.
- Prefer snake_case for functions/modules, PascalCase for classes, and descriptive names for long-running pipeline steps.
- Keep functions small and focused; put shared logic in `src/utils.py` or model methods.
- Avoid reformatting entire files; limit changes to the relevant sections.

## Testing & Validation
- There is currently no dedicated automated test suite; validate changes by running `python src/main.py` and any affected scripts.
- When adding tests, prefer `pytest` with a `tests/` directory and mirror the `src/` structure.
- For schema or query changes, verify against a local SQLite copy under `data/` and confirm that main flows complete without errors.

## Commits & Pull Requests
- Use clear, imperative commit messages (e.g. `Add player cache map`, `Fix tournament class resolution`).
- Keep commits focused: one logical change per commit when possible.
- PRs should include: a short summary, affected scripts/modules, any DB/schema impacts, and manual verification steps (commands you ran).

## Security, Data, and Agent Notes
- Never commit credentials, browser profiles, or `.env` files; rely on `python-dotenv` and local `.env`.
- Avoid committing new large spreadsheets, PDFs, or database files; prefer documenting how to regenerate them.
- Agents editing this repo should respect these guidelines, preserve existing style, and avoid broad refactors unless explicitly requested.

## Scraper & Resolver Guardrails
- When editing `src/scrapers` or `src/resolvers`, take the safest approach: avoid changing working PDF handling logic or formats unless necessary and verified.
- Any tweak that touches PDF parsing/format support should be validated against the currently working sources so nothing regresses.

## Virtual Environment Setup
- Install dependencies in the repo virtualenv: `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`.
- If commands fail because `python`/`pip` are missing, use `python3` and ensure the `.venv/bin` directory is in your `PATH` before running scripts (`source .venv/bin/activate`).
- Some tooling (e.g., `pdfplumber`, `mutool`) is needed for scraper/resolver development, so confirm they are present in `.venv/bin` before running PDF inspection scripts.

# Project pingiskollen guidelines
- When making updates to any script, especially scrapers, be VERY careful to not mess with something that is already working. We are creating scrapers that should be able to parse a lot of different PDF formats for example, so fixing one by breaking another is NOT OK! 
- Please test your code changes properly first, use the .venv virtual environemnt.
- Add proper comments and documentation in the code as you review or update it.