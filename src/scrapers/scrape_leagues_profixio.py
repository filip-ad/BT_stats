# src/scrapers/scrape_leagues_profixio.py
#
# Scrapes seasons, leagues, fixtures, and match reports from Profixio.
# Uses plain requests/BeautifulSoup (pages are static).

"""
Context (from manual site walk):
  - Season switcher links look like serieoppsett_sesong.php?id=768; the ID is stable per season.
  - League pages are serieoppsett.php?t=SBTF_SERIE_AVD<digits>&k=LS<digits>&p=1 where k is the league-season key.
  - Fixture table rows contain a “Detaljer” link with kampid, which is the stable match identifier.
  - Match reports at serieoppsett_viskamper_rapport.php?kampid=<id> list set scores and player names (no player IDs).
  - Pages are server-rendered; no auth or JS required, so requests + BeautifulSoup is sufficient.
"""

import datetime
import logging
import re
import os
import time
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from config import (
    SCRAPE_LEAGUES_SEASON_IDS,
    SCRAPE_LEAGUES_ONLY_CURRENT,
    SCRAPE_LEAGUES_MAX_SEASONS,
    SCRAPE_LEAGUES_MAX_LEAGUES_PER_SEASON,
    SCRAPE_LEAGUES_MAX_FIXTURES,
    SCRAPE_LEAGUES_SKIP_SEEN_MATCHES,
    SCRAPE_LEAGUES_SKIP_SEEN_MATCHES_DAYS,
    SCRAPE_LEAGUES_REQUEST_DELAY,
    SCRAPE_LEAGUES_SEASONS_ORDER,
    SCRAPE_LEAGUES_CACHE_HTML,
    SCRAPE_LEAGUES_CACHE_HTML_DIR,
    SCRAPE_LEAGUES_CACHE_HTML_DAYS,
)
from models.league_raw import LeagueRaw
from models.league_fixture_raw import LeagueFixtureRaw
from models.league_fixture_match_raw import LeagueFixtureMatchRaw
from utils import OperationLogger


BASE_ROOT               = "https://www.profixio.com/fx/"
SERIE_URL               = urljoin(BASE_ROOT, "serieoppsett.php?org=SBTF.SE.BT")
SEASON_SWITCH_URL       = urljoin(BASE_ROOT, "serieoppsett_sesong.php")
MATCH_REPORT_URL        = urljoin(BASE_ROOT, "serieoppsett_viskamper_rapport.php")
REQUEST_TIMEOUT         = 20

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8",
}


def scrape_all_league_data_profixio(cursor, run_id=None) -> None:
    """
    Main entry point: orchestrates season discovery, league listing, fixtures, and match reports.
    Applies testing-friendly filters from config (current season + 1 league by default).
    """
    logger = OperationLogger(
        verbosity       = 2,
        print_output    = False,
        log_to_db       = True,
        cursor          = cursor,
        object_type     = "league_raw",
        run_type        = "scrape_profixio_leagues",
        run_id          = run_id,
    )

    session             = _init_session()
    start_time          = time.time()
    leagues_seen        = fixtures_seen = matches_seen = 0

    seasons = _discover_seasons(session, logger)
    if not seasons:
        logger.failed({}, "Could not determine any seasons on Profixio")
        logger.summarize()
        return

    seasons = _filter_seasons(seasons, logger)
    if not seasons:
        logger.failed({}, "No seasons left after filters")
        logger.summarize()
        return

    logger.set_run_remark(
        f"Season_ids={SCRAPE_LEAGUES_SEASON_IDS or 'auto/current'}, "
        f"only_current={SCRAPE_LEAGUES_ONLY_CURRENT}, "
        f"max_seasons={SCRAPE_LEAGUES_MAX_SEASONS or 'all'}, "
        f"max_leagues_per_season={SCRAPE_LEAGUES_MAX_LEAGUES_PER_SEASON or 'all'}, "
        f"max_fixtures={SCRAPE_LEAGUES_MAX_FIXTURES or 'all'}"
    )

    for season in seasons:
        leagues = _scrape_league_catalog_for_season(session, season, logger)
        if SCRAPE_LEAGUES_MAX_LEAGUES_PER_SEASON:
            leagues = leagues[:SCRAPE_LEAGUES_MAX_LEAGUES_PER_SEASON]

        logger.info(f"Processing {len(leagues)} leagues for season {season['label']}")

        i = 0

        for league_raw in leagues:
            logger.inc_processed()
            logger_keys = {
                "season":           season["label"],
                "league_id_ext":    league_raw["league_id_ext"],
                "league":           league_raw["name"],
            }

            i += 1

            logger.info(logger_keys.copy(), f"Processing league {i}/{len(leagues)}", to_console=True)

            league_obj = LeagueRaw.from_dict(league_raw)
            is_valid, msg = league_obj.validate()
            if not is_valid:
                logger.failed(logger_keys, f"League validation failed: {msg}")
                continue

            action = league_obj.upsert(cursor)
            if action:
                leagues_seen += 1
                logger.success(logger_keys.copy(), f"LeagueRaw {action}")
            else:
                logger.failed(logger_keys.copy(), "LeagueRaw upsert failed")
                continue

            fixtures, fixture_matches = _scrape_league_fixtures(session, league_raw, season, logger, cursor)
            if SCRAPE_LEAGUES_MAX_FIXTURES:
                fixtures = fixtures[:SCRAPE_LEAGUES_MAX_FIXTURES]
                allowed_ids = {f["league_fixture_id_ext"] for f in fixtures}
                fixture_matches = [m for m in fixture_matches if m["league_fixture_id_ext"] in allowed_ids]

            for fixture_data in fixtures:
                fixture_obj = LeagueFixtureRaw.from_dict(fixture_data)
                fixture_valid, f_msg = fixture_obj.validate()
                fixture_keys = {**logger_keys, "fixture_id": fixture_obj.league_fixture_id_ext}
                if not fixture_valid:
                    logger.failed(fixture_keys, f"Fixture validation failed: {f_msg}")
                    continue
                fixture_action = fixture_obj.upsert(cursor)
                if fixture_action:
                    fixtures_seen += 1
                    logger.inc_processed()
                    logger.success(fixture_keys, f"Fixture {fixture_action}")
                else:
                    logger.inc_processed()
                    logger.failed(fixture_keys, "Fixture upsert failed")

            for match_data in fixture_matches:
                match_obj = LeagueFixtureMatchRaw.from_dict(match_data)
                match_valid, m_msg = match_obj.validate()
                match_keys = {
                    **logger_keys,
                    "fixture_id": match_obj.league_fixture_id_ext,
                    "fixture_match_id": match_obj.league_fixture_match_id_ext,
                }
                if not match_valid:
                    logger.failed(match_keys, f"Fixture match validation failed: {m_msg}")
                    continue
                match_action = match_obj.upsert(cursor)
                if match_action:
                    matches_seen += 1
                    logger.success(match_keys, f"Fixture match {match_action}")
                    logger.inc_processed()
                else:
                    logger.failed(match_keys, "Fixture match upsert failed")
                    logger.inc_processed()

    logger.info(
        f"Finished in {time.time() - start_time:.1f}s "
        f"(seasons: {len(seasons)}, leagues: {leagues_seen}, fixtures: {fixtures_seen}, matches: {matches_seen})"
    )
    logger.summarize()


# ----------------------------------------------------------------------
# Season discovery and filtering
# ----------------------------------------------------------------------
def _init_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def _fetch_html(session: requests.Session, url: str) -> Optional[str]:
    try:
        if SCRAPE_LEAGUES_REQUEST_DELAY:
            time.sleep(SCRAPE_LEAGUES_REQUEST_DELAY)
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            return None
        return resp.text
    except Exception:
        return None


def _discover_seasons(session: requests.Session, logger: OperationLogger) -> List[Dict[str, str]]:
    html = _fetch_html(session, SERIE_URL)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    nav = soup.select_one(".sidebar-nav")
    if not nav:
        return []

    seasons_map: Dict[str, Dict[str, str]] = {}
    for link in nav.find_all("a", href=True):
        if "serieoppsett_sesong.php?id=" not in link["href"]:
            continue
        m = re.search(r"id=(\d+)", link["href"])
        if not m:
            continue
        season_id = m.group(1)
        label = link.get_text(strip=True) or f"Season {season_id}"
        seasons_map[season_id] = {
            "label": label,
            "id_ext": season_id,
            "url": urljoin(BASE_ROOT, link["href"]),
            "is_current": label.startswith("*"),
        }

    seasons = list(seasons_map.values())
    order = (SCRAPE_LEAGUES_SEASONS_ORDER or "newest").lower()
    newest_first = order != "oldest"

    def _season_sort_key(s: Dict[str, str]) -> tuple:
        start_year, _ = _season_years_from_label(s["label"])
        year_key = -(start_year or 0) if newest_first else (start_year or 0)
        # Order by requested direction, then label as tiebreaker
        return (year_key, s["label"])

    seasons.sort(key=_season_sort_key)
    logger.info(f"Discovered {len(seasons)} seasons")
    return seasons


def _filter_seasons(seasons: List[Dict[str, str]], logger: OperationLogger) -> List[Dict[str, str]]:
    if SCRAPE_LEAGUES_SEASON_IDS:
        seasons = [s for s in seasons if s["id_ext"] in SCRAPE_LEAGUES_SEASON_IDS]

    if SCRAPE_LEAGUES_ONLY_CURRENT and seasons:
        current = [s for s in seasons if s.get("is_current")]
        seasons = current or seasons[:1]

    if SCRAPE_LEAGUES_MAX_SEASONS:
        seasons = seasons[:SCRAPE_LEAGUES_MAX_SEASONS]

    if seasons:
        logger.info(f"Season filter applied: {[s['label'] for s in seasons]}")
    return seasons


# ----------------------------------------------------------------------
# League list parsing
# ----------------------------------------------------------------------
def _scrape_league_catalog_for_season(
    session: requests.Session, season: Dict[str, str], logger: OperationLogger
) -> List[Dict[str, Any]]:
    switch_url = f"{SEASON_SWITCH_URL}?id={season['id_ext']}"
    session.get(switch_url, timeout=REQUEST_TIMEOUT)
    html = _fetch_html(session, SERIE_URL)
    if not html:
        logger.warning({"season": season["label"]}, "No HTML after switching season")
        return []
    return _parse_league_rows_from_html(html, season, logger)


def _parse_league_rows_from_html(
    html: str, season: Dict[str, str], logger: OperationLogger
) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    nav = soup.select_one(".sidebar-nav")
    if not nav:
        logger.warning({"season": season["label"]}, "Sidebar not found when parsing leagues")
        return []

    leagues: List[Dict[str, Any]] = []
    current_section: Optional[str] = None

    for li in nav.select("ul.nav > li"):
        classes = li.get("class") or []
        text = li.get_text(" ", strip=True)
        if "divisjon" in classes:
            current_section = text
            continue

        if not current_section or "Säsonger" in (current_section or ""):
            continue

        anchors = [a for a in li.find_all("a", href=True) if "k=LS" in a["href"]]
        if not anchors:
            continue

        for a in anchors:
            href = a["href"]
            m = re.search(r"k=LS(\d+)", href)
            if not m:
                continue

            league_id_ext = m.group(1)
            league_level, district = _section_to_level(current_section)
            raw = {
                "league_id_ext": league_id_ext,
                "season_label": season["label"],
                "season_id_ext": season["id_ext"],
                "league_level": league_level,
                "district_description": district,
                "name": a.get_text(strip=True),
                "organiser": "SBTF" if league_level != "District" else district,
                "active": 1,
                "url": urljoin(BASE_ROOT, href),
                "startdate": None,
                "enddate": None,
                "data_source_id": 3,
            }
            leagues.append(raw)

    logger.info(f"Parsed {len(leagues)} leagues from sidebar for {season['label']}")
    return leagues


def _section_to_level(section_label: str) -> Tuple[str, Optional[str]]:
    if "Nationella serier" in section_label:
        return "National", None
    if "Regionala serier" in section_label:
        return "Regional", None
    return "District", section_label


# ----------------------------------------------------------------------
# Fixtures and match reports
# ----------------------------------------------------------------------
def _scrape_league_fixtures(
    session: requests.Session,
    league_raw: Dict[str, Any],
    season: Dict[str, str],
    logger: OperationLogger,
    cursor,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    started_at = time.time()
    fixtures: List[Dict[str, Any]] = []
    matches: List[Dict[str, Any]] = []

    page_start = time.time()
    html = _fetch_html(session, league_raw["url"])
    league_page_ms = (time.time() - page_start) * 1000
    if not html:
        logger.warning(
            {"league_id_ext": league_raw["league_id_ext"], "league": league_raw["name"]},
            "Could not fetch league page",
        )
        return fixtures, matches

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if len(tables) < 2:
        logger.warning(
            {"league_id_ext": league_raw["league_id_ext"], "league": league_raw["name"]},
            "No fixtures table found on league page",
        )
        return fixtures, matches

    fixture_table = tables[1]
    fixtures = _parse_fixture_table(fixture_table, league_raw, season["label"])
    if SCRAPE_LEAGUES_MAX_FIXTURES:
        fixtures = fixtures[:SCRAPE_LEAGUES_MAX_FIXTURES]

    cache_stats = {"hits": 0, "downloads": 0, "bytes": 0, "fetch_ms": 0}
    match_loop_start = time.time()

    for fixture in fixtures:
        fixture_id = fixture["league_fixture_id_ext"]
        if _should_skip_fixture_matches(fixture, cursor):
            logger.info(
                {"league_fixture_id_ext": fixture_id, "league": league_raw["name"]},
                f"Skip match fetch (already seen and older than {SCRAPE_LEAGUES_SKIP_SEEN_MATCHES_DAYS}d)",
            )
            continue

        fixture_date, detail_matches, team_override = _scrape_match_report(
            session, fixture_id, league_raw, logger, cache_stats
        )
        if fixture_date:
            fixture["startdate"] = fixture_date
        if team_override:
            home_override, away_override = team_override
            fixture["home_team_name"] = home_override or fixture.get("home_team_name")
            fixture["away_team_name"] = away_override or fixture.get("away_team_name")
        matches.extend(detail_matches)

    total_ms = (time.time() - started_at) * 1000
    match_loop_ms = (time.time() - match_loop_start) * 1000
    logger.info(
        {"league_id_ext": league_raw["league_id_ext"], "league": league_raw["name"]},
        (
            f"Timing: league_page={league_page_ms:.0f}ms, match_loop={match_loop_ms:.0f}ms, total={total_ms:.0f}ms; "
            f"Cache: hits={cache_stats['hits']}, downloads={cache_stats['downloads']}, "
            f"bytes_written={cache_stats['bytes']}, fetch_ms={cache_stats['fetch_ms']:.0f}ms"
        ),
    )
    return fixtures, matches


def _parse_fixture_table(
    table, league_raw: Dict[str, Any], season_label: str
) -> List[Dict[str, Any]]:
    fixtures: List[Dict[str, Any]] = []
    current_round: Optional[str] = None

    for row in table.find_all("tr"):
        cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
        if not cells:
            continue

        if len(cells) == 1 and _looks_like_round_header(cells[0]):
            current_round = cells[0]
            continue

        link = row.find("a", href=True)
        if not link or "kampid=" not in link["href"]:
            continue
        link_href = link["href"]
        m = re.search(r"kampid=(\d+)", link_href)
        if not m:
            continue
        fixture_id = m.group(1)
        fixture_url = urljoin(BASE_ROOT, link_href)

        date_text = cells[0] if cells else None
        time_text = cells[1] if len(cells) > 1 else None
        home_team = cells[2] if len(cells) > 2 else None
        away_team = cells[4] if len(cells) > 4 else (cells[3] if len(cells) > 3 else None)
        result_cell = _find_result_cell(cells)

        home_score = away_score = None
        status = "scheduled"
        if result_cell:
            scores = re.findall(r"\d+", result_cell)
            if len(scores) >= 2:
                home_score, away_score = int(scores[0]), int(scores[1])
                status = "completed"

        startdate = _infer_date_from_tokens(date_text, time_text, season_label)

        fixtures.append(
            {
                "league_fixture_id_ext": fixture_id,
                "league_id_ext": league_raw["league_id_ext"],
                "startdate": startdate,
                "round": current_round,
                "home_team_name": home_team,
                "away_team_name": away_team,
                "home_score": home_score,
                "away_score": away_score,
                "status": status,
                "url": fixture_url,
                "data_source_id": 3,
            }
        )

    return fixtures


def _find_result_cell(cells: List[str]) -> Optional[str]:
    for c in reversed(cells):
        if not c or c.lower() == "detaljer":
            continue
        if c == "-":
            continue
        return c
    return None


def _looks_like_round_header(text: str) -> bool:
    return text.lower().startswith(("omgång", "omgang", "omg", "runde"))


def _scrape_match_report(
    session: requests.Session,
    fixture_id: str,
    league_raw: Dict[str, Any],
    logger: OperationLogger,
    cache_stats: Optional[Dict[str, int]] = None,
) -> Tuple[Optional[datetime.date], List[Dict[str, Any]], Optional[Tuple[Optional[str], Optional[str]]]]:
    url = f"{MATCH_REPORT_URL}?kampid={fixture_id}"
    html = None
    cache_path = _fixture_cache_path(fixture_id, league_raw)

    fetch_start = time.time()
    if SCRAPE_LEAGUES_CACHE_HTML and cache_path and os.path.isfile(cache_path):
        try:
            mtime = datetime.datetime.fromtimestamp(os.path.getmtime(cache_path)).date()
            if SCRAPE_LEAGUES_CACHE_HTML_DAYS is None or mtime >= (
                datetime.date.today() - datetime.timedelta(days=SCRAPE_LEAGUES_CACHE_HTML_DAYS)
            ):
                with open(cache_path, "r", encoding="utf-8") as f:
                    html = f.read()
                if cache_stats is not None:
                    cache_stats["hits"] += 1
        except Exception:
            html = None

    if not html:
        html = _fetch_html(session, url)
        if html and SCRAPE_LEAGUES_CACHE_HTML and cache_path:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            try:
                with open(cache_path, "w", encoding="utf-8") as f:
                    f.write(html)
                if cache_stats is not None:
                    cache_stats["downloads"] += 1
                    cache_stats["bytes"] += len(html.encode("utf-8"))
            except Exception:
                pass

    if cache_stats is not None:
        cache_stats["fetch_ms"] += (time.time() - fetch_start) * 1000

    if not html:
        logger.warning(
            {"league_fixture_id_ext": fixture_id, "league": league_raw["name"]},
            "Match report not reachable",
        )
        return None, [], None

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if len(tables) < 2:
        return None, [], None

    fixture_date = _parse_match_report_date(tables[0])
    home_team, away_team = _parse_match_report_teams(tables[1])

    matches: List[Dict[str, Any]] = []
    match_rows = tables[1].find_all("tr")
    if len(match_rows) < 2:
        return fixture_date, matches, (home_team, away_team)

    # The first data row contains all matches flattened into a single sequence of cells.
    flat_cells = [c.get_text(" ", strip=True) for c in match_rows[1].find_all(["td", "th"])]
    flat_cells = [c for c in flat_cells if c != ""]

    label_re = re.compile(r"^[SDsd]\d+$")
    score_re = re.compile(r"^\d+\s*-\s*\d+$")

    def _is_label(tok: str) -> bool:
        return bool(label_re.match(tok))

    idx = 0
    while idx < len(flat_cells):
        if not _is_label(flat_cells[idx]):
            idx += 1
            continue

        label = flat_cells[idx]
        league_fixture_match_id_ext = f"{fixture_id}-{label}"
        if idx + 4 >= len(flat_cells):
            break

        home_player_id = flat_cells[idx + 1]
        home_player = flat_cells[idx + 2]
        away_player_id = flat_cells[idx + 3]
        away_player = flat_cells[idx + 4]
        idx += 5

        trailing: List[str] = []
        while idx < len(flat_cells) and not _is_label(flat_cells[idx]):
            trailing.append(flat_cells[idx])
            idx += 1

        fixture_standing: Optional[str] = None
        fixture_idx: Optional[int] = None
        for j in range(len(trailing) - 1, -1, -1):
            if score_re.match(trailing[j]):
                fixture_standing = trailing[j]
                fixture_idx = j
                break

        trailing_before_fixture = trailing[:fixture_idx] if fixture_idx is not None else trailing
        trailing_after_fixture = trailing[fixture_idx + 1:] if fixture_idx is not None else []

        is_double = any(
            ident and "dbl" in ident.lower().replace(".", "")
            for ident in (label, home_player_id, away_player_id)
        )

        if is_double:
            partner_tokens = [t for t in trailing_after_fixture if t and t != "-"]
            partner_home = partner_tokens[0] if len(partner_tokens) > 0 else None
            partner_away = partner_tokens[1] if len(partner_tokens) > 1 else None
            if partner_home:
                home_player = " | ".join([p for p in [home_player, partner_home] if p])
            if partner_away:
                away_player = " | ".join([p for p in [away_player, partner_away] if p])

        set_tokens = [t for t in trailing_before_fixture if t and t != "-"]
        tokens = ", ".join(set_tokens) if set_tokens else None

        if not fixture_standing or fixture_standing == "-":
            continue

        matches.append(
            {
                "league_fixture_id_ext": fixture_id,
                "league_fixture_match_id_ext": league_fixture_match_id_ext,
                "home_player_id_ext": home_player_id,
                "home_player_name": home_player,
                "away_player_id_ext": away_player_id,
                "away_player_name": away_player,
                "tokens": tokens,
                "fixture_standing": fixture_standing,
                "data_source_id": 3,
            }
        )

    return fixture_date, matches, (home_team, away_team)


def _fixture_cache_path(fixture_id: str, league_raw: Dict[str, Any]) -> Optional[str]:
    season_id = league_raw.get("season_id_ext") or "unknown_season"
    league_id = league_raw.get("league_id_ext") or "unknown_league"
    base_dir = SCRAPE_LEAGUES_CACHE_HTML_DIR
    if not base_dir:
        return None
    return os.path.join(base_dir, str(season_id), str(league_id), f"{fixture_id}.html")


def _should_skip_fixture_matches(fixture: Dict[str, Any], cursor) -> bool:
    """
    Skip match report fetch if:
      - skipping is enabled,
      - fixture has a startdate older than the freshness window, and
      - we already have any match rows for this fixture.
    """
    if not SCRAPE_LEAGUES_SKIP_SEEN_MATCHES:
        return False

    startdate = fixture.get("startdate")
    if not startdate:
        return False

    freshness_cutoff = datetime.date.today() - datetime.timedelta(days=SCRAPE_LEAGUES_SKIP_SEEN_MATCHES_DAYS)
    if startdate >= freshness_cutoff:
        return False

    cursor.execute(
        """
        SELECT 1
        FROM league_fixture_match_raw
        WHERE league_fixture_id_ext = ? AND data_source_id = 3
        LIMIT 1;
        """,
        (fixture["league_fixture_id_ext"],),
    )
    return cursor.fetchone() is not None


def _parse_match_report_date(table) -> Optional[datetime.date]:
    for row in table.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in row.find_all("td")]
        if len(cells) >= 2 and "Datum" in cells[0]:
            try:
                parts = cells[1].replace(".", "-")
                return datetime.datetime.strptime(parts, "%d-%m-%Y").date()
            except Exception:
                return None
    return None


def _parse_match_report_teams(table) -> Tuple[Optional[str], Optional[str]]:
    rows = table.find_all("tr")
    if not rows:
        return None, None
    header_cells = rows[0].find_all(["td", "th"])
    if len(header_cells) >= 3:
        return header_cells[1].get_text(" ", strip=True), header_cells[2].get_text(" ", strip=True)
    return None, None


# ----------------------------------------------------------------------
# Date helpers
# ----------------------------------------------------------------------
def _season_years_from_label(label: str) -> Tuple[Optional[int], Optional[int]]:
    m = re.search(r"(20\\d{2})[/-](20\\d{2})", label)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r"(20\\d{2})", label)
    if m:
        start_year = int(m.group(1))
        return start_year, start_year + 1
    return None, None


def _infer_date_from_tokens(
    date_text: Optional[str], time_text: Optional[str], season_label: str
) -> Optional[datetime.date]:
    if not date_text:
        return None

    tokens = date_text.split()
    dm_token = None
    for tok in reversed(tokens):
        if re.search(r"\\d{1,2}[./]\\d{1,2}", tok):
            dm_token = tok
            break
    if not dm_token:
        return None

    try:
        day, month = dm_token.replace(".", "/").split("/")
        day_i, month_i = int(day), int(month)
    except Exception:
        return None

    start_year, end_year = _season_years_from_label(season_label)
    year = start_year or datetime.date.today().year
    if end_year and month_i < 7:
        year = end_year

    try:
        return datetime.date(year, month_i, day_i)
    except ValueError:
        return None
