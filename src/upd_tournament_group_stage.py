# src/upd_tournament_group_stage.py

from __future__ import annotations
import io, re, logging, time, requests
from typing import List, Dict, Optional, Tuple
import pdfplumber

from db import get_conn
from utils import parse_date, print_db_insert_results, name_keys_for_lookup_all_splits, OperationLogger
from config import (
    SCRAPE_PARTICIPANTS_MAX_CLASSES,
    SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
    SCRAPE_PARTICIPANTS_TNMT_ID_EXTS,
    SCRAPE_PARTICIPANTS_ORDER,
    SCRAPE_PARTICIPANTS_CUTOFF_DATE,
)
from models.club import Club
from models.player import Player
from models.participant import Participant
from models.tournament_class import TournamentClass
from models.tournament_class_group import TournamentClassGroup
from models.tournament_class_stage import TournamentClassStage
from models.match import Match
from models.participant_player import ParticipantPlayer

RESULTS_URL_TMPL = "https://resultat.ondata.se/ViewClassPDF.php?classID={class_id}&stage=3"

# ───────────────────────── Main entrypoint ─────────────────────────

def upd_tournament_group_stage():
    conn, cursor = get_conn()
    t0 = time.perf_counter()

    logger = OperationLogger(
    verbosity       = 2, 
    print_output    = False, 
    log_to_db       = False, 
    cursor          = cursor
    )

    cutoff_date = parse_date(SCRAPE_PARTICIPANTS_CUTOFF_DATE) if SCRAPE_PARTICIPANTS_CUTOFF_DATE else None

    classes = TournamentClass.get_filtered_classes(
        cursor=cursor,
        class_id_exts       = SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,          # accept single id or list
        tournament_id_exts  = SCRAPE_PARTICIPANTS_TNMT_ID_EXTS,           # NEW: filter by tournament(s) when provided
        data_source_id      = 1 if (SCRAPE_PARTICIPANTS_CLASS_ID_EXTS or SCRAPE_PARTICIPANTS_TNMT_ID_EXTS) else None,
        cutoff_date         = cutoff_date,                                 # optional: omit or keep as you need
        require_ended       = False,                                       # group stage often exists before KO finishes
        allowed_type_ids    = [1],                                         # singles only
        allowed_structure_ids = [1, 2],                                    # NEW: Groups+KO or Groups-only (group games)
        max_classes         = SCRAPE_PARTICIPANTS_MAX_CLASSES,
        order               = SCRAPE_PARTICIPANTS_ORDER,
    )

    print(f"ℹ️  Updating tournament GROUP STAGE for {len(classes)} classes…")
    logging.info(f"Updating tournament GROUP STAGE for {len(classes)} classes")

    club_map = Club.cache_name_map(cursor)
    part_by_class_fast = ParticipantPlayer.cache_by_class_name_fast(cursor)
    stage_cache = TournamentClassStage.cache_all(cursor)  # e.g., {"GROUP": 1, "R16": 5, ...}
    group_stage_id = stage_cache.get("GROUP")

    totals = {"groups": 0, "matches": 0, "kept": 0, "skipped": 0}
    db_results = []

    # Loop classes
    for idx, tc in enumerate(classes, 1):
        label = f"{tc.shortname or tc.class_description or tc.tournament_class_id} (ext:{tc.tournament_class_id_ext})"
        print(f"ℹ️  [{idx}/{len(classes)}] Class {label} (id_ext: {tc.tournament_class_id_ext}, id: {tc.tournament_class_id})  date={tc.date}")
        logging.info(f"[{idx}/{len(classes)}] Class {label} (id_ext: {tc.tournament_class_id_ext}, id: {tc.tournament_class_id})  date={tc.date}")

        # Set up cache maps
        class_fast   = part_by_class_fast.get(tc.tournament_class_id, {})
        by_code      = class_fast.get("by_code", {})
        by_name_club = class_fast.get("by_name_club", {})
        by_name_only = class_fast.get("by_name_only", {})        

        item_key = f"tid: {tc.tournament_class_id}, tid_ext: {tc.tournament_class_id_ext}"
        if not by_name_club and not by_name_only:
            # logging.warning(f"No participants found for class {tc.tournament_class_id} (ext={tc.tournament_class_id_ext})")
            logger.failed(item_key, "No participants found for class")
            continue

        url = RESULTS_URL_TMPL.format(class_id=tc.tournament_class_id_ext)
        # try:
        #     r = requests.get(url, timeout=30)
        #     r.raise_for_status()
        # except Exception as e:
        #     reason = f"Download failed: {e}"
        #     print(f"❌ {reason}")
        #     logger.failed(item_key, reason)
        #     conn.commit()
        #     continue

        # try:
        #     groups = _parse_groups_pdf(r.content)
        # except Exception as e:
        #     reason = f"PDF parsing failed: {e}"
        #     print(f"❌ {reason}")
        #     logger.failed(item_key, reason)
        #     conn.commit()
        #     continue

        pdf_bytes = fetch_pdf(url)

        if not pdf_bytes:
            reason = f"Download failed after retries: {url}"
            print(f"❌ {reason}")
            logger.failed(item_key, reason)
            conn.commit()
            continue

        try:
            groups = _parse_groups_pdf(pdf_bytes)
        except Exception as e:
            reason = f"PDF parsing failed: {e}"
            print(f"❌ {reason}")
            logger.failed(item_key, reason)
            conn.commit()
            continue

        g_cnt = len(groups)
        m_cnt = sum(len(g["matches"]) for g in groups)
        totals["groups"] += g_cnt
        totals["matches"] += m_cnt
        logging.info(f"✅ Parsed {g_cnt} pools / {m_cnt} matches for {tc.shortname or tc.class_short or tc.tournament_class_id} (ext={tc.tournament_class_id_ext})")

        kept = skipped = 0
        logging.info(f"PDF URL: {url}")

        # 1) Upsert the pool row and get a real group_id
        for g_idx, g in enumerate(groups, 1):
            group_desc = g["name"]  # e.g. "Pool 1"

            res = TournamentClassGroup(
                tournament_class_group_id=None,
                tournament_class_id=tc.tournament_class_id,
                description=group_desc,
                sort_order=g_idx
            ).upsert(cursor)

            group_id = res["tournament_class_group_id"]
            logging.info(
                f"   🏷️  Pool {group_desc}: {res['status']} (id={group_id}) "
                f"[class_id={tc.tournament_class_id}, class_id_ext={tc.tournament_class_id_ext}]"
            )

            # Collect unique participant_ids in this pool
            member_pids: set[int] = set()

            # 2) Iterate matches in this pool
            for i, mm in enumerate(g["matches"], 1):

                status = mm.get("status")
                if status == "WO":
                # Handle walkover: e.g., set winner if known, or skip saving games
                    best = None
                    games = []
                    item_key = f"mid: {mm['match_id_ext']}, p1: {mm['p1']['name']}, p2: {mm['p2']['name']}"
                    logger.warning(item_key, "WO detected for match")
                else:
                    best = _infer_best_of_from_sign(mm['tokens'])
                    games = _tokens_to_games_from_sign(mm['tokens'])

                match_id_ext = mm.get("match_id_ext")
                p1_code      = mm.get("p1_code")
                p2_code      = mm.get("p2_code")

                p1_pid, how1 = resolve_participant(
                    p1_code, mm["p1"]["name"], mm["p1"]["club"],
                    by_code, by_name_club, by_name_only, club_map,
                    cursor=cursor,
                    logger=logger,
                    item_key=f"{tc.shortname or tc.tournament_class_id} (ext:{tc.tournament_class_id_ext})"
                )

                p2_pid, how2 = resolve_participant(
                    p2_code, mm["p2"]["name"], mm["p2"]["club"],
                    by_code, by_name_club, by_name_only, club_map,
                    cursor=cursor,
                    logger=logger,
                    item_key=f"{tc.shortname or tc.tournament_class_id} (ext:{tc.tournament_class_id_ext})"
                )


                if not p1_pid or not p2_pid:
                    skipped += 1
                    logging.warning(
                        f"       SKIP [POOL {g_idx}/{g_cnt}] "
                        f"{mm['p1']['name']} ({p1_code or '-'}) vs {mm['p2']['name']} ({p2_code or '-'}) "
                        f"→ unmatched participant(s). how=[{how1},{how2}] tokens={mm['tokens']}"
                    )
                    continue

                kept += 1
                member_pids.update((p1_pid, p2_pid))

                best    = _infer_best_of_from_sign(mm['tokens'])
                games   = _tokens_to_games_from_sign(mm['tokens'])

                # 3) Persist match + sides + games
                mx = Match(
                    match_id=None,
                    best_of=best,
                    date=tc.date,

                    match_id_ext=match_id_ext,                      # from PDF
                    data_source_id=1,                               # <-- pass YOUR data_source_id explicitly

                    competition_type_id=1,                          # TournamentClass
                    tournament_class_id=tc.tournament_class_id,
                    tournament_class_group_id=group_id,             # from TournamentGroup.upsert(...)
                    tournament_class_stage_id=group_stage_id,       # optional: set if/when you have it
                )
                mx.add_side_participant(1, p1_pid)
                mx.add_side_participant(2, p2_pid)
                for no, (s1, s2) in enumerate(games, start=1):
                    mx.add_game(no, s1, s2)

                res = mx.save_to_db(cursor)

                item_key = f"match_id_ext={mx.match_id_ext or 'N/A'}"

                status = res.get("status")

                if status in ("inserted", "updated", "success"):
                    logger.success(item_key, f"Match {status}")
                elif status == "failed":
                    logger.failed(item_key, f"Match save failed: {res.get('reason')}")
                elif status == "skipped":
                    logger.skipped(item_key, "Skipped match")
                elif status == "warning":
                    logger.warning(item_key, res.get("reason", "Match saved with warnings"))
                else:
                    logger.warning(item_key, f"Unknown match save status: {status}")

            # 4) Upsert pool members once per pool (after processing its matches)
            for pid in member_pids:
                TournamentClassGroup(
                    tournament_class_group_id=group_id,
                    tournament_class_id=tc.tournament_class_id,
                    description=group_desc,
                    sort_order=g_idx
                ).add_member(cursor, pid)

        totals["kept"] += kept
        totals["skipped"] += skipped
        logging.info(f"   ✅ Valid matches kept: {kept}   ⏭️  Skipped: {skipped}")
        conn.commit()

    elapsed = time.perf_counter() - t0
    print(f"ℹ️  Group stage parse complete in {elapsed:.2f}s")
    print(f"ℹ️  Totals — pools: {totals['groups']}, matches parsed: {totals['matches']}, kept: {totals['kept']}, skipped: {totals['skipped']}")
    print("")
    logger.summarize()
    print("")
    conn.close()


# ───────────────────────── PDF parsing helpers ─────────────────────────

_RE_POOL = re.compile(r"\bPool\s+\d+\b", re.IGNORECASE)

# _RE_MATCH_LEFT = re.compile(
#     # r"^(?P<mid>\d{3})\s+"
#     r"^(?P<mid>\d{1,3})\s+"
#     r"(?P<p1code>\d{3})\s+(?P<p1>.+?)\s*-\s*"
#     r"(?P<p2code>\d{3})\s+(?P<p2>.+?)"
#     r"(?:\s+(?P<rest>[0-9,\s+\-:]+))?$"
# )

_RE_NAME_CLUB = re.compile(r"^(?P<name>.+?)(?:,\s*(?P<club>.+))?$")
_RE_LEADING_CODE = re.compile(r"^\s*(?:\d{1,3})\s+(?=\S)")

# After we've stripped MID (if any), parse with or without player codes:
_RE_REMAINDER_WITH_CODES = re.compile(
    r"^\s*(?P<p1code>\d{1,3})\s+(?P<p1>.+?)\s*[-–]\s*(?P<p2code>\d{1,3})\s+(?P<p2>.+?)"
    r"(?:\s+(?P<rest>[\d,\s:+-]+))?$"
)
_RE_REMAINDER_NO_CODES = re.compile(
    r"^\s*(?P<p1>.+?)\s*[-–]\s*(?P<p2>.+?)"
    r"(?:\s+(?P<rest>[\d,\s:+-]+))?$"
)

# Fallback patterns that *include* MID at start (when no bold_mid detected)
_RE_MATCH_WITH_CODES = re.compile(
    r"^\s*(?P<mid>\d{1,3})\s+(?P<p1code>\d{1,3})\s+(?P<p1>.+?)\s*[-–]\s*(?P<p2code>\d{1,3})\s+(?P<p2>.+?)"
    r"(?:\s+(?P<rest>[\d,\s:+-]+))?$"
)
_RE_MATCH_NO_CODES = re.compile(
    r"^\s*(?P<mid>\d{1,3})\s+(?P<p1>.+?)\s*[-–]\s*(?P<p2>.+?)"
    r"(?:\s+(?P<rest>[\d,\s:+-]+))?$"
)

def _parse_groups_pdf(pdf_bytes: bytes) -> List[Dict]:
    rows = _extract_rows_group_stage_with_attrs(pdf_bytes)
    groups: List[Dict] = []
    current: Optional[Dict] = None

    def _debug_unmatched(row_text: str):
        if re.match(r"^\s*\d+\s+\S", row_text) or " - " in row_text or " – " in row_text:
            logging.debug(f"[group-parse] unmatched row: {row_text}")

    for row in rows:
        text = row["text"]

        # Pool header?
        m_pool = _RE_POOL.search(text)
        if m_pool:
            current = {"name": m_pool.group(0), "matches": []}
            groups.append(current)
            continue
        if not current:
            continue

        mid = row["bold_mid"]
        tail = row["tail_text"]

        # --- Branch A: bold MID ---
        if mid:
            m = _RE_REMAINDER_WITH_CODES.match(tail) or _RE_REMAINDER_NO_CODES.match(tail)
            if not m:
                _debug_unmatched(text)
                continue

            p1_str, p2_str = m.group("p1").strip(), m.group("p2").strip()

            # Guards: must look like "Name, Club - Name, Club"; skip footers/links
            lt = text.lower()
            if "," not in p1_str or "," not in p2_str:
                continue
            if "tt coordinator" in lt or "programlicens" in lt or "http://" in lt or "https://" in lt:
                continue

            p1code = m.groupdict().get("p1code")
            p2code = m.groupdict().get("p2code")
            rest   = m.group("rest") or ""

            current["matches"].append({
                "match_id_ext": mid,
                "p1_code": p1code,
                "p2_code": p2code,
                "p1": _split_name_club(p1_str),
                "p2": _split_name_club(p2_str),
                "tokens": _tokenize_right(rest),
            })
            continue

        # --- Branch B: plain MID at row start ---
        m = _RE_MATCH_WITH_CODES.match(text) or _RE_MATCH_NO_CODES.match(text)
        if m:
            p1_str, p2_str = m.group("p1").strip(), m.group("p2").strip()
            lt = text.lower()
            if "," not in p1_str or "," not in p2_str:
                continue
            if "tt coordinator" in lt or "programlicens" in lt or "http://" in lt or "https://" in lt:
                continue

            match_id_ext = m.group("mid").strip()
            p1code = m.groupdict().get("p1code")
            p2code = m.groupdict().get("p2code")
            rest   = m.group("rest") or ""

            current["matches"].append({
                "match_id_ext": match_id_ext,
                "p1_code": p1code,
                "p2_code": p2code,
                "p1": _split_name_club(p1_str),
                "p2": _split_name_club(p2_str),
                "tokens": _tokenize_right(rest),
            })
            continue

        # --- Branch C: truly no MID; parse names anyway ---
        m2 = _RE_REMAINDER_WITH_CODES.match(text) or _RE_REMAINDER_NO_CODES.match(text)
        if m2:
            p1_str, p2_str = m2.group("p1").strip(), m2.group("p2").strip()
            lt = text.lower()
            if "," not in p1_str or "," not in p2_str:
                continue
            if "tt coordinator" in lt or "programlicens" in lt or "http://" in lt or "https://" in lt:
                continue

            rest = m2.group("rest") or ""
            current["matches"].append({
                "match_id_ext": None,
                "p1_code":      m2.groupdict().get("p1code"),
                "p2_code":      m2.groupdict().get("p2code"),
                "p1":           _split_name_club(p1_str),
                "p2":           _split_name_club(p2_str),
                "tokens":       _tokenize_right(rest),
            })
            continue

        _debug_unmatched(text)

    return groups

def _split_name_club(raw: str) -> Dict[str, Optional[str]]:
    s = _RE_LEADING_CODE.sub("", raw.strip())
    m = _RE_NAME_CLUB.match(s)
    name = (m.group("name") if m else s).strip()
    club = (m.group("club") if m else None)
    return {"raw": raw, "name": name, "club": (club.strip() if club else None)}

# def _tokenize_right(s: str) -> List[str]:
#     if not s:
#         return []
#     return [t.replace(" ", "") for t in re.findall(r"\d+\s*-\s*\d+|[+-]?\d+|\d+\s*:\s*\d+", s)]

def _tokenize_right(s: str) -> List[str]:
    if not s:
        return []
    # Normalize commas to spaces, trim extras
    s = re.sub(r"\s*,\s*", " ", s.strip())
    # Find signed or unsigned digits (for signed scores)
    return re.findall(r"[+-]?\d+", s)

def _infer_best_of_from_sign(tokens: List[str]) -> Optional[int]:
    """
    Infer 'best of' from sign-based tokens.
    Uses the actual games won by the match winner:
    best_of = winner_games * 2 - 1
    """
    p1_games = 0
    p2_games = 0

    for raw in tokens:
        s = raw.strip().replace(" ", "")
        if not re.fullmatch(r"[+-]?\d+", s):
            continue
        v = int(s)
        if v >= 0:
            p1_games += 1
        else:
            p2_games += 1

    if p1_games == 0 and p2_games == 0:
        return None  # no games found

    winner_games = max(p1_games, p2_games)
    return winner_games * 2 - 1

def _tokens_to_games_from_sign(tokens: List[str]) -> List[Tuple[int, int]]:
    """
    Strict deuce:
      +x → P1 won, P2 scored x → score is (max(11, x+2), x)
      -x → P2 won, P1 scored x → score is (x, max(11, x+2))
    x is the loser’s points.
    """
    games: List[Tuple[int, int]] = []
    for raw in tokens:
        s = raw.replace(" ", "")
        if not re.fullmatch(r"[+-]?\d+", s):
            continue
        v = int(s)
        if v >= 0:
            loser = v
            p1 = max(11, loser + 2)  # winner
            p2 = loser               # loser
            games.append((p1, p2))
        else:
            loser = -v
            p1 = loser               # loser on side1
            p2 = max(11, loser + 2)  # winner on side2
            games.append((p1, p2))
    return games

def _extract_rows_group_stage_with_attrs(pdf_bytes: bytes) -> list[dict]:
    """
    Returns a list of rows with attributes:
      { "text": "...", "words": [pdfplumber word dict...], "bold_mid": "123" or None, "tail_text": "..." }
    - bold_mid: first 1–3 digit token that is bold and sits at the start of the row
    - tail_text: row text after removing the bold_mid token (if present)
    """
    rows: list[dict] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            words = page.extract_words(x_tolerance=2, y_tolerance=3, keep_blank_chars=False) or []
            if not words:
                continue

            # group words into rows by y position
            row_map: dict[int, list[dict]] = {}
            rid, last_top = 0, None
            for w in sorted(words, key=lambda w: (round(w["top"], 1), w["x0"])):
                top = round(w["top"], 1)
                if last_top is None or abs(top - last_top) > 3.0:
                    rid += 1
                    last_top = top
                    row_map[rid] = []
                row_map[rid].append(w)

            for words_in_row in row_map.values():
                words_in_row.sort(key=lambda w: w["x0"])
                row_text = " ".join(w["text"] for w in words_in_row).strip()
                if not row_text:
                    continue

                # find bold 1–3 digit token at row start (tolerate 1st token)
                bold_mid = None
                tail_words = words_in_row[:]
                if tail_words:
                    w0 = tail_words[0]
                    font = w0.get("fontname", "")
                    if w0["text"].isdigit() and 1 <= len(w0["text"]) <= 3 and ("Bold" in font or "bold" in font.lower()):
                        bold_mid = w0["text"]
                        tail_words = tail_words[1:]

                tail_text = " ".join(w["text"] for w in tail_words).strip()
                rows.append({
                    "text": row_text,
                    "words": words_in_row,
                    "bold_mid": bold_mid,
                    "tail_text": tail_text,
                })
    return rows


# ───────────────────────── Resolving helpers ─────────────────────────

# def resolve_participant(
#     code: str | None,
#     name: str,
#     club: str | None,
#     by_code: dict[str, int],
#     by_name_club: dict[tuple[str, int], int],
#     by_name_only: dict[str, list[int]],
#     club_map: dict[str, Club],
# ) -> tuple[Optional[int], str]:
#     # 1) Code (strongest)
#     if code:
#         pid = by_code.get(code) or by_code.get(code.lstrip("0") or "0")
#         if pid:
#             return pid, "code"

#     # 2) Name + club
#     keys = name_keys_for_lookup_all_splits(name)
#     if club:
#         cobj = club_map.get(Club._normalize(club))
#         if cobj:
#             for k in keys:
#                 pid = by_name_club.get((k, cobj.club_id))
#                 if pid:
#                     return pid, "name+club"

#     # 3) Name-only (only if unique)
#     for k in keys:
#         lst = by_name_only.get(k, [])
#         if len(lst) == 1:
#             return lst[0], "name-only"

#     return None, "unmatched"

from models.club import Club
from utils import name_keys_for_lookup_all_splits
from typing import Optional, Tuple

def resolve_participant(
    code: str | None,
    name: str,
    club: str | None,
    by_code: dict[str, int],
    by_name_club: dict[tuple[str, int], int],
    by_name_only: dict[str, list[int]],
    club_map: dict[str, Club],
    *,
    cursor,                          # NEW: pass db cursor
    logger,                          # NEW: pass OperationLogger
    item_key: str = ""               # optional context for logging
) -> Tuple[Optional[int], str]:
    """
    Resolve a participant to a player_id using multiple strategies:
      1) Tournament participant code
      2) Name + club (via Club.resolve)
      3) Name only (if unique)
    Returns (player_id, strategy)
    """
    # 1) Code (strongest)
    if code:
        pid = by_code.get(code) or by_code.get(code.lstrip("0") or "0")
        if pid:
            return pid, "code"

    # 2) Name + club
    keys = name_keys_for_lookup_all_splits(name)
    if club:
        cobj = Club.resolve(
            cursor=cursor,
            clubname_raw=club,
            club_map=club_map,
            logger=logger,
            item_key=item_key or f"resolve_participant:{name}",
            allow_prefix=True
        )
        if cobj and cobj.club_id != 9999:  # skip "Unknown club"
            for k in keys:
                pid = by_name_club.get((k, cobj.club_id))
                if pid:
                    return pid, "name+club"

    # 3) Name-only (only if unique)
    for k in keys:
        lst = by_name_only.get(k, [])
        if len(lst) == 1:
            return lst[0], "name-only"

    return None, "unmatched"


def fetch_pdf(url: str, retries: int = 3, timeout: int = 30) -> bytes | None:
    """Download a PDF with exponential backoff + jitter."""
    import random, time
    headers = {"User-Agent": "Mozilla/5.0 (compatible; BTstats/1.0)"}
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except requests.exceptions.Timeout:
            delay = 2 ** attempt + random.uniform(0, 1)
            logging.warning(f"Timeout fetching {url} (attempt {attempt}/{retries}), retrying in {delay:.1f}s")
            time.sleep(delay)
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed ({url}): {e}")
            break
    return None