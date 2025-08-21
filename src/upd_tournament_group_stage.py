from __future__ import annotations
import io, re, logging, datetime, time, requests
from typing import List, Dict, Optional, Tuple
import pdfplumber

from db import get_conn
from utils import print_db_insert_results, normalize_key, name_keys_for_lookup_all_splits
from config import (
    SCRAPE_PARTICIPANTS_MAX_CLASSES,
    SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
    SCRAPE_PARTICIPANTS_ORDER,
)
from models.club import Club
from models.player import Player
from models.tournament_class import TournamentClass
from models.participant import Participant
from models.tournament_group import TournamentGroup
from models.match import Match
from models.tournament_stage import TournamentStage

RESULTS_URL_TMPL = "https://resultat.ondata.se/ViewClassPDF.php?classID={class_id}&stage=3"

# ───────────────────────── Main entrypoint ─────────────────────────

def upd_tournament_group_stage():
    conn, cur = get_conn()
    t0 = time.perf_counter()

    classes_by_ext = TournamentClass.cache_by_id_ext(cur)
    if SCRAPE_PARTICIPANTS_CLASS_ID_EXTS != 0:
        tc = classes_by_ext.get(SCRAPE_PARTICIPANTS_CLASS_ID_EXTS)
        classes = [tc] if tc else []
    else:
        classes = list(classes_by_ext.values())

    order = (SCRAPE_PARTICIPANTS_ORDER or "").lower()
    if order == "newest":
        classes.sort(key=lambda tc: tc.date or datetime.date.min, reverse=True)
    elif order == "oldest":
        classes.sort(key=lambda tc: tc.date or datetime.date.min)

    if SCRAPE_PARTICIPANTS_MAX_CLASSES and SCRAPE_PARTICIPANTS_MAX_CLASSES > 0:
        classes = classes[:SCRAPE_PARTICIPANTS_MAX_CLASSES]

    # Filter for singles and tournament type 1 and 2 (Groups + KO and Groups only)
    classes = [tc for tc in classes if (tc.structure_id in (1, 2))]

    print(f"ℹ️  Updating tournament GROUP STAGE for {len(classes)} classes…")
    logging.info(f"Updating tournament GROUP STAGE for {len(classes)} classes")

    club_map = Club.cache_name_map(cur)
    part_by_class_fast = PlayerParticipant.cache_by_class_name_fast(cur)

    totals = {"groups": 0, "matches": 0, "kept": 0, "skipped": 0}
    db_results = []

    # Loop classes
    for idx, tc in enumerate(classes, 1):
        label = f"{tc.shortname or tc.class_description or tc.tournament_class_id} (ext:{tc.tournament_class_id_ext})"
        print(f"ℹ️  [{idx}/{len(classes)}] Class {label} (id_ext: {tc.tournament_class_id_ext}, id: {tc.tournament_class_id})  date={tc.date}")
        logging.info(f"[{idx}/{len(classes)}] Class {label} (id_ext: {tc.tournament_class_id_ext}, id: {tc.tournament_class_id})  date={tc.date}")

        # class_fast = part_by_class_fast.get(tc.tournament_class_id, {})
        # by_name_club = class_fast.get("by_name_club", {})
        # by_name_only = class_fast.get("by_name_only", {})

        # Set up cache maps
        class_fast   = part_by_class_fast.get(tc.tournament_class_id, {})
        by_code      = class_fast.get("by_code", {})
        by_name_club = class_fast.get("by_name_club", {})
        by_name_only = class_fast.get("by_name_only", {})        

        if not by_name_club and not by_name_only:
            logging.warning(f"No participants found for class {tc.tournament_class_id} (ext={tc.tournament_class_id_ext})")
            db_results.append({
                "status": "skipped", 
                "reason": f"No participants in class_id", 
                "warnings": ""})
            continue

        url = RESULTS_URL_TMPL.format(class_id=tc.tournament_class_id_ext)
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
        except Exception as e:
            reason = f"Download failed: {e}"
            print(f"❌ {reason}")
            logging.error(reason)
            db_results.append({"status": "failed", "reason": reason, "warnings": ""})
            conn.commit()
            continue

        try:
            groups = _parse_groups_pdf(r.content)
        except Exception as e:
            reason = f"PDF parsing failed: {e}"
            print(f"❌ {reason}")
            logging.error(reason)
            db_results.append({"status": "failed", "reason": reason, "warnings": ""})
            conn.commit()
            continue

        g_cnt = len(groups)
        m_cnt = sum(len(g["matches"]) for g in groups)
        totals["groups"] += g_cnt
        totals["matches"] += m_cnt
        logging.info(f"✅ Parsed {g_cnt} pools / {m_cnt} matches for {tc.shortname or tc.class_short or tc.tournament_class_id} (ext={tc.tournament_class_id_ext})")

        kept = skipped = 0
        logging.info(f"PDF URL: {url}")

        # for g_idx, g in enumerate(groups, 1):

        #     pool_name = g["name"]

        #     # 1) Upsert the pool row and get a real group_id
        #     group = TournamentGroup(
        #         group_id=None,
        #         tournament_class_id=tc.tournament_class_id,
        #         name=pool_name,
        #         sort_order=g_idx
        #     ).upsert(cur)
        #     group_id = group.group_id
        #     member_pids: set[int] = set()

        #     for i, m in enumerate(g["matches"], 1):

        #         def resolve_fast(code, name, club):
        #             # 1) Participant code (O(1))
        #             if code:
        #                 pid = by_code.get(code)
        #                 if pid is None:
        #                     # try canonical (077 == 77)
        #                     cz = code.lstrip("0") or "0"
        #                     pid = by_code.get(cz)
        #                 if pid is not None:
        #                     return pid  # early return, no name work

        #             # 2) Fallback: name + club
        #             # (only executed if code was None or not found)
        #             keys = name_keys_for_lookup_all_splits(name)  # only computed on fallback
        #             if club:
        #                 cobj = club_map.get(Club._normalize(club))
        #                 if cobj:
        #                     cid = cobj.club_id
        #                     for k in keys:
        #                         pid = by_name_club.get((k, cid))
        #                         if pid:
        #                             return pid

        #             # 3) Fallback: name-only (unique only)
        #             for k in keys:
        #                 lst = by_name_only.get(k)
        #                 if lst and len(lst) == 1:
        #                     return lst[0]
                        
        #             return None
                
        #         p1_pid = resolve_fast(m["p1_code"], m["p1"]["name"], m["p1"]["club"])
        #         p2_pid = resolve_fast(m["p2_code"], m["p2"]["name"], m["p2"]["club"])

        #         match_id_ext    = m.get("match_id_ext")
        #         p1_code         = m.get("p1_code")
        #         p2_code         = m.get("p2_code")
        #         p1_pid          = _resolve_fast(m["p1"]["name"], m["p1"]["club"], by_name_club, by_name_only, club_map)
        #         p2_pid          = _resolve_fast(m["p2"]["name"], m["p2"]["club"], by_name_club, by_name_only, club_map)

        #         if not p1_pid or not p2_pid:
        #             skipped += 1
        #             logging.warning(f"       SKIP [POOL {g_idx}/{g_cnt}] {m['p1']['name']} vs {m['p2']['name']} → unmatched participant(s). tokens={m['tokens']}")
        #             continue

        #         kept += 1
        #         member_pids.update([p1_pid, p2_pid])

        #         best    = _infer_best_of_from_sign(m['tokens'])
        #         games   = _tokens_to_games_from_sign(m['tokens'])
        #         summary = _score_summary_games_won(m['tokens'])

        #         # 2) Persist match + sides + games using YOUR model
        #         mx = Match(
        #             match_id=None,
        #             tournament_class_id         = tc.tournament_class_id,
        #             tournament_match_id_ext     = match_id_ext,
        #             fixture_id                  = None,
        #             tournament_stage_id         = 1,
        #             group_id                    = group_id,
        #             best_of                     = best,
        #             date                        = None,
        #             score_summary               = summary,
        #             notes                       = None,
        #         )
        #         mx.add_side_participant(1, p1_pid)
        #         mx.add_side_participant(2, p2_pid)
        #         for no, (s1, s2) in enumerate(games, start=1):
        #             mx.add_game(no, s1, s2)

        #         db_results.append(mx.save_to_db(cur))

        #         # 3) Upsert pool members once per pool
        #         for pid in member_pids:
        #             group.add_member(cur, pid)   

        for g_idx, g in enumerate(groups, 1):
            pool_name = g["name"]

            # 1) Upsert the pool row and get a real group_id
            group = TournamentGroup(
                group_id=None,
                tournament_class_id=tc.tournament_class_id,
                name=pool_name,
                sort_order=g_idx
            ).upsert(cur)
            group_id = group.group_id

            # Collect unique participant_ids in this pool
            member_pids: set[int] = set()

            # Local refs (tiny speedups)
            _by_code_get       = by_code.get
            _by_name_club_get  = by_name_club.get
            _by_name_only_get  = by_name_only.get
            _club_map_get      = club_map.get
            _club_norm         = Club._normalize

            def resolve_fast(code: str | None, name: str, club: str | None):
                # 1) Participant code (O(1))
                if code:
                    pid = _by_code_get(code)
                    if pid is None:
                        cz = code.lstrip("0") or "0"   # treat 077 == 77
                        pid = _by_code_get(cz)
                    if pid is not None:
                        return pid, "code"

                # 2) Fallback: name + club (only compute keys on fallback)
                keys = name_keys_for_lookup_all_splits(name)
                if club:
                    cobj = _club_map_get(_club_norm(club))
                    if cobj:
                        cid = cobj.club_id
                        for k in keys:
                            pid = _by_name_club_get((k, cid))
                            if pid:
                                return pid, "name+club"

                # 3) Fallback: name-only (unique only)
                for k in keys:
                    lst = _by_name_only_get(k)
                    if lst and len(lst) == 1:
                        return lst[0], "name-only"

                return None, "unmatched"

            # 2) Iterate matches in this pool
            for i, mm in enumerate(g["matches"], 1):
                match_id_ext = mm.get("match_id_ext")
                p1_code      = mm.get("p1_code")
                p2_code      = mm.get("p2_code")

                p1_pid, how1 = resolve_fast(p1_code, mm["p1"]["name"], mm["p1"]["club"])
                p2_pid, how2 = resolve_fast(p2_code, mm["p2"]["name"], mm["p2"]["club"])

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
                summary = _score_summary_games_won(mm['tokens'])

                # 3) Persist match + sides + games
                mx = Match(
                    match_id=None,
                    tournament_class_id     = tc.tournament_class_id,
                    tournament_match_id_ext = match_id_ext,
                    fixture_id              = None,
                    tournament_stage_id     = 1,           # GROUP
                    group_id                = group_id,
                    best_of                 = best,
                    date                    = None,
                    score_summary           = summary,
                    notes                   = None,
                )
                mx.add_side_participant(1, p1_pid)
                mx.add_side_participant(2, p2_pid)
                for no, (s1, s2) in enumerate(games, start=1):
                    mx.add_game(no, s1, s2)

                db_results.append(mx.save_to_db(cur))

            # 4) Upsert pool members once per pool (after processing its matches)
            for pid in member_pids:
                group.add_member(cur, pid)

        totals["kept"] += kept
        totals["skipped"] += skipped
        logging.info(f"   ✅ Valid matches kept: {kept}   ⏭️  Skipped: {skipped}")
        conn.commit()

    elapsed = time.perf_counter() - t0
    print(f"ℹ️  Group stage parse complete in {elapsed:.2f}s")
    print(f"ℹ️  Totals — pools: {totals['groups']}, matches parsed: {totals['matches']}, kept: {totals['kept']}, skipped: {totals['skipped']}")
    print("")
    print_db_insert_results(db_results)
    print("")
    conn.close()


# ───────────────────────── PDF parsing helpers ─────────────────────────

_RE_POOL = re.compile(r"\bPool\s+\d+\b", re.IGNORECASE)

# _RE_MATCH_LEFT = re.compile(
#     r"^\s*(?P<mid>\d{1,5})\s+(?P<p1>.+?)\s*-\s*(?P<p2>.+?)\s*$"
# )
# _RE_MATCH_LEFT = re.compile(
#     r"^(?P<mid>\d{3})\s+(?P<p1code>\d{3})\s+(?P<p1>.+?)\s*-\s*(?P<p2code>\d{3})\s+(?P<p2>.+?)$"
# )

_RE_MATCH_LEFT = re.compile(
    # r"^(?P<mid>\d{3})\s+"
    r"^(?P<mid>\d{1,3})\s+"
    r"(?P<p1code>\d{3})\s+(?P<p1>.+?)\s*-\s*"
    r"(?P<p2code>\d{3})\s+(?P<p2>.+?)"
    r"(?:\s+(?P<rest>[0-9,\s+\-:]+))?$"
)

_RE_NAME_CLUB = re.compile(r"^(?P<name>.+?)(?:,\s*(?P<club>.+))?$")
_RE_LEADING_CODE = re.compile(r"^\s*(?:\d{1,3})\s+(?=\S)")

def _parse_groups_pdf(
        pdf_bytes: bytes
    ) -> List[Dict]:
    rows = _extract_rows_group_stage(pdf_bytes)
    groups: List[Dict] = []
    current: Optional[Dict] = None

    for row in rows:
        m_pool = _RE_POOL.search(row)
        if m_pool:
            current = {"name": m_pool.group(0), "matches": []}
            groups.append(current)
            continue

        m = _RE_MATCH_LEFT.match(row)

        if m and current:

            match_id_ext    = m.group("mid").strip()
            p1_code         = m.group("p1code")
            p2_code         = m.group("p2code")
            p1              = _split_name_club(m.group("p1").strip())
            p2              = _split_name_club(m.group("p2").strip())
            rest            = m.group("rest") or ""
            tokens          = _tokenize_right(rest)

            current["matches"].append(
                {
                "match_id_ext": match_id_ext,
                "p1_code":      p1_code,
                "p2_code":      p2_code,
                "p1":           p1,
                "p2":           p2,
                "tokens":       tokens
                }
            )

    return groups

def _extract_rows_group_stage(pdf_bytes: bytes) -> list[str]:
    """
    Extracts each visual row from the PDF in exact left-to-right order.
    Used for group stage parsing where match id must stay at the start.
    """
    rows = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            words = page.extract_words(
                x_tolerance=2,
                y_tolerance=3,
                keep_blank_chars=False
            ) or []
            if not words:
                continue

            # group words into rows by vertical position
            row_map: dict[int, list[dict]] = {}
            rid, last_top = 0, None
            for w in sorted(words, key=lambda w: (round(w["top"], 1), w["x0"])):
                top = round(w["top"], 1)
                if last_top is None or abs(top - last_top) > 3.0:
                    rid += 1
                    last_top = top
                    row_map[rid] = []
                row_map[rid].append(w)

            # join words left-to-right for each row
            for words_in_row in row_map.values():
                words_in_row.sort(key=lambda w: w["x0"])
                row_text = " ".join(w["text"] for w in words_in_row).strip()
                if row_text:
                    rows.append(row_text)
    return rows

def _split_name_club(raw: str) -> Dict[str, Optional[str]]:
    s = _RE_LEADING_CODE.sub("", raw.strip())
    m = _RE_NAME_CLUB.match(s)
    name = (m.group("name") if m else s).strip()
    club = (m.group("club") if m else None)
    return {"raw": raw, "name": name, "club": (club.strip() if club else None)}

def _tokenize_right(s: str) -> List[str]:
    if not s:
        return []
    return [t.replace(" ", "") for t in re.findall(r"\d+\s*-\s*\d+|[+-]?\d+|\d+\s*:\s*\d+", s)]

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


def derive_best_of_from_summary(score_summary: str) -> int:
    try:
        p1_games, p2_games = map(int, score_summary.split("-"))
    except ValueError:
        return None
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

# ───────────────────────── Resolving helpers ─────────────────────────

def resolve_participant(
    code: str | None,
    name: str,
    club: str | None,
    by_code: dict[str, int],
    by_name_club: dict[tuple[str, int], int],
    by_name_only: dict[str, list[int]],
    club_map: dict[str, Club],
) -> int | None:
    # 1) Code (strongest)
    if code:
        pid = by_code.get(code) or by_code.get(code.lstrip("0") or "0")
        if pid:
            return pid

    # 2) Name + club
    keys = name_keys_for_lookup_all_splits(name)
    if club:
        cobj = club_map.get(Club._normalize(club))
        if cobj:
            for k in keys:
                pid = by_name_club.get((k, cobj.club_id))
                if pid:
                    return pid

    # 3) Name-only (only if unique)
    for k in keys:
        lst = by_name_only.get(k, [])
        if len(lst) == 1:
            return lst[0]
    return None

def _resolve_fast(
        name: str, 
        club: Optional[str],
        by_name_club: Dict[Tuple[str, int], int],
        by_name_only: Dict[str, List[int]],
        club_map: Dict[str, Club]
    ) -> Optional[int]:
    keys = name_keys_for_lookup_all_splits(name)
    if club:
        club_obj = club_map.get(Club._normalize(club))
        if club_obj:
            for k in keys:
                pid = by_name_club.get((k, club_obj.club_id))
                if pid:
                    return pid
    for k in keys:
        lst = by_name_only.get(k, [])
        if len(lst) == 1:
            return lst[0]
    return None

def _score_summary_games_won(tokens: list[str]) -> str:
    """
    Returns score summary in match-game form, e.g. '3-2'.
    Strict deuce rules apply for interpreting winners from tokens.
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

    return f"{p1_games}-{p2_games}"




