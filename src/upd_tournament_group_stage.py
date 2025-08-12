from __future__ import annotations
import io, re, logging, datetime, time, requests
from typing import List, Dict, Optional, Tuple
import pdfplumber

from db import get_conn
from utils import print_db_insert_results, normalize_key
from config import (
    SCRAPE_CLASS_PARTICIPANTS_MAX_CLASSES,
    SCRAPE_CLASS_PARTICIPANTS_CLASS_ID_EXT,
    SCRAPE_CLASS_PARTICIPANTS_ORDER,
)
from models.club import Club
from models.player import Player
from models.tournament_class import TournamentClass
from models.player_participant import PlayerParticipant
from models.tournament_group import TournamentGroup
from models.match import Match
from models.tournament_stage import TournamentStage

RESULTS_URL_TMPL = "https://resultat.ondata.se/ViewClassPDF.php?classID={class_id}&stage=3"

# ───────────────────────── PDF parsing helpers ─────────────────────────

_RE_POOL = re.compile(r"\bPool\s+\d+\b", re.IGNORECASE)
_RE_MATCH_LEFT = re.compile(r"^\s*\d+\s+(.+?)\s*-\s*(.+?)\s*$")   # "123 A, Club - B, Club"
_RE_NAME_CLUB = re.compile(r"^(?P<name>.+?)(?:,\s*(?P<club>.+))?$")
_RE_LEADING_CODE = re.compile(r"^\s*(?:\d{1,3})\s+(?=\S)")

def _parse_groups_pdf(pdf_bytes: bytes) -> List[Dict]:
    rows = _extract_two_column_rows(pdf_bytes)
    groups: List[Dict] = []
    current: Optional[Dict] = None

    for left, right in rows:
        L = (left or "").strip()
        R = (right or "").strip()
        if not L and not R:
            continue

        m_pool = _RE_POOL.search(L)
        if m_pool:
            current = {"name": m_pool.group(0), "matches": []}
            groups.append(current)
            continue

        m = _RE_MATCH_LEFT.match(L)
        if m and current:
            p1 = _split_name_club(m.group(1).strip())
            p2 = _split_name_club(m.group(2).strip())
            tokens = _tokenize_right(R)
            current["matches"].append({"p1": p1, "p2": p2, "right_raw": R, "tokens": tokens})

    return groups

def _extract_two_column_rows(pdf_bytes: bytes) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            words = page.extract_words(x_tolerance=2, y_tolerance=3, keep_blank_chars=False) or []
            if not words:
                continue
            mid_x = (page.bbox[2] + page.bbox[0]) / 2.0
            rows: Dict[int, Dict[str, List[str]]] = {}
            rid, last_top = 0, None
            for w in sorted(words, key=lambda w: (round(w["top"], 1), w["x0"])):
                top = round(w["top"], 1)
                if last_top is None or abs(top - last_top) > 3.0:
                    rid += 1
                    last_top = top
                    rows[rid] = {"L": [], "R": []}
                (rows[rid]["L"] if w["x0"] < mid_x else rows[rid]["R"]).append(w["text"])
            for cols in rows.values():
                L = " ".join(cols["L"]).strip()
                R = " ".join(cols["R"]).strip()
                if L or R:
                    out.append((L, R))
    return out

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

def _tokens_to_games(tokens: List[str]) -> List[Tuple[int, int]]:
    games: List[Tuple[int, int]] = []
    for t in tokens:
        if "-" in t:
            try:
                a, b = t.split("-", 1)
                games.append((int(a), int(b)))
            except:
                pass
    return games

def _infer_best_of(tokens: List[str]) -> Optional[int]:
    sets = [t for t in tokens if "-" in t]
    return len(sets) if sets else (5 if tokens else None)

def _infer_best_of_from_sign(tokens: List[str]) -> Optional[int]:
    # count integer-like tokens only
    return sum(1 for t in tokens if t and re.fullmatch(r"[+-]?\d+", t.strip()))

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

def _score_summary_from_sign(tokens: List[str]) -> Optional[str]:
    g = _tokens_to_games_from_sign(tokens)
    return ", ".join(f"{a}-{b}" for a, b in g) if g else None


# ───────────────────────── Resolving helpers ─────────────────────────

def _name_keys_for_lookup(name: str) -> List[str]:
    clean = normalize_key(name)
    parts = clean.split()
    keys = []
    for i in range(1, len(parts)):
        ln = " ".join(parts[:i])
        fn = " ".join(parts[i:])
        keys.append(normalize_key(f"{fn} {ln}"))
    seen, out = set(), []
    for k in keys:
        if k not in seen:
            out.append(k)
            seen.add(k)
    return out

def _resolve_fast(name: str, club: Optional[str],
                  by_name_club: Dict[Tuple[str, int], int],
                  by_name_only: Dict[str, List[int]],
                  club_map: Dict[str, Club]) -> Optional[int]:
    keys = _name_keys_for_lookup(name)
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

def _score_summary(tokens: List[str]) -> Optional[str]:
    if not tokens:
        return None
    # keep set-like strings (e.g. "11-7") and time-like ("3:2") if they appear
    pieces = [t.replace(" ", "") for t in tokens if "-" in t or ":" in t]
    return ", ".join(pieces) if pieces else None    

# ───────────────────────── Main entrypoint ─────────────────────────

def upd_tournament_group_stage():
    conn, cur = get_conn()
    t0 = time.perf_counter()

    # Ensure GROUP stage exists, fail fast with a clear message if not
    group_sid = TournamentStage.id_by_code(cur, "GROUP")
    if group_sid is None:
        logging.error("Stage 'GROUP' not found in table stage. Seed the stage table first.")
        print("❌ Stage 'GROUP' not found. Seed the stage table first.")
        conn.close()
        return


    classes_by_ext = TournamentClass.cache_by_id_ext(cur)
    if SCRAPE_CLASS_PARTICIPANTS_CLASS_ID_EXT is not None:
        tc = classes_by_ext.get(SCRAPE_CLASS_PARTICIPANTS_CLASS_ID_EXT)
        classes = [tc] if tc else []
    else:
        classes = list(classes_by_ext.values())

    order = (SCRAPE_CLASS_PARTICIPANTS_ORDER or "").lower()
    if order == "newest":
        classes.sort(key=lambda tc: tc.date or datetime.date.min, reverse=True)
    elif order == "oldest":
        classes.sort(key=lambda tc: tc.date or datetime.date.min)

    if SCRAPE_CLASS_PARTICIPANTS_MAX_CLASSES and SCRAPE_CLASS_PARTICIPANTS_MAX_CLASSES > 0:
        classes = classes[:SCRAPE_CLASS_PARTICIPANTS_MAX_CLASSES]

    print(f"ℹ️  Updating tournament GROUP STAGE for {len(classes)} classes…")
    logging.info(f"Updating tournament GROUP STAGE for {len(classes)} classes")

    club_map = Club.cache_name_map(cur)
    part_by_class_fast = PlayerParticipant.cache_by_class_name_fast(cur)

    totals = {"groups": 0, "matches": 0, "kept": 0, "skipped": 0}
    db_results = []

    # Loop classes
    for idx, tc in enumerate(classes, 1):
        label = f"{tc.shortname or tc.class_description or tc.tournament_class_id} (ext:{tc.tournament_class_id_ext})"
        print(f"ℹ️  [{idx}/{len(classes)}] Class {label}  date={tc.date}")
        logging.info(f"[{idx}/{len(classes)}] {label}")

        class_fast = part_by_class_fast.get(tc.tournament_class_id, {})
        by_name_club = class_fast.get("by_name_club", {})
        by_name_only = class_fast.get("by_name_only", {})

        if not by_name_club and not by_name_only:
            db_results.append({"status": "skipped", "reason": f"No participants in class_id={tc.tournament_class_id}", "warnings": ""})
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
        print(f"✅ Parsed {g_cnt} pools / {m_cnt} matches for {tc.shortname or tc.class_short or tc.tournament_class_id} (ext={tc.tournament_class_id_ext})")

        kept = skipped = 0
        _ = TournamentStage.id_by_code(cur, "GROUP")

        logging.info(f"PDF URL: {url}")

        # GROUP stage guaranteed by pre-flight
        for g_idx, g in enumerate(groups, 1):
            pool_name = g["name"]
            logging.info(f"    [POOL] {pool_name} — {len(g['matches'])} matches")

            # 1) Upsert the pool row and get a real group_id
            group = TournamentGroup(
                group_id=None,
                tournament_class_id=tc.tournament_class_id,
                name=pool_name,
                sort_order=g_idx
            ).upsert(cur)
            group_id = group.group_id
            member_pids: set[int] = set()

            for i, m in enumerate(g["matches"], 1):
                p1_pid = _resolve_fast(m["p1"]["name"], m["p1"]["club"], by_name_club, by_name_only, club_map)
                p2_pid = _resolve_fast(m["p2"]["name"], m["p2"]["club"], by_name_club, by_name_only, club_map)

                if not p1_pid or not p2_pid:
                    skipped += 1
                    logging.warning(f"       SKIP [{i}] {m['p1']['name']} vs {m['p2']['name']} → unmatched participant(s). tokens={m['tokens']}")
                    continue

                kept += 1
                member_pids.update([p1_pid, p2_pid])
                logging.info(f"       KEEP [{i}] prtcp={p1_pid} vs prtcp_id2={p2_pid} tokens={m['tokens']}")

                best = _infer_best_of_from_sign(m['tokens'])
                games = _tokens_to_games_from_sign(m['tokens'])
                summary = _score_summary_from_sign(m['tokens'])

                logging.info(f"[GROUP] g={g_idx} i={i} tokens={m['tokens']} → best_of={best}, "
             f"games={games}, summary='{summary}'")

                # 2) Persist match + sides + games using YOUR model
                mx = Match(
                    match_id=None,
                    tournament_class_id=tc.tournament_class_id,
                    fixture_id=None,
                    stage_code="GROUP",
                    group_id=group_id,
                    best_of=best,
                    date=None,
                    score_summary=summary,
                    notes=None,
                )
                mx.add_side_participant(1, p1_pid)
                mx.add_side_participant(2, p2_pid)
                for no, (s1, s2) in enumerate(games, start=1):
                    mx.add_game(no, s1, s2)

                res = mx.save_to_db(cur)
                logging.info(f"[GROUP] Inserted match_id={res.get('match_id')} for g={g_idx} i={i}")
                if res.get("status") != "success":
                    logging.warning(f"       WARN saving match: {res}")
                    db_results.append({"status": "failed", "reason": res.get("reason", "unknown")})

                
                # DEBUG: After save_to_db()
                logging.info(f"[GROUP] Inserted match_id={res.get('match_id')} for g={g_idx} i={i}")
    

                # 3) Upsert pool members once per pool
                for pid in member_pids:
                    group.add_member(cur, pid)   

                # DEBUG: After adding pool members
                logging.info(f"[GROUP] Pool '{pool_name}' members added: {sorted(member_pids)} "
                            f"(count={len(member_pids)})")

 


        totals["kept"] += kept
        totals["skipped"] += skipped
        print(f"   ✅ Valid matches kept: {kept}   ⏭️  Skipped: {skipped}")
        conn.commit()

    elapsed = time.perf_counter() - t0
    print(f"ℹ️  Group stage parse complete in {elapsed:.2f}s")
    print(f"ℹ️  Totals — pools: {totals['groups']}, matches parsed: {totals['matches']}, kept: {totals['kept']}, skipped: {totals['skipped']}")
    print("")
    print_db_insert_results(db_results)
    print("")
    conn.close()
