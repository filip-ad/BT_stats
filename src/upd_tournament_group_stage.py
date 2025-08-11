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
            if not words: continue
            mid_x = (page.bbox[2] + page.bbox[0]) / 2.0
            rows: Dict[int, Dict[str, List[str]]] = {}
            rid, last_top = 0, None
            for w in sorted(words, key=lambda w: (round(w["top"], 1), w["x0"])):
                top = round(w["top"], 1)
                if last_top is None or abs(top - last_top) > 3.0:
                    rid += 1; last_top = top; rows[rid] = {"L": [], "R": []}
                (rows[rid]["L"] if w["x0"] < mid_x else rows[rid]["R"]).append(w["text"])
            for cols in rows.values():
                L = " ".join(cols["L"]).strip()
                R = " ".join(cols["R"]).strip()
                if L or R: out.append((L, R))
    return out

def _split_name_club(raw: str) -> Dict[str, Optional[str]]:
    m = _RE_NAME_CLUB.match(raw)
    name = (m.group("name") if m else raw).strip()
    club = (m.group("club") if m else None)
    return {"raw": raw, "name": name, "club": (club.strip() if club else None)}

def _tokenize_right(s: str) -> List[str]:
    if not s: return []
    return [t.replace(" ", "") for t in re.findall(r"\d+\s*-\s*\d+|[+-]?\d+|\d+\s*:\s*\d+", s)]

def _tokens_to_games(tokens: List[str]) -> List[Tuple[int,int]]:
    games: List[Tuple[int,int]] = []
    for t in tokens:
        if "-" in t:
            try:
                a,b = t.split("-",1)
                games.append((int(a), int(b)))
            except: pass
    return games

def _infer_best_of(tokens: List[str]) -> Optional[int]:
    sets = [t for t in tokens if "-" in t]
    return len(sets) if sets else (5 if tokens else None)

# ───────────────────────── Resolving helpers ─────────────────────────

def _resolve_participant_id_for_name(
    name: str,
    club: Optional[str],
    player_name_map: Dict[str, List[int]],
    unverified_name_map: Dict[str, int],
    class_part_by_player: Dict[int, Tuple[int,int]],
    club_map: Dict[str, int],   # not strictly used yet; placeholder for stricter club checks
) -> Optional[int]:
    """
    Resolve 'Name, Club' to a participant_id by:
      1) name map → candidate player_ids
      2) fallback to unverified fullname map (exact raw)
      3) ensure candidate player_id ∈ participants for this class (class_part_by_player)
    """
    # 1) verified names/aliases
    key = normalize_key(name)
    candidate_pids = list(player_name_map.get(key, []))

    # 2) fallback: unverified fullname (raw exact clean, NOT normalized-key)
    if not candidate_pids and name in unverified_name_map:
        candidate_pids = [unverified_name_map[name]]

    # 3) pick first candidate that is a participant in class
    for pid in candidate_pids:
        tup = class_part_by_player.get(pid)
        if not tup:
            continue
        participant_id, _club_id_in_class = tup
        # Optional: compare club to class club_id via club_map if you want strictness.
        return participant_id

    return None

# ───────────────────────── Main entrypoint ─────────────────────────

def upd_tournament_group_stage():
    conn, cur = get_conn()
    t0 = time.perf_counter()

    # classes via cache_by_id_ext
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

    # caches
    club_map            = Club.cache_name_map(cur)
    player_name_map     = Player.cache_name_map(cur)
    unverified_name_map = Player.cache_unverified_name_map(cur)
    part_by_class_player= PlayerParticipant.cache_by_class_id_player(cur)  # class_id → {player_id: (participant_id, club_id)}

    totals = {"groups":0, "matches":0, "kept":0, "skipped":0}
    db_results = []

    for idx, tc in enumerate(classes, 1):
        label = f"{tc.shortname or tc.class_description or tc.tournament_class_id} (ext:{tc.tournament_class_id_ext})"
        print(f"ℹ️  [{idx}/{len(classes)}] Class {label}  date={tc.date}")
        logging.info(f"[{idx}/{len(classes)}] {label}")

        if not tc or not tc.tournament_class_id_ext:
            db_results.append({"status":"skipped","reason":"Missing class or external class_id","warnings":""})
            continue

        class_part_by_player = part_by_class_player.get(tc.tournament_class_id, {})
        if not class_part_by_player:
            db_results.append({"status":"skipped","reason":f"No participants in class_id={tc.tournament_class_id}","warnings":""})
            continue

        # Optional: clear old pool data (still commented for now)
        # cleared = TournamentGroup.clear_for_class(cur, tc.tournament_class_id)
        # print(f"  ↳ Cleared: {cleared}")

        # Download PDF
        url = RESULTS_URL_TMPL.format(class_id=tc.tournament_class_id_ext)
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
        except Exception as e:
            reason = f"Download failed: {e}"
            print(f"❌ {reason}"); logging.error(reason)
            db_results.append({"status":"failed","reason":reason,"warnings":""})
            conn.commit()
            continue

        # Parse
        try:
            groups = _parse_groups_pdf(r.content)
        except Exception as e:
            reason = f"PDF parsing failed: {e}"
            print(f"❌ {reason}"); logging.error(reason)
            db_results.append({"status":"failed","reason":reason,"warnings":""})
            conn.commit()
            continue

        g_cnt = len(groups)
        m_cnt = sum(len(g["matches"]) for g in groups)
        totals["groups"]  += g_cnt
        totals["matches"] += m_cnt
        print(f"✅ Parsed {g_cnt} pools / {m_cnt} matches for {tc.shortname or tc.class_short or tc.tournament_class_id} (ext={tc.tournament_class_id_ext})")

        kept = skipped = 0

        # Stage id check (will be used by Match.save_to_db later)
        _ = TournamentStage.id_by_code(cur, "GROUP")  # ensures stage exists

        for g_idx, g in enumerate(groups, 1):
            pool_name = g["name"]
            logging.info(f"    [POOL] {pool_name} — {len(g['matches'])} matches")

            # Upsert group (commented for now)
            group = TournamentGroup(None, tc.tournament_class_id, pool_name, sort_order=g_idx)
            # group.upsert(cur)

            # Collect members set for optional insertion
            member_pids: set[int] = set()

            for i, m in enumerate(g["matches"], 1):
                p1_pid = _resolve_participant_id_for_name(
                    m["p1"]["name"], m["p1"]["club"], player_name_map, unverified_name_map,
                    class_part_by_player, club_map
                )
                p2_pid = _resolve_participant_id_for_name(
                    m["p2"]["name"], m["p2"]["club"], player_name_map, unverified_name_map,
                    class_part_by_player, club_map
                )

                if not p1_pid or not p2_pid:
                    skipped += 1
                    logging.warning(f"       SKIP [{i}] {m['p1']['name']} vs {m['p2']['name']} → unmatched participant(s). tokens={m['tokens']}")
                    continue

                kept += 1
                member_pids.update([p1_pid, p2_pid])
                logging.info(f"       KEEP [{i}] pid1={p1_pid} vs pid2={p2_pid} tokens={m['tokens']}")

                # Build match object (DB insert commented)
                mx = Match(
                    tournament_class_id=tc.tournament_class_id,
                    fixture_id=None,
                    stage_code="GROUP",
                    group_id=None,                 # set to group.group_id after upsert
                    best_of=_infer_best_of(m['tokens']),
                    date=None, score_summary=None, notes=None
                )
                mx.add_side_participant(1, p1_pid)
                mx.add_side_participant(2, p2_pid)
                for no, (s1, s2) in enumerate(_tokens_to_games(m['tokens']), start=1):
                    mx.add_game(no, s1, s2)

                # if group.group_id is not None:
                #     mx.group_id = group.group_id
                # res = mx.save_to_db(cur)
                # db_results.append(res)

            # Insert members (commented)
            # if group.group_id:
            #     for pid in member_pids:
            #         group.add_member(cur, pid, seed_in_group=None)

        totals["kept"]    += kept
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
