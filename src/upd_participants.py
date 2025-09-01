# src/upd_participants.py

import logging
import unicodedata
import requests
import random, time
import pdfplumber
import re
from io import BytesIO
from datetime import date
import time
from typing import List, Dict, Optional, Tuple
from db import get_conn
from utils import name_keys_for_lookup_all_splits, parse_date, OperationLogger, normalize_key
from config import (
    SCRAPE_PARTICIPANTS_CUTOFF_DATE,
    SCRAPE_PARTICIPANTS_CLASS_ID_EXTS, 
    SCRAPE_PARTICIPANTS_ORDER,
    SCRAPE_PARTICIPANTS_MAX_CLASSES,
    SCRAPE_PARTICIPANTS_TNMT_ID_EXTS
)
from models.tournament_class import TournamentClass
from models.club import Club
from models.player_license import PlayerLicense
from models.player import Player
from models.participant import Participant
from models.participant_player import ParticipantPlayer
import statistics as stats  # NEW: for robust font-size baselines


PDF_BASE = "https://resultat.ondata.se/ViewClassPDF.php"

def upd_participants():
    """
    Populate participant and participant_player by downloading each class's 'stage=1' PDF.
    Uses OperationsLogger for logging and summarization.
    """
    conn, cursor = get_conn()

    # Set up logging
    # =============================================================================
    logger = OperationLogger(
        verbosity       = 2, 
        print_output    = False, 
        log_to_db       = False, 
        cursor          = cursor
        )

    start_time = time.time()

    try:

        try:
            cutoff_date = parse_date(SCRAPE_PARTICIPANTS_CUTOFF_DATE)
        except ValueError as ve:
            logger.failed("global", f"Invalid cutoff date format: {ve}")
            print(f"❌ Invalid cutoff date format: {ve}")
            return

        classes = TournamentClass.get_filtered_classes(
            cursor,
            class_id_exts       = SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
            tournament_id_exts  = SCRAPE_PARTICIPANTS_TNMT_ID_EXTS,
            data_source_id      = 1 if SCRAPE_PARTICIPANTS_CLASS_ID_EXTS else None,  # Assume default 1
            cutoff_date         = cutoff_date,
            require_ended       = True, # Only include ended tournaments, status_id = 3
            allowed_type_ids    = [1],  # Singles only (type_id 1)
            max_classes         = SCRAPE_PARTICIPANTS_MAX_CLASSES,
            order               = SCRAPE_PARTICIPANTS_ORDER
        )

        if not classes:
            logger.skipped("global", "No valid singles classes matching filters")
            print("⚠️  No valid singles classes matching filters.")
            return

        print(f"ℹ️  Filtered to {len(classes)} valid singles classes{' via SCRAPE_PARTICIPANTS_CLASS_ID_EXTS (overriding cutoff)' if SCRAPE_PARTICIPANTS_CLASS_ID_EXTS else f' after cutoff date: {cutoff_date or "none"}'}.")

        logging.info(f"Scraping participants for {len(classes)} valid singles classes with cutoff date: {cutoff_date or 'none'}")
        print(f"ℹ️  Scraping participants for {len(classes)} valid singles classes, cutoff: {cutoff_date or 'none'}")

        # Build lookup caches
        club_map                    = Club.cache_name_map(cursor)
        license_name_club_map       = PlayerLicense.cache_name_club_map(cursor)
        player_name_map             = Player.cache_name_map_verified(cursor)
        player_unverified_name_map  = Player.cache_name_map_unverified(cursor)
        unverified_appearance_map   = Player.cache_unverified_appearances(cursor)

        partial_classes = 0
        partial_participants = 0

        for i, tc in enumerate(classes, 1):
            item_key = f"{tc.shortname} (id: {tc.tournament_class_id}, ext_id: {tc.tournament_class_id_ext})"

            try:
                pdf_bytes = download_pdf(f"{PDF_BASE}?classID={tc.tournament_class_id_ext}&stage=1")
                if not pdf_bytes:
                    logger.failed(item_key, "PDF download failed")
                    continue

                raw_participants, expected_count, seeded_count = parse_players_pdf(pdf_bytes, logger, item_key)
                found = len(raw_participants)
                if expected_count is not None and found != expected_count:
                    partial_classes += 1
                    partial_participants += abs(found - expected_count)
                if not raw_participants:
                    logger.skipped(item_key, "No participants parsed from PDF")
                    continue

                if expected_count is not None and len(raw_participants) != expected_count:
                    extended_key = f"{tc.shortname} in tournament {tc.tournament_id} (id: {tc.tournament_class_id}, ext_id: {tc.tournament_class_id_ext})"
                    logger.warning(extended_key, f"Could not parse all expected participants.")

                deleted = Participant.remove_for_class(cursor, tc.tournament_class_id)

                found = len(raw_participants)
                icon = "✅" if (expected_count is not None and expected_count == found) else ("❌ " if expected_count is not None else "❌ ")

                msg = (
                    f"{icon} [{i}/{len(classes)}] Parsed class {tc.shortname} {tc.date} "
                    f"(id: {tc.tournament_class_id}, ext_id: {tc.tournament_class_id_ext}, tid: {tc.tournament_id}). "
                    f"Expected {expected_count if expected_count is not None else '—'}, "
                    f"found {found}, seeded: {seeded_count}, deleted: {deleted} old participants."
                )

                print(msg)
                logging.info(msg)

                for raw in raw_participants:
                    fullname_raw = raw.get("raw_name")
                    clubname_raw = raw.get("club_name")
                    if not fullname_raw or not clubname_raw:
                        logger.error(item_key, "Missing name or club in raw data")
                        continue

                    seed = int(raw.get("seed")) if str(raw.get("seed")).isdigit() else None
                    t_ptcp_id_ext = raw.get("tournament_participant_id_ext")

                    club = Club.resolve(cursor, clubname_raw, club_map, logger, item_key, allow_prefix=True, fallback_to_unknown=True)
                    if club.club_id == 9999:
                         logger.warning(clubname_raw, f"Club not found. Using 'Unknown club (id: {9999})'")
                    if not club:
                        logger.failed(item_key, f"Club not found for '{clubname_raw}'")
                        continue
                    club_id = club.club_id

                    temp_participant = Participant(tournament_class_id=tc.tournament_class_id)
                    temp_participant.club_id = club_id

                    player_id, match_type = match_player(
                        cursor,
                        temp_participant,
                        fullname_raw,
                        clubname_raw,
                        tc.date,
                        license_name_club_map,
                        player_name_map,
                        player_unverified_name_map,
                        unverified_appearance_map,
                        logger,
                        item_key,
                        tc.tournament_class_id_ext
                    )
                    if player_id is None:
                        logger.failed(item_key, "No match for player")
                        continue

                    participant_data = {
                        "tournament_class_id": tc.tournament_class_id,
                        "tournament_class_seed": seed,
                        "tournament_class_final_position": None
                    }
                    participant = Participant.from_dict(participant_data)
                    val_res = participant.validate()
                    if val_res["status"] != "success":
                        logger.failed(item_key, f"Participant validation failed: {val_res['reason']}")
                        continue

                    ins_res = participant.insert(cursor)
                    if ins_res["status"] != "success":
                        logger.failed(item_key, f"Participant insert failed: {ins_res['reason']}")
                        continue

                    pp_data = {
                        "participant_player_id_ext": t_ptcp_id_ext,
                        "participant_id": participant.participant_id,
                        "player_id": player_id,
                        "club_id": club_id
                    }
                    participant_player = ParticipantPlayer.from_dict(pp_data)
                    val_res = participant_player.validate()
                    if val_res["status"] != "success":
                        logger.failed(item_key, f"ParticipantPlayer validation failed: {val_res['reason']}")
                        continue

                    ins_res = participant_player.insert(cursor)
                    if ins_res["status"] != "success":
                        logger.failed(item_key, f"ParticipantPlayer insert failed: {ins_res['reason']}")
                        continue

                    logger.success(item_key, f"ParticipantPlayer inserted successfully (match type: {match_type})")

                    if "unverified" in match_type:
                        status = Player.link_unverified_appearance(cursor, player_id, club_id, tc.date)
                        if status == "created":
                            logger.success(item_key, "Linked unverified player appearance")
                        # else:
                        #     logger.warning(item_key, "Skipped duplicate unverified appearance")


            except Exception as e:
                logger.failed(item_key, f"Exception during processing: {e}")
                logging.error(f"Exception processing {item_key}: {e}") # exc_info=True, stacklevel=2
                continue

        e = time.time() - start_time
        if e >= 60:
            mins = int(e // 60)
            secs = e % 60
            msg = f"ℹ️  Participants update completed in {mins} minute{'s' if mins > 1 else ''} and {secs:.2f} seconds."
        else:
            msg = f"ℹ️  Participants update completed in {e:.2f} seconds."
        print(msg)
        if partial_classes > 0:
            print(f"⚠️  Partially parsed classes: {partial_classes} (participants impacted: {partial_participants})")
            logging.warning(f"Partially parsed classes: {partial_classes}, participants impacted: {partial_participants}")
        else:
            print("✅ No partial parses detected.")
        logger.summarize()

    except Exception as e:
        logging.error(f"Error in upd_participants: {e}")
        print(f"❌ Error in upd_participants: {e}")

    finally:
        conn.commit()

def match_player(
    cursor,
    participant: Participant,
    fullname_raw: str,
    clubname_raw: str,
    class_date: date,
    license_name_club_map,
    player_name_map,
    player_unverified_name_map,
    unverified_appearance_map,
    logger: OperationLogger,
    item_key: str,
    tournament_class_id_ext: str,
) -> Tuple[Optional[int], Optional[str]]:
    strategies = [
        match_by_license_exact,
        match_by_license_substring,
        match_by_any_season_exact,
        match_by_any_season_substring,
        match_by_transition_exact,
        match_by_transition_substring,
        match_by_unverified_with_club
    ]
    warnings = []

    for strategy in strategies:
        outcome = strategy(
            cursor,
            fullname_raw,
            clubname_raw,
            class_date,
            license_name_club_map,
            player_name_map,
            participant.club_id,
            # warnings,
            logger,
            item_key,
            unverified_appearance_map if strategy == match_by_unverified_with_club else None
        )
        if outcome:
            pid, match_type = outcome
            if warnings:
                for w in warnings:
                    logger.warning(item_key, w)
            return pid, match_type

    pid = fallback_unverified(
        cursor,
        fullname_raw,
        clubname_raw,
        player_unverified_name_map,
        logger,
        item_key
    )
    if pid:
        extended_key = f"[cid/cid_ext] {participant.tournament_class_id}/{tournament_class_id_ext} [player_id] {pid} [fullname_raw] {fullname_raw} [club] {clubname_raw}"
        logger.warning(extended_key, "Matched with unverified player as fallback")
        return pid, "unverified"
    return None, None

def match_by_license_exact(
    cursor,
    fullname_raw: str,
    clubname_raw: str,
    class_date: date,
    license_name_club_map,
    player_name_map,
    club_id: int,
    # warnings: List[str],
    logger: OperationLogger,
    item_key: str,
    _
) -> Optional[Tuple[int, str]]:
    keys = name_keys_for_lookup_all_splits(fullname_raw)
    candidates = set()
    for k in keys:
        key = (k, club_id)
        if key in license_name_club_map:
            for lic in license_name_club_map[key]:
                if lic["valid_from"] <= class_date <= lic["valid_to"]:
                    candidates.add(lic["player_id"])
    if len(candidates) == 1:
        return list(candidates)[0], "license_exact"
    return None

def match_by_license_substring(
    cursor,
    fullname_raw: str,
    clubname_raw: str,
    class_date: date,
    license_name_club_map,
    player_name_map,
    club_id: int,
    # warnings: List[str],
    logger: OperationLogger,
    item_key: str,
    _
) -> Optional[Tuple[int, str]]:
    clean = normalize_key(fullname_raw)
    parts = clean.split()
    if len(parts) > 2:
        return None
    first_tok, last_tok = parts[0], parts[-1]
    candidates = set()
    for (full_key, cid), rows in license_name_club_map.items():
        if cid != club_id:
            continue
        if len(full_key.split()) < 3:
            continue
        if first_tok in full_key and last_tok in full_key:
            for row in rows:
                if row["valid_from"] <= class_date <= row["valid_to"]:
                    candidates.add(row["player_id"])
    if len(candidates) == 1:
        return list(candidates)[0], "license_substring"
    return None

def match_by_any_season_exact(
    cursor,
    fullname_raw: str,
    clubname_raw: str,
    class_date: date,
    license_name_club_map,
    player_name_map,
    club_id: int,
    # warnings: List[str],
    logger: OperationLogger,
    item_key: str,
    _
) -> Optional[Tuple[int, str]]:
    keys = name_keys_for_lookup_all_splits(fullname_raw)
    candidates = set()
    for k in keys:
        key = (k, club_id)
        if key in license_name_club_map:
            for lic in license_name_club_map[key]:
                candidates.add(lic["player_id"])
    if len(candidates) == 1:
        # warnings.append("Matched by name with license in club, but not necessarily valid on class date")
        logger.warning(fullname_raw + " " + clubname_raw + " " + str(class_date) + " " + item_key, "Matched by name with license in club, but not necessarily valid on class date")
        return list(candidates)[0], "any_season_exact"
    return None

def match_by_any_season_substring(
    cursor,
    fullname_raw: str,
    clubname_raw: str,
    class_date: date,
    license_name_club_map,
    player_name_map,
    club_id: int,
    # warnings: List[str],
    logger: OperationLogger,
    item_key: str,
    _
) -> Optional[Tuple[int, str]]:
    clean = normalize_key(fullname_raw)
    parts = clean.split()
    if len(parts) > 2:
        return None
    first_tok, last_tok = parts[0], parts[-1]
    candidates = set()
    for (full_key, cid), rows in license_name_club_map.items():
        if cid != club_id:
            continue
        if len(full_key.split()) < 3:
            continue
        if first_tok in full_key and last_tok in full_key:
            for row in rows:
                candidates.add(row["player_id"])
    if len(candidates) == 1:
        # warnings.append("Matched by substring with license in club, but not necessarily valid on class date")
        logger.warning(item_key, "Matched by substring with license in club, but not necessarily valid on class date")
        return list(candidates)[0], "any_season_substring"
    return None

def match_by_transition_exact(
    cursor,
    fullname_raw: str,
    clubname_raw: str,
    class_date: date,
    license_name_club_map,
    player_name_map,
    club_id: int,
    # warnings: List[str],
    logger: OperationLogger,
    item_key: str,
    _
) -> Optional[Tuple[int, str]]:
    pids = get_name_candidates(fullname_raw, player_name_map)
    if not pids:
        return None
    placeholders = ",".join("?" for _ in pids)
    sql = f"""
        SELECT DISTINCT player_id FROM player_transition
        WHERE (club_id_to = ? OR club_id_from = ?)
        AND transition_date <= ?
        AND player_id IN ({placeholders})
    """
    params = [club_id, club_id, class_date] + pids
    try:
        cursor.execute(sql, params)
    except Exception as e:
        logging.error(f"Error executing SQL for transition_exact: {e}")
        return None
    trans = [r[0] for r in cursor.fetchall()]
    if len(trans) == 1:
        return trans[0], "transition_exact"
    return None

def match_by_transition_substring(
    cursor,
    fullname_raw: str,
    clubname_raw: str,
    class_date: date,
    license_name_club_map,
    player_name_map,
    club_id: int,
    # warnings: List[str],
    logger: OperationLogger,
    item_key: str,
    _
) -> Optional[Tuple[int, str]]:
    clean = normalize_key(fullname_raw)
    parts = clean.split()
    if len(parts) > 2:
        return None
    first_tok, last_tok = parts[0], parts[-1]
    sub_pids = set()
    for (full_key, cid), rows in license_name_club_map.items():
        if cid != club_id:
            continue
        if len(full_key.split()) < 3:
            continue
        if first_tok in full_key and last_tok in full_key:
            for row in rows:
                sub_pids.add(row["player_id"])
    if not sub_pids:
        return None
    placeholders = ",".join("?" for _ in sub_pids)
    sql = f"""
        SELECT DISTINCT player_id FROM player_transition
        WHERE (club_id_to = ? OR club_id_from = ?)
        AND transition_date <= ?
        AND player_id IN ({placeholders})
    """
    params = [club_id, club_id, class_date] + list(sub_pids)
    cursor.execute(sql, params)
    trans = [r[0] for r in cursor.fetchall()]
    if len(trans) == 1:
        return trans[0], "transition_substring"
    return None

def match_by_unverified_with_club(
    cursor,
    fullname_raw: str,
    clubname_raw: str,
    class_date: date,
    license_name_club_map,
    player_name_map,
    club_id: int,
    # warnings: List[str],
    logger: OperationLogger,
    item_key: str,
    unverified_appearance_map
) -> Optional[Tuple[int, str]]:
    clean = normalize_key(fullname_raw)
    if clean in unverified_appearance_map:
        for entry in unverified_appearance_map[clean]:
            if entry["club_id"] == club_id:
                return entry["player_id"], "unverified player with club"
    return None

def fallback_unverified(
    cursor, 
    fullname_raw: str, 
    clubname_raw: str,
    player_unverified_name_map: Dict[str, int], 
    logger: OperationLogger, 
    item_key: str
) -> Optional[int]:
    clean = normalize_key(fullname_raw)
    existing = player_unverified_name_map.get(clean)
    if existing is not None:
        return existing

    res = Player.insert_unverified(cursor, fullname_raw)
    if res["status"] in ("created", "reused") and res["player_id"]:
        player_unverified_name_map[clean] = res["player_id"]
        if res["status"] == "created":
            logger.warning(item_key, "Created new unverified player")
        else:
            logger.warning(item_key, "Reused existing unverified player")
        return res["player_id"]

    logger.failed(item_key, f"Failed to insert/reuse unverified player for {fullname_raw}")
    return None

def get_name_candidates(
    fullname_raw: str, 
    player_name_map: Dict[str, List[int]]
) -> List[int]:
    keys = name_keys_for_lookup_all_splits(fullname_raw)
    matches = set()
    for k in keys:
        matches.update(player_name_map.get(k, []))
    return list(matches)

def download_pdf(pdf_url, retries=3, timeout=30):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; BTstats/1.0)"}
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(pdf_url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except requests.exceptions.Timeout:
            delay = 2 ** attempt + random.uniform(0, 1)
            logging.warning(f"Timeout {attempt}/{retries}, retrying in {delay:.1f}s")
            time.sleep(delay)
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed ({pdf_url}): {e}")
            break
    return None

def parse_players_pdf(
        pdf_bytes: bytes, 
        logger: OperationLogger, 
        item_key: str
    ) -> tuple[list[dict], int | None, int | None]:
    """
    Extract participant entries from PDF.
    Returns: (participants, expected_count, seed_counter)
    where each participant is {"raw_name": str, "club_name": str, "seed": Optional[int], "tournament_participant_id_ext": Optional[str]}.
    """
    participants: list[dict] = []
    unique_entries: set[tuple[str, str]] = set()
    seed_counter = 1

    full_text_blocks: list[str] = []
    bold_name_keys: set[str] = set()
    expected_count: int | None = None

    # # Regexes to parse
    # EXPECTED_PARTICIPANT_COUNT_RE = re.compile(
    #     r'\b\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2})?\s*\(\s*(\d+)\s+[^\d()]+\)',re.I,)
    EXPECTED_PARTICIPANT_COUNT_RE = re.compile(
    r'(?:\b\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2})?\s*)?\(\s*(\d+)\s+[^\d()]+\)', re.I
    )
    # 1) With leading TPID
    PART_WITH_ID_RE = re.compile(
        r'^\s*(?P<tpid>\d{1,5})\s+(?P<name>[^,]+?)\s*,\s*(?P<club>\S.*\S)\s*$', re.M)
    # 2) No TPID: "Name, Club"
    PART_NO_ID_RE = re.compile(r'^\s*(?P<name>[^,]+?)\s*,\s*(?P<club>\S.*\S)\s*$', re.M)
    
    # 3) Fallback: (optional TPID) Name  ␠␠ Club   (two or more spaces)
    PART_WIDE_SPACES_RE = re.compile(r'^\s*(?:(?P<tpid>\d{1,5})\s+)?(?P<name>[^\s,].*?[^\s,])\s{2,}(?P<club>.+\S)\s*$', re.M)
    
    # 4) Seeds
    BOLD_RE             = re.compile(r"(bold|black|heavy|demi|semibold|semi-bold|sb)\b", re.I)

    # Regexes to cut
    TITLE_RE            = re.compile(r'Deltagarlista\s*', re.I | re.M)  # Strip title line
    CLASS_HEADER_RE     = re.compile(r'^[A-Z],\s*Klass\s*\d+\s*$', re.M | re.I)  # Strip repeated class headers like "H,Klass 5"
    FOOTER_ANY_LINE_RE  = re.compile(r'(?i)(tt\s*coordinator|coordinator\.com|programlicens|'r'tävlingen\s+genomförs|användas\s+vid\s+tävlingar|arrangerade\s+av)')
    # For detecting header/category lines
    CATEGORY_TOKENS     = {"vet", "veteran", "junior", "pojkar", "flickor", "herrar", "damer", "klass", "class"}   
    # For identifying invalid players, and decreasing expected players count
    PLACEHOLDER_NAME_RE = re.compile(r"\b(vakant|vacant|reserv|reserve)\b", re.I)

    # --- Exclusion guard: player-name blacklist (extend as needed) ---
    EXCLUDED_NAME_PATTERNS = [
        r"^kval\.\s*top\s*12\b.*åkirke?by",   # matches "Kval. Top 12 - 2017 i Åkirkeby" (Åkirkeby / Akirkeby)
        r"^kval\b",                           # any generic "Kval..." lines
        # r"^grupp\s*[a-z0-9]+$",             # example: "Grupp A", if needed
    ]
    EXCLUDED_NAME_RES = [re.compile(p, re.I) for p in EXCLUDED_NAME_PATTERNS]   



    def _strip_above_and_header(block: str) -> tuple[str, int | None]:
        m = EXPECTED_PARTICIPANT_COUNT_RE.search(block)
        if m:
            end_of_header_line = block.find('\n', m.end())
            if end_of_header_line == -1:
                end_of_header_line = len(block)
            return block[end_of_header_line + 1:], int(m.group(1))
        return block, None

    pdf = pdfplumber.open(BytesIO(pdf_bytes))
    try:
        for page in pdf.pages:
            w, h = page.width, page.height
            
            # Extract words early
            words = page.extract_words(
                use_text_flow=True, keep_blank_chars=False, extra_attrs=["fontname"]
            )
            if not words: continue
            
            # Split words into left and right columns based on center x
            left_words = [wd for wd in words if (wd['x0'] + wd['x1']) / 2 < w / 2]
            right_words = [wd for wd in words if (wd['x0'] + wd['x1']) / 2 >= w / 2]
            
            # Cluster into lines separately for left and right
            def cluster_lines(column_words):
                lines = {}
                tol = 2
                for wd in column_words:
                    t = int(round(wd["top"]))
                    key = next((k for k in lines if abs(k - t) <= tol), t)
                    lines.setdefault(key, []).append(wd)
                return lines
            
            left_lines = cluster_lines(left_words)
            right_lines = cluster_lines(right_words)
            
            # Find header bottom for left
            header_bottom_left = None
            cnt_left = None
            for key in sorted(left_lines.keys()):
                line_words = left_lines[key]
                line_text = ' '.join(wd['text'] for wd in line_words)
                m = EXPECTED_PARTICIPANT_COUNT_RE.search(line_text)
                if m:
                    header_bottom_left = max(wd['bottom'] for wd in line_words)
                    cnt_left = int(m.group(1))
                    break
            
            # Find header bottom for right (unlikely, but for completeness)
            header_bottom_right = None
            cnt_right = None
            for key in sorted(right_lines.keys()):
                line_words = right_lines[key]
                line_text = ' '.join(wd['text'] for wd in line_words)
                m = EXPECTED_PARTICIPANT_COUNT_RE.search(line_text)
                if m:
                    header_bottom_right = max(wd['bottom'] for wd in line_words)
                    cnt_right = int(m.group(1))
                    break
            
            # Set global expected_count if found (prefer left, as header is usually there)
            if expected_count is None:
                if cnt_left is not None:
                    expected_count = cnt_left
                elif cnt_right is not None:
                    expected_count = cnt_right
            
            # Dynamic TOP per column
            TOP_left = (header_bottom_left + 5) if header_bottom_left else 50
            TOP_right = (header_bottom_right + 5) if header_bottom_right else 50
            
            BOTTOM = 50
            
            left = page.crop((0, TOP_left, w/2, h - BOTTOM)).extract_text() or ""
            right = page.crop((w/2, TOP_right, w, h - BOTTOM)).extract_text() or ""
            
            for raw_block in (left, right):
                # Clean block
                block = re.sub(r'Directly qualified.*$', "", raw_block, flags=re.M)
                block = re.sub(r'Group stage.*$', "", block, flags=re.M)
                block = TITLE_RE.sub('', block)
                block = CLASS_HEADER_RE.sub('', block)
                block = FOOTER_ANY_LINE_RE.sub('', block)
                
                # Strip header if still present (fallback)
                block, cnt = _strip_above_and_header(block)
                
                # # Debug
                # print(block)
                
                # Update count if not set yet (though now set from line detection)
                if expected_count is None and cnt is not None:
                    expected_count = cnt
                if block:
                    full_text_blocks.append(block)
            
            # ---- bold name detection (using all lines from both columns) ----
            all_lines = {**left_lines, **right_lines}  # Merge, but since keys may overlap, use all words instead
            # Actually, reuse original lines clustering on all words for bold
            lines: dict[int, list[dict]] = {}
            tol = 2
            for wd in words:
                t = int(round(wd["top"]))
                key = next((k for k in lines if abs(k - t) <= tol), t)
                lines.setdefault(key, []).append(wd)
            
            for _, line_words in lines.items():
                # reconstruct "name before comma"
                name_tokens = []
                for wd in line_words:
                    txt = wd["text"]
                    if "," in txt:
                        before = txt.split(",", 1)[0].strip()
                        if before:
                            name_tokens.append(before)
                        break
                    else:
                        name_tokens.append(txt)
                if not name_tokens: continue
                
                # drop leading index like "12"
                if name_tokens and re.fullmatch(r"\d+", name_tokens[0]):
                    name_tokens = name_tokens[1:]
                
                name_candidate = " ".join(name_tokens).strip()
                
                # must contain letters to be a plausible name
                if not re.search(r"[A-Za-zÅÄÖåäöØøÆæÉéÈèÜüß]", name_candidate):
                    continue
                
                any_bold = any(
                    (BOLD_RE.search((wd.get("fontname") or "")) is not None)
                    for wd in line_words[: max(1, len(name_tokens))]
                )
                if any_bold:
                    bold_name_keys.add(normalize_key(name_candidate))
    finally:
        pdf.close()

    def _try_match(line: str):
        # Try in order: with ID, no ID, wide-spaces fallback
        for rx in (PART_WITH_ID_RE, PART_NO_ID_RE, PART_WIDE_SPACES_RE):
            m = rx.match(line)
            if m:
                return m
        return None

    placeholder_skips = 0
    for block in full_text_blocks:
        for raw_line in block.splitlines():
            line = raw_line.strip()
            if not line:
                continue

            m = _try_match(line)
            if not m:
                continue

            tpid                            = m.groupdict().get('tpid')
            tournament_participant_id_ext   = (tpid.strip() if tpid else None)  # keep None if missing
            raw_name                        = m.group('name').strip()
            club_name                       = m.group('club').strip()

            # --- EXCLUSION GUARD (player-name blacklist) ---
            if any(rx.search(raw_name) for rx in EXCLUDED_NAME_RES):
                logger.warning(item_key, f"Skipping excluded name pattern")
                continue


            # Skip category tokens
            if club_name and club_name.lower() in CATEGORY_TOKENS:
                logger.failed(item_key, f"Skipping participant with 'klass' or 'class' in club name")
                continue

            # Skip obvious category words accidentally captured as a "club"
            if raw_name.lower() in CATEGORY_TOKENS:
                logger.failed(item_key, f"Skipping participant with 'klass' or 'class' in name")
                continue
                
            # Club can't be just a number    
            if re.match(r'^\d+$', club_name):  
                logger.warning(item_key, f"Skipping participant with purely numeric club name")
                continue

            # # Check if names and club names are too short
            # if len(raw_name) < 3 or len(club_name) < 3:  # Too short to be valid
            #     logger.failed(item_key, f"Skipping participant with name or club name < 3 characters")
            #     print(item_key, f"Skipping participant with short field: raw_name='{raw_name}', club='{club_name}'")
            #     continue

            if len(raw_name) < 3:
                logger.failed(item_key, f"Skipping participant with too short name: '{raw_name}' (club='{club_name}')")
                continue

            # --- validate club_name (looser, allow short clubs like OB, AIK)
            if not any(unicodedata.category(ch).startswith("L") for ch in club_name):
                logger.failed(item_key, f"Skipping participant with suspicious club: '{club_name}' (name='{raw_name}')")
                continue

            # NEW: skip placeholder entries like "vakant vakant"
            if PLACEHOLDER_NAME_RE.search(raw_name):
                placeholder_skips += 1
                continue

            key = (normalize_key(raw_name), normalize_key(club_name))
            if key in unique_entries:
                continue
            unique_entries.add(key)

            is_seeded = normalize_key(raw_name) in bold_name_keys
            seed_val = seed_counter if is_seeded else None
            if is_seeded:
                seed_counter += 1

            participants.append({
                "raw_name":                      raw_name,
                "club_name":                     club_name,
                "seed":                          seed_val,
                "tournament_participant_id_ext": tournament_participant_id_ext
            })

    if placeholder_skips > 0:
        logger.warning(item_key, f"Participants skipped due to invalid placeholder names")

    effective_expected = (
        (expected_count - placeholder_skips) if expected_count is not None else None
    )

    return participants, effective_expected, seed_counter-1




