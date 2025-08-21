# src/upd_participants.py

import logging
import requests
import pdfplumber
import re
from io import BytesIO
from datetime import date, datetime
import time
from typing import Any, List, Dict, Optional, Tuple
from db import get_conn
from utils import parse_date, OperationLogger, normalize_key
from config import SCRAPE_TOURNAMENTS_CUTOFF_DATE, SCRAPE_PARTICIPANTS_CLASS_ID_EXTS, SCRAPE_PARTICIPANTS_ORDER
from models.tournament_class import TournamentClass
from models.club import Club
from models.player_license import PlayerLicense
from models.player import Player
from models.participant import Participant
from models.participant_player import ParticipantPlayer

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
        log_to_db       = True, 
        cursor          = cursor
        )

    start_time = time.time()

    try:
        try:
            cutoff_date = parse_date(SCRAPE_TOURNAMENTS_CUTOFF_DATE)
        except ValueError as ve:
            logger.failed("global", f"Invalid cutoff date format: {ve}")
            print(f"❌ Invalid cutoff date format: {ve}")
            return

        # Fetch classes (singles only for now, filtered by cutoff or by list of class id ext)
        # Also checks tournament.is_valid = 1 making sure the class structure is known so we know how to parse
        # =============================================================================
        if SCRAPE_PARTICIPANTS_CLASS_ID_EXTS != 0:
            classes = TournamentClass.get_by_ext_ids(cursor, SCRAPE_PARTICIPANTS_CLASS_ID_EXTS)
            print(f"ℹ️  Filtered to {len(classes)} specific classes via SCRAPE_CLASS_PARTICIPANTS_CLASS_ID_EXT (overriding cutoff).")
        else:
            classes = TournamentClass.get_valid_singles_after_cutoff(cursor, cutoff_date)
            if not classes:
                logger.skipped("global", "No singles classes after cutoff date")
                print("⚠️  No singles classes after cutoff date.")
                return
            
        order = (SCRAPE_PARTICIPANTS_ORDER or "").lower()
        if order == "newest":
            classes.sort(key=lambda tc: tc.date or datetime.date.min, reverse=True)
        elif order == "oldest":
            classes.sort(key=lambda tc: tc.date or datetime.date.min)
            
        logging.info(f"Scraping participants for {len(classes)} valid only singles classes with cutoff date: {cutoff_date}")
        print(f"ℹ️  Scraping participants for {len(classes)} valid only singles classes, cutoff: {cutoff_date}")

        # Build lookup caches exactly once
        club_map                    = Club.cache_name_map(cursor)
        license_name_club_map       = PlayerLicense.cache_name_club_map(cursor)
        player_name_map             = Player.cache_name_map(cursor)
        player_unverified_name_map  = Player.cache_unverified_name_map(cursor)

        for i, tc in enumerate(classes, 1):
            item_key = f"{tc.shortname} (id: {tc.tournament_class_id}, ext_id: {tc.tournament_class_id_ext})"

            try:
                # Scrape (download PDF)
                # =============================================================================
                pdf_bytes = download_pdf(f"{PDF_BASE}?classID={tc.tournament_class_id_ext}&stage=1")
                if not pdf_bytes:
                    logger.failed(item_key, "PDF download failed")
                    continue

                # Parse PDF
                # =============================================================================
                raw_participants, expected_count, seeded_count = parse_players_pdf(
                    pdf_bytes, 
                    logger, 
                    item_key
                )
                if not raw_participants:
                    logger.skipped(item_key, "No participants parsed from PDF")
                    continue

                if expected_count is not None and len(raw_participants) != expected_count:
                    item_key = f"{tc.shortname} in tournament {tc.tournament_id} (id: {tc.tournament_class_id}, ext_id: {tc.tournament_class_id_ext})"
                    logger.warning(item_key, f"Could not parse all expected participants.")

                # Wipe old participants for this class - will also delete ParticipantPlayer entries due to DELETE ON CASCADE
                deleted = Participant.remove_for_class(
                    cursor, 
                    tc.tournament_class_id
                )

                found = len(raw_participants)
                icon = "✅" if (expected_count is not None and expected_count == found) else ("❌ " if expected_count is not None else "❌ ")

                msg = (
                    f"{icon} [{i}/{len(classes)}] Parsed class {tc.shortname} {tc.date}"
                    f"(id: {tc.tournament_class_id}, ext_id: {tc.tournament_class_id_ext}, tid: {tc.tournament_id}). "
                    f"Expected {expected_count if expected_count is not None else '—'}, "
                    f"found {found}, seeded: {seeded_count}, deleted: {deleted} old participants."
                )

                print(msg)
                logging.info(msg)


                for raw in raw_participants:
                    # Parse raw to structured and match player
                    # =============================================================================

                    # REVIEW MATCHING STRATEGIES!!! Do we look for license first?!

                    
                    parsed_data, matched_type = parse_raw_participant(
                        cursor, 
                        raw, 
                        tc.tournament_class_id,
                        tc.tournament_class_id_ext,
                        tc.date, 
                        club_map, 
                        license_name_club_map, 
                        player_name_map, 
                        player_unverified_name_map, 
                        logger, 
                        item_key
                    )
                    if parsed_data is None:
                        continue

                    # Create and insert Participant
                    # =============================================================================
                    participant = Participant.from_dict(parsed_data["participant"])
                    val_res = participant.validate()
                    if val_res["status"] != "success":
                        logger.failed(item_key, f"Participant validation failed: {val_res['reason']}")
                        continue

                    ins_res = participant.insert(cursor)
                    if ins_res["status"] != "success":
                        logger.failed(item_key, f"Participant insert failed: {ins_res['reason']}")
                        continue

                    # Create and insert ParticipantPlayer
                    # =============================================================================
                    participant_player = ParticipantPlayer.from_dict(parsed_data["participant_player"])
                    participant_player.participant_id = participant.participant_id
                    val_res = participant_player.validate()
                    if val_res["status"] != "success":
                        logger.failed(item_key, f"Participating player validation failed: {val_res['reason']}")
                        continue

                    ins_res = participant_player.insert(cursor)
                    if ins_res["status"] != "success":
                        logger.failed(item_key, f"Participating player insert failed: {ins_res['reason']}")
                        continue
                    logger.success(item_key, f"Participating player inserted successfully (match type: {matched_type})")


            except Exception as e:
                logger.failed(item_key, f"Exception during processing: {e}")
                continue

        print(f"ℹ️  Participants update completed in {(e:=time.time()-start_time)//60} minute{'s' if (m:=int(e//60))>1 else ''} and {e%60:.2f} seconds." if (e:=time.time()-start_time)>=60 else f"ℹ️  Participants update completed in {e:.2f} seconds.")
        logger.summarize()

    except Exception as e:
        logging.error(f"Error in upd_participants: {e}")
        print(f"❌ Error in upd_participants: {e}")

    finally:
        conn.commit()
        conn.close()

def download_pdf(pdf_url: str, retries: int = 3, timeout: int = 30) -> Optional[bytes]:
    """
    Download a PDF with retry logic.
    """
    headers = {"User-Agent": "Mozilla/5.0 (compatible; BTstats/1.0)"}
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(pdf_url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except requests.exceptions.Timeout:
            logging.warning(f"Timeout fetching {pdf_url} (attempt {attempt}/{retries})")
            time.sleep(2)
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

    # def _strip_above_and_header(block: str) -> tuple[str, int | None]:
    #     m = EXPECTED_PARTICIPANT_COUNT_RE.search(block)
    #     if m:
    #         end_of_header_line = block.find('\n', m.end())
    #         if end_of_header_line == -1:
    #             end_of_header_line = len(block)
    #         # logging.info(f"\nStripped block: \n{block}")
    #         return block[end_of_header_line + 1:], int(m.group(1))
    #     # logging.info(f"\nUnstripped block: \n{block}")
    #     return block, None

    # pdf = pdfplumber.open(BytesIO(pdf_bytes))
    # try:
    #     for page in pdf.pages:

    #         # ---- text blocks (handle two columns) ----
    #         w, h = page.width, page.height
    #         TOP         = 100
    #         BOTTOM      = 50
    #         left  = page.crop((0,     TOP, w/2,   h - BOTTOM)).extract_text() or ""
    #         right = page.crop((w/2,   TOP, w,     h - BOTTOM)).extract_text() or ""
            
    #         for raw_block in (left, right):
               
    #             # remove sections you already cut elsewhere
    #             block = re.sub              (r'Directly qualified.*$', "", raw_block, flags=re.M)
    #             block = re.sub              (r'Group stage.*$',        "", block, flags=re.M)
    #             block = TITLE_RE.sub('', block)
    #             block = CLASS_HEADER_RE.sub('', block)
    #             block = FOOTER_ANY_LINE_RE.sub('', block)

    #             # cut everything above the per-block header (kills titles)
    #             block, cnt = _strip_above_and_header(block)

    #             # Debug placeholder
    #             print(block)

    #             if expected_count is None and cnt is not None:
    #                 expected_count = cnt
    #             if block:
    #                 full_text_blocks.append(block)

    #         # ---- bold name detection for seeding ----
    #         words = page.extract_words(
    #             use_text_flow=True,
    #             keep_blank_chars=False,
    #             extra_attrs=["fontname"]
    #         )
    #         if not words:
    #             continue

    #         # cluster tokens by line (y top within tolerance)
    #         lines: dict[int, list[dict]] = {}
    #         tol = 2
    #         for w in words:
    #             t = int(round(w["top"]))
    #             key = next((k for k in lines.keys() if abs(k - t) <= tol), t)
    #             lines.setdefault(key, []).append(w)

    #         for _, line_words in lines.items():
    #             # reconstruct "name before comma"
    #             name_tokens = []
    #             for w in line_words:
    #                 txt = w["text"]
    #                 if "," in txt:
    #                     before = txt.split(",", 1)[0].strip()
    #                     if before:
    #                         name_tokens.append(before)
    #                     break
    #                 else:
    #                     name_tokens.append(txt)
    #             if not name_tokens:
    #                 continue

    #             # drop leading index like "12"
    #             if name_tokens and re.fullmatch(r"\d+", name_tokens[0]):
    #                 name_tokens = name_tokens[1:]

    #             name_candidate = " ".join(name_tokens).strip()

    #             # must contain letters to be a plausible name
    #             if not re.search(r"[A-Za-zÅÄÖåäöØøÆæÉéÈèÜüß]", name_candidate):
    #                 continue

    #             any_bold = any(
    #                 (BOLD_RE.search((w.get("fontname") or "")) is not None)
    #                 for w in line_words[: max(1, len(name_tokens))]
    #             )
    #             if any_bold:
    #                 bold_name_keys.add(normalize_key(name_candidate))
    # finally:
    #     pdf.close()

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
                logger.failed(item_key, f"Skipping participant with purely numeric club name")
                continue

            # Check if names and club names are too short
            if len(raw_name) < 3 or len(club_name) < 3:  # Too short to be valid
                logger.failed(item_key, f"Skipping participant with name or club name < 3 characters")
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

def find_club(
        cursor,
        clubname_raw: str, 
        club_map: Dict[str, Club], 
        logger: OperationLogger, 
        item_key: str
    ) -> Optional[Club]:
    norm = normalize_key(clubname_raw)
    club = club_map.get(norm)
    if not club and len(norm) >= 5:
        prefix_keys = [k for k in club_map if k.startswith(norm)]
        if len(prefix_keys) == 1:
            club = club_map[prefix_keys[0]]
            logger.warning(item_key, "Club name matched by prefix")
            # Log aliases if needed
    if not club:
        original_item_key = item_key  # Save original for logging to file (assuming it's player/context)
        club = Club.get_by_id(cursor, 9999) # Unknown club
        item_key = clubname_raw
        logger.warning(item_key, f"Club not found. Using 'Unknown club (id: 9999)'")
        with open('missing_clubs.txt', 'a') as f:
            f.write(f"Player/Context: {original_item_key}, Club Raw: {clubname_raw}\n")
    return club

def match_player(
        cursor, 
        participant: Participant, 
        fullname_raw: str, 
        clubname_raw: str,
        class_date: date, 
        license_name_club_map, 
        player_name_map, 
        player_unverified_name_map, 
        logger: OperationLogger, 
        item_key: str,
        tournament_class_id_ext: str,
    ) -> Tuple[Optional[int], Optional[str]]:
    strategies = [
        match_by_name,
        match_by_name_with_license,
        match_by_transition,
        match_by_any_season_license,
        match_by_name_substring_license
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
            warnings
        )
        if outcome:
            pid, match_type = outcome
            if warnings:
                for w in warnings:
                    logger.warning(item_key, w)
            return pid, match_type

    # Fallback to unverified (after all strategies)
    pid = fallback_unverified(
        cursor, 
        fullname_raw, 
        clubname_raw,
        player_unverified_name_map, 
        logger, 
        item_key
    )

    if not fullname_raw:
        return None
    clean = " ".join(fullname_raw.strip().split())
    
    if pid:
        item_key = (f"[cid/cid_ext] {participant.tournament_class_id}/{tournament_class_id_ext} [player_id] {pid} [fullname_raw] {clean} [club] {clubname_raw}")
        logger.warning(item_key, "Fallback to unverified player")
        return pid, "unverified"
    return None, None

def match_by_name(
        cursor, 
        fullname_raw: str, 
        clubname_raw: str,
        class_date: date, 
        license_name_club_map, 
        player_name_map, 
        club_id: int,
        warnings: List[str]
    ) -> Optional[Tuple[int, str]]:
    pids = get_name_candidates(fullname_raw, player_name_map)
    if len(pids) != 1:
        return None
    pid = pids[0]

    valid = False
    for (fn, ln, cid), rows in license_name_club_map.items():
        if cid != club_id:
            continue
        for lic in rows:
            if lic["player_id"] != pid:
                continue
            if lic["valid_from"] <= class_date <= lic["valid_to"]:
                valid = True
                break
        if valid:
            break

    if not valid:
        warnings.append("Player did not have a valid license in the club on the day of the tournament")

    return pid, "unique_name"

def match_by_name_with_license(
        cursor, 
        fullname_raw: str, 
        clubname_raw: str,
        class_date: date, 
        license_name_club_map, 
        player_name_map, 
        club_id: int, 
        warnings: List[str]
    ) -> Optional[Tuple[int, str]]:
    pids = get_name_candidates(fullname_raw, player_name_map)
    if len(pids) <= 1:
        return None

    # Query for valid licenses
    placeholders = ",".join("?" for _ in pids)
    sql = f"""
        SELECT DISTINCT player_id FROM player_license
        WHERE player_id IN ({placeholders})
        AND club_id = ?
        AND valid_from <= ?
        AND valid_to >= ?
    """
    params = [*pids, club_id, class_date, class_date]
    cursor.execute(sql, params)
    valid = [r[0] for r in cursor.fetchall()]

    if len(valid) == 1:
        return valid[0], "license"

    if len(valid) > 1:
        # Ambiguous - log or handle
        return None

    # Check expired
    sql_expired = f"""
        SELECT DISTINCT player_id FROM player_license
        WHERE player_id IN ({placeholders})
        AND club_id = ?
    """
    cursor.execute(sql_expired, [*pids, club_id])
    expired = [r[0] for r in cursor.fetchall()]

    if len(expired) == 1:
        warnings.append("Matched via expired license")
        return expired[0], "expired_license"

    return None

def match_by_transition(
        cursor, 
        fullname_raw: str, 
        clubname_raw: str,
        class_date: date, 
        license_name_club_map, 
        player_name_map, 
        club_id: int, 
        warnings: List[str]
    ) -> Optional[Tuple[int, str]]:
    pids = get_name_candidates(fullname_raw, player_name_map)
    if not pids:
        return None
    placeholders = ",".join("?" for _ in pids)
    sql = f"""
        SELECT player_id FROM player_transition
        WHERE (club_id_to = ? OR club_id_from = ?)
        AND transition_date <= ?
        AND player_id IN ({placeholders})
    """
    params = [club_id, club_id, class_date, *pids]
    cursor.execute(sql, params)
    trans = [r[0] for r in cursor.fetchall()]
    if len(trans) == 1:
        return trans[0], "transition"

    return None

def match_by_any_season_license(
        cursor, 
        fullname_raw: str, 
        clubname_raw: str,
        class_date: date, 
        license_name_club_map, 
        player_name_map, 
        club_id: int, 
        warnings: List[str]
    ) -> Optional[Tuple[int, str]]:
    pids = get_name_candidates(fullname_raw, player_name_map)
    if not pids:
        return None
    placeholders = ",".join("?" for _ in pids)
    sql = f"""
        SELECT DISTINCT player_id FROM player_license
        WHERE club_id = ?
        AND player_id IN ({placeholders})
    """
    cursor.execute(sql, [club_id, *pids])
    all_l = [r[0] for r in cursor.fetchall()]
    if len(all_l) == 1:
        return all_l[0], "any_season_license"

    return None

def match_by_name_substring_license(
        cursor, 
        fullname_raw: str, 
        clubname_raw: str,
        class_date: date, 
        license_name_club_map, 
        player_name_map, 
        club_id: int, 
        warnings: List[str]
    ) -> Optional[Tuple[int, str]]:
    clean = normalize_key(fullname_raw)
    raw_parts = clean.split()

    if len(raw_parts) > 2:
        return None

    first_tok, last_tok = raw_parts[0], raw_parts[-1]

    candidates = []
    for (fn, ln, cid), rows in license_name_club_map.items():
        if cid != club_id:
            continue
        candidate_key = normalize_key(f"{fn} {ln}")
        cand_parts = candidate_key.split()
        if len(cand_parts) < 3:
            continue

        if first_tok in candidate_key and last_tok in candidate_key:
            candidates.extend(rows)

    if not candidates:
        return None

    valid_ids = set()
    expired_ids = set()
    for row in candidates:
        pid = row["player_id"]
        vf, vt = row["valid_from"], row["valid_to"]
        if vf <= class_date <= vt:
            valid_ids.add(pid)
        else:
            expired_ids.add(pid)

    if len(valid_ids) == 1:
        return list(valid_ids)[0], "substring_license"

    if len(valid_ids) > 1:
        return None  # Ambiguous

    if len(expired_ids) == 1:
        warnings.append("Matched via expired substring license")
        return list(expired_ids)[0], "expired_substring_license"

    return None

def fallback_unverified(
        cursor, 
        fullname_raw: str, 
        clubname_raw: str,
        player_unverified_name_map: Dict[str, int], 
        logger: OperationLogger, 
        item_key: str
    ) -> Optional[int]:
    clean = " ".join(fullname_raw.strip().split())
    existing = player_unverified_name_map.get(clean)
    if existing is not None:
        return existing

    # Create new unverified player (assume Player has insert_unverified)
    new_id = Player.insert_unverified(cursor, clean)
    if new_id:
        player_unverified_name_map[clean] = new_id
        logger.warning(item_key, "Created new unverified player")
        return new_id
    return None

def get_name_candidates(
        fullname_raw: str, 
        player_name_map: Dict[str, List[int]]
    ) -> List[int]:
    clean = normalize_key(fullname_raw)
    parts = clean.split()
    keys = [
        normalize_key(f"{fn} {ln}")
        for i in range(1, len(parts))
        for ln in [" ".join(parts[:i])]
        for fn in [" ".join(parts[i:])]
    ]
    matches = set()
    for k in keys:
        matches.update(player_name_map.get(k, []))
    
    return list(matches)

def parse_raw_participant(
        cursor, 
        raw: Dict[str, Any], 
        tournament_class_id: int, 
        tournament_class_id_ext: str,
        class_date: date, 
        club_map, 
        license_name_club_map,
        player_name_map,
        player_unverified_name_map, 
        logger: OperationLogger, 
        item_key: str
    ) -> Tuple[Optional[Dict[str, Dict[str, Any]]], Optional[str]]:
    """
    Parse raw participant data, match club and player using existing logic.
    Returns dict with 'participant' and 'participant_player' data, or None on failure.
    """

    try: 
        # Using square bracket loop forces keys to exist, otherwise raising KeyError
        fullname_raw        = raw["raw_name"]
        clubname_raw        = raw["club_name"]
    except KeyError as e:
        logger.error(item_key, f"Missing name or club in raw data: {e}")
        return None

    # seed            = (int(v) if (v := raw.get("seed")) not in (None, "", "-", "—") and str(v).strip().isdigit() else None)
    # t_ptcp_id_ext   = raw.get("tournament_participant_id_ext", "").strip() or None

    v = raw.get("seed")
    seed = int(v) if isinstance(v, (int, str)) and str(v).strip().isdigit() else None

    val = raw.get("tournament_participant_id_ext")
    t_ptcp_id_ext = (val.strip() if isinstance(val, str) and val.strip() else None)

    club = find_club(
        cursor, 
        clubname_raw, 
        club_map, 
        logger, 
        item_key
    )
    if not club:
        logger.failed(item_key, f"Club not found for '{clubname_raw}'")
        return None
    
    club_id = club.club_id

    # Create temp Participant for matching (club_id used in strategies)
    temp_participant = Participant(tournament_class_id=tournament_class_id)
    temp_participant.club_id = club_id  # Add club_id to Participant if needed, or pass separately

    player_id, match_type = match_player(
        cursor, 
        temp_participant, 
        fullname_raw, 
        clubname_raw,
        class_date, 
        license_name_club_map, 
        player_name_map, 
        player_unverified_name_map, 
        logger, 
        item_key,
        tournament_class_id_ext
    )
    if player_id is None:
        logger.failed(item_key, f"No match for player")
        return None, None

    result_dict = {
        "participant": {
            "tournament_class_id": tournament_class_id,
            "tournament_class_seed": seed,
            "tournament_class_final_position": None
        },
        "participant_player": {
            "participant_player_id_ext": t_ptcp_id_ext,
            "participant_id": None,
            "player_id": player_id,
            "club_id": club_id
        }
    }
    return result_dict, match_type