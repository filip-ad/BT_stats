# src/scrapers/scrape_participants_ondata.py
import logging
from models.tournament import Tournament
from utils import _download_pdf_ondata_by_tournament_class_and_stage, normalize_key
from models.tournament_class import TournamentClass
from models.tournament_class_entry_raw import TournamentClassEntryRaw
from utils import OperationLogger, parse_date
from config import (
    SCRAPE_PARTICIPANTS_CUTOFF_DATE,
    SCRAPE_PARTICIPANTS_MAX_CLASSES,
    SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
    SCRAPE_PARTICIPANTS_TNMT_ID_EXTS,
    SCRAPE_PARTICIPANTS_ORDER
)
import pdfplumber
import re
import unicodedata
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
import time

def scrape_participants_ondata(cursor, include_positions: bool = True, run_id=None) -> List[TournamentClass]:
    """Scrape and populate raw participant data from PDFs for filtered tournament classes.
    Returns the list of processed TournamentClass instances.
    """
    logger = OperationLogger(
        verbosity       = 2,
        print_output    = False,
        log_to_db       = True,
        cursor          = cursor,
        object_type     = "participant",
        run_type        = "scrape",
        run_id          = run_id
    )

    cutoff_date = parse_date(SCRAPE_PARTICIPANTS_CUTOFF_DATE)

    classes = TournamentClass.get_filtered_classes(
        cursor,
        data_source_id      = 1 ,
        cutoff_date         = cutoff_date,
        require_ended       = True,
        allowed_type_ids    = [1],
        max_classes         = SCRAPE_PARTICIPANTS_MAX_CLASSES,
        class_id_exts       = SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
        tournament_id_exts  = SCRAPE_PARTICIPANTS_TNMT_ID_EXTS,
        order               = SCRAPE_PARTICIPANTS_ORDER
    )

    logger.info("Scraping tournament class entries...")
    logger.info(f"Processing {len(classes)} tournament classes (cutoff date: {cutoff_date})")
    start_time = time.time()

    tournaments = Tournament.get_all(cursor)
    tournaments_dict = {t.tournament_id: t for t in tournaments}

    if not classes:
        logger.skipped({}, "No valid singles classes matching filters")
        return []

    partial_classes = 0
    partial_participants = 0
    total_failures = 0

    for i, tc in enumerate(classes, 1):
        logger_keys = {
            'tournament_id': str(tc.tournament_id or 'N/A'),
            'tournament_id_ext': 'N/A',
            'tournament_shortname': 'N/A',
            'tournament_class_shortname': tc.shortname or 'N/A',
            'tournament_class_id': str(tc.tournament_class_id or 'N/A'),
            'tournament_class_id_ext': str(tc.tournament_class_id_ext or 'N/A'),
            'tournament_url': 'N/A',
            'stage_1_url': 'N/A'
        }
        
        # Lookup using dict
        tournament = tournaments_dict.get(tc.tournament_id)
        tid_ext = str(tournament.tournament_id_ext).zfill(6) if tournament and tournament.tournament_id_ext else None
        tournament_shortname = tournament.shortname if tournament else "N/A"
        logger_keys['tournament_id_ext'] = tid_ext
        logger_keys['tournament_shortname'] = tournament_shortname or "N/A"
        logger_keys['tournament_url'] = tournament.url if tournament and tournament.url else "N/A"
        logger_keys['stage_1_url'] = f"https://resultat.ondata.se/ViewClassPDF.php?tournamentID={tid_ext}&classID={tc.tournament_class_id_ext}&stage=1"

        # Remove existing raw data for this class
        deleted_count = TournamentClassEntryRaw.remove_for_class(cursor, tc.tournament_class_id_ext)
        if deleted_count > 0:
            # logger.info(logger_keys, f"Removed {deleted_count} existing raw player participants", to_console=False)
            pass

        # Download and parse initial participants (stage=1)
        pdf_path, was_downloaded, message = _download_pdf_ondata_by_tournament_class_and_stage(
            tid_ext, tc.tournament_class_id_ext, stage=1, force_download=False
        )
        initial_success = False
        if message:
            if not ("Cached" in message or "Downloaded" in message):
                logger.failed(logger_keys.copy(), message)
                total_failures += 1
                continue

        if pdf_path:
            participants, effective_expected_count = _parse_initial_participants_pdf(
                pdf_path, tc.tournament_class_id_ext, tid_ext, 1  # data_source_id=1
            )
            if not participants:
                logger.failed(logger_keys.copy(), "No participants parsed from initial PDF")
                print(f"❌ [{i}/{len(classes)}] Parsed class {tc.shortname or 'N/A'}, {tournament_shortname}, {tc.startdate or 'N/A'} "
                      f"(tcid: {tc.tournament_class_id}, tcid_ext: {tc.tournament_class_id_ext}, tid: {tc.tournament_id}, tid_ext: {tid_ext}). "
                      f"Expected {effective_expected_count if effective_expected_count is not None else '—'}, "
                      f"found 0, seeded: 0, deleted: {deleted_count} old participants.")
                total_failures += 1
                continue
            else:
                # Insert raw participants
                for participant_data in participants:
                    raw_entry = TournamentClassEntryRaw.from_dict(participant_data)
                    is_valid, error_message = raw_entry.validate()
                    if is_valid:
                        raw_entry.compute_hash()
                        raw_entry.insert(cursor)
                    else:
                        logger.warning(logger_keys.copy(), f"Validation failed: {error_message}")
                        continue

                initial_success = True

                found = len(participants)
                seeded_count = sum(1 for p in participants if p.get("seed_raw") is not None)
                # Determine icon based on effective expected vs found count
                icon = "✅" if effective_expected_count is not None and found == effective_expected_count else "❌" if effective_expected_count is not None else "—"
                if effective_expected_count is not None and found != effective_expected_count:
                    partial_classes += 1
                    partial_participants += abs(found - effective_expected_count)
                    logger.warning(logger_keys.copy(), f"Scraped participants did not match expected count")

        # Handle final positions if requested
        final_positions_found = 0
        final_success = True
        if include_positions and initial_success:
            final_stage = tc.get_final_stage()
            if final_stage is None:
                logger.warning(logger_keys.copy(), "No valid final stage determined")
                final_success = False
            else:
                final_pdf_path, downloaded, message = _download_pdf_ondata_by_tournament_class_and_stage(
                    tid_ext, tc.tournament_class_id_ext, final_stage, force_download=False
                )
                if message:
                    if not ("Cached" in message or "Downloaded" in message):
                        logger.warning(logger_keys.copy(), f"Failed to scrape final positions: {message}")
                        final_success = False

                if final_pdf_path and final_success:
                    positions = _parse_final_positions_pdf(final_pdf_path, tc.tournament_class_id_ext, tid_ext, 1)
                    if positions:
                        final_positions_found = len(positions)
                        for pos_data in positions:
                            TournamentClassEntryRaw.update_final_position(
                                cursor,
                                tournament_class_id_ext=tc.tournament_class_id_ext,
                                fullname_raw=pos_data["fullname_raw"],
                                clubname_raw=pos_data["clubname_raw"],
                                data_source_id=pos_data["data_source_id"],
                                final_position_raw=pos_data["final_position_raw"],
                            )
                        # logger.success(logger_keys, f"Updated {len(positions)} raw player participants with final positions")
                    else:
                        logger.warning(logger_keys.copy(), "No positions parsed from final PDF")
                        final_success = False
                elif not final_success:
                    pass  # No additional failure increment here

            # Update print statement with final positions if applicable
            if initial_success:
                if final_success and final_positions_found > 0:
                    logger.success(logger_keys.copy(), f"All expected participants inserted, including seeds and final positions")
                elif initial_success:
                    logger.success(logger_keys.copy(), f"All expected participants inserted (could not resolve seeds and/or final positions)")
                print(f"{icon} [{i}/{len(classes)}] Parsed class {tc.shortname or 'N/A'}, {tournament_shortname}, {tc.startdate or 'N/A'} "
                      f"(tcid: {tc.tournament_class_id}, tcid_ext: {tc.tournament_class_id_ext}, tid: {tc.tournament_id}, tid_ext: {tid_ext}). "
                      f"Expected {effective_expected_count if effective_expected_count is not None else '—'}, "
                      f"found {found}, seeded: {seeded_count}, deleted: {deleted_count} old participants, "
                      f"final positions found: {final_positions_found}")

    logger.info(f"Participants update completed in {time.time() - start_time:.2f} seconds")
    if partial_classes > 0:
        logger.info(f"Partially parsed classes: {partial_classes} (participants impacted: {partial_participants})")
    logger.summarize()
    return classes

def _parse_initial_participants_pdf(pdf_path: Path, tournament_class_id_ext: str, tournament_id_ext: str, data_source_id: int) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """Parse initial participant data (names, clubs, seeds) from a stage=1 PDF.
    Returns (participants, expected_count) where participants is a list of dicts with raw data.
    """
    participants = []
    expected_count = None
    unique_entries = set()  # Set to track unique keys
    seed_counter = 1
    bold_name_keys = set()
    placeholder_skips = 0

    # Regexes
    EXPECTED_PARTICIPANT_COUNT_RE = re.compile(r'(?:\b\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2})?\s*)?\(\s*(\d+)\s+[^\d()]+\)', re.I)
    PART_WITH_ID_RE = re.compile(r'^\s*(?P<tpid>\d{1,5})\s+(?P<name>[^,]+?)\s*,\s*(?P<club>\S.*\S)\s*$', re.M)
    PART_NO_ID_RE = re.compile(r'^\s*(?P<name>[^,]+?)\s*,\s*(?P<club>\S.*\S)\s*$', re.M)
    PART_WIDE_SPACES_RE = re.compile(r'^\s*(?:(?P<tpid>\d{1,5})\s+)?(?P<name>[^\s,].*?[^\s,])\s{2,}(?P<club>.+\S)\s*$', re.M)
    BOLD_RE = re.compile(r"(bold|black|heavy|demi|semibold|semi-bold|sb)\b", re.I)
    PLACEHOLDER_NAME_RE = re.compile(r"\b(vakant|vacant|reserv|reserve)\b", re.I)
    TITLE_RE = re.compile(r'Deltagarlista\s*', re.I | re.M)
    CLASS_HEADER_RE = re.compile(r'^[A-Z],\s*Klass\s*\d+\s*$', re.M | re.I)
    FOOTER_ANY_LINE_RE = re.compile(r'(?i)(tt\s*coordinator|coordinator\.com|programlicens|tävlingen\s+genomförs|användas\s+vid\s+tävlingar|arrangerade\s+av)')
    # Restore category tokens from previous version
    CATEGORY_TOKENS = {"vet", "veteran", "junior", "pojkar", "flickor", "herrar", "damer", "klass", "class", "norges", "cup", "stiga", "trondheim", "turnering"}
    MIN_NAME_LENGTH = 3
    MIN_CLUB_LENGTH = 2

    def _try_match(line: str):
        for rx in (PART_WITH_ID_RE, PART_NO_ID_RE, PART_WIDE_SPACES_RE):
            m = rx.match(line)
            if m:
                return m
        return None

    def _strip_above_and_header(block: str) -> Tuple[str, Optional[int]]:
        m = EXPECTED_PARTICIPANT_COUNT_RE.search(block)
        if m:
            end_of_header_line = block.find('\n', m.end())
            if end_of_header_line == -1:
                end_of_header_line = len(block)
            return block[end_of_header_line + 1:], int(m.group(1))
        return block, None

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                w, h = page.width, page.height
                
                # Extract words early
                words = page.extract_words(use_text_flow=True, keep_blank_chars=False, extra_attrs=["fontname"])
                if not words:
                    continue

                # Cluster into lines for bold detection
                lines = {}
                tol = 2
                for wd in words:
                    t = int(round(wd["top"]))
                    key = next((k for k in lines if abs(k - t) <= tol), t)
                    lines.setdefault(key, []).append(wd)

                # Detect bold names
                for _, line_words in lines.items():
                    name_tokens = []
                    for wd in line_words:
                        txt = wd["text"]
                        if "," in txt:
                            before = txt.split(",", 1)[0].strip()
                            if before:
                                name_tokens.append(before)
                            break
                        name_tokens.append(txt)
                    if not name_tokens:
                        continue
                    if re.fullmatch(r"\d+", name_tokens[0]):
                        name_tokens = name_tokens[1:]
                    name_candidate = " ".join(name_tokens).strip()
                    if re.search(r"[A-Za-zÅÄÖåäöØøÆæÉéÈèÜüß]", name_candidate):
                        if any(BOLD_RE.search(wd.get("fontname") or "") for wd in line_words[:max(1, len(name_tokens))]):
                            bold_name_keys.add(normalize_key(name_candidate))

                # Split into left and right columns
                left_words = [wd for wd in words if (wd['x0'] + wd['x1']) / 2 < w / 2]
                right_words = [wd for wd in words if (wd['x0'] + wd['x1']) / 2 >= w / 2]

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

                # Detect header bottom
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

                if expected_count is None:
                    if cnt_left is not None:
                        expected_count = cnt_left
                    elif cnt_right is not None:
                        expected_count = cnt_right

                TOP_left = (header_bottom_left + 5) if header_bottom_left else 50
                TOP_right = (header_bottom_right + 5) if header_bottom_right else 50
                BOTTOM = 50

                left = page.crop((0, TOP_left, w/2, h - BOTTOM)).extract_text() or ""
                right = page.crop((w/2, TOP_right, w, h - BOTTOM)).extract_text() or ""

                for raw_block in (left, right):
                    block = re.sub(r'Directly qualified.*$', "", raw_block, flags=re.M)
                    block = re.sub(r'Group stage.*$', "", block, flags=re.M)
                    block = TITLE_RE.sub('', block)
                    block = CLASS_HEADER_RE.sub('', block)
                    block = FOOTER_ANY_LINE_RE.sub('', block)
                    block, cnt = _strip_above_and_header(block)
                    if expected_count is None and cnt is not None:
                        expected_count = cnt
                    if block:
                        for raw_line in block.splitlines():
                            line = raw_line.strip()
                            if not line:
                                continue

                            m = _try_match(line)
                            if not m:
                                continue

                            tpid = m.groupdict().get('tpid')
                            participant_player_id_ext = tpid.strip() if tpid else None
                            raw_name = m.group('name').strip()
                            club_name = m.group('club').strip()

                            # Apply previous version's validation checks
                            if len(raw_name) < MIN_NAME_LENGTH or len(club_name) < MIN_CLUB_LENGTH:
                                continue
                            if raw_name.lower() in CATEGORY_TOKENS or club_name.lower() in CATEGORY_TOKENS:
                                continue
                            if PLACEHOLDER_NAME_RE.search(raw_name):
                                placeholder_skips += 1
                                continue
                            if not any(unicodedata.category(ch).startswith("L") for ch in club_name):
                                continue

                            # Use participant_player_id_ext for deduplication if available, fallback to (name, club)
                            dedup_key = (
                                participant_player_id_ext if participant_player_id_ext else normalize_key(raw_name),
                                normalize_key(club_name)
                            )
                            if dedup_key in unique_entries:
                                continue
                            unique_entries.add(dedup_key)

                            is_seeded = normalize_key(raw_name) in bold_name_keys
                            seed_raw = str(seed_counter) if is_seeded else None
                            if is_seeded:
                                seed_counter += 1

                            participants.append({
                                "tournament_id_ext": tournament_id_ext,
                                "tournament_class_id_ext": tournament_class_id_ext,
                                "participant_player_id_ext": participant_player_id_ext,
                                "data_source_id": data_source_id,
                                "fullname_raw": raw_name,
                                "clubname_raw": club_name,
                                "seed_raw": seed_raw,
                                "final_position_raw": None,
                                "raw_group_id": None
                            })

    except Exception as e:
        logging.error({"pdf_path": str(pdf_path), "error": str(e)}, "Exception during PDF parsing")
        return [], None

    effective_expected_count = expected_count - placeholder_skips if expected_count is not None else None
    return participants, effective_expected_count

def _parse_final_positions_pdf(pdf_path: Path, tournament_class_id_ext: str, tournament_id_ext: str, data_source_id: int) -> List[Dict[str, Any]]:
    """Parse final positions from a PDF (e.g., stage=6 or 4).
    Returns a list of dicts with raw position data.
    """
    positions = []
    unique_entries = set()

    # Regex for position patterns (e.g., "1. Name, Club" or "1 Name, Club")
    POSITION_RE = re.compile(r'^\s*(?P<pos>\d+)\.?\s+(?P<name>[^,]+?)\s*,\s*(?P<club>\S.*\S)\s*$', re.M)
    PLACEHOLDER_NAME_RE = re.compile(r"\b(vakant|vacant|reserv|reserve)\b", re.I)

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                for line in text.splitlines():
                    line = line.strip()
                    if not line:
                        continue

                    m = POSITION_RE.match(line)
                    if not m:
                        continue

                    raw_name = m.group('name').strip()
                    club_name = m.group('club').strip()
                    position_raw = m.group('pos').strip()

                    # Skip placeholders
                    if PLACEHOLDER_NAME_RE.search(raw_name):
                        continue
                    if len(raw_name) < 3 or not any(unicodedata.category(ch).startswith("L") for ch in club_name):
                        continue

                    key = (raw_name.lower(), club_name.lower())
                    if key in unique_entries:
                        continue
                    unique_entries.add(key)

                    positions.append({
                        "tournament_id_ext": tournament_id_ext,
                        "tournament_class_id_ext": tournament_class_id_ext,
                        "participant_player_id_ext": None,  # Can be enhanced with TPID if present
                        "data_source_id": data_source_id,
                        "fullname_raw": raw_name,
                        "clubname_raw": club_name,
                        "final_position_raw": position_raw,
                        "raw_group_id": None
                    })

    except Exception as e:
        logging.error({"pdf_path": str(pdf_path), "error": str(e)}, "Exception during PDF parsing")
        return []

    return positions