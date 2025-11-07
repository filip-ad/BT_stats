# src/scrapers/scrape_tournament_class_entries_ondata.py

from datetime import date
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
from typing import Optional, Tuple, List, Dict, Any, Set
import time

# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['29625'] # complex, two columns, many line breaks due to long names | https://resultat.ondata.se/ViewClassPDF.php?classID=29625&stage=2
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['9998'] # easy, 2 groups with 2 seeds          | https://resultat.ondata.se/ViewClassPDF.php?classID=9998&stage=2

# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['279'] # other language                        | https://resultat.ondata.se/ViewClassPDF.php?tournamentID=000012&classID=279&stage=2
# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['193'] # other language + direct qualifiers    | https://resultat.ondata.se/ViewClassPDF.php?tournamentID=000007&classID=193&stage=2

# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['30156'] # regular one, but bugs, some missing, some incorrectly assigned..

# 30232
# 30017
# 29926
# 30804

# SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['30232', '30017', '29926', '30804']



def scrape_tournament_class_entries_ondata(cursor, include_positions: bool = True, run_id=None) -> List[TournamentClass]:
    """Scrape and populate raw participant data from PDFs for filtered tournament classes.
    Returns the list of processed TournamentClass instances.
    """
    logger = OperationLogger(
        verbosity       = 2,
        print_output    = False,
        log_to_db       = True,
        cursor          = cursor,
        object_type     = "tournament_entry",
        run_type        = "scrape",
        run_id          = run_id
    )

    cutoff_date = parse_date(SCRAPE_PARTICIPANTS_CUTOFF_DATE)

    classes = TournamentClass.get_filtered_classes(
        cursor,
        data_source_id          = 1 ,
        cutoff_date             = cutoff_date,
        require_ended           = True,
        allowed_type_ids        = [1],
        allowed_structure_ids   = [1, 2],
        max_classes             = SCRAPE_PARTICIPANTS_MAX_CLASSES,
        class_id_exts           = SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
        tournament_id_exts      = SCRAPE_PARTICIPANTS_TNMT_ID_EXTS,
        order                   = SCRAPE_PARTICIPANTS_ORDER
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
    total_participants = 0
    total_expected = 0

    for i, tc in enumerate(classes, 1):

        debug = False

        logger.inc_processed()

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

        # Force refresh if the tournament ended within the last 90 days
        today = date.today()
        ref_date = (tc.startdate or today)
        force_refresh = False
        if ref_date:
            try:
                ref_date = ref_date.date() if hasattr(ref_date, "date") else ref_date
                if (today - ref_date).days <= 90:
                    force_refresh = True
            except Exception:
                pass


        pdf_path, was_downloaded, message = _download_pdf_ondata_by_tournament_class_and_stage(
            tid_ext, tc.tournament_class_id_ext, stage=1, force_download=force_refresh
        )
        initial_success = False
        if message:
            if not ("Cached" in message or "Downloaded" in message):
                logger.failed(logger_keys.copy(), message)
                total_failures += 1
                continue

        if pdf_path:
            participants, effective_expected_count = _parse_initial_participants_pdf(
                pdf_path, tc.tournament_class_id_ext, tid_ext, 1, tc.tournament_class_type_id
            )
            if not participants:
                logger.failed(logger_keys.copy(), "No participants parsed from initial PDF")
                total_failures += 1
                continue

            else:

                if debug:
                    print(f"DEBUG: Participants parsed for tid_ext: {tid_ext}, tcid_ext: {tc.tournament_class_id_ext}:")
                    for p in participants:
                        print(p.get("fullname_raw"), "-", p.get("clubname_raw"), "- seed:", p.get("seed_raw"))
                    print("DEBUG: End of participants list")

                total_expected += effective_expected_count if effective_expected_count is not None else 0
                total_participants += effective_expected_count
                for participant_data in participants:
                    raw_entry = TournamentClassEntryRaw.from_dict(participant_data)
                    is_valid, error_message = raw_entry.validate()
                    if is_valid:
                        raw_entry.compute_hash()
                        raw_entry.insert(cursor)
                    else:
                        logger.failed(logger_keys.copy(), f"Validation failed: {error_message}")
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

        # parse stage=2 (groups) for class types that include group play ----
        groups_found = 0
        group_seeds_found = 0
        if initial_success and tc.tournament_class_type_id in (1, 2):
            stage2_pdf_path, downloaded2, msg2 = _download_pdf_ondata_by_tournament_class_and_stage(
                tid_ext, tc.tournament_class_id_ext, stage=2, force_download=force_refresh
            )
            if msg2 and not ("Cached" in msg2 or "Downloaded" in msg2):
                logger.warning(logger_keys.copy(), f"Failed to fetch groups (stage=2): {msg2}")
            if stage2_pdf_path:
                group_rows = _parse_groups_stage_pdf_using_stage1(stage2_pdf_path, participants, log_prefix=f"STG2[{tc.tournament_class_id_ext}]")

                if group_rows:
                    # Count groups and seeds
                    groups_found = len({g["group_id_raw"] for g in group_rows if g.get("group_id_raw")})
                    group_seeds_found = sum(1 for g in group_rows if g.get("seed_in_group_raw"))

                    updated, err = TournamentClassEntryRaw.batch_update_groups(
                        cursor,
                        tournament_class_id_ext=tc.tournament_class_id_ext,
                        data_source_id=1,
                        groups=group_rows
                    )
                    if err:
                        logger.warning(logger_keys.copy(), f"Group update error: {err}")
                else:
                    logger.warning(logger_keys.copy(), "No groups parsed from stage=2 PDF")


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
                    tid_ext, tc.tournament_class_id_ext, final_stage, force_download=force_refresh
                )
                if message:
                    if not ("Cached" in message or "Downloaded" in message):
                        logger.warning(logger_keys.copy(), f"Failed to scrape final positions: {message}")
                        final_success = False
                if final_pdf_path and final_success:
                    positions = _parse_final_positions_pdf(final_pdf_path, tc.tournament_class_id_ext, tid_ext, 1)
                    if positions:
                        final_positions_found = len(positions)

                        # Batch update final positions
                        updated_count, error_message = TournamentClassEntryRaw.batch_update_final_positions(
                            cursor,
                            tournament_class_id_ext=tc.tournament_class_id_ext,
                            data_source_id=1,
                            positions=positions
                        )
                        if error_message:
                            logger.warning(logger_keys.copy(), error_message)
                    else:
                        logger.warning(logger_keys.copy(), "No positions parsed from final PDF")
                        final_success = False
                elif not final_success:
                    pass  # No additional failure increment here

            # # Update print statement with final positions if applicable
            # if initial_success:
            #     if final_success and final_positions_found > 0:
            #         logger.success(logger_keys.copy(), f"All expected participants inserted, including seeds and final positions")
            #     elif initial_success:
            #         logger.success(logger_keys.copy(), f"All expected participants inserted (could not resolve seeds and/or final positions)")
            #     logger.info(logger_keys.copy(), f"[{i}/{len(classes)}] Parsed class {tc.shortname or 'N/A'}, {tournament_shortname}, {tc.startdate or 'N/A'} "
            #           f"(tcid: {tc.tournament_class_id}, tcid_ext: {tc.tournament_class_id_ext}, tid: {tc.tournament_id}, tid_ext: {tid_ext}). "
            #           f"Expected {effective_expected_count if effective_expected_count is not None else '—'}, "
            #           f"found {found}, seeded: {seeded_count}, deleted: {deleted_count} old participants, "
            #           f"final positions found: {final_positions_found}", emoji=icon, to_console=True, show_key=False)
                
            if initial_success:
                if final_success and final_positions_found > 0:
                    logger.success(
                        logger_keys.copy(),
                        "All expected participants inserted, including seeds, groups, group seeds and final positions"
                    )
                else:
                    logger.success(
                        logger_keys.copy(),
                        "All expected participants inserted (could not resolve seeds and/or final positions)"
                    )

                # Add groups + group seeds to the trailing summary
                logger.info(
                    logger_keys.copy(),
                    f"[{i}/{len(classes)}] Parsed class {tc.shortname or 'N/A'}, {tournament_shortname}, {tc.startdate or 'N/A'} "
                    f"(tcid: {tc.tournament_class_id}, tcid_ext: {tc.tournament_class_id_ext}, tid: {tc.tournament_id}, tid_ext: {tid_ext}). "
                    f"Expected {effective_expected_count if effective_expected_count is not None else '—'}, "
                    f"found {found}, seeded: {seeded_count}, deleted: {deleted_count} old participants, "
                    f"final positions found: {final_positions_found}, "
                    f"groups found: {groups_found}, group seeds found: {group_seeds_found}",
                    emoji=icon, to_console=True, show_key=False
                )

    logger.info(f"Participants update completed in {time.time() - start_time:.2f} seconds. Total participants processed: {total_participants} vs expected: {total_expected}. Total failures: {total_failures}.")
    if partial_classes > 0:
        logger.info(f"Partially parsed classes: {partial_classes} (participants impacted: {partial_participants})")
    logger.summarize()
    return classes

def _parse_initial_participants_pdf(
        pdf_path: Path, 
        tournament_class_id_ext: str, 
        tournament_id_ext: str, 
        data_source_id: int, 
        tournament_class_type_id: int
    ) -> Tuple[List[Dict[str, Any]], Optional[int]]:
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
    CATEGORY_TOKENS = {"vet", "veteran", "junior", "pojkar", "flickor", "herrar", "damer", "klass", "class", "norges", "cup", "stiga", "trondheim", "turnering"}
    MIN_NAME_LENGTH = 3
    MIN_CLUB_LENGTH = 2

    def canon_group_label(word: str, num: str) -> str:
        # Normalize group labels in DB to “Pool N”
        return f"Pool {int(num)}"

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
                            tournament_player_id_ext = tpid.strip() if tpid else None
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
                                tournament_player_id_ext if tournament_player_id_ext else normalize_key(raw_name),
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
                                "tournament_id_ext":        tournament_id_ext,
                                "tournament_class_id_ext":  tournament_class_id_ext,
                                "tournament_player_id_ext": tournament_player_id_ext,
                                "data_source_id":           data_source_id,
                                "fullname_raw":             raw_name,
                                "clubname_raw":             club_name,
                                "seed_raw":                 seed_raw,
                                "final_position_raw":       None,
                                "group_id_raw":             None, 
                                "seed_in_group_raw":        None
                            })

    except Exception as e:
        logging.error({"pdf_path": str(pdf_path), "error": str(e)}, "Exception during PDF parsing")
        return [], None
    
    # Assign entry_group_id_int based on tournament class type (singles-only for now)
    # TODO: Extend for doubles (type_id=2,3) by grouping pairs; treat unknown (9) as singles
    if tournament_class_type_id in [1, 9]:  # Singles or Unknown
            # Sort: seed_raw ascending (1 is best), then fullname_raw ascending for ties
            def sort_key(p):
                seed_val = int(p.get("seed_raw", 0)) if p.get("seed_raw") and p.get("seed_raw").isdigit() else float("inf")
                tpid_val = int(p.get("tournament_player_id_ext", 0)) if p.get("tournament_player_id_ext") else 0
                name_val = normalize_key(p.get("fullname_raw", ""))
                return (seed_val, tpid_val, name_val)

            sorted_participants = sorted(participants, key=sort_key)
            for i, participant_data in enumerate(sorted_participants, 1):
                participant_data["entry_group_id_int"] = i
    else:
        # TODO: Handle doubles/mixed (type_id=2,3): Parse pairs from PDF, assign same ID to group members
        # For now, raise or log error if non-singles (since filter excludes them)
        # raise ValueError(f"Unsupported tournament_class_type_id: {tournament_class_type_id}")
        pass

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
                        "tournament_player_id_ext": None,  # Can be enhanced with TPID if present
                        "data_source_id": data_source_id,
                        "fullname_raw": raw_name,
                        "clubname_raw": club_name,
                        "final_position_raw": position_raw
                    })

    except Exception as e:
        logging.error({"pdf_path": str(pdf_path), "error": str(e)}, "Exception during PDF parsing")
        return []

    return positions

# # GROK
def _parse_groups_stage_pdf_using_stage1(
    pdf_path: Path,
    stage1_entries: List[Dict[str, Any]],
    *,
    log_prefix: str = "STG2",
    debug: bool = True,
) -> List[Dict[str, Any]]:
    """
    Parse stage=2 ('Poolförteckning'/'Grupp') using stage-1 index.
    NEW: robust to (a) two-column pool layouts on the same page and
    (b) wrapped lines for name and/or club.

    Returns rows shaped like:
      {
        "tournament_player_id_ext": "...",
        "fullname_raw": "...",
        "clubname_raw": "...",
        "group_id_raw": "Pool N",
        "seed_in_group_raw": "1|2|..." or None
      }
    """
    import pdfplumber, re, unicodedata, difflib
    from collections import defaultdict

    debug = False

    # Accept Pool / Grupp / Pulje  (case-insensitive, with extra text after the number)
    POOL_WORDS = ("pool", "grupp", "pulje")
    POOL_RE = re.compile(r'(?i)\b(?:pool|grupp|pulje)\s*(\d+)\b')

    # “Directly qualified” sections (skip rows until next pool header)
    DIRECT_QUAL_RE = re.compile(
        r'(?i)\b('
        r'direktekvalifi\w+|'        # NO: Direktekvalifisert / -fiserte
        r'direkte\s+kvalifi\w+|'     # NO/DK: Direkte kvalifisert/kvalificeret
        r'direkt\w*kvalifi\w+|'      # SV: Direktkvalificerade / -erad
        r'direct(?:ly)?\s+qualified' # EN (rare)
        r')\b'
    )

    # Titles to ignore entirely
    SKIP_TITLES = re.compile(
        r'(?i)^\s*('
        r'poolförteckning|'          # SV
        r'pulje\s*oversikt|'         # NO/DK variants
        r'spelare\s+med|'            # SV
        r'spillere\s+med|'           # NO/DK
        r'spelere\s+med|'            # loose variant
        r'spiller\s+med'             # fallback
        r')\b.*$'
    )

    # Footer junk (incl. “Set: 5, Poäng: 11, Diff: 2” etc.)
    FOOTER_NOISE = re.compile(
        r'(?i)(tt\s*coordinator|coordinator\.com|programlicens|'
        r'sets?\s*:\s*\d|po[aä]ng\s*:\s*\d|diff\s*:\s*\d|'
        r'turnering|gjennomføres|med\s+hjelp|programmet|'
        r'tävlingen\s+genomförs|användas\s+vid\s+tävlingar|arrangerade\s+av)'
    )

    # ----------------------------- schedule-detection regexes -----------------------------
    SCHEDULE_HINT_RE = re.compile(r'(?i)\bkl[\s.:]*\d{1,2}[:.]\d{2}\b|\bbord\s*\d+\b|\btable\s*\d+\b')
    TIME_RE = re.compile(r'(?i)\bkl[\s.:]*\d{1,2}[:.]\d{2}\b')
    TABLE_RE = re.compile(r'(?i)\bbord\s*\d+\b|\btable\s*\d+\b')
    SLASHNUM_RE = re.compile(r'^\s*/\s*\d{1,2}\s*$')  # leading "/3" etc.

    # Tokens that sometimes leak into player lines near footers
    NOISE_RE = re.compile(
        r'(?i)(tt\s*coordinator|coordinator\.com|programlicens|tävlingen|genomförs|användas|tävlingar|arrangerade|'
        r'btk\s*enig|turnering|gjennomføres|progra|lisensen|får|bare|brukes)'
    )

    DATE_RE = re.compile(r'\b\d{1,2}/\d{1,2}\b')

    CLUB_SUFFIXES = ("BTK","PK","BTF","IF","IK","IL","SK","BK","KK","FF","AIS","AIK","IFK","GIF","GF","IBK","TTK")


    # ----------------------------- helpers -----------------------------
    def canon_group_label(word: str, num: str) -> str:
        # Normalize all group labels to “Pool N” for DB consistency
        return f"Pool {int(num)}"

    def tokens_center(tokens: List[List[Dict[str, Any]]]) -> Optional[float]:
        xs = [(c["x0"] + c["x1"]) * 0.5 for tok in tokens for c in tok]
        return (sum(xs) / len(xs)) if xs else None

    def has_letters(s: str) -> bool:
        return any(unicodedata.category(ch).startswith("L") for ch in s or "")

    def nk(s: str) -> str:
        from utils import normalize_key
        return normalize_key(s or "")

    def strip_zeros(tpid: Optional[str]) -> Optional[str]:
        if tpid is None:
            return None
        t = str(tpid).strip()
        return t if t == "0" else t.lstrip("0")

    def ratio(a: str, b: str) -> float:
        return difflib.SequenceMatcher(a=nk(a), b=nk(b)).ratio()

    def token_set_ratio(a: str, b: str) -> float:
        sa = set(nk(a).split()); sb = set(nk(b).split())
        if not sa or not sb:
            return 0.0
        return len(sa & sb) / len(sa | sb)

    def name_score(a: str, b: str) -> float:
        return 0.6 * ratio(a, b) + 0.4 * token_set_ratio(a, b)

    def combined_score(n_a: str, n_b: str, c_a: str, c_b: str) -> float:
        return 0.7 * name_score(n_a, n_b) + 0.3 * ratio(c_a, c_b)

    def is_boldish(fontname: str) -> bool:
        fn = (fontname or "").lower()
        return ("bold" in fn) or ("black" in fn) or ("heavy" in fn) or ("demi" in fn) or ("semibold" in fn)

    def is_italicish(fontname: str) -> bool:
        fn = (fontname or "").lower()
        return ("italic" in fn) or ("oblique" in fn) or ("slanted" in fn)

    def tok_text(tok: List[Dict[str, Any]]) -> str:
        return "".join(c["text"] for c in tok).strip()

    def deglue_name_like(s: str) -> str:
        if not s:
            return s
        # Fix common PDF encoding artifacts for Nordic characters
        s = s.replace("/A", "Å").replace("/O", "Ø").replace("/E", "Æ")
        s = s.replace("/a", "å").replace("/o", "ø").replace("/e", "æ")
        # NEW: Join split all-caps sequences (e.g., 'SÆT HER' -> 'SÆTHER', 'BIN DER' -> 'BINDER')
        s = re.sub(r'([A-ZÅÄÖØÆÉÈÜß]{2,})\s+([A-ZÅÄÖØÆÉÈÜß]{2,})', r'\1\2', s)
        # NEW: Insert spaces around glued 'og' in clubs/names (e.g., 'Voldaog' -> 'Volda og')
        s = re.sub(r'(?<=[a-zA-ZåäöøæéèüßÅÄÖØÆÉÈÜß]{2})og(?=[a-zA-ZåäöøæéèüßÅÄÖØÆÉÈÜß]{2})', ' og ', s, flags=re.I)
        # Insert space for glued "og" in names/clubs (common in Norwegian)
        s = re.sub(r'([A-Za-zÆØÅæøå])(og)([A-ZÆØÅæøå])', r'\1 \2 \3', s, flags=re.I)
        s = re.sub(r'([A-Za-zÆØÅæøå])og\b', r'\1 og', s, flags=re.I)
        # Existing patterns for ungluing capitalized names
        s = re.sub(r'(?<=[a-zåäöøæéèüß])(?=[A-ZÅÄÖØÆÉÈÜß])', ' ', s)
        s = re.sub(r'([A-ZÅÄÖØÆÉÈÜß]{2,})([A-ZÅÄÖØÆÉÈÜß][a-zåäöøæéèüß\-]+)', r'\1 \2', s)
        # join broken ALL-CAPS parts before a Capitalized given: "THORFINNSS ON Bendik" -> "THORFINNSSON Bendik"
        s = re.sub(r'\b([A-ZÅÄÖØÆÉÈÜß]{2,})\s+([A-ZÅÄÖØÆÉÈÜß]{2,})(\s+[A-ZÅÄÖØÆÉÈÜß][a-zåäöøæéèüß\-]+)', r'\1\2\3', s)
        # split double-caps surname clusters: "SKEIELILAND Erlend" -> "SKEIE LILAND Erlend" (heuristic for common glues)
        s = re.sub(r'([A-ZÅÄÖØÆÉÈÜß]{3,})([A-ZÅÄÖØÆÉÈÜß]{3,})(?=\s+[A-ZÅÄÖØÆÉÈÜß][a-zåäöøæéèüß\-]+)', r'\1 \2', s)
        return re.sub(r'\s{2,}', ' ', s).strip()

    # --- token helpers for schedule-mode ---
    WORD_RE = re.compile(r"[A-Za-zÅÄÖåäöØøÆæÉéÈèÜüß\-]+")
    def _strip_diacritics(s: str) -> str:
        return ''.join(ch for ch in unicodedata.normalize('NFKD', s) if not unicodedata.combining(ch))

    def _wtokens(s: str) -> List[str]:
        s = s or ""
        return [_strip_diacritics(w).lower() for w in WORD_RE.findall(s)]


    # ----------------------------- indices from stage-1 -----------------------------
    by_tpid: Dict[str, Dict[str, Any]] = {}
    by_name_club: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for e in stage1_entries:
        tpid = e.get("tournament_player_id_ext")
        fn = e.get("fullname_raw") or ""
        cl = e.get("clubname_raw") or ""
        if tpid is not None and str(tpid).strip() != "":
            by_tpid[str(tpid).strip()] = e
            no0 = strip_zeros(str(tpid))
            if no0 and no0 not in by_tpid:
                by_tpid[no0] = e
        by_name_club[(nk(fn), nk(cl))] = e

    if debug:
        logging.info("[%s] stage1 index built: TPIDs=%d, nameclub=%d", log_prefix, len(by_tpid), len(by_name_club))

    # Precompute normalized names for substring lookup (longest first)
    stage1_name_index = sorted(
        [(nk(e["fullname_raw"]), e) for e in stage1_entries],
        key=lambda x: len(x[0]),
        reverse=True
    )

    results: List[Dict[str, Any]] = []
    matched_keys: Set[str] = set()
    order_ix_by_group: Dict[str, int] = defaultdict(int)

    # ----------------------------- inner builders -----------------------------
    class RowBuilder:
        """Accumulates multi-line name/club for one player row inside a pool."""
        def __init__(self):
            self.left_tokens: List[List[Dict[str, Any]]] = []
            self.right_tokens: List[List[Dict[str, Any]]] = []
            self.left_fonts: Set[str] = set()
            self.has_italic_left: bool = False
            self.tpid_text: Optional[str] = None  # extracted once (first line usually)
            self.group_id: Optional[str] = None
            self._last_split_x: Optional[float] = None  # hint for continuation side choice

        def add_line(self, left_tokens: List[List[Dict[str, Any]]], right_tokens: List[List[Dict[str, Any]]], split_hint_x: Optional[float]):
            if left_tokens:
                # capture TPID (even if glued)
                left_chars = [c for tok in left_tokens for c in tok]
                i = 0
                while i < len(left_chars) and left_chars[i]["text"].isdigit():
                    i += 1
                # Guard: if the next char is '/', it's a date like "25/3" → NOT a TPID
                if i < len(left_chars) and left_chars[i]["text"] == "/":
                    i = 0  # cancel TPID detection
                if self.tpid_text is None and 0 < i <= 5:
                    self.tpid_text = "".join(ch["text"] for ch in left_chars[:i])
                # fonts for seeding decision
                for tok in left_tokens:
                    for c in tok:
                        fn = (c.get("fontname") or "").lower()
                        self.left_fonts.add(fn)
                        if not c.get("upright", True):
                            self.has_italic_left = True
                self.left_tokens.extend(left_tokens)

            if right_tokens:
                self.right_tokens.extend(right_tokens)

            if split_hint_x:
                self._last_split_x = split_hint_x

        def has_any_tokens(self) -> bool:
            return bool(self.left_tokens or self.right_tokens)

        def _join_left_text(self) -> str:
            s = " ".join("".join(c["text"] for c in tok).strip() for tok in self.left_tokens).strip()
            s = re.sub(r'^\s*/\s*\d{1,2}\s+', '', s)  # drop leading "/3", "/5", ...
            j = 0
            while j < len(s) and s[j].isdigit():
                j += 1
            return deglue_name_like(s[j:].strip())

        def _join_right_text(self) -> str:
            raw = " ".join("".join(c["text"] for c in tok).strip() for tok in self.right_tokens).strip()
            m_suffix = re.match(rf"^(.*?)(?:{'|'.join(CLUB_SUFFIXES)})$", raw)
            if m_suffix:
                for suf in sorted(CLUB_SUFFIXES, key=len, reverse=True):
                    if raw.endswith(suf) and not raw.endswith(" " + suf):
                        raw = raw[: -len(suf)].rstrip() + " " + suf
                        break
            return deglue_name_like(raw)

        def flush(self) -> Optional[Dict[str, Any]]:
            if not self.has_any_tokens():
                return None

            name_text = self._join_left_text()
            club_text = self._join_right_text()
            if not has_letters(name_text) or not has_letters(club_text):
                return None
            if SLASHNUM_RE.match(name_text) or TIME_RE.search(name_text) or TABLE_RE.search(name_text):
                return None

            # choose entry (TPID first, then fuzzy)
            chosen = None
            if self.tpid_text:
                for cand in (self.tpid_text, strip_zeros(self.tpid_text)):
                    if cand and cand in by_tpid:
                        chosen = by_tpid[cand]; break
            if not chosen:
                # fuzzy against stage-1
                best_score = -1.0; best_entry = None
                for (nk_name, nk_club), entry in by_name_club.items():
                    score = combined_score(name_text, nk_name, club_text, nk_club)
                    if score > best_score:
                        best_score, best_entry = score, entry
                if best_entry and best_score >= 0.7:
                    chosen = best_entry

            # NEW Fallback: if club matches closely and name is reasonable
            if not chosen:
                for (nk_name, nk_club), entry in by_name_club.items():
                    club_r = ratio(club_text, entry["clubname_raw"])
                    n_score = name_score(name_text, entry["fullname_raw"])
                    if club_r >= 0.9 and n_score >= 0.5:
                        chosen = entry
                        break

            # Fallback: compare names with spaces/hyphens removed (helps UPPER+UPPER glues)
            if not chosen:
                ns_pdf = re.sub(r'[\s\-]+', '', nk(name_text))
                nk_club_text = nk(club_text)
                for (nk_name_stg1, nk_club_stg1), entry in by_name_club.items():
                    if nk_club_stg1 != nk_club_text:
                        continue
                    ns_stage1 = re.sub(r'[\s\-]+', '', nk_name_stg1)
                    if ns_pdf == ns_stage1 or ns_pdf in ns_stage1 or ns_stage1 in ns_pdf:
                        chosen = entry
                        break

            # Fallback: same club + name substring match (handles residual spacing weirdness)
            if not chosen:
                nk_name_pdf = nk(name_text)
                nk_club_pdf = nk(club_text)
                candidates = []
                for (nk_name_stg1, nk_club_stg1), entry in by_name_club.items():
                    if nk_club_stg1 != nk_club_pdf:
                        continue
                    if nk_name_stg1 in nk_name_pdf or nk_name_pdf in nk_name_stg1:
                        candidates.append(entry)
                if len(candidates) == 1:
                    chosen = candidates[0]
                elif len(candidates) > 1:
                    # tie-break by best ratio on names
                    chosen = max(candidates, key=lambda e: ratio(name_text, nk(e["fullname_raw"])))

            if not chosen:
                if debug:
                    logging.info("[%s][TXT][MISS] '%s' | '%s' (tpid=%s)",
                                 log_prefix, name_text, club_text, self.tpid_text)
                return None

            # duplicate guard
            mk = str(chosen.get("entry_group_id_int") or f"{nk(chosen['fullname_raw'])}|{nk(chosen['clubname_raw'])}")
            if mk in matched_keys:
                return None
            matched_keys.add(mk)

            # consider only the core "TPID + name" tokens for seeding (ignore any trailing junk)
            def token_has_bi(tok: List[Dict[str, Any]]) -> bool:
                fns = {(c.get("fontname") or "").lower() for c in tok}
                if any(("bold" in fn) or ("black" in fn) or ("heavy" in fn) or ("demi" in fn) or ("semibold" in fn) for fn in fns):
                    return True
                if any(not c.get("upright", True) for c in tok):  # italic/oblique
                    return True
                return False

            core_ix = len(self.left_tokens)
            for i, tok in enumerate(self.left_tokens):
                if "(" in tok_text(tok):   # stop before/at the rating parenthesis
                    core_ix = i + 1
                    break
            left_core = self.left_tokens[:max(1, min(2, core_ix))]  # first 1–2 tokens or up to "("
            seedish = any(token_has_bi(t) for t in left_core)

            res = {
                "tournament_player_id_ext": chosen.get("tournament_player_id_ext"),
                "fullname_raw": chosen["fullname_raw"],
                "clubname_raw": chosen["clubname_raw"],
                "group_id_raw": self.group_id,
                "seed_in_group_raw": "__PENDING_SEED__" if seedish else None,
            }

            if debug:
                logging.info(
                    "[%s][TXT][HIT] %s -> %s (pdf='%s'/'%s' db='%s'/'%s') seed=%s",
                    log_prefix,
                    chosen.get("tournament_player_id_ext"),
                    self.group_id,
                    name_text, club_text,
                    chosen["fullname_raw"], chosen["clubname_raw"],
                    seedish,
                )

            return res

    # ----------------------------- PDF parse -----------------------------
    try:
        with pdfplumber.open(pdf_path) as pdf:
            current_group: Optional[str] = None
            y_tol = 2.0
            small_gap = 4.5        # tokenization inside a line
            min_col_gap_frac = 0.18 # ≥18% page width => treat as inter-column gutter

            def page_columns(chars, page_w):
                """Split page chars into left/right columns by finding the largest X gap."""
                if not chars:
                    return [], []
                centers = sorted(( (c["x0"] + c["x1"]) * 0.5 ) for c in chars)
                gaps = []
                for i in range(len(centers) - 1):
                    gaps.append((centers[i+1] - centers[i], (centers[i] + centers[i+1]) * 0.5))
                if not gaps:
                    split_x = page_w * 0.5
                else:
                    max_gap, mid = max(gaps, key=lambda g: g[0])
                    split_x = mid if max_gap >= page_w * min_col_gap_frac else page_w * 0.5
                left = [c for c in chars if (c["x0"] + c["x1"]) * 0.5 <= split_x]
                right = [c for c in chars if (c["x0"] + c["x1"]) * 0.5 > split_x]
                return left, right

            def lines_by_y(_chars):
                """Group chars into y-lines."""
                lines: Dict[int, List[Dict[str, Any]]] = {}
                for ch in sorted(_chars, key=lambda c: (c["top"], c["x0"])):
                    y = int(round(ch["top"]))
                    key = next((k for k in lines if abs(k - y) <= y_tol), y)
                    lines.setdefault(key, []).append(ch)
                return [sorted(lines[k], key=lambda c: c["x0"]) for k in sorted(lines.keys())]

            def tokenize_line(line_chars):
                """Small-gap word tokens left->right."""
                tokens: List[List[Dict[str, Any]]] = []
                cur: List[Dict[str, Any]] = []
                prev = None
                for ch in line_chars:
                    t = ch["text"]
                    if t.isspace():
                        if cur:
                            tokens.append(cur); cur = []
                        prev = ch
                        continue
                    if prev is not None and not prev["text"].isspace():
                        gap = ch["x0"] - prev["x1"]
                        if gap > small_gap and cur:
                            tokens.append(cur); cur = []
                    cur.append(ch)
                    prev = ch
                if cur:
                    tokens.append(cur)
                return tokens

            def split_name_club(tokens):
                """
                Split into left(name+id)/right(club) by largest inter-token gap.
                Always returns (left_tokens, right_tokens, split_x_mid_or_None).
                """
                if not tokens:
                    return [], [], None
                if len(tokens) == 1:
                    # Single wrapped token on its own line; caller will decide side using last split hint.
                    return tokens, [], tokens_center(tokens)

                gaps = []
                for j in range(len(tokens) - 1):
                    left_x1 = max(c["x1"] for c in tokens[j])
                    right_x0 = min(c["x0"] for c in tokens[j + 1])
                    gaps.append((right_x0 - left_x1, j, (left_x1 + right_x0) * 0.5))
                max_gap, split_ix, split_x_mid = max(gaps, key=lambda g: g[0])
                left_tokens = [tok for tok in tokens[: split_ix + 1]]
                right_tokens = [tok for tok in tokens[split_ix + 1 :]]
                return left_tokens, right_tokens, split_x_mid

            for p_index, page in enumerate(pdf.pages, start=1):
                page_chars = [c for c in (page.chars or []) if c.get("text", "").strip()]
                if not page_chars:
                    continue

                w = page.width
                left_chars, right_chars = page_columns(page_chars, w)

                # process each column independently, top->bottom
                for col_index, col_chars in enumerate([left_chars, right_chars], start=1):
                    lines = lines_by_y(col_chars)
                    pending = RowBuilder()
                    in_direct = False  # <--- NEW: inside this column, we may be in a “Direct qualifiers” block

                    # pool -> set(nk(name)) assigned in this column
                    pool_members: Dict[str, Set[str]] = defaultdict(set)

                    for line_chars in lines:
                        raw_line = "".join(c["text"] for c in line_chars).strip()   
                        if not raw_line or SKIP_TITLES.match(raw_line) or FOOTER_NOISE.search(raw_line):
                            continue
                        is_schedule_line = bool(SCHEDULE_HINT_RE.search(raw_line) or DATE_RE.search(raw_line))

                        tokens = tokenize_line(line_chars)
                        header_hit = False

                        # --- NEW 1: “Directly qualified” block header?
                        if DIRECT_QUAL_RE.search(raw_line):
                            # flush any pending row before switching sections
                            if pending.has_any_tokens():
                                res = pending.flush()
                                if res:
                                    order_ix_by_group[res["group_id_raw"]] += 1
                                    res["_ord_ix"] = order_ix_by_group[res["group_id_raw"]] - 1
                                    results.append(res)
                            current_group = "Direct"
                            in_direct = False  # parse the rows under direct qualifiers
                            header_hit = True

                        # --- Existing: explicit two-token header (e.g., "Pulje 10 ...")
                        if not header_hit and len(tokens) >= 2:
                            t0 = tok_text(tokens[0]).lower()
                            t1 = tok_text(tokens[1])
                            if t0 in POOL_WORDS and t1.isdigit():
                                # flush previous row
                                if pending.has_any_tokens():
                                    res = pending.flush()
                                    if res:
                                        order_ix_by_group[res["group_id_raw"]] += 1
                                        res["_ord_ix"] = order_ix_by_group[res["group_id_raw"]] - 1
                                        results.append(res)
                                current_group = canon_group_label(t0, t1)   # “Pool N” in DB
                                in_direct = False                           # leaving “direct” section
                                header_hit = True

                        # --- Existing fallback: header found anywhere on the line (allows noise after number)
                        if not header_hit:
                            m = POOL_RE.search(raw_line)
                            if m:
                                if pending.has_any_tokens():
                                    res = pending.flush()
                                    if res:
                                        order_ix_by_group[res["group_id_raw"]] += 1
                                        res["_ord_ix"] = order_ix_by_group[res["group_id_raw"]] - 1
                                        results.append(res)
                                current_group = canon_group_label("pool", m.group(1))  # normalize
                                in_direct = False
                                header_hit = True

                        if header_hit:
                            if debug:
                                logging.info("[%s][TXT][p%d] header -> %s :: %s",
                                             log_prefix, p_index, current_group or "Direct qualifiers", raw_line)
                            continue

                        # If we’re inside a “Direct qualifiers” section, ignore rows
                        if in_direct:
                            continue

                        if not current_group:
                            # no active pool yet
                            continue

                        # split name/club for this one physical line
                        left_tokens, right_tokens, split_mid = split_name_club(tokens)

                        # === schedule-mode fast path (token-based) ===
                        if current_group and is_schedule_line:
                            line_clean = raw_line
                            line_clean = DATE_RE.sub(" ", line_clean)      # "25/3"
                            line_clean = TIME_RE.sub(" ", line_clean)      # "kl09:00"
                            line_clean = TABLE_RE.sub(" ", line_clean)     # "bord16"
                            line_clean = re.sub(r'(?<!\S)/\s*\d{1,2}(?!\S)', ' ', line_clean)
                            line_clean = re.sub(r'\s*,\s*', ' ', line_clean)
                            line_clean = deglue_name_like(line_clean)

                            line_ws = set(_wtokens(line_clean))

                            hits: List[Dict[str, Any]] = []
                            for e in stage1_entries:
                                # require at least two name tokens to appear on the line
                                name_ws = [w for w in _wtokens(e["fullname_raw"]) if len(w) > 1]
                                if len(name_ws) >= 2 and sum(1 for w in name_ws if w in line_ws) >= 2:
                                    key = nk(e["fullname_raw"])
                                    if key not in pool_members[current_group]:
                                        hits.append(e)

                            if hits:
                                for e in hits[:6]:  # pools have ~3–5 players per line; 6 is safe upper bound
                                    pool_members[current_group].add(nk(e["fullname_raw"]))
                                continue  # line consumed → next physical line
                            else:
                                # schedule with no names: just skip
                                continue

                        # filter noise after schedule check (noise might look like schedule)
                        left_tokens  = [t for t in left_tokens  if not NOISE_RE.search(tok_text(t))]
                        right_tokens = [t for t in right_tokens if not NOISE_RE.search(tok_text(t))]

                        if not left_tokens and not right_tokens:
                            continue

                        # If it's a continuation line with only ONE side present (name OR club),
                        # decide the side by comparing token center to the previous split_x.
                        if (bool(left_tokens) ^ bool(right_tokens)):
                            lone_tokens = left_tokens if left_tokens else right_tokens
                            center = tokens_center(lone_tokens)
                            ref_split = pending._last_split_x or split_mid
                            if ref_split and center:
                                if center > ref_split:
                                    # belongs to CLUB (right)
                                    right_tokens = lone_tokens; left_tokens = []
                                else:
                                    # belongs to NAME (left)
                                    left_tokens = lone_tokens; right_tokens = []

                        # NEW: Detect new row if left (name) present and previous row in progress
                        if left_tokens and pending.has_any_tokens():
                            # Flush previous player
                            res = pending.flush()
                            if res:
                                order_ix_by_group[res["group_id_raw"]] += 1
                                res["_ord_ix"] = order_ix_by_group[res["group_id_raw"]] - 1
                                results.append(res)
                            pending = RowBuilder()

                        # Always add current line to (new or continuing) pending
                        if not pending.group_id:
                            pending.group_id = current_group
                        pending.add_line(left_tokens, right_tokens, split_mid)

                    # flush at end of column
                    if pending.has_any_tokens():
                        res = pending.flush()
                        if res:
                            order_ix_by_group[res["group_id_raw"]] += 1
                            res["_ord_ix"] = order_ix_by_group[res["group_id_raw"]] - 1
                            results.append(res)

                    if pool_members:
                        for g, member_keys in pool_members.items():
                            for nk_name in member_keys:
                                entry = next((e for (nn, e) in stage1_name_index if nn == nk_name), None)
                                if not entry:
                                    continue
                                mk = str(entry.get("entry_group_id_int") or f"{nk(entry['fullname_raw'])}|{nk(entry['clubname_raw'])}")
                                if mk in matched_keys:
                                    continue
                                matched_keys.add(mk)
                                results.append({
                                    "tournament_player_id_ext": entry.get("tournament_player_id_ext"),
                                    "fullname_raw": entry["fullname_raw"],
                                    "clubname_raw": entry["clubname_raw"],
                                    "group_id_raw": g,
                                    "seed_in_group_raw": None,
                                })

    except Exception as e:
        logging.error("[%s] TXT pass failed for %s: %s", log_prefix, str(pdf_path), str(e), exc_info=True)

    if not results:
        _log_unmatched_stage1(stage1_entries, matched_keys, log_prefix, debug)
        return results

    # ----------------------------- assign seed numbers per pool -----------------------------
    results.sort(key=lambda r: (r["group_id_raw"], r.get("_ord_ix", 0)))
    seed_ctr: Dict[str, int] = defaultdict(int)
    assigned = 0
    for r in results:
        if r.get("seed_in_group_raw") == "__PENDING_SEED__":
            g = r["group_id_raw"]
            seed_ctr[g] += 1
            r["seed_in_group_raw"] = str(seed_ctr[g])
            assigned += 1
        r.pop("_ord_ix", None)

    if assigned == 0:
        if debug:
            logging.info("[%s] No bold names in stage-2 PDF → group seeds left NULL.", log_prefix)
    else:
        if debug:
            logging.info("[%s] Group seeds assigned from bold fonts in stage-2 PDF: %d rows.", log_prefix, assigned)

    _log_unmatched_stage1(stage1_entries, matched_keys, log_prefix, debug)
    return results

# # ChatGTP
# def _parse_groups_stage_pdf_using_stage1(
#     pdf_path: Path,
#     stage1_entries: List[Dict[str, Any]],
#     *,
#     log_prefix: str = "STG2",
#     debug: bool = True,
# ) -> List[Dict[str, Any]]:
#     """
#     Parse stage=2 ('Poolförteckning'/'Grupp') using stage-1 index.
#     NEW: robust to (a) two-column pool layouts on the same page and
#     (b) wrapped lines for name and/or club.

#     Returns rows shaped like:
#       {
#         "tournament_player_id_ext": "...",
#         "fullname_raw": "...",
#         "clubname_raw": "...",
#         "group_id_raw": "Pool N",
#         "seed_in_group_raw": "1|2|..." or None
#       }
#     """
#     import pdfplumber, re, unicodedata, difflib
#     from collections import defaultdict

#     debug = True

#     # Accept Pool / Grupp / Pulje  (case-insensitive, with extra text after the number)
#     POOL_WORDS = ("pool", "grupp", "pulje")
#     POOL_RE = re.compile(r'(?i)\b(?:pool|grupp|pulje)\s*(\d+)\b')

#     # “Directly qualified” sections (skip rows until next pool header)
#     DIRECT_QUAL_RE = re.compile(
#         r'(?i)\b('
#         r'direktekvalifi\w+|'        # NO: Direktekvalifisert / -fiserte
#         r'direkte\s+kvalifi\w+|'     # NO/DK: Direkte kvalifisert/kvalificeret
#         r'direkt\w*kvalifi\w+|'      # SV: Direktkvalificerade / -erad
#         r'direct(?:ly)?\s+qualified' # EN (rare)
#         r')\b'
#     )

#     # Titles to ignore entirely
#     SKIP_TITLES = re.compile(
#         r'(?i)^\s*('
#         r'poolförteckning|'          # SV
#         r'pulje\s*oversikt|'         # NO/DK variants
#         r'spelare\s+med|'            # SV
#         r'spillere\s+med|'           # NO/DK
#         r'spelere\s+med|'            # loose variant
#         r'spiller\s+med'             # fallback
#         r')\b.*$'
#     )

#     # Footer junk (incl. “Set: 5, Poäng: 11, Diff: 2” etc.)
#     FOOTER_NOISE = re.compile(
#         r'(?i)(tt\s*coordinator|coordinator\.com|programlicens|'
#         r'sets?\s*:\s*\d|po[aä]ng\s*:\s*\d|diff\s*:\s*\d|'
#         r'tävlingen\s+genomförs|användas\s+vid\s+tävlingar|arrangerade\s+av)'
#     )

#     # ----------------------------- schedule-detection regexes -----------------------------
#     SCHEDULE_HINT_RE = re.compile(r'(?i)\bkl[\s.:]*\d{1,2}[:.]\d{2}\b|\bbord\s*\d+\b|\btable\s*\d+\b')
#     TIME_RE = re.compile(r'(?i)\bkl[\s.:]*\d{1,2}[:.]\d{2}\b')
#     TABLE_RE = re.compile(r'(?i)\bbord\s*\d+\b|\btable\s*\d+\b')
#     SLASHNUM_RE = re.compile(r'^\s*/\s*\d{1,2}\s*$')  # leading "/3" etc.

#     # Tokens that sometimes leak into player lines near footers
#     NOISE_RE = re.compile(
#         r'(?i)(tt\s*coordinator|coordinator\.com|programlicens|tävlingen|genomförs|användas|tävlingar|arrangerade|'
#         r'btk\s*enig|turnering|gjennomføres|progra|lisensen|får|bare|brukes)'
#     )

#     DATE_RE = re.compile(r'\b\d{1,2}/\d{1,2}\b')

#     CLUB_SUFFIXES = ("BTK","PK","BTF","IF","IK","IL","SK","BK","KK","FF","AIS","AIK","IFK","GIF","GF","IBK","TTK")


#     # ----------------------------- helpers -----------------------------

#     def canon_group_label(word: str, num: str) -> str:
#         # Normalize all group labels to “Pool N” for DB consistency
#         return f"Pool {int(num)}"

#     def tokens_center(tokens: List[List[Dict[str, Any]]]) -> Optional[float]:
#         xs = [(c["x0"] + c["x1"]) * 0.5 for tok in tokens for c in tok]
#         return (sum(xs) / len(xs)) if xs else None

#     def has_letters(s: str) -> bool:
#         return any(unicodedata.category(ch).startswith("L") for ch in s or "")

#     def nk(s: str) -> str:
#         from utils import normalize_key
#         return normalize_key(s or "")

#     def strip_zeros(tpid: Optional[str]) -> Optional[str]:
#         if tpid is None:
#             return None
#         t = str(tpid).strip()
#         return t if t == "0" else t.lstrip("0")

#     def ratio(a: str, b: str) -> float:
#         return difflib.SequenceMatcher(a=nk(a), b=nk(b)).ratio()

#     def token_set_ratio(a: str, b: str) -> float:
#         sa = set(nk(a).split()); sb = set(nk(b).split())
#         if not sa or not sb:
#             return 0.0
#         return len(sa & sb) / len(sa | sb)

#     def name_score(a: str, b: str) -> float:
#         return 0.6 * ratio(a, b) + 0.4 * token_set_ratio(a, b)

#     def combined_score(n_a: str, n_b: str, c_a: str, c_b: str) -> float:
#         return 0.7 * name_score(n_a, n_b) + 0.3 * ratio(c_a, c_b)

#     def is_boldish(fontname: str) -> bool:
#         fn = (fontname or "").lower()
#         return ("bold" in fn) or ("black" in fn) or ("heavy" in fn) or ("demi" in fn) or ("semibold" in fn)

#     def is_italicish(fontname: str) -> bool:
#         fn = (fontname or "").lower()
#         return ("italic" in fn) or ("oblique" in fn) or ("slanted" in fn)

#     def tok_text(tok: List[Dict[str, Any]]) -> str:
#         return "".join(c["text"] for c in tok).strip()

#     def deglue_name_like(s: str) -> str:
#         if not s:
#             return s
#         s = re.sub(r'(?<=[a-zåäöøæéèüß])(?=[A-ZÅÄÖØÆÉÈÜß])', ' ', s)
#         s = re.sub(r'([A-ZÅÄÖØÆÉÈÜß]{2,})([A-ZÅÄÖØÆÉÈÜß][a-zåäöøæéèüß\-]+)', r'\1 \2', s)
#         # "THORFINNSS ON Bendik" -> "THORFINNSSON Bendik"
#         s = re.sub(r'\b([A-ZÅÄÖØÆÉÈÜß]{2,})\s+([A-ZÅÄÖØÆÉÈÜß]{2,})(\s+[A-ZÅÄÖØÆÉÈÜß][a-zåäöøæéèüß\-]+)', r'\1\2\3', s)
#         # "SKEIELILAND Erlend" -> "SKEIE LILAND Erlend"
#         s = re.sub(r'([A-ZÅÄÖØÆÉÈÜß]{2,})([A-ZÅÄÖØÆÉÈÜß]{2,})(?=\s+[A-ZÅÄÖØÆÉÈÜß][a-zåäöøæéèüß\-]+)', r'\1 \2', s)
#         return re.sub(r'\s{2,}', ' ', s).strip()


    
#     # --- token helpers for schedule-mode ---
#     WORD_RE = re.compile(r"[A-Za-zÅÄÖØÆÉÈÜßåäöøæéèüß\-]+")

#     def _strip_diacritics(s: str) -> str:
#         import unicodedata
#         return ''.join(ch for ch in unicodedata.normalize('NFKD', s) if not unicodedata.combining(ch))

#     def _wtokens(s: str) -> List[str]:
#         s = s or ""
#         return [_strip_diacritics(w).lower() for w in WORD_RE.findall(s)]



#     # ----------------------------- indices from stage-1 -----------------------------
#     by_tpid: Dict[str, Dict[str, Any]] = {}
#     by_name_club: Dict[Tuple[str, str], Dict[str, Any]] = {}
#     for e in stage1_entries:
#         tpid = e.get("tournament_player_id_ext")
#         fn = e.get("fullname_raw") or ""
#         cl = e.get("clubname_raw") or ""
#         if tpid is not None and str(tpid).strip() != "":
#             by_tpid[str(tpid).strip()] = e
#             no0 = strip_zeros(str(tpid))
#             if no0 and no0 not in by_tpid:
#                 by_tpid[no0] = e
#         by_name_club[(nk(fn), nk(cl))] = e

#     if debug:
#         logging.info("[%s] stage1 index built: TPIDs=%d, nameclub=%d", log_prefix, len(by_tpid), len(by_name_club))

#     # Precompute normalized names for substring lookup (longest first)
#     stage1_name_index = sorted(
#         [(nk(e["fullname_raw"]), e) for e in stage1_entries],
#         key=lambda x: len(x[0]),
#         reverse=True
#     )

#     results: List[Dict[str, Any]] = []
#     matched_keys: Set[str] = set()
#     order_ix_by_group: Dict[str, int] = defaultdict(int)

#     # ----------------------------- inner builders -----------------------------
#     class RowBuilder:
#         """Accumulates multi-line name/club for one player row inside a pool."""
#         def __init__(self):
#             self.left_tokens: List[List[Dict[str, Any]]] = []
#             self.right_tokens: List[List[Dict[str, Any]]] = []
#             self.left_fonts: Set[str] = set()
#             self.has_italic_left: bool = False
#             self.tpid_text: Optional[str] = None  # extracted once (first line usually)
#             self.group_id: Optional[str] = None
#             self._last_split_x: Optional[float] = None  # hint for continuation side choice

#         def add_line(self, left_tokens: List[List[Dict[str, Any]]], right_tokens: List[List[Dict[str, Any]]], split_hint_x: Optional[float]):
#             if left_tokens:
#                 # capture TPID (even if glued)
#                 left_chars = [c for tok in left_tokens for c in tok]
#                 i = 0
#                 while i < len(left_chars) and left_chars[i]["text"].isdigit():
#                     i += 1
#                 # Guard: if the next char is '/', it's a date like "25/3" → NOT a TPID
#                 if i < len(left_chars) and left_chars[i]["text"] == "/":
#                     i = 0  # cancel TPID detection
#                 if self.tpid_text is None and 0 < i <= 5:
#                     self.tpid_text = "".join(ch["text"] for ch in left_chars[:i])
#                 # fonts for seeding decision
#                 for tok in left_tokens:
#                     for c in tok:
#                         fn = (c.get("fontname") or "").lower()
#                         self.left_fonts.add(fn)
#                         if not c.get("upright", True):
#                             self.has_italic_left = True
#                 self.left_tokens.extend(left_tokens)

#             if right_tokens:
#                 self.right_tokens.extend(right_tokens)

#             if split_hint_x:
#                 self._last_split_x = split_hint_x

#         def has_any_tokens(self) -> bool:
#             return bool(self.left_tokens or self.right_tokens)

#         def _join_left_text(self) -> str:
#             s = " ".join("".join(c["text"] for c in tok).strip() for tok in self.left_tokens).strip()
#             s = re.sub(r'^\s*/\s*\d{1,2}\s+', '', s)  # drop leading "/3", "/5", ...
#             j = 0
#             while j < len(s) and s[j].isdigit():
#                 j += 1
#             return deglue_name_like(s[j:].strip())


#         def _join_right_text(self) -> str:
#             raw = " ".join("".join(c["text"] for c in tok).strip() for tok in self.right_tokens).strip()
#             m_suffix = re.match(rf"^(.*?)(?:{'|'.join(CLUB_SUFFIXES)})$", raw)
#             if m_suffix:
#                 for suf in sorted(CLUB_SUFFIXES, key=len, reverse=True):
#                     if raw.endswith(suf) and not raw.endswith(" " + suf):
#                         raw = raw[: -len(suf)].rstrip() + " " + suf
#                         break
#             return deglue_name_like(raw)


#         def flush(self) -> Optional[Dict[str, Any]]:
#             if not self.has_any_tokens():
#                 return None

#             name_text = self._join_left_text()
#             club_text = self._join_right_text()
#             if not has_letters(name_text) or not has_letters(club_text):
#                 return None
#             if SLASHNUM_RE.match(name_text) or TIME_RE.search(name_text) or TABLE_RE.search(name_text):
#                 return None

#             # choose entry (TPID first, then fuzzy)
#             chosen = None
#             if self.tpid_text:
#                 for cand in (self.tpid_text, strip_zeros(self.tpid_text)):
#                     if cand and cand in by_tpid:
#                         chosen = by_tpid[cand]; break
#             if not chosen:
#                 # fuzzy against stage-1
#                 best_score = -1.0; best_entry = None
#                 for (nk_name, nk_club), entry in by_name_club.items():
#                     score = combined_score(name_text, nk_name, club_text, nk_club)
#                     if score > best_score:
#                         best_score, best_entry = score, entry
#                 if best_entry and best_score >= 0.78:
#                     chosen = best_entry

#             # Fallback: compare names with spaces/hyphens removed (helps UPPER+UPPER glues)
#             if not chosen:
#                 ns_pdf = re.sub(r'[\s\-]+', '', nk(name_text))
#                 nk_club_text = nk(club_text)
#                 for (nk_name, nk_club), entry in by_name_club.items():
#                     if nk_club != nk_club_text:
#                         continue
#                     ns_stage1 = re.sub(r'[\s\-]+', '', nk_name)
#                     if ns_pdf == ns_stage1 or ns_pdf in ns_stage1 or ns_stage1 in ns_pdf:
#                         chosen = entry
#                         break

#             # Fallback: same club + name substring match (handles residual spacing weirdness)
#             if not chosen:
#                 nk_name_pdf = nk(name_text)
#                 nk_club_pdf = nk(club_text)
#                 candidates = []
#                 for (nk_name_stg1, nk_club_stg1), entry in by_name_club.items():
#                     if nk_club_stg1 != nk_club_pdf:
#                         continue
#                     if nk_name_stg1 in nk_name_pdf or nk_name_pdf in nk_name_stg1:
#                         candidates.append(entry)
#                 if len(candidates) == 1:
#                     chosen = candidates[0]
#                 elif len(candidates) > 1:
#                     # tie-break by best ratio on names
#                     chosen = max(candidates, key=lambda e: ratio(name_text, nk(e["fullname_raw"])))


#             if not chosen:
#                 if debug:
#                     logging.info("[%s][TXT][MISS] '%s' | '%s' (tpid=%s)",
#                                 log_prefix, name_text, club_text, self.tpid_text)
#                 return None


#             # duplicate guard
#             mk = str(chosen.get("entry_group_id_int") or f"{nk(chosen['fullname_raw'])}|{nk(chosen['clubname_raw'])}")
#             if mk in matched_keys:
#                 return None
#             matched_keys.add(mk)

#             # consider only the core "TPID + name" tokens for seeding (ignore any trailing junk)
#             def token_has_bi(tok: List[Dict[str, Any]]) -> bool:
#                 fns = { (c.get("fontname") or "").lower() for c in tok }
#                 if any(("bold" in fn) or ("black" in fn) or ("heavy" in fn) or ("demi" in fn) or ("semibold" in fn) for fn in fns):
#                     return True
#                 if any(not c.get("upright", True) for c in tok):  # italic/oblique
#                     return True
#                 return False

#             core_ix = len(self.left_tokens)
#             for i, tok in enumerate(self.left_tokens):
#                 if "(" in tok_text(tok):   # stop before/at the rating parenthesis
#                     core_ix = i + 1
#                     break
#             left_core = self.left_tokens[:max(1, min(2, core_ix))]  # first 1–2 tokens or up to "("
#             seedish = any(token_has_bi(t) for t in left_core)


#             res = {
#                 "tournament_player_id_ext": chosen.get("tournament_player_id_ext"),
#                 "fullname_raw": chosen["fullname_raw"],
#                 "clubname_raw": chosen["clubname_raw"],
#                 "group_id_raw": self.group_id,
#                 "seed_in_group_raw": "__PENDING_SEED__" if seedish else None,
#             }

#             if debug:
#                 logging.info(
#                     "[%s][TXT][HIT] %s -> %s (pdf='%s'/'%s' db='%s'/'%s') seed=%s",
#                     log_prefix,
#                     chosen.get("tournament_player_id_ext"),
#                     self.group_id,
#                     name_text, club_text,
#                     chosen["fullname_raw"], chosen["clubname_raw"],
#                     seedish,
#                 )

#             return res

#     # ----------------------------- PDF parse -----------------------------
#     try:
#         with pdfplumber.open(pdf_path) as pdf:
#             current_group: Optional[str] = None
#             y_tol = 2.0
#             small_gap = 4.5        # tokenization inside a line
#             min_col_gap_frac = 0.18 # ≥18% page width => treat as inter-column gutter

#             def page_columns(chars, page_w):
#                 """Split page chars into left/right columns by finding the largest X gap."""
#                 if not chars:
#                     return [], []
#                 centers = sorted(( (c["x0"] + c["x1"]) * 0.5 ) for c in chars)
#                 gaps = []
#                 for i in range(len(centers) - 1):
#                     gaps.append((centers[i+1] - centers[i], (centers[i] + centers[i+1]) * 0.5))
#                 if not gaps:
#                     split_x = page_w * 0.5
#                 else:
#                     max_gap, mid = max(gaps, key=lambda g: g[0])
#                     split_x = mid if max_gap >= page_w * min_col_gap_frac else page_w * 0.5
#                 left = [c for c in chars if (c["x0"] + c["x1"]) * 0.5 <= split_x]
#                 right = [c for c in chars if (c["x0"] + c["x1"]) * 0.5 > split_x]
#                 return left, right

#             def lines_by_y(_chars):
#                 """Group chars into y-lines."""
#                 lines: Dict[int, List[Dict[str, Any]]] = {}
#                 for ch in sorted(_chars, key=lambda c: (c["top"], c["x0"])):
#                     y = int(round(ch["top"]))
#                     key = next((k for k in lines if abs(k - y) <= y_tol), y)
#                     lines.setdefault(key, []).append(ch)
#                 return [sorted(lines[k], key=lambda c: c["x0"]) for k in sorted(lines.keys())]

#             def tokenize_line(line_chars):
#                 """Small-gap word tokens left->right."""
#                 tokens: List[List[Dict[str, Any]]] = []
#                 cur: List[Dict[str, Any]] = []
#                 prev = None
#                 for ch in line_chars:
#                     t = ch["text"]
#                     if t.isspace():
#                         if cur:
#                             tokens.append(cur); cur = []
#                         prev = ch
#                         continue
#                     if prev is not None and not prev["text"].isspace():
#                         gap = ch["x0"] - prev["x1"]
#                         if gap > small_gap and cur:
#                             tokens.append(cur); cur = []
#                     cur.append(ch)
#                     prev = ch
#                 if cur:
#                     tokens.append(cur)
#                 return tokens

#             def split_name_club(tokens):
#                 """
#                 Split into left(name+id)/right(club) by largest inter-token gap.
#                 Always returns (left_tokens, right_tokens, split_x_mid_or_None).
#                 """
#                 if not tokens:
#                     return [], [], None
#                 if len(tokens) == 1:
#                     # Single wrapped token on its own line; caller will decide side using last split hint.
#                     return tokens, [], tokens_center(tokens)

#                 gaps = []
#                 for j in range(len(tokens) - 1):
#                     left_x1 = max(c["x1"] for c in tokens[j])
#                     right_x0 = min(c["x0"] for c in tokens[j + 1])
#                     gaps.append((right_x0 - left_x1, j, (left_x1 + right_x0) * 0.5))
#                 max_gap, split_ix, split_x_mid = max(gaps, key=lambda g: g[0])
#                 left_tokens = [tok for tok in tokens[: split_ix + 1]]
#                 right_tokens = [tok for tok in tokens[split_ix + 1 :]]
#                 return left_tokens, right_tokens, split_x_mid

#             for p_index, page in enumerate(pdf.pages, start=1):
#                 page_chars = [c for c in (page.chars or []) if c.get("text", "").strip()]
#                 if not page_chars:
#                     continue

#                 w = page.width
#                 left_chars, right_chars = page_columns(page_chars, w)

#                 # process each column independently, top->bottom
#                 for col_index, col_chars in enumerate([left_chars, right_chars], start=1):
#                     lines = lines_by_y(col_chars)
#                     pending = RowBuilder()
#                     in_direct = False  # <--- NEW: inside this column, we may be in a “Direct qualifiers” block

#                     # pool -> set(nk(name)) assigned in this column
#                     pool_members: Dict[str, Set[str]] = defaultdict(set)

#                     for line_chars in lines:
#                         raw_line = "".join(c["text"] for c in line_chars).strip()   
#                         if not raw_line or SKIP_TITLES.match(raw_line) or FOOTER_NOISE.search(raw_line):
#                             continue
#                         is_schedule_line = bool(SCHEDULE_HINT_RE.search(raw_line) or DATE_RE.search(raw_line))

#                         tokens = tokenize_line(line_chars)
#                         header_hit = False

#                         # --- NEW 1: “Directly qualified” block header?
#                         if DIRECT_QUAL_RE.search(raw_line):
#                             # flush any pending row before switching sections
#                             if pending.has_any_tokens():
#                                 res = pending.flush()
#                                 if res:
#                                     order_ix_by_group[res["group_id_raw"]] += 1
#                                     res["_ord_ix"] = order_ix_by_group[res["group_id_raw"]] - 1
#                                     results.append(res)
#                             current_group = None
#                             in_direct = True         # everything until next Pool/Pulje/Grupp is ignored
#                             header_hit = True

#                         # --- Existing: explicit two-token header (e.g., "Pulje 10 ...")
#                         if not header_hit and len(tokens) >= 2:
#                             t0 = tok_text(tokens[0]).lower()
#                             t1 = tok_text(tokens[1])
#                             if t0 in POOL_WORDS and t1.isdigit():
#                                 # flush previous row
#                                 if pending.has_any_tokens():
#                                     res = pending.flush()
#                                     if res:
#                                         order_ix_by_group[res["group_id_raw"]] += 1
#                                         res["_ord_ix"] = order_ix_by_group[res["group_id_raw"]] - 1
#                                         results.append(res)
#                                 current_group = canon_group_label(t0, t1)   # “Pool N” in DB
#                                 in_direct = False                           # leaving “direct” section
#                                 header_hit = True

#                         # --- Existing fallback: header found anywhere on the line (allows noise after number)
#                         if not header_hit:
#                             m = POOL_RE.search(raw_line)
#                             if m:
#                                 if pending.has_any_tokens():
#                                     res = pending.flush()
#                                     if res:
#                                         order_ix_by_group[res["group_id_raw"]] += 1
#                                         res["_ord_ix"] = order_ix_by_group[res["group_id_raw"]] - 1
#                                         results.append(res)
#                                 current_group = canon_group_label("pool", m.group(1))  # normalize
#                                 in_direct = False
#                                 header_hit = True

#                         if header_hit:
#                             if debug:
#                                 logging.info("[%s][TXT][p%d] header -> %s :: %s",
#                                             log_prefix, p_index, current_group or "Direct qualifiers", raw_line)
#                             continue

#                         # If we’re inside a “Direct qualifiers” section, ignore rows
#                         if in_direct:
#                             continue

#                         if not current_group:
#                             # no active pool yet
#                             continue

#                         # ... keep your existing split_name_club / new-row-or-continuation logic unchanged ...


#                         # split name/club for this one physical line
#                         left_tokens, right_tokens, split_mid = split_name_club(tokens)

#                         # === schedule-mode fast path (token-based) ===
#                         if current_group and is_schedule_line:
#                             line_clean = raw_line
#                             line_clean = DATE_RE.sub(" ", line_clean)      # "25/3"
#                             line_clean = TIME_RE.sub(" ", line_clean)      # "kl09:00"
#                             line_clean = TABLE_RE.sub(" ", line_clean)     # "bord16"
#                             line_clean = re.sub(r'(?<!\S)/\s*\d{1,2}(?!\S)', ' ', line_clean)
#                             line_clean = re.sub(r'\s*,\s*', ' ', line_clean)
#                             line_clean = deglue_name_like(line_clean)

#                             line_ws = set(_wtokens(line_clean))

#                             # --- DEBUG TAP: shows why we match or not (first N lines per page are enough)
#                             logging.info("[%s][SCHED][%s] line='%s' tokens=%s",
#                                         log_prefix, current_group, line_clean, sorted(list(line_ws))[:20])

#                             hits: List[Dict[str, Any]] = []
#                             for e in stage1_entries:
#                                 name_ws = [w for w in _wtokens(e["fullname_raw"]) if len(w) > 1]
#                                 if not name_ws:
#                                     continue
#                                 need = 2 if len(name_ws) >= 2 else 1  # allow single-token names like "Stian"
#                                 common = sum(1 for w in name_ws if w in line_ws)
#                                 if common >= need:
#                                     key = nk(e["fullname_raw"])
#                                     if key not in pool_members[current_group]:
#                                         hits.append(e)
#                                         logging.info("[%s][SCHED][HIT] %-25s  name_tokens=%s  common=%d",
#                                                     log_prefix, e["fullname_raw"][:25], name_ws, common)

#                             if hits:
#                                 for e in hits[:6]:  # pools ~3–5 players per line; 6 safe
#                                     pool_members[current_group].add(nk(e["fullname_raw"]))
#                                 continue  # line consumed → next line




#                         left_tokens  = [t for t in left_tokens  if not NOISE_RE.search(tok_text(t))]
#                         right_tokens = [t for t in right_tokens if not NOISE_RE.search(tok_text(t))]

#                         # determine whether this is a new row or continuation
#                         # New row if left_tokens starts with digits (TPID) or glued digits
#                         is_new_row = False
#                         if left_tokens:
#                             first_tok = tok_text(left_tokens[0])
#                             if re.fullmatch(r"\d{1,5}", first_tok):
#                                 is_new_row = True
#                             else:
#                                 lt_chars = [c for t in left_tokens for c in t]
#                                 if lt_chars and lt_chars[0]["text"].isdigit():
#                                     is_new_row = True

#                         # If it's a continuation line with only ONE side present (name OR club),
#                         # decide the side by comparing token center to the previous split_x.
#                         if not is_new_row and (bool(left_tokens) ^ bool(right_tokens)):
#                             lone_tokens = left_tokens if left_tokens else right_tokens
#                             center = tokens_center(lone_tokens)
#                             ref_split = pending._last_split_x or split_mid
#                             if ref_split and center:
#                                 if center > ref_split:
#                                     # belongs to CLUB (right)
#                                     right_tokens = lone_tokens; left_tokens = []
#                                 else:
#                                     # belongs to NAME (left)
#                                     left_tokens = lone_tokens; right_tokens = []


#                         if is_new_row:
#                             # flush previous
#                             if pending.has_any_tokens():
#                                 res = pending.flush()
#                                 if res:
#                                     order_ix_by_group[res["group_id_raw"]] += 1
#                                     res["_ord_ix"] = order_ix_by_group[res["group_id_raw"]] - 1
#                                     results.append(res)
#                                 pending = RowBuilder()

#                             pending.group_id = current_group
#                             pending.add_line(left_tokens, right_tokens, split_mid)
#                         else:
#                             # continuation: decide which side(s) to append
#                             if not pending.group_id:
#                                 pending.group_id = current_group
#                             # If both sides present, just add both; if only one, add to that side.
#                             if left_tokens or right_tokens:
#                                 pending.add_line(left_tokens, right_tokens, split_mid)
#                             else:
#                                 # nothing recognizable on this physical line
#                                 continue

#                     # flush at end of column
#                     if pending.has_any_tokens():
#                         res = pending.flush()
#                         if res:
#                             order_ix_by_group[res["group_id_raw"]] += 1
#                             res["_ord_ix"] = order_ix_by_group[res["group_id_raw"]] - 1
#                             results.append(res)

#                     if pool_members:
#                         for g, member_keys in pool_members.items():
#                             for nk_name in member_keys:
#                                 entry = next((e for (nn, e) in stage1_name_index if nn == nk_name), None)
#                                 if not entry:
#                                     continue
#                                 mk = str(entry.get("entry_group_id_int") or f"{nk(entry['fullname_raw'])}|{nk(entry['clubname_raw'])}")
#                                 if mk in matched_keys:
#                                     continue
#                                 matched_keys.add(mk)
#                                 results.append({
#                                     "tournament_player_id_ext": entry.get("tournament_player_id_ext"),
#                                     "fullname_raw": entry["fullname_raw"],
#                                     "clubname_raw": entry["clubname_raw"],
#                                     "group_id_raw": g,
#                                     "seed_in_group_raw": None,
#                                 })

#     except Exception as e:
#         logging.error("[%s] TXT pass failed for %s: %s", log_prefix, str(pdf_path), str(e), exc_info=True)

#     if not results:
#         _log_unmatched_stage1(stage1_entries, matched_keys, log_prefix, debug)
#         return results

#     # ----------------------------- assign seed numbers per pool -----------------------------
#     results.sort(key=lambda r: (r["group_id_raw"], r.get("_ord_ix", 0)))
#     seed_ctr: Dict[str, int] = defaultdict(int)
#     assigned = 0
#     for r in results:
#         if r.get("seed_in_group_raw") == "__PENDING_SEED__":
#             g = r["group_id_raw"]
#             seed_ctr[g] += 1
#             r["seed_in_group_raw"] = str(seed_ctr[g])
#             assigned += 1
#         r.pop("_ord_ix", None)

#     if assigned == 0:
#         if debug:
#             logging.info("[%s] No bold names in stage-2 PDF → group seeds left NULL.", log_prefix)
#     else:
#         if debug:
#             logging.info("[%s] Group seeds assigned from bold fonts in stage-2 PDF: %d rows.", log_prefix, assigned)

#     _log_unmatched_stage1(stage1_entries, matched_keys, log_prefix, debug)
#     return results


def _log_unmatched_stage1(stage1_entries, matched_keys: Set[str], log_prefix: str, debug: bool) -> None:
    if not debug:
        return
    from utils import normalize_key
    missing = []
    for e in stage1_entries:
        mk = str(e.get("entry_group_id_int") or f"{normalize_key(e['fullname_raw'])}|{normalize_key(e['clubname_raw'])}")
        if mk not in matched_keys:
            missing.append(f"{e.get('tournament_player_id_ext')} {e['fullname_raw']} / {e['clubname_raw']}")
    if missing:
        if debug:
            logging.info("[%s] Unmatched stage-1 entries (%d): %s", log_prefix, len(missing), "; ".join(missing))
    else:
        if debug:
            logging.info("[%s] All stage-1 entries matched into groups.", log_prefix)
