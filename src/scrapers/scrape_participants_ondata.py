# src/scrapers/scrape_participants_ondata.py
from utils import _download_pdf_ondata_by_tournament_class_and_stage
from models.tournament_class import TournamentClass
from models.participant_raw_tournament import ParticipantRawTournament
from utils import OperationLogger, parse_date
from config import SCRAPE_PARTICIPANTS_CUTOFF_DATE, SCRAPE_PARTICIPANTS_MAX_CLASSES
import pdfplumber
import re
import unicodedata
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any

def scrape_participants_ondata(cursor, include_positions: bool = True) -> List[TournamentClass]:
    """Scrape and populate raw participant data from PDFs for filtered tournament classes.
    Returns the list of processed TournamentClass instances.
    """
    logger = OperationLogger(verbosity=2, print_output=False, log_to_db=True, cursor=cursor)
    cutoff_date = parse_date(SCRAPE_PARTICIPANTS_CUTOFF_DATE)

    classes = TournamentClass.get_filtered_classes(
        cursor,
        data_source_id=1,
        cutoff_date=cutoff_date,
        require_ended=True,
        allowed_type_ids=[1],
        max_classes=SCRAPE_PARTICIPANTS_MAX_CLASSES,
        order="newest"
    )

    if not classes:
        logger_keys = {'global': 'true'}
        logger.skipped(logger_keys, "No valid singles classes matching filters")
        return []

    for tc in classes:
        logger_keys = {
            'tournament_class_id': str(tc.tournament_class_id or 'N/A'),
            'tournament_class_id_ext': str(tc.tournament_class_id_ext or 'N/A'),
            'tournament_id': str(tc.tournament_id or 'N/A')
        }

        # Remove existing raw data for this class
        deleted_count = ParticipantRawTournament.remove_for_class(cursor, tc.tournament_class_id_ext)
        if deleted_count > 0:
            logger.info(logger_keys, f"Removed {deleted_count} existing raw participants", to_console=False)

        # Download and parse initial participants (stage=1)
        pdf_path, was_downloaded, message = _download_pdf_ondata_by_tournament_class_and_stage(tc.tournament_id_ext, tc.tournament_class_id_ext, 1)
        if message:
            if "Cached" in message:
                logger.info(logger_keys, message)
            elif "Downloaded" in message:
                logger.success(logger_keys, message)
            else:
                logger.failed(logger_keys, message)
                continue

        if pdf_path:
            participants, expected_count = _parse_initial_participants_pdf(pdf_path)
            if not participants:
                logger.warning(logger_keys, "No participants parsed from initial PDF")
                continue

            # Insert raw participants
            for participant_data in participants:
                participant_data.update({
                    "tournament_id_ext": tc.tournament_id_ext,
                    "tournament_class_id_ext": tc.tournament_class_id_ext
                })
                raw_participant = ParticipantRawTournament.from_dict(participant_data)
                if raw_participant.validate():
                    raw_participant.save_to_db(cursor)
            logger.success(logger_keys, f"Inserted {len(participants)} raw participants")

            # Handle final positions if requested
            if include_positions:
                final_stage = tc.get_final_stage()
                if final_stage is None:
                    logger.warning(logger_keys, "No valid final stage determined")
                else:
                    final_pdf_path, downloaded, message = _download_pdf(tc.tournament_id_ext, tc.tournament_class_id_ext, final_stage)
                    if message:
                        if "Cached" in message:
                            logger.info(logger_keys, message)
                        elif "Downloaded" in message:
                            logger.success(logger_keys, message)
                        else:
                            logger.warning(logger_keys, message)
                            continue

                    if final_pdf_path:
                        positions = _parse_final_positions_pdf(final_pdf_path)
                        if positions:
                            # Update existing raw rows with positions (simple match by name/club for now)
                            for pos_data in positions:
                                pos_data.update({
                                    "tournament_id_ext": tc.tournament_id_ext,
                                    "tournament_class_id_ext": tc.tournament_class_id_ext
                                })
                                cursor.execute(
                                    """
                                    UPDATE participant_raw_tournament
                                    SET final_position_raw = ?
                                    WHERE tournament_class_id_ext = ? AND fullname_raw = ? AND clubname_raw = ?
                                    """,
                                    (pos_data["final_position_raw"], tc.tournament_class_id_ext, pos_data["fullname_raw"], pos_data["clubname_raw"])
                                )
                            logger.success(logger_keys, f"Updated {len(positions)} raw participants with final positions")
                        else:
                            logger.warning(logger_keys, "No positions parsed from final PDF")

    logger.summarize()
    return classes

def _parse_initial_participants_pdf(pdf_path: Path) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """Parse initial participant data (names, clubs, seeds) from a stage=1 PDF.
    Returns (participants, expected_count) where participants is a list of dicts with raw data.
    """
    participants = []
    expected_count = None
    unique_entries = set()

    # Regexes
    EXPECTED_PARTICIPANT_COUNT_RE = re.compile(r'(?:\b\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2})?\s*)?\(\s*(\d+)\s+[^\d()]+\)', re.I)
    PART_WITH_ID_RE = re.compile(r'^\s*(?P<tpid>\d{1,5})\s+(?P<name>[^,]+?)\s*,\s*(?P<club>\S.*\S)\s*$', re.M)
    PART_NO_ID_RE = re.compile(r'^\s*(?P<name>[^,]+?)\s*,\s*(?P<club>\S.*\S)\s*$', re.M)
    PART_WIDE_SPACES_RE = re.compile(r'^\s*(?:(?P<tpid>\d{1,5})\s+)?(?P<name>[^\s,].*?[^\s,])\s{2,}(?P<club>.+\S)\s*$', re.M)
    BOLD_RE = re.compile(r"(bold|black|heavy|demi|semibold|semi-bold|sb)\b", re.I)
    PLACEHOLDER_NAME_RE = re.compile(r"\b(vakant|vacant|reserv|reserve)\b", re.I)

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                words = page.extract_words(use_text_flow=True, keep_blank_chars=False, extra_attrs=["fontname"])
                if not words:
                    continue

                # Cluster words into lines
                lines = {}
                tol = 2
                for wd in words:
                    t = int(round(wd["top"]))
                    key = next((k for k in lines if abs(k - t) <= tol), t)
                    lines.setdefault(key, []).append(wd)

                # Detect expected count from header
                for line_key in sorted(lines.keys()):
                    line_words = lines[line_key]
                    line_text = ' '.join(wd['text'] for wd in line_words)
                    m = EXPECTED_PARTICIPANT_COUNT_RE.search(line_text)
                    if m and expected_count is None:
                        expected_count = int(m.group(1))
                        break

                # Parse participant lines
                for line_key in sorted(lines.keys()):
                    line_words = lines[line_key]
                    name_tokens = []
                    for wd in line_words:
                        txt = wd["text"]
                        if "," in txt:
                            before_comma = txt.split(",", 1)[0].strip()
                            if before_comma:
                                name_tokens.append(before_comma)
                            break
                        name_tokens.append(txt)
                    if not name_tokens:
                        continue

                    raw_name = " ".join(name_tokens).strip()
                    if not re.search(r"[A-Za-zÅÄÖåäöØøÆæÉéÈèÜüß]", raw_name):
                        continue

                    # Match line patterns
                    line_text = page.extract_text().splitlines()[line_key // tol]  # Approximate line
                    m = PART_WITH_ID_RE.match(line_text) or PART_NO_ID_RE.match(line_text) or PART_WIDE_SPACES_RE.match(line_text)
                    if not m:
                        continue

                    tpid = m.groupdict().get('tpid')
                    tournament_participant_id_ext = tpid.strip() if tpid else None
                    raw_name = m.group('name').strip()
                    club_name = m.group('club').strip()

                    # Skip placeholders and invalid formats
                    if PLACEHOLDER_NAME_RE.search(raw_name):
                        continue
                    if len(raw_name) < 3 or not any(unicodedata.category(ch).startswith("L") for ch in club_name):
                        continue

                    key = (raw_name.lower(), club_name.lower())
                    if key in unique_entries:
                        continue
                    unique_entries.add(key)

                    # Detect seeds via bold
                    is_seeded = any(BOLD_RE.search(wd.get("fontname") or "") for wd in line_words[:max(1, len(name_tokens))])
                    seed_raw = str(len(unique_entries)) if is_seeded else None

                    participants.append({
                        "tournament_id_ext": "",
                        "tournament_class_id_ext": "",
                        "data_source_id": 1,
                        "fullname_raw": raw_name,
                        "clubname_raw": club_name,
                        "seed_raw": seed_raw,
                        "final_position_raw": None,
                        "tournament_participant_id_ext": tournament_participant_id_ext
                    })

    except Exception as e:
        print(f"Error parsing PDF {pdf_path}: {e}")
        return [], None

    return participants, expected_count

def _parse_final_positions_pdf(pdf_path: Path) -> List[Dict[str, Any]]:
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
                        "tournament_id_ext": "",
                        "tournament_class_id_ext": "",
                        "data_source_id": 1,
                        "fullname_raw": raw_name,
                        "clubname_raw": club_name,
                        "final_position_raw": position_raw,
                        "tournament_participant_id_ext": None  # Can be enhanced with TPID if present
                    })

    except Exception as e:
        print(f"Error parsing PDF {pdf_path} for positions: {e}")
        return []

    return positions