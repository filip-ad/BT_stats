# src/upd_participant_positions.py

import io
import re
import time
from typing import List, Tuple
import requests
import pdfplumber
import logging
import traceback
import random

from db import get_conn
from models.participant_player import ParticipantPlayer
from utils import OperationLogger, parse_date, name_keys_for_lookup_all_splits, normalize_key
from config import (
    SCRAPE_PARTICIPANTS_CUTOFF_DATE,
    SCRAPE_PARTICIPANTS_CLASS_ID_EXTS, 
    SCRAPE_PARTICIPANTS_ORDER,
    SCRAPE_PARTICIPANTS_MAX_CLASSES,
    SCRAPE_PARTICIPANTS_TNMT_ID_EXTS
)
from models.club import Club
from models.player import Player
from models.tournament_class import TournamentClass
from models.participant import Participant
from models.player_license import PlayerLicense  # NEW: For license cache in match_player

RESULTS_URL_TMPL = "https://resultat.ondata.se/ViewClassPDF.php?classID={class_id}&stage={stage}"

def upd_player_positions():
    conn, cursor = get_conn()
    t0 = time.perf_counter()
    logger = OperationLogger(
        verbosity       = 2, 
        print_output    = False, 
        log_to_db       = True, 
        cursor          = cursor
    )

    cutoff_date = parse_date(SCRAPE_PARTICIPANTS_CUTOFF_DATE) if SCRAPE_PARTICIPANTS_CUTOFF_DATE else None
    if cutoff_date is None and SCRAPE_PARTICIPANTS_CUTOFF_DATE not in (None, "0"):
        logger.failed("global", "Invalid cutoff date format")
        print("❌ Invalid cutoff date format")
        return

    # 1) Load and filter classes (now with structure_id filter)
    classes = TournamentClass.get_filtered_classes(
        cursor,
        class_id_exts=SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
        tournament_id_exts=SCRAPE_PARTICIPANTS_TNMT_ID_EXTS,
        data_source_id=1 if (SCRAPE_PARTICIPANTS_CLASS_ID_EXTS or SCRAPE_PARTICIPANTS_TNMT_ID_EXTS) else None,
        cutoff_date=cutoff_date,
        require_ended=True,
        allowed_type_ids=[1],  # Singles only (type_id 1)
        allowed_structure_ids=[1, 2, 3],  # KO only, Group Only, Group + KO
        max_classes=SCRAPE_PARTICIPANTS_MAX_CLASSES,
        order=SCRAPE_PARTICIPANTS_ORDER
    )

    if not classes:
        logger.skipped("global", "No valid singles classes matching filters")
        print("⚠️  No valid singles classes matching filters.")
        return

    print(f"ℹ️  Filtered to {len(classes)} valid singles classes{' via specific IDs (class or tournament, overriding cutoff)' if (SCRAPE_PARTICIPANTS_CLASS_ID_EXTS or SCRAPE_PARTICIPANTS_TNMT_ID_EXTS) else f' after cutoff date: {cutoff_date or "none"}'}.")

    logging.info(f"Updating tournament class positions for {len(classes)} classes...")
    print(f"ℹ️  Updating tournament class positions for {len(classes)} classes...")

    # 2) Build static caches
    club_map                    = Club.cache_name_map(cursor)
    part_by_class_player        = Participant.cache_by_class_player(cursor)

    # 3) Process classes
    total_parsed = total_updated = total_skipped = 0
    for idx, tc in enumerate(classes, 1):

        label = f"{tc.shortname or tc.longname}, {tc.date} (id: {tc.tournament_class_id}, ext:{tc.tournament_class_id_ext})"
        item_key = f"Class id ext: {tc.tournament_class_id_ext}"

        if not tc or not tc.tournament_class_id_ext:
            logger.skipped(item_key, f"Skipping: Missing class or external class_id")
            continue

        class_part_by_player = part_by_class_player.get(tc.tournament_class_id, {})
        if not class_part_by_player:
            logger.skipped(item_key, f"Skipping: No participants in class_id")
            print(f"ℹ️  [{idx}/{len(classes)}] Skipping class {label} - no participants found.")
            continue

        # Clear old positions
        cleared = Participant.clear_final_positions(cursor, tc.tournament_class_id)
        logging.info(f"Cleared final_position for {cleared} participants (class_id={tc.tournament_class_id})")

        # # Download PDF
        # stage = 4 if tc.tournament_class_structure_id == 2 else 6
        # url = RESULTS_URL_TMPL.format(class_id=tc.tournament_class_id_ext, stage=stage)
        # try:
        #     r = requests.get(url, timeout=30)
        #     r.raise_for_status()
        # except Exception as e:
        #     reason = f"Download failed: {e}"
        #     logger.failed(item_key, reason)
        #     conn.commit()
        #     continue

        # # Parse PDF
        # try:
        #     if tc.tournament_class_structure_id == 2:
        #         rows = _parse_group_positions(r.content)
        #     else:
        #         rows = _parse_positions(r.content)
        # except Exception as e:
        #     stack_trace = traceback.format_exc()
        #     logger.failed(item_key, f"PDF parsing failed: {e}\nStack trace:\n{stack_trace}")
        #     print(f"❌ PDF parsing failed for {label}: {e}")
        #     conn.commit()
        #     continue

        # Download PDF with retry/backoff
        stage = 4 if tc.tournament_class_structure_id == 2 else 6
        url = RESULTS_URL_TMPL.format(class_id=tc.tournament_class_id_ext, stage=stage)

        pdf_bytes = fetch_pdf(url)
        if not pdf_bytes:
            logger.failed(item_key, f"Download failed after retries: {url}")
            conn.commit()
            continue

        # Parse PDF
        try:
            if tc.tournament_class_structure_id == 2:
                rows = _parse_group_positions(pdf_bytes)
            else:
                rows = _parse_positions(pdf_bytes)
        except Exception as e:
            stack_trace = traceback.format_exc()
            logger.failed(item_key, f"PDF parsing failed")
            print(f"❌ PDF parsing failed for {label}: {e}")
            conn.commit()
            continue

        parsed_count = len(rows)
        total_parsed += parsed_count
        logging.info(f"Parsed {parsed_count} positions from PDF.")

        if parsed_count == 0:
            logger.failed(item_key, "No positions parsed from PDF.")
            conn.commit()
            continue

        roster_index = Participant.build_class_roster_index(cursor, tc.tournament_class_id)
       
        updated = skipped = 0
        for pos, fullname, club_raw in rows:
            # 1) Resolve club_id from the PDF club name (already in your code)
            club = Club.resolve(cursor, club_raw, club_map, logger, item_key, allow_prefix=True, fallback_to_unknown=False)
            item_key = (pos, fullname, club_raw)
            if not club:
                logger.skipped(item_key, f"Skipping position {pos}: Club not found for '{club_raw}'")
                logger.warning(item_key, f"Skipping position: Club not found")
                skipped += 1
                continue
            club_id = club.club_id

            # 2) Find the participant in THIS class by name (+prefer exact club match)
            participant_id = Participant.find_participant_for_class_by_name_club(
                roster_index=roster_index,
                fullname=fullname,
                club_id=club_id,
                club_map=club_map,
            )

            if participant_id is None:
                # Try again without club (e.g., club not set on participant_player); still bounded to the class roster
                participant_id = Participant.find_participant_for_class_by_name_club(
                    roster_index=roster_index,
                    fullname=fullname,
                    club_id=None,
                    club_map=club_map,
                )

            if participant_id is None:
                logger.skipped(item_key, f"Skipping position: No unique match for 'name / club' in class roster")
                logger.warning(item_key, f"Skipping position: No unique match for 'name / club' in class roster")
                skipped += 1
                continue

            # 3) Update position
            result = Participant.update_final_position(cursor, participant_id, pos)
            if result["status"] == "success":
                logger.success(item_key, f"Position updated successfully")
                updated += 1
            else:
                logger.skipped(item_key, f"Position update skipped: {result.get('reason')}")
                skipped += 1

        logging.info(f"[{idx}/{len(classes)}] Processed class {label}. Cleared {cleared} positions, parsed {parsed_count}, updated {updated}, skipped {skipped}.")
        print(f"ℹ️  [{idx}/{len(classes)}] Processed class {label}. Cleared {cleared} positions, parsed {parsed_count}, updated {updated}, skipped {skipped}.")
        conn.commit()
        total_updated += updated
        total_skipped += skipped

    # 4) Summary
    t1 = time.perf_counter()
    elapsed = t1 - t0
    logging.info(f"Positions update complete in {elapsed:.2f}s")
    logging.info(f"Total positions parsed: {total_parsed}, updated: {total_updated}, skipped: {total_skipped}")
    logger.summarize()
    conn.close()

def _parse_positions(pdf_bytes: bytes) -> List[Tuple[int, str, str]]:
    """Parse positions from PDF, attempting table extraction first, then falling back to text."""
    out = []
    _PLACERING_HDR_RE = re.compile(r"^\s*(placering|placeringar|plassering|plasseringer|sijoitus|sijoitukset|position|positions?|placement|placements?|results?|ranking)\s*$", re.IGNORECASE)
    _PLACERING_LINE_RE = re.compile(r"^\s*(\d+)\s+(.+?)\s*,\s*(.+?)\s*$")

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            # Try table extraction
            tables = page.extract_tables()
            for table in tables:
                for row in table[1:]:  # Skip header
                    if len(row) >= 3 and row[0] and row[0].strip().isdigit():
                        try:
                            out.append((int(row[0]), row[1].strip(), row[2].strip()))
                        except (ValueError, AttributeError):
                            continue
            
            # Note: I recommend removing the if out: return out to collect from all pages/tables, similar to groups, but that's optional.
            # if out:
            #     return out  # Return if table extraction succeeded

            # Fallback to text parsing
            text = page.extract_text() or ""
            lines = text.splitlines()
            try:
                start_idx = next(i for i, ln in enumerate(lines) if _PLACERING_HDR_RE.search(ln))
            except StopIteration:
                OperationLogger().warning("Placering header not found in PDF.")
                return out
            for ln in lines[start_idx + 1:]:
                s = (ln or "").strip()
                if not s or s.lower().startswith("setsiffror"):
                    break
                m = _PLACERING_LINE_RE.match(s)
                if m:
                    try:
                        out.append((int(m.group(1)), m.group(2).strip(), m.group(3).strip()))
                    except ValueError:
                        continue
    return out

def _parse_group_positions(pdf_bytes: bytes) -> List[Tuple[int, str, str]]:
    """Parse positions from groups-only PDF (stage=4), handling combined pos/name/club in first cell and multiple groups."""
    out = []
    # Local regexes for groups-only
    GROUP_HDR_RE = re.compile(r"poolresultat|gruppresultat", re.IGNORECASE)
    GROUP_LINE_RE = re.compile(r"^\s*(?:[\d/.-]+\s+)*(\d+)\s+(.+?)\s*,\s*(.+?)(?:\s+[\d/.-].*)?$", re.IGNORECASE)
    
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            # Try table extraction (collect all, no early return)
            tables = page.extract_tables()
            # logging.info(f"Found {len(tables)} tables on page {page.page_number}")
            for table in tables:
                for row in table[1:]:  # Skip header
                    if not row or not row[0]:
                        continue
                    
                    s = row[0].strip()
                    # logging.info(f"Processing row first cell: '{s}'")
                    
                    # Try combined format first (e.g., "10 1 Name, Club")
                    m = GROUP_LINE_RE.match(s)
                    if m:
                        try:
                            logging.info(f"Processing combined format: '{s}'")
                            pos = int(m.group(1))
                            name = m.group(2).strip()
                            club_raw = m.group(3).strip()
                            out.append((pos, name, club_raw))
                            # logging.info(f" out: {out}")
                            continue  # Success, skip separate check
                        except ValueError:
                            pass
                    
                    # Fall back to separate columns (pos in row[0], name in row[1], club in row[2])
                    if len(row) >= 3 and row[0].strip().isdigit():
                        try:
                            pos = int(row[0].strip())
                            name = row[1].strip()
                            club_raw = row[2].strip()
                            out.append((pos, name, club_raw))
                        except ValueError:
                            pass

            # Fallback to text parsing
            text = page.extract_text() or ""
            lines = text.splitlines()
            try:
                start_idx = next(i for i, ln in enumerate(lines) if GROUP_HDR_RE.search(ln))
                # logging.info(f"Found results header at line {start_idx}: '{lines[start_idx]}'")
            except StopIteration:
                # logging.info(f"No results header found on page {page.page_number}")
                continue
            parsed_from_text = 0
            for ln in lines[start_idx + 1:]:
                s = (ln or "").strip()
                if not s:
                    continue  # Skip empty lines
                if s.lower().startswith("setsiffror"):
                    break
                m = GROUP_LINE_RE.match(s)
                if m:
                    try:
                        pos = int(m.group(1))
                        name = m.group(2).strip()
                        club_raw = m.group(3).strip()
                        out.append((pos, name, club_raw))
                        parsed_from_text += 1
                        # logging.info(f"Parsed from text: pos={pos}, name='{name}', club='{club_raw}'")
                    except ValueError:
                        logging.info(f"Failed to parse int pos from text line: '{s}'")
                        continue
    return out


def fetch_pdf(url: str, retries: int = 3, timeout: int = 30) -> bytes | None:
    """Download a PDF with exponential backoff and jitter."""
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