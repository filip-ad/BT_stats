import io
import re
import time
from typing import List, Tuple
import requests
import pdfplumber
import logging

from db import get_conn
from utils import OperationLogger
from config import (
    SCRAPE_PARTICIPANTS_MAX_CLASSES,
    SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
    SCRAPE_PARTICIPANTS_ORDER,
)
from models.club import Club
from models.player import Player
from models.tournament_class import TournamentClass
from models.participant import Participant

RESULTS_URL_TMPL = "https://resultat.ondata.se/ViewClassPDF.php?classID={class_id}&stage=6"

_PLACERING_HDR_RE = re.compile(r"^\s*(placering|placeringar|plassering|plasseringer|sijoitus|sijoitukset|position|positions?|placement|placements?|results?|ranking)\s*$", re.IGNORECASE)
_PLACERING_LINE_RE = re.compile(r"^\s*(\d+)\s+(.+?)\s*,\s*(.+?)\s*$")

def upd_player_positions():
    conn, cursor = get_conn()
    t0 = time.perf_counter()
    logger = OperationLogger(
        verbosity       = 2, 
        print_output    = False, 
        log_to_db       = True, 
        cursor          = cursor
    )

    # 1) Load and filter classes
    classes = TournamentClass.get_filtered_classes(
        cursor,
        class_id_ext=SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
        max_classes=SCRAPE_PARTICIPANTS_MAX_CLASSES,
        order=SCRAPE_PARTICIPANTS_ORDER
    )
    logging.info(f"Updating tournament class positions for {len(classes)} classes...")
    print(f"ℹ️  Updating tournament class positions for {len(classes)} classes...")

    # 2) Build static caches
    club_map                = Club.cache_name_map(cursor)
    player_name_map         = Player.cache_name_map_verified(cursor)
    unverified_name_map     = Player.cache_name_map_unverified(cursor)
    part_by_class_player    = Participant.cache_by_class_player(cursor)

    # 3) Process classes
    total_parsed = total_updated = total_skipped = 0
    for idx, tc in enumerate(classes, 1):

        item_key = f"Class id ext: {tc.tournament_class_id_ext}"

        label = f"{tc.shortname or tc.longname or tc.tournament_class_id}, {tc.date} (ext:{tc.tournament_class_id_ext})"


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

        # Download PDF
        url = RESULTS_URL_TMPL.format(class_id=tc.tournament_class_id_ext)
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
        except Exception as e:
            reason = f"Download failed: {e}"
            logger.failed(item_key, reason)
            conn.commit()
            continue

        # Parse PDF
        try:
            rows = _parse_positions(r.content)
        except Exception as e:
            logger.failed(item_key, f"PDF parsing failed: {e}")
            print(f"❌ PDF parsing failed: {e}")
            conn.commit()
            continue

        parsed_count = len(rows)
        total_parsed += parsed_count
        logging.info(f"Parsed {parsed_count} positions from PDF.")

        if parsed_count == 0:
            logger.failed(item_key, "No positions parsed from PDF.")
            conn.commit()
            continue

        # Update positions
        updated = skipped = 0
        for pos, fullname, club in rows:
            result = Participant.update_final_position(
                cursor, tc.tournament_class_id, fullname, club, pos,
                player_name_map, unverified_name_map, class_part_by_player, club_map
            )
            if result["status"] == "success":
                logger.success(item_key, f"Position successfully updated")
                updated += 1
            else:
                logger.skipped(item_key, f"Position update skipped")
                skipped += 1

        logging.info(f"[{idx}/{len(classes)}] Processed class {label} date={tc.date}. Deleted {cleared} existing positions, updated {updated} positions, skipped {skipped} positions.")
        print(f"ℹ️  [{idx}/{len(classes)}] Processed class {label} date={tc.date}. Deleted {cleared} existing positions, updated {updated} positions, skipped {skipped} positions.")

        conn.commit()
        total_updated += updated
        total_skipped += skipped
        logging.info(f"Updated {updated}, skipped {skipped} for {label}")

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
            if out:
                return out  # Return if table extraction succeeded

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