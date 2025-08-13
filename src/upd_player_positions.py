# src/upd_player_participant_positions.py

import io, re, logging, datetime, requests, pdfplumber
import time

from db import get_conn
from utils import print_db_insert_results
from config import (
    SCRAPE_CLASS_PARTICIPANTS_MAX_CLASSES,
    SCRAPE_CLASS_PARTICIPANTS_CLASS_ID_EXT,
    SCRAPE_CLASS_PARTICIPANTS_ORDER,
)
from models.club import Club
from models.player import Player
from models.tournament_class import TournamentClass
from models.player_participant import PlayerParticipant

RESULTS_URL_TMPL = "https://resultat.ondata.se/ViewClassPDF.php?classID={class_id}&stage=6"

_PLACERING_HDR_RE  = re.compile(r"^\s*(placering|placeringar|plassering|plasseringer|sijoitus|sijoitukset|position|positions?|placement|placements?|results?|ranking)\s*$", re.IGNORECASE)
_PLACERING_LINE_RE = re.compile(r"^\s*(\d+)\s+(.+?)\s*,\s*(.+?)\s*$")

def upd_player_positions():
    
    conn, cur = get_conn()
    t0 = time.perf_counter()

    # 1) classes via cache_by_id_ext (fast single-class path)
    classes_by_ext = TournamentClass.cache_by_id_ext(cur)

    # ── 1) Load + filter classes (by external id) ──────────────────────────
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

    logging.info(f"ℹ️  Updating tournament class positions for {len(classes)} classes...")
    print(f"ℹ️  Updating tournament class positions for {len(classes)} classes...")

    # ── 2) Build static caches once ────────────────────────────────────────
    club_map              = Club.cache_name_map(cur)
    player_name_map       = Player.cache_name_map(cur)            # verified + aliases
    unverified_name_map   = Player.cache_unverified_name_map(cur) # fullname_raw for unverified
    part_by_class_player  = PlayerParticipant.cache_by_class_id_player(cur)  # class → {player_id: (participant_id, club_id)}

    # ── 3) Process classes ────────────────────────────────────────────────
    total_parsed = total_updated = total_skipped = 0
    db_results = []  # for print_db_insert_results()
    warnings = []

    for idx, tc in enumerate(classes, 1):
        label = f"{tc.shortname or tc.longname or tc.tournament_class_id} (ext:{tc.tournament_class_id_ext})"
        # logging.info(f"[{idx}/{len(classes)}] Class {label}  date={tc.date}")
        # print(f"ℹ️  [{idx}/{len(classes)}] Class {label}  date={tc.date}")

        if not tc or not tc.tournament_class_id_ext:
            logging.warning(f"  ↳ Skipping: Missing class {tc} or external class_id {tc.tournament_class_id_ext}")
            db_results.append({
                "status": "skipped", 
                "reason": "Missing class or external class_id", 
                "warnings": ""
            })
            continue

        class_part_by_player = part_by_class_player.get(tc.tournament_class_id, {})
        if not class_part_by_player:
            logging.warning(f"  ↳ Skipping: No participants in class_id={tc.tournament_class_id}")
            db_results.append({
                "status": "skipped", 
                "reason": "No participants in class_id", 
                "warnings": ""
            })
            continue

        # clear old positions for safe re-runs
        cleared = PlayerParticipant.clear_final_positions_for_class(cur, tc.tournament_class_id)
        # logging.info(f"  ↳ Cleared final_position for {cleared} participants (class_id={tc.tournament_class_id})")
        # print(f"  ↳ Cleared final_position for {cleared} participants (class_id={tc.tournament_class_id})")

        # Download stage=6 PDF
        url = RESULTS_URL_TMPL.format(class_id=tc.tournament_class_id_ext)
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
        except Exception as e:
            reason = f"Download failed: {e}"
            logging.error(f"  ↳ {reason}")
            print(f"❌ {reason}")
            db_results.append({
                "status": "failed", 
                "reason": reason, 
                "warnings": ""
            })
            conn.commit()  # commit the clear
            continue

        # Parse stage=6 PDF
        try:
            rows = _parse_positions(_pdf_to_lines(r.content))
        except Exception as e:
            reason = f"PDF parsing failed: {e}"
            logging.error(f"  ↳ {reason}")
            print(f"❌ {reason}")
            db_results.append({
                "status": "failed",
                "reason": reason,
                "warnings": ""
            })
            conn.commit()  # commit the clear
            continue

        parsed_count = len(rows)        # expected = number of lines (ties allowed)
        total_parsed += parsed_count
        # logging.info(f"  ↳ Parsed {parsed_count} positions from PDF.")
        # print(f"✅ Parsed {parsed_count}/{parsed_count} positions for class {tc.shortname or tc.longname or tc.tournament_class_id} (class_ext={tc.tournament_class_id_ext} in tournament id {tc.tournament_id})")

        if parsed_count == 0:
            db_results.append({
                "status": "skipped", 
                "reason": "No positions parsed", 
                "warnings": ""
            })
            conn.commit()
            continue

        # Update loop
        updated = skipped = 0
        for pos, fullname, club in rows:
            ok = PlayerParticipant.upd_final_position_by_participant(
                cur,
                tc.tournament_class_id,
                fullname,
                club,
                pos,
                player_name_map,
                unverified_name_map,
                class_part_by_player,
                club_map
            )
            if ok:
                updated += 1
                db_results.append({
                    "status": "success",
                    "reason": "Final position updated",
                    "warnings": ""
                })
            else:
                skipped += 1
                db_results.append({
                    "status": "skipped",
                    "reason": "No participant match (name/club or not in class)",
                    "warnings": ""
                })

        conn.commit()
        total_updated += updated
        total_skipped += skipped
        logging.info(f"  ✅ Updated {updated}, skipped {skipped} for {label}")
        print(      f"   ✅ Updated: {updated}   ⏭️  Skipped: {skipped}   ❌ Failed: 0")
        
    # ── 4) Totals + DB summary ────────────────────────────────────────────
    t1 = time.perf_counter()
    elapsed = t1 - t0
    print(f"ℹ️  Positions update complete in {elapsed:.2f}s")
    print(f"ℹ️  Total positions parsed: {total_parsed}/{total_parsed}")
    logging.info(f"Done. Total parsed={total_parsed}, updated={total_updated}, skipped={total_skipped}")

    print_db_insert_results(db_results)
    conn.close()

def _pdf_to_lines(pdf_bytes: bytes):
    lines = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines.extend(text.splitlines())
    return lines

def _parse_positions(lines):
    out = []
    try:
        start_idx = next(i for i, ln in enumerate(lines) if _PLACERING_HDR_RE.search(ln))
    except StopIteration:
        logging.warning("Placering header not found in PDF.")
        return out
    for ln in lines[start_idx + 1:]:
        s = (ln or "").strip()
        if not s:
            continue
        if s.lower().startswith("setsiffror"):
            break
        m = _PLACERING_LINE_RE.match(s)
        if m:
            out.append((int(m.group(1)), m.group(2).strip(), m.group(3).strip()))
    return out

