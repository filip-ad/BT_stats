# src/upd_tournament_participants.py

import logging
import requests
import pdfplumber
import re
from io import BytesIO
from datetime import datetime, date
import time
from db import get_conn
from utils import print_db_insert_results, sanitize_name, normalize_key, parse_date
from models.player_license import PlayerLicense
from models.club import Club
from models.tournament_class import TournamentClass
from config import SCRAPE_CLASS_PARTICIPANTS_MAX_CLASSES, SCRAPE_CLASS_PARTICIPANTS_CLASS_ID_EXT, SCRAPE_CLASS_PARTICIPANTS_ORDER
from models.tournament_participant import TournamentParticipant
from models.player import Player

PDF_BASE = "https://resultat.ondata.se/ViewClassPDF.php"

def upd_tournament_participants():
    """
    Populate tournament_class_participant by downloading each class's 'stage=1' PDF.
    """
    overall_start = time.perf_counter()

    conn, cursor = get_conn()
    logging.info("Starting participant update…")
    print("ℹ️  Updating tournament class participants…")

    # 1a) Load all classes
    classes = TournamentClass.cache_all(cursor)

    # 🔎 Filter: only singles for now
    classes = [tc for tc in classes if (tc.type_id == 1)]

    if not classes:
        logging.warning("No singles classes found (type='singles').")
        print("⚠️  No singles classes found (type='singles'). Did you run upd_tournament_classes() to classify types?")
        conn.close()
        return

    # 1b) If config says “only this one external ID”, filter to it
    if SCRAPE_CLASS_PARTICIPANTS_CLASS_ID_EXT != 0:
        wanted = SCRAPE_CLASS_PARTICIPANTS_CLASS_ID_EXT
        classes = [tc for tc in classes if tc.tournament_class_id_ext == wanted]
        if not classes:
            logging.error(f"No class found with external ID {wanted}")
            print(f"❌ No tournament class with external ID {wanted}")
            conn.close()
            return

    # 1c) Otherwise, sort by date (newest/oldest) if configured
    else:
        order = (SCRAPE_CLASS_PARTICIPANTS_ORDER or "").lower()
        if order == "newest":
            # most recent dates first
            classes.sort(key=lambda tc: tc.date or date.min, reverse=True)
        elif order == "oldest":
            # earliest dates first
            classes.sort(key=lambda tc: tc.date or date.min)
        elif order:
            logging.warning(f"Unknown SCRAPE_CLASS_PARTICIPANTS_ORDER='{order}', skipping sort")

        # then optionally limit to the first N (0 or negative → no limit)
        max_n = SCRAPE_CLASS_PARTICIPANTS_MAX_CLASSES or 0
        if max_n > 0:
            classes = classes[:max_n]

    # 2) Build lookup caches exactly once
    club_map                    = Club.cache_name_map(cursor)
    license_name_club_map       = PlayerLicense.cache_name_club_map(cursor)
    player_name_map             = Player.cache_name_map(cursor)
    player_unverified_name_map  = Player.cache_unverified_name_map(cursor)

    results = []
    total_parsed   = 0
    total_expected = 0

    for tc in classes:
        if not tc.tournament_class_id_ext:
            logging.warning(f"Skipping class {tc.tournament_class_id}: no external class ID")
            continue

        # — Download phase —
        t1 = time.perf_counter()
        try: 
            pdf_url    = f"{PDF_BASE}?classID={tc.tournament_class_id_ext}&stage=1"
            pdf_bytes  = download_pdf(pdf_url)
        except Exception as e:
            logging.error(f"❌ PDF download failed for class_ext={tc.tournament_class_id_ext}: {e}")
            results.append({
                "status": "failed",
                "key":    tc.tournament_class_id_ext,
                "reason": f"PDF download failed: {e}"
            })
            continue
        t2 = time.perf_counter()

        if not pdf_bytes:
            logging.error(f"❌ PDF download failed for class_ext={tc.tournament_class_id_ext}")
            results.append({
                "status": "failed",
                "key":    tc.tournament_class_id_ext,
                "reason": "PDF download failed"
            })
            continue

        # — Parse phase —
        t3 = time.perf_counter()
        try: 
            participants, expected_count = parse_players_pdf(pdf_bytes)
            total_parsed += len(participants)
            if expected_count is not None:
                total_expected += expected_count
        except Exception as e:
            logging.error(f"❌ PDF parsing failed for class_ext={tc.tournament_class_id_ext}: {e}")
            results.append({
                "status": "failed",
                "key":    tc.tournament_class_id_ext,
                "reason": f"PDF parsing failed: {e}"
            })
            continue
        t4 = time.perf_counter()

        # Wipe out any old participants for this class:
        deleted = PlayerParticipant.remove_for_class(cursor, tc.tournament_class_id)
        logging.info(f"Deleted {deleted} old participants for class (ext_id: {tc.tournament_class_id_ext}, id: {tc.tournament_class_id})")
        print(f"ℹ️  Deleted {deleted} old participants for class (ext_id: {tc.tournament_class_id_ext}, id: {tc.tournament_class_id})")

        # If we didn’t get exactly the expected number, save for later review
        if expected_count is not None and len(participants) != expected_count:
            missing = expected_count - len(participants)
            cursor.execute("""
                INSERT OR IGNORE INTO player_participant_missing (
                    tournament_class_id,
                    tournament_class_id_ext,
                    participant_url,
                    nbr_of_missing_players
                )
                VALUES (?, ?, ?, ?)
            """, (
                tc.tournament_class_id,
                tc.tournament_class_id_ext,
                pdf_url,
                missing
            ))
            logging.warning(
                f"{tc.shortname}: parsed {len(participants)}/{expected_count}, "
                "recording in player_participant_missing"
            )

        # # normalize class_date
        # class_date = (
        #     tc.date if isinstance(tc.date, date)
        #     else datetime.fromisoformat(tc.date).date()
        # )

        class_date = parse_date(tc.date, context="upd_player_participants.py")

        # — DB insert & collect results —
        t5 = time.perf_counter()
        class_results = []
        for idx, raw in enumerate(participants, start=1):
            raw_name        = raw["raw_name"]
            club_name       = raw["club_name"].strip()
            seed_val        = raw.get("seed")  
            t_ptcp_id_ext   = raw.get("tournament_participant_id_ext")

            pp = PlayerParticipant.from_dict({
                "tournament_class_id":              tc.tournament_class_id,
                "tournament_participant_id_ext":    t_ptcp_id_ext,
                "fullname_raw":                     raw_name,
                "club_name_raw":                    club_name,
                "seed":                             seed_val
            })
            result = pp.save_to_db(
                cursor,
                class_date,
                club_map,
                license_name_club_map,
                player_name_map,
                player_unverified_name_map
            )
            # attach idx & raw/club for later logging
            result.update({"idx": idx, "raw_name": raw_name, "club_name": club_name})
            class_results.append(result)
            results.append(result)  # only here, not again later

            # — only extract IDs for success/skipped rows, never for failures —
            if result["status"] != "failed":
                # stash the club_id
                result["club_id"] = pp.club_id

                # for canonical participants
                if result.get("category") == "canonical":
                    result["player_id"] = pp.player_id

                # for raw fallbacks
                elif result.get("category") == "raw":
                    # key is "raw_<raw_id>"
                    raw_part = result["key"].split("_", 1)[1]
                    result["raw_player_id"] = int(raw_part)

        t6 = time.perf_counter()

        # ── Config: True to log only FAILED main lines, False to include all ──
        LOG_ONLY_FAILED = True

        # ── 1) Sort by idx ──
        sorted_results = sorted(class_results, key=lambda r: r["idx"])

        # ── 2) Print parsed vs expected count ──
        parsed  = len(participants)
        exp_cnt = expected_count or parsed
        icon    = "✅" if parsed == exp_cnt else "❌"
        print(f"{icon} Parsed {parsed}/{exp_cnt} participants for class {tc.shortname} "
            f"(class_ext={tc.tournament_class_id_ext} in tournament id {tc.tournament_id})")

        # ── 3) Emit each row ──
        for r in sorted_results:

            # 2a) Build core fields
            idx  = f"{r['idx']:02d}"
            st   = r["status"].upper().ljust(7)
            name = r["raw_name"]
            club = r["club_name"]
            cid  = r.get("club_id","?")

            # pick the right ID tag
            if "player_id" in r:
                idtag = f" pid:{r['player_id']}"
            elif "raw_player_id" in r:
                idtag = f" rpid:{r['raw_player_id']}"
            else:
                idtag = ""

            # 2b) Main line
            line = (
                f"[{idx}] {st} {name}, {club} "
                f"[cid:{cid}{idtag}]     {r['reason']}"
            )

            # 2c) Append ambiguous‐candidate list if this is that case
            if r["status"] == "failed" and "candidates" in r:
                cand_str = ", ".join(
                    f"{c['player_id']}:{c.get('name','<no-name>')}"
                    for c in r["candidates"]
                )
                line += f" [candidates={cand_str}]"

            if not LOG_ONLY_FAILED or r["status"] == "failed":
                # 3) Log & print main line
                if r["status"] == "failed":
                    logging.error(line)
                else:
                    logging.info(line)
                # print(line)

            # 4) Always emit warnings
            for w in r.get("warnings", []):
                warn_line = f"[{idx}] WARNING {st} {name}, {club} [cid:{cid}{idtag}]     {w}"
                logging.warning(warn_line)
                # print(warn_line)

            # ── 4) Per‐class summary ──
        inserted = sum(1 for r in class_results if r["status"] == "success")
        skipped  = sum(1 for r in class_results if r["status"] == "skipped")
        failed   = sum(1 for r in class_results if r["status"] == "failed")
        print(f"   ✅ Inserted: {inserted}   ⏭️  Skipped: {skipped}   ❌ Failed: {failed}")

      # — overall commit & summary —
    conn.commit()
    total = time.perf_counter() - overall_start
    print(f"ℹ️  Participant update complete in {total:.2f}s")
    logging.info(f"Total update took {total:.2f}s")
    # overall parsed vs expected
    if total_expected > 0:
        print(f"ℹ️  Total participants parsed: {total_parsed}/{total_expected}")
    else:
        print(f"ℹ️  Total participants parsed: {total_parsed}")
    print_db_insert_results(results)
    conn.close()

def download_pdf(url: str, retries: int = 3, timeout: int = 30) -> bytes | None:
    """
    Download a PDF with retry logic.
    """
    headers = {"User-Agent": "Mozilla/5.0 (compatible; BTstats/1.0)"}
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except requests.exceptions.Timeout:
            logging.warning(f"Timeout fetching {url} (attempt {attempt}/{retries})")
            time.sleep(2)
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed ({url}): {e}")
            break
    return None

def extract_columns(page):
    """
    Split a page into left/right halves so we can catch two-column layouts.
    """
    w, h = page.width, page.height
    left   = page.crop((0,    0,   w/2, h)).extract_text() or ""
    right  = page.crop((w/2,  0,   w,   h)).extract_text() or ""
    return left, right

def parse_players_pdf(
        pdf_bytes: bytes
    ) -> tuple[list[dict], int | None]:
    """
    Extract participant entries from PDF.
    Returns: (participants, expected_count)
    where each participant is {"raw_name": str, "club_name": str, "seed": Optional[int]}.
    """
    participants: list[dict] = []
    unique_entries: set[tuple[str, str]] = set()

    # --- constants/regex ---
    HEADER_RE = re.compile(
        r'\b\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2})?\s*\(\s*(\d+)\s+[^\d()]+\)',
        re.I,
    )

    PART_RE = re.compile(
        r'^\s*(?P<tpid>\d{1,3})\s+(?P<name>[^,]+?)\s*,\s*(?P<club>\S.*)$',
        re.MULTILINE
    )    

    BOLD_RE = re.compile(r"(bold|black|heavy|demi|semibold|semi-bold|sb)\b", re.I)

    CATEGORY_TOKENS = {"vet", "veteran", "junior", "pojkar", "flickor", "herrar", "damer"}

    def _strip_above_header(block: str) -> tuple[str, int | None]:
        m = HEADER_RE.search(block)
        if m:
            return block[m.end():], int(m.group(1))
        return block, None

    full_text_blocks: list[str] = []
    bold_name_keys: set[str] = set()
    expected_count: int | None = None

    pdf = pdfplumber.open(BytesIO(pdf_bytes))
    try:
        for page in pdf.pages:
            # ---- text blocks (handle two columns) ----
            left, right = extract_columns(page)
            for raw_block in (left, right):
                # remove sections you already cut elsewhere
                block = re.sub(r'Directly qualified.*$', "", raw_block, flags=re.M)
                block = re.sub(r'Group stage.*$',        "", block, flags=re.M)

                # cut everything above the per-block header (kills titles)
                block, cnt = _strip_above_header(block)
                if expected_count is None and cnt is not None:
                    expected_count = cnt

                if block:
                    full_text_blocks.append(block)

            # ---- bold name detection for seeding ----
            words = page.extract_words(
                use_text_flow=True,
                keep_blank_chars=False,
                extra_attrs=["fontname"]
            )
            if not words:
                continue

            # cluster tokens by line (y top within tolerance)
            lines: dict[int, list[dict]] = {}
            tol = 2
            for w in words:
                t = int(round(w["top"]))
                key = next((k for k in lines.keys() if abs(k - t) <= tol), t)
                lines.setdefault(key, []).append(w)

            for _, line_words in lines.items():
                # reconstruct "name before comma"
                name_tokens = []
                for w in line_words:
                    txt = w["text"]
                    if "," in txt:
                        before = txt.split(",", 1)[0].strip()
                        if before:
                            name_tokens.append(before)
                        break
                    else:
                        name_tokens.append(txt)
                if not name_tokens:
                    continue

                # drop leading index like "12"
                if name_tokens and re.fullmatch(r"\d+", name_tokens[0]):
                    name_tokens = name_tokens[1:]

                name_candidate = " ".join(name_tokens).strip()

                # must contain letters to be a plausible name
                if not re.search(r"[A-Za-zÅÄÖåäöØøÆæÉéÈèÜüß]", name_candidate):
                    continue

                any_bold = any(
                    (BOLD_RE.search((w.get("fontname") or "")) is not None)
                    for w in line_words[: max(1, len(name_tokens))]
                )
                if any_bold:
                    bold_name_keys.add(normalize_key(sanitize_name(name_candidate)))
    finally:
        pdf.close()

    # ---- parse participants from the cleaned blocks ----
    seed_counter = 1
    for block in full_text_blocks:
        for line in block.splitlines():
            line = line.strip()
            if not line:
                continue
            m = PART_RE.match(line)
            if not m:
                continue

            tournament_participant_id_ext   = m.group('tpid') or None  # preserves leading zeros
            raw_name                        = sanitize_name(m.group('name'))
            club_name                       = m.group('club').strip()

            # skip obvious class/category words accidentally in "club" position
            if club_name.lower() in CATEGORY_TOKENS:
                continue

            key = (raw_name, club_name)
            if key in unique_entries:
                continue
            unique_entries.add(key)

            is_seeded = normalize_key(raw_name) in bold_name_keys
            seed_val = seed_counter if is_seeded else None
            if is_seeded:
                # logging.info(f"Seed {seed_val}: {raw_name}")
                seed_counter += 1

            participants.append({
                "raw_name":                         raw_name,
                "club_name":                        club_name,
                "seed":                             seed_val,
                "tournament_participant_id_ext":    tournament_participant_id_ext
            })

    logging.info(f"Parsed {len(participants)} unique participant entries (seeded: {seed_counter-1})")
    return participants, expected_count
