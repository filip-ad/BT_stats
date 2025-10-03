# scrapers/scrape_tournament_class_knockout_matches_ondata.py

from __future__ import annotations
import io, re, logging
from typing import List, Dict, Any, Tuple
import pdfplumber

from utils import (
    parse_date,
    OperationLogger,
    _download_pdf_ondata_by_tournament_class_and_stage,
)
from config import (
    SCRAPE_PARTICIPANTS_MAX_CLASSES,
    SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
    SCRAPE_PARTICIPANTS_TNMT_ID_EXTS,
    SCRAPE_PARTICIPANTS_ORDER,
    SCRAPE_PARTICIPANTS_CUTOFF_DATE,
)
from models.tournament import Tournament
from models.tournament_class import TournamentClass
from models.tournament_class_match_raw import TournamentClassMatchRaw

def scrape_tournament_class_knockout_matches_ondata(cursor, run_id=None):
    """
    Parse KO (stage=5) bracket PDFs from OnData and write raw rows.
    One DB row per KO match; tournament_class_stage_id set per round (R16/QF/SF/F etc).
    """
    logger = OperationLogger(
        verbosity           = 2,
        print_output        = False,
        log_to_db           = True,
        cursor              = cursor,
        object_type         = "tournament_class_match_raw",
        run_type            = "scrape",
        run_id              = run_id
    )

    cutoff_date = parse_date(SCRAPE_PARTICIPANTS_CUTOFF_DATE) if SCRAPE_PARTICIPANTS_CUTOFF_DATE else None

    classes = TournamentClass.get_filtered_classes(
        cursor                  = cursor,
        class_id_exts           = SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
        tournament_id_exts      = SCRAPE_PARTICIPANTS_TNMT_ID_EXTS,
        data_source_id          = 1 if (SCRAPE_PARTICIPANTS_CLASS_ID_EXTS or SCRAPE_PARTICIPANTS_TNMT_ID_EXTS) else None,
        cutoff_date             = cutoff_date,
        require_ended           = False,
        allowed_structure_ids   = [1, 3],   # 1 = Groups+KO, 3 = KO only
        allowed_type_ids        = [1],      # singles for now
        max_classes             = SCRAPE_PARTICIPANTS_MAX_CLASSES,
        order                   = SCRAPE_PARTICIPANTS_ORDER,
    )

    tournament_ids = [tc.tournament_id for tc in classes if tc.tournament_id is not None]
    tid_to_ext = Tournament.get_id_ext_map_by_id(cursor, tournament_ids)

    logger.info(f"Scraping tournament class KO matches for {len(classes)} classes from Ondata")

    print(classes)

    total_seen = total_inserted = total_skipped = 0

    for idx, tc in enumerate(classes, 1):
        tid_ext = tid_to_ext.get(tc.tournament_id)
        cid_ext = tc.tournament_class_id_ext

        logger_keys = {
            "class_idx":                f"{idx}/{len(classes)}",
            "tournament":               tc.shortname or tc.longname or "N/A",
            "tournament_id":            str(tc.tournament_id or "None"),
            "tournament_id_ext":        str(tid_ext or "None"),
            "tournament_class_id":      str(tc.tournament_class_id or "None"),
            "tournament_class_id_ext":  str(cid_ext or "None"),
            "date":                     str(getattr(tc, "startdate", None) or "None"),
            "stage":                    5,
        }

        # Download (or reuse cache) stage=5
        pdf_path, downloaded, msg = _download_pdf_ondata_by_tournament_class_and_stage(
            tournament_id_ext   = tid_ext or "",
            class_id_ext        = cid_ext or "",
            stage               = 5,
            force_download      = False,
        )
        if msg:
            # logger.info(logger_keys.copy(), msg)
            pass

        if not pdf_path:
            logger.failed(logger_keys.copy(), "No valid KO PDF (stage=5) for class")
            continue

        # Parse KO PDF into rounds
        try:
            with open(pdf_path, "rb") as f:
                pdf_bytes = f.read()
            rounds = _parse_knockout_pdf(pdf_bytes)
        except Exception as e:
            logger.failed(logger_keys.copy(), f"KO PDF parsing failed: {e}")
            continue

        # Remove existing raw rows for KO stages for this class (whichever rounds we detected)
        stage_ids_to_clear = {r["stage_id"] for r in rounds if r.get("stage_id")}
        # If nothing detected, clear common KO stages to avoid dups on re-run
        if not stage_ids_to_clear:
            stage_ids_to_clear = {2, 3, 4, 5, 6, 7, 8}
        for sid in sorted(stage_ids_to_clear):
            TournamentClassMatchRaw.remove_for_class(
                cursor,
                tournament_class_id_ext=cid_ext,
                tournament_class_stage_id=sid,
                data_source_id=1,
            )

        kept = skipped = 0

        for r in rounds:
            stage_id = r.get("stage_id")
            for mm in r.get("matches", []):
                total_seen += 1

                p1 = mm.get("p1", {}) or {}
                p2 = mm.get("p2", {}) or {}

                p1_code = mm.get("p1_code") or None
                p2_code = mm.get("p2_code") or None

                tokens_raw = mm.get("tokens", [])
                tokens_csv = _normalize_sign_tokens(tokens_raw)
                best_of = None if tokens_csv == "WO" else _infer_best_of_from_sign(tokens_raw)

                def name_with_club(d):
                    n = (d.get("name") or "").strip()
                    c = (d.get("club") or None)
                    return f"{n}" if not c else f"{n}, {c}"

                raw_line_text = (
                    f"{(mm.get('match_id_ext') or '').strip()} "
                    f"{(p1_code or '').strip()} {name_with_club(p1)} - "
                    f"{(p2_code or '').strip()} {name_with_club(p2)} "
                    f"{tokens_csv}"
                ).strip()

                row = TournamentClassMatchRaw(
                    tournament_id_ext=tid_ext or "",
                    tournament_class_id_ext=cid_ext or "",
                    group_id_ext=None,                         # KO has no pool
                    match_id_ext=(mm.get("match_id_ext") or None),

                    s1_player_id_ext=p1_code,
                    s2_player_id_ext=p2_code,
                    s1_fullname_raw=p1.get("name"),
                    s2_fullname_raw=p2.get("name"),
                    s1_clubname_raw=p1.get("club"),
                    s2_clubname_raw=p2.get("club"),

                    game_point_tokens=tokens_csv or None,
                    best_of=best_of,
                    raw_line_text=raw_line_text,

                    tournament_class_stage_id=stage_id or 6,   # default QF if unknown
                    data_source_id=1,
                )

                match_keys = logger_keys.copy()
                match_keys.update({
                    "group_id_ext": "KO",
                    "match_id_ext": row.match_id_ext or "None",
                    "round_stage_id": str(row.tournament_class_stage_id),
                })

                # We keep validation super-light for RAW
                is_valid, err = row.validate()
                if not is_valid:
                    skipped += 1
                    total_skipped += 1
                    logger.failed(match_keys, f"Validation failed: {err}")
                    continue

                try:
                    row.compute_hash()
                    row.insert(cursor)
                    kept += 1
                    total_inserted += 1
                    logger.success(match_keys, "Raw KO match saved")
                    if hasattr(logger, "inc_processed"):
                        logger.inc_processed()
                except Exception as e:
                    skipped += 1
                    total_skipped += 1
                    logger.failed(match_keys, f"Insert failed: {e}")

        logger.info(logger_keys.copy(), f"Inserted: {kept}   Skipped: {skipped}")

    logger.info(f"Scraping completed. Inserted: {total_inserted}, Skipped: {total_skipped}, Matches seen: {total_seen}")
    logger.summarize()



# ───────────────────────── Helpers: tokens ─────────────────────────

def _tokenize_right(s: str) -> list[str]:
    """Return signed tokens as strings or ['WO'] for walkovers."""
    if not s:
        return []
    s = s.strip()
    if re.fullmatch(r"WO", s, flags=re.IGNORECASE):
        return ["WO"]
    s = re.sub(r"\s*,\s*", " ", s)
    return re.findall(r"[+-]?\d+", s)

def _normalize_sign_tokens(tokens: List[str]) -> str:
    """
    Convert ['+9','-8','11'] -> '9, -8, 11'
    If WO → returns 'WO'
    """
    if tokens and all(t.upper() == "WO" for t in tokens):
        return "WO"
    norm: List[str] = []
    for raw in tokens or []:
        t = str(raw).strip()
        if t.startswith("+"):
            norm.append(t[1:])
        else:
            norm.append(t)
    return ", ".join(norm)

def _infer_best_of_from_sign(tokens: list[str]) -> int | None:
    """best_of = 2*max(wins) - 1 from signed tokens."""
    p1 = p2 = 0
    for raw in tokens or []:
        if not re.fullmatch(r"[+-]?\d+", raw.strip()):
            continue
        v = int(raw)
        if v >= 0: p1 += 1
        else:      p2 += 1
    if p1 == 0 and p2 == 0:
        return None
    return 2 * max(p1, p2) - 1


# ───────────────────────── Helpers: PDF parsing (KO) ─────────────────────────
# We detect "entry lines" (with a code + name, optionally club), cluster them by x-position
# (columns = bracket rounds), pair adjacent entries vertically as matches, and grab
# result tokens to the right of that pair (within the horizontal gap until the next column).

# Matches an entry with club: "046 Wang Tom, IFK Täby BTK"
_RE_ENTRY_WITH_CLUB = re.compile(
    r"^\s*(?P<code>\d{1,3}(?:/\d{1,3})?)\s+(?P<name>.+?)\s*,\s*(?P<club>.+?)\s*$"
)
# Matches a shorter entry without club: "046 Wang T"
_RE_ENTRY_SIMPLE = re.compile(
    r"^\s*(?P<code>\d{1,3}(?:/\d{1,3})?)\s+(?P<name>.+?)\s*$"
)

_SEG_GAP = 36.0  # px gap between words that indicates a new segment/column piece

def _segment_to_entry(seg_words: list[dict]) -> dict | None:
    """
    Convert a contiguous set of words on the same row into a bracket entry if possible.
    Returns a dict with geometry and parsed fields, or None if not an entry.
    """
    if not seg_words:
        return None

    text = " ".join(w["text"] for w in seg_words).strip()

    # Require that the segment starts with a player code (prevents picking up headers etc.)
    if not re.match(r"^\s*\d{1,3}(?:/\d{1,3})?\b", text):
        return None

    # Sometimes result tokens trail at the end; strip them and try again.
    # Example: "... , Club 11, -9, 3"
    cleaned = re.sub(r"(?:WO|[+-]?\d+(?:\s*[,\s]\s*[+-]?\d+)*)\s*$", "", text, flags=re.IGNORECASE).strip()

    m = _RE_ENTRY_WITH_CLUB.match(cleaned) or _RE_ENTRY_SIMPLE.match(cleaned)
    if not m:
        return None

    x0 = min(w["x0"] for w in seg_words)
    x1 = max(w["x1"] for w in seg_words)
    top = min(w["top"] for w in seg_words)
    bottom = max(w["bottom"] for w in seg_words)
    page = seg_words[0]["_page"]
    page_w = page.width

    return {
        "text": cleaned,
        "code": m.group("code").strip(),
        "name": m.group("name").strip(),
        "club": (m.group("club").strip() if "club" in m.groupdict() and m.group("club") else None),
        "x0": x0, "x1": x1, "top": top, "bottom": bottom,
        "page": page, "page_w": page_w,
    }

def _extract_entry_rows(pdf_bytes: bytes) -> list[dict]:
    """
    Build entry rows with geometry by:
      1) grouping page words into y-rows,
      2) splitting each row into left→right segments by large x-gaps,
      3) parsing each segment as a KO bracket entry.
    """
    entries: list[dict] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            # attach page ref so we can later extract tokens
            words = page.extract_words(x_tolerance=2, y_tolerance=3, keep_blank_chars=False) or []
            for w in words:
                w["_page"] = page

            # group into y-rows
            row_map: dict[int, list[dict]] = {}
            rid, last_top = 0, None
            for w in sorted(words, key=lambda w: (round(w["top"], 1), w["x0"])):
                top = round(w["top"], 1)
                if last_top is None or abs(top - last_top) > 3.0:
                    rid += 1
                    last_top = top
                    row_map[rid] = []
                row_map[rid].append(w)

            # split each row into segments by large x-gap and parse each segment
            for row_words in row_map.values():
                row_words.sort(key=lambda w: w["x0"])
                seg: list[dict] = []
                prev_x1 = None

                def _flush():
                    nonlocal seg
                    if seg:
                        ent = _segment_to_entry(seg)
                        if ent:
                            entries.append(ent)
                        seg = []

                for w in row_words:
                    if prev_x1 is None or (w["x0"] - prev_x1) <= _SEG_GAP:
                        seg.append(w)
                    else:
                        _flush()
                        seg = [w]
                    prev_x1 = w["x1"]
                _flush()

    return entries


def _cluster_columns(entries: list[dict], tolerance: float = 25.0) -> list[dict]:
    """
    Cluster entry rows by their x0 into columns. Returns a list of columns:
      [{"x0": float, "x1": float, "rows": [entry_row, ...]}] sorted left→right.
    """
    cols: list[dict] = []
    for e in sorted(entries, key=lambda r: r["x0"]):
        placed = False
        for c in cols:
            if abs(e["x0"] - c["x0"]) <= tolerance:
                c["rows"].append(e)
                c["x0"] = min(c["x0"], e["x0"])
                c["x1"] = max(c["x1"], e["x1"])
                placed = True
                break
        if not placed:
            cols.append({"x0": e["x0"], "x1": e["x1"], "rows": [e]})
    # sort & normalize row order
    for c in cols:
        c["rows"].sort(key=lambda r: r["top"])
    cols.sort(key=lambda c: c["x0"])
    return cols

def _tokens_in_strip(page, x_left: float, x_right: float, y_top: float, y_bottom: float) -> list[str]:
    """
    Collect tokens in a horizontal strip to the right of a pair of entries.
    """
    words = page.extract_words(x_tolerance=2, y_tolerance=3, keep_blank_chars=False) or []
    s = []
    for w in words:
        if x_left <= w["x0"] <= x_right and y_top <= w["top"] <= y_bottom:
            s.append(w["text"])
    text = " ".join(s).strip()
    # Tighten: only keep the last numeric/WO-looking sequence in the strip
    m = re.search(r"(WO|[+-]?\d+(?:\s*[,\s]\s*[+-]?\d+)*)\s*$", text, flags=re.IGNORECASE)
    return _tokenize_right(m.group(1)) if m else []

# Map #pairs in a column → tournament_class_stage_id
_STAGE_BY_PAIRCOUNT = {
    1: 8,    # Final
    2: 7,    # SF
    4: 6,    # QF
    8: 5,    # R16
    16: 4,   # R32
    32: 3,   # R64
    64: 2,   # R128
}

def _parse_knockout_pdf(pdf_bytes: bytes) -> list[dict]:
    """
    Returns a list of 'round' dicts:
      [{"stage_id": int, "matches": [ {p1,p1_code,p2,p2_code,tokens}, ... ]}, ...]
    """
    # Find bracket entries with geometry (each segment = one entry)
    entry_rows = _extract_entry_rows(pdf_bytes)
    if not entry_rows:
        logging.debug("[KO parse] No entry rows found.")
        return []

    logging.info(f"[KO parse] detected {len(entry_rows)} entry segments")
    cols = _cluster_columns(entry_rows, tolerance=25.0)
    logging.info(f"[KO parse] columns={len(cols)}; sizes={[len(c['rows']) for c in cols]}")
    rounds: list[dict] = []

    for ci, col in enumerate(cols):
        rows = col["rows"]
        # Pair consecutive entries vertically
        pairs: list[Tuple[dict, dict]] = []
        i = 0
        while i + 1 < len(rows):
            a, b = rows[i], rows[i + 1]
            # sanity: keep pairs that are reasonably close vertically
            if abs(b["top"] - a["bottom"]) <= 25.0 or (b["top"] - a["top"]) <= 60.0:
                pairs.append((a, b))
                i += 2
            else:
                # If the spacing is weird, still step by 2 to avoid infinite loops,
                # but you may refine this threshold later.
                pairs.append((a, b))
                i += 2

        # Decide the stage for this column by pair count (fallback: None)
        stage_id = _STAGE_BY_PAIRCOUNT.get(len(pairs), None)

        # Determine horizontal strip where set tokens live (between this column and the next)
        x_left = col["x1"] + 6
        x_right = (cols[ci + 1]["x0"] - 6) if (ci + 1 < len(cols)) else (rows[0]["page_w"] - 6)

        matches: list[dict] = []
        for a, b in pairs:
            y_top = min(a["top"], b["top"]) - 4
            y_bot = max(a["bottom"], b["bottom"]) + 4
            tokens = _tokens_in_strip(a["page"], x_left, x_right, y_top, y_bot)  # any page works; both in same page

            matches.append({
                "p1": {"name": a["name"], "club": a["club"]},
                "p2": {"name": b["name"], "club": b["club"]},
                "p1_code": a["code"],
                "p2_code": b["code"],
                "tokens": tokens,
                "match_id_ext": None,   # KO PDFs often omit explicit match IDs
            })

        rounds.append({"stage_id": stage_id, "matches": matches})

    return rounds