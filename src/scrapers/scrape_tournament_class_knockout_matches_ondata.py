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
            if not rounds:
                logger.warning(logger_keys.copy(), "KO parser returned 0 rounds (no bracket entries detected).")

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
                if not mm.get("p2"):
                    continue  # skip byes
                total_seen += 1

                p1 = mm.get("p1", {}) or {}
                p2 = mm.get("p2", {}) or {}

                p1_code = p1.get("code") or None
                p2_code = p2.get("code") or None

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

# ───────────────────────── Helpers: extract KO bracket entries ─────────────────────────
# code + name + club  e.g. "046 Wang Tom, IFK Täby BTK"
_RE_ENTRY_WITH_CLUB = re.compile(
    r"^\s*(?P<code>\d{1,3}(?:/\d{1,3})?)\s+(?P<name>.+?)\s*,\s*(?P<club>.+?)\s*$"
)
# code + name  e.g. "150 Ott D"
_RE_ENTRY_SIMPLE = re.compile(
    r"^\s*(?P<code>\d{1,3}(?:/\d{1,3})?)\s+(?P<name>.+?)\s*$"
)
# name + club (no code)  e.g. "Ohlsén Vigg, Laholms BTK Serve"
_RE_ENTRY_NAME_CLUB = re.compile(
    r"^\s*(?P<name>[^,]+?)\s*,\s*(?P<club>.+?)\s*$"
)
# short name only (no code, no comma)  e.g. "Wang L", "Zhu A", "Ott D"
# (allow diacritics, hyphens, apostrophes; last token is 1–3 letters + optional dot)
_RE_ENTRY_SHORT = re.compile(
    r"^\s*(?P<name>[A-Za-zÅÄÖåäöÉéÍíÓóÚúÑñÜüÆæØøÇç'’\-.]+(?:\s+[A-Za-zÅÄÖåäöÉéÍíÓóÚúÑñÜüÆæØøÇç'’\-.]+)*)\s+[A-Za-zÅÄÖÉÍÓÚÑÜ]{1,3}\.?\s*$"
)

_RE_TOKEN = re.compile(r"^(?P<tokens>(?:[+-]?\d+(?:\s*,\s*[+-]?\d+)*|WO))$", re.IGNORECASE)

_SEG_GAP            = 36.0  # px gap between words that indicates a new segment/column piece
_PAIR_GAP_MAX       = 12.0  # Strict gap for pairing opponents
_LOOSE_PAIR_GAP_MAX = 200.0  # Loose gap for later rounds, increased

def _segment_to_entry(seg_words: list[dict]) -> dict | None:
    """
    Convert a contiguous set of words on the same row into a bracket entry if possible.
    Returns a dict with geometry and parsed fields, or None if not an entry.
    """
    if not seg_words:
        return None
    # Raw segment text
    text = " ".join(w["text"] for w in seg_words).strip()
    if not text or len(text) < 2:
        return None
    lower = text.lower()
    if any(bad in lower for bad in ("slutspel", "pool", "sets", "poäng", "poäng", "diff", "bröt", "brot")):
        return None
    # If the entire segment is just tokens, it's not an entry
    if re.fullmatch(r"(?:WO|[+-]?\d+(?:\s*[,\s]\s*[+-]?\d+)*)", text, flags=re.IGNORECASE):
        return None
    # Extract trailing tokens
    trailing_pattern = r"(WO|[+-]?\d+(?:\s*[,\s]\s*[+-]?\d+)*)\s*$"
    m_trailing = re.search(trailing_pattern, text, flags=re.IGNORECASE)
    if m_trailing:
        raw_trailing = m_trailing.group(1)
        cleaned = text[:m_trailing.start()].strip()
    else:
        raw_trailing = None
        cleaned = text.strip()
    if not cleaned:
        return None
    # Helper: must contain at least one letter
    def _has_alpha(s: str) -> bool:
        return re.search(r"[A-Za-zÅÄÖåäöÉéÍíÓóÚúÑñÜüÆæØøÇç]", s) is not None
    m = (
        _RE_ENTRY_WITH_CLUB.match(cleaned)
        or _RE_ENTRY_SIMPLE.match(cleaned)
        or _RE_ENTRY_NAME_CLUB.match(cleaned)
        or _RE_ENTRY_SHORT.match(cleaned)
    )
    if not m:
        return None
    code = (m.groupdict().get("code") or None)
    name = m.group("name").strip()
    club = m.groupdict().get("club")
    club = club.strip() if club else None
    # Reject if the 'name' part has no letters (prevents "8, 5, 8, ..." etc)
    if not _has_alpha(name):
        return None
    x0 = min(w["x0"] for w in seg_words)
    x1 = max(w["x1"] for w in seg_words)
    top = min(w["top"] for w in seg_words)
    bottom = max(w["bottom"] for w in seg_words)
    page = seg_words[0]["_page"]
    page_w = page.width
    return {
        "type": "entry",
        "text": cleaned,
        "code": (code.strip() if code else None),
        "name": name,
        "club": club,
        "x0": x0, "x1": x1, "top": top, "bottom": bottom,
        "page": page, "page_w": page_w,
        "tokens": _tokenize_right(raw_trailing) if raw_trailing else []
    }

def _segment_to_token(seg_words: list[dict]) -> dict | None:
    text = " ".join(w["text"] for w in seg_words).strip()
    if re.fullmatch(r"(?:WO|[+-]?\d+(?:\s*[,\s]\s*[+-]?\d+)*)", text, flags=re.IGNORECASE):
        x0 = min(w["x0"] for w in seg_words)
        x1 = max(w["x1"] for w in seg_words)
        top = min(w["top"] for w in seg_words)
        bottom = max(w["bottom"] for w in seg_words)
        page = seg_words[0]["_page"]
        page_w = page.width
        return {
            "type": "token",
            "tokens": _tokenize_right(text),
            "x0": x0, "x1": x1, "top": top, "bottom": bottom,
            "page": page, "page_w": page_w
        }
    return None


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
            words = page.extract_words(x_tolerance=2, y_tolerance=3, keep_blank_chars=False) or []
            for w in words:
                w["_page"] = page

            # group into y-rows
            row_map: dict[int, list[dict]] = {}
            rid, last_top = 0, None
            for w in sorted(words, key=lambda w: (round(w["top"], 1), w["x0"])):  # keep it tight by y
                top = round(w["top"], 1)
                if last_top is None or abs(top - last_top) > 3.0:
                    rid += 1
                    last_top = top
                    row_map[rid] = []
                row_map[rid].append(w)

            # split row into segments and parse each segment
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
                        else:
                            token_ent = _segment_to_token(seg)
                            if token_ent:
                                entries.append(token_ent)
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

def _cluster_columns(entries: list[dict], tolerance: float = 60.0) -> list[dict]:  # Increased tolerance to 60.0
    """
    Cluster entry rows by their x0 into columns. Returns a list of columns:
      [{"x0": float, "x1": float, "rows": [entry_row, ...]}] sorted left→right.
    """
    cols = []
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
      [{"stage_id": int, "matches": [ {p1, p1_code, p2, p2_code, tokens}, ... ]}, ...]
    """
    items = _extract_entry_rows(pdf_bytes)
    if not items:
        logging.info("[KO parse] No items found.")
        return []
    logging.info(f"[KO parse] detected {len(items)} items (entries + tokens)")
    cols = _cluster_columns(items, tolerance=60.0)  # Increased tolerance
    logging.info(f"[KO parse] columns={len(cols)}; sizes={[len(c['rows']) for c in cols]}")

    # Build matches per column, including byes as p2=None
    pairs_per_col: list[list[dict]] = []
    for col in cols:
        entry_rows = [r for r in col["rows"] if r.get("type") == "entry"]
        col_matches: list[dict] = []
        i = 0
        while i < len(entry_rows):
            if i + 1 < len(entry_rows):
                a, b = entry_rows[i], entry_rows[i + 1]
                vgap = b["top"] - a["bottom"]
                atop_gap = b["top"] - a["top"]
                logging.info(f"[KO parse] Column x0={col['x0']:.1f}, Row {i}: top={a['top']:.1f}, bottom={a['bottom']:.1f}, text={a['text']}")
                logging.info(f"[KO parse] Column x0={col['x0']:.1f}, Row {i+1}: top={b['top']:.1f}, bottom={b['bottom']:.1f}, text={b['text']}")
                logging.info(f"[KO parse] Potential pair vgap={vgap:.1f}, atop_gap={atop_gap:.1f}")
                if vgap <= _PAIR_GAP_MAX and atop_gap <= _PAIR_GAP_MAX * 2:
                    col_matches.append({"p1": a, "p2": b, "tokens": []})
                    logging.info("[KO parse] Strict pair added")
                    i += 2
                    continue
            a = entry_rows[i]
            col_matches.append({"p1": a, "p2": None, "tokens": []})
            logging.info("[KO parse] Bye/single added")
            i += 1
        # Fix: if no pairs made (all singles), but even number of rows, fallback to loose pairing
        num_pairs = len([m for m in col_matches if m["p2"] is not None])
        if num_pairs == 0 and len(entry_rows) % 2 == 0 and len(entry_rows) >= 2:
            logging.info(f"[KO parse] No strict pairs, falling back to loose pairing for column x0={col['x0']:.1f}")
            col_matches = []
            i = 0
            while i < len(entry_rows):
                if i + 1 < len(entry_rows):
                    a, b = entry_rows[i], entry_rows[i + 1]
                    vgap = b["top"] - a["bottom"]
                    atop_gap = b["top"] - a["top"]
                    if vgap <= _LOOSE_PAIR_GAP_MAX and atop_gap <= _LOOSE_PAIR_GAP_MAX * 2:
                        col_matches.append({"p1": a, "p2": b, "tokens": []})
                        logging.info("[KO parse] Loose pair added")
                        i += 2
                        continue
                a = entry_rows[i]
                col_matches.append({"p1": a, "p2": None, "tokens": []})
                logging.info("[KO parse] Loose bye added")
                i += 1
        pairs_per_col.append(col_matches)

    # Identify columns with at least one real match (p2 not None)
    match_col_indices = [ci for ci, ps in enumerate(pairs_per_col) if ps and any(m["p2"] is not None for m in ps)]
    if not match_col_indices:
        logging.warning("[KO parse] No match columns detected after pairing.")
        return []

    # Stage mapping from RIGHT
    stage_by_ci: dict[int, int] = {}
    for rank_from_right, ci in enumerate(reversed(match_col_indices)):
        stage_by_ci[ci] = max(2, 8 - rank_from_right)

    # Build round objects
    rounds: list[dict] = []
    for ci in match_col_indices:
        matches: list[dict] = []
        for m in pairs_per_col[ci]:
            if m["p2"] is None:
                continue  # skip byes
            match = {
                "p1": m["p1"],
                "p2": m["p2"],
                "tokens": m["p1"]["tokens"] or m["p2"]["tokens"] or [],  # if any has tokens
                "match_id_ext": None,
            }
            matches.append(match)
        stage_id = stage_by_ci.get(ci)
        rounds.append({"stage_id": stage_id, "matches": matches})

    # Assign tokens from next column's aligned entry or global tokens
    token_rows = [item for item in items if item.get("type") == "token"]
    for r_idx in range(len(rounds)):
        current_round = rounds[r_idx]
        ci = match_col_indices[r_idx]
        next_ci = ci + 1
        while next_ci < len(cols) and not cols[next_ci]["rows"]:
            next_ci += 1
        has_next_entry_tokens = False
        if next_ci < len(cols):
            next_col = cols[next_ci]
            next_rows = [r for r in next_col["rows"] if r.get("type") == "entry"]
            for match in current_round["matches"]:
                if match["tokens"]:
                    continue
                p1 = match["p1"]
                p2 = match["p2"]
                min_top = min(p1["top"], p2["top"])
                max_bottom = max(p1["bottom"], p2["bottom"])
                center = (min_top + max_bottom) / 2
                closest = min(next_rows, key=lambda r: abs(r["top"] - center), default=None) if next_rows else None
                if closest and abs(closest["top"] - center) <= 30.0 and closest["tokens"]:
                    match["tokens"] = closest["tokens"]
                    logging.info(f"[KO parse] Assigned tokens {match['tokens']} from next entry {closest['text']} at center {center:.1f}, entry top {closest['top']:.1f}")
                    has_next_entry_tokens = True
        # If no assignment from next entry, use global tokens
        if not has_next_entry_tokens:
            for match in current_round["matches"]:
                if match["tokens"]:
                    continue
                p1 = match["p1"]
                p2 = match["p2"]
                min_top = min(p1["top"], p2["top"])
                max_bottom = max(p1["bottom"], p2["bottom"])
                center = (min_top + max_bottom) / 2
                col_x1 = max(p1["x1"], p2["x1"])
                candidates = [t for t in token_rows if t["x0"] > col_x1 - 50]
                if not candidates:
                    continue
                closest = min(candidates, key=lambda t: abs(t["top"] - center) + 0.01 * abs(t["x0"] - col_x1))
                delta_y = abs(closest["top"] - center)
                if delta_y <= 30.0:
                    match["tokens"] = closest["tokens"]
                    logging.info(f"[KO parse] Assigned tokens {match['tokens']} from global token at center {center:.1f}, token top {closest['top']:.1f}, delta_y={delta_y:.1f}")

    # Debug counts
    debug_counts = {}
    for r in rounds:
        debug_counts[r["stage_id"]] = debug_counts.get(r["stage_id"], 0) + len(r["matches"])
    logging.info(f"[KO parse] stage_counts={debug_counts}")

    return rounds