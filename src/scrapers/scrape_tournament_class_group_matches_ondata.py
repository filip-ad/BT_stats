# scrapers/scrape_group_matches_ondata.py

from __future__ import annotations
from datetime import date
import logging
from typing import List
import io, re
import unicodedata
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

SCRAPE_PARTICIPANTS_CLASS_ID_EXTS = ['30834']

debug = False

def scrape_tournament_class_group_matches_ondata(cursor, run_id=None):
    """
    Scrape GROUP stage match rows (stage=3) from OnData, store into tournament_class_match_raw.
    One DB row per match (symmetric S1/S2 fields), one logger success + inc_processed per match.
    """
    logger = OperationLogger(
        verbosity               = 2,
        print_output            = False,
        log_to_db               = True,
        cursor                  = cursor,
        object_type             = "tournament_class_match_raw",
        run_type                = "scrape",
        run_id                  = run_id,
    )

    cutoff_date = parse_date(SCRAPE_PARTICIPANTS_CUTOFF_DATE) if SCRAPE_PARTICIPANTS_CUTOFF_DATE else None

    classes = TournamentClass.get_filtered_classes(
        cursor                  = cursor,
        class_id_exts           = SCRAPE_PARTICIPANTS_CLASS_ID_EXTS,
        tournament_id_exts      = SCRAPE_PARTICIPANTS_TNMT_ID_EXTS,
        data_source_id          = 1 if (SCRAPE_PARTICIPANTS_CLASS_ID_EXTS or SCRAPE_PARTICIPANTS_TNMT_ID_EXTS) else None,
        cutoff_date             = cutoff_date,
        require_ended           = False,
        allowed_structure_ids   = [1, 2],           # Groups+KO or Groups-only
        allowed_type_ids        = [1],              # singles for now
        max_classes             = SCRAPE_PARTICIPANTS_MAX_CLASSES,
        order                   = SCRAPE_PARTICIPANTS_ORDER,
    )

    tournament_ids  = [tc.tournament_id for tc in classes if tc.tournament_id is not None]
    tid_to_ext      = Tournament.get_id_ext_map_by_id(cursor, tournament_ids)

    # run_keys        = {"source": "ondata", "stage": 3, "classes": len(classes)}
    logger.info(f"Scraping tournament class group matches for {len(classes)} classes from Ondata")

    total_matches       = 0
    total_inserted      = 0
    total_skipped       = 0

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
            "stage":                    3,
        }

        # Remove previous raw rows for this class/source
        removed = TournamentClassMatchRaw.remove_for_class(
            cursor,
            tournament_class_id_ext     = cid_ext,
            data_source_id              = 1,
            tournament_class_stage_id   = 1,   # GROUP
        )

        if removed:
            # logger.info(logger_keys.copy(), f"Removed {removed} existing raw rows")
            pass

        # Download / reuse cached PDF (strictly stage=3)

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

        pdf_path, downloaded, msg = _download_pdf_ondata_by_tournament_class_and_stage(
            tournament_id_ext=tid_ext or "",
            class_id_ext=cid_ext or "",
            stage=3,
            force_download=force_refresh,
        )
        if msg:
            # logger.info(logger_keys.copy(), msg)
            pass

        if not pdf_path:
            reason = "No valid PDF at stage=3 for class"
            logger.failed(logger_keys.copy(), reason)
            continue

        # Parse groups
        try:
            with open(pdf_path, "rb") as f:
                pdf_bytes = f.read()
            groups = _parse_groups_pdf(pdf_bytes)
            before = sum(len(g.get("matches", [])) for g in groups)
            groups = _dedupe_groups(groups)
            after  = sum(len(g.get("matches", [])) for g in groups)
            if before != after:
                logger.info(logger_keys.copy(), f"De-duplicated overlay rows: {before - after} removed (from {before} → {after})")

            # logger.info(logger_keys.copy(), f"Parsed {len(groups)} pools")
        except Exception as e:
            logger.failed(logger_keys.copy(), f"PDF parsing failed: {e}")
            continue

        # Infer group best-of once (for WO rows we may not know best_of from tokens)
        best_of_by_group = _infer_group_best_of(groups)

        # Build per-group name index from stage=3 to help map '*' rows in stage=4
        names_by_group = _collect_names_by_group(groups)

        # If any WO present in this class, peek stage=4 to identify which player broke
        any_wo = any(
            (mm.get("tokens") and len(mm["tokens"]) == 1 and str(mm["tokens"][0]).upper() == "WO")
            for g in groups for mm in g.get("matches", [])
        )
        wo_flags_by_group: dict[str, set[str]] = {}
        if any_wo:
            wo_flags_by_group = _load_stage4_break_flags(
                tid_ext, cid_ext, names_by_group, logger=logger, logger_keys=logger_keys, force_refresh=force_refresh
            )

        kept = 0
        skipped = 0

        # Insert raw rows (one per match)
        for g in groups:
            group_desc = g.get("name")
            for mm in g.get("matches", []):
                total_matches += 1

                p1 = mm.get("p1", {}) or {}
                p2 = mm.get("p2", {}) or {}
                p1_code = mm.get("p1_code") or None
                p2_code = mm.get("p2_code") or None

                tokens_raw = mm.get("tokens", [])
                tokens_csv = _normalize_sign_tokens(tokens_raw)

                # Default best_of from numeric tokens; for WO we'll fill from group later
                if isinstance(tokens_raw, list) and len(tokens_raw) == 1 and str(tokens_raw[0]).upper() == "WO":
                    tokens_csv = "WO"
                    best_of = None
                else:
                    best_of = _infer_best_of_from_sign(tokens_raw)

                # If it's a WO, try to upgrade to WO:S1/S2 using stage=4 flags
                if tokens_csv == "WO" and group_desc:
                    flagged = wo_flags_by_group.get(group_desc, set())
                    if flagged:
                        n1 = _norm((p1.get("name") or ""))
                        n2 = _norm((p2.get("name") or ""))
                        if n1 in flagged and n2 not in flagged:
                            # side 1 forfeited; side 2 wins
                            tokens_csv = "WO:S1"
                        elif n2 in flagged and n1 not in flagged:
                            # side 2 forfeited; side 1 wins
                            tokens_csv = "WO:S2"
                    # Fill best_of for WO from the group-level inference if available
                    if best_of is None:
                        best_of = best_of_by_group.get(group_desc)

                # print(tokens_csv)

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
                    group_id_ext=group_desc,                # GROUP scraper => pool label
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
                    tournament_class_stage_id=1,           # GROUP
                    data_source_id=1,
                )

                # Omitt warnings for missing data for debugging
                missing = []
                if not row.s1_fullname_raw:
                    missing.append("s1_fullname_raw")
                if not row.s2_fullname_raw:
                    missing.append("s2_fullname_raw")
                if row.s1_clubname_raw is None:
                    missing.append("s1_clubname_raw")
                if row.s2_clubname_raw is None:
                    missing.append("s2_clubname_raw")
                if not row.game_point_tokens:
                    missing.append("game_point_tokens")
                if not row.best_of:
                    missing.append("best_of")
                if not row.raw_line_text:
                    missing.append("raw_line_text")

                if missing:
                    logger.warning(logger_keys.copy(), f"Missing/invalid fields: {', '.join(missing)}")

                match_keys = logger_keys.copy()
                match_keys.update({
                    "group_id_ext":     group_desc          or "None",
                    "match_id_ext":     row.match_id_ext    or "None",
                })

                is_valid, error_message = row.validate()
                if is_valid:
                    row.compute_hash()
                    # row.insert(cursor)
                else:
                    logger.failed(logger_keys.copy(), f"Validation failed: {error_message}")
                    continue

                try:
                    row.insert(cursor)
                    kept += 1
                    total_inserted += 1
                    logger.success(match_keys, "Raw match saved")

                    if debug:
                        print(row)

                    if hasattr(logger, "inc_processed"):
                        logger.inc_processed()
                except Exception as e:
                    skipped += 1
                    total_skipped += 1
                    logger.failed(match_keys, f"Insert failed: {e}")

        logger.info(logger_keys.copy(), f"Removed: {removed}   Inserted: {kept}   Skipped: {skipped}")

    logger.info(f"Scraping complete. Inserted: {total_inserted}, Skipped: {total_skipped}, Matches seen: {total_matches}")
    logger.summarize()

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

# ───────────────────────── Name / stage-4 helpers ─────────────────────────

def _norm(s: str | None) -> str:
    """Simple name normalizer: lowercase, strip accents/punct, collapse spaces."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _collect_names_by_group(groups: list[dict]) -> dict[str, set[str]]:
    """Map 'Pool X' -> set of normalized player names seen in stage=3 PDF."""
    by_group: dict[str, set[str]] = {}
    for g in groups:
        gname = g.get("name")
        if not gname:
            continue
        names = by_group.setdefault(gname, set())
        for mm in g.get("matches", []):
            p1 = (mm.get("p1") or {}).get("name")
            p2 = (mm.get("p2") or {}).get("name")
            if p1: names.add(_norm(p1))
            if p2: names.add(_norm(p2))
    return by_group

def _infer_group_best_of(groups: list[dict]) -> dict[str, int]:
    """
    For each group, infer a representative 'best_of' from any non-WO match in that group.
    """
    best_of_by_group: dict[str, int] = {}
    for g in groups:
        gname = g.get("name")
        if not gname:
            continue
        for mm in g.get("matches", []):
            toks = mm.get("tokens", [])
            if toks and not (len(toks) == 1 and str(toks[0]).upper() == "WO"):
                bo = _infer_best_of_from_sign(toks)
                if bo:
                    best_of_by_group[gname] = bo
                    break
    return best_of_by_group

def _load_stage4_break_flags(
    tournament_id_ext: str | None,
    class_id_ext: str | None,
    names_by_group: dict[str, set[str]],
    *,
    logger,
    logger_keys: dict,
    force_refresh: bool = False
) -> dict[str, set[str]]:
    """
    Download+parse stage=4 PDF and return { 'Pool X': {normalized_name, ...}, ... }
    for players marked broken/withdrawn ('*' or 'Bröt').
    """
    flagged_by_group: dict[str, set[str]] = {}

    pdf_path, downloaded, msg = _download_pdf_ondata_by_tournament_class_and_stage(
        tournament_id_ext=tournament_id_ext or "",
        class_id_ext=class_id_ext or "",
        stage=4,
        force_download=force_refresh,
    )
    if msg:
        # logger.info(logger_keys.copy(), msg)
        pass
    if not pdf_path:
        return flagged_by_group

    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()

    # Parse like stage=3: group into rows, track current pool header
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            words = page.extract_words(x_tolerance=2, y_tolerance=3, keep_blank_chars=False) or []
            if not words:
                continue

            row_map: dict[int, list[dict]] = {}
            rid, last_top = 0, None
            for w in sorted(words, key=lambda w: (round(w["top"], 1), w["x0"])):
                top = round(w["top"], 1)
                if last_top is None or abs(top - last_top) > 3.0:
                    rid += 1
                    last_top = top
                    row_map[rid] = []
                row_map[rid].append(w)

            current_group: str | None = None
            for row_words in row_map.values():
                row_text = " ".join(w["text"] for w in sorted(row_words, key=lambda w: w["x0"])).strip()
                if not row_text:
                    continue

                m_pool = _RE_POOL.search(row_text)
                if m_pool:
                    current_group = m_pool.group(0)
                    continue
                if not current_group:
                    continue

                lt = row_text.lower()
                if "*" not in row_text and "bröt" not in lt:
                    continue

                # disambiguate: match the row against known names from this group
                group_names = names_by_group.get(current_group, set())
                if not group_names:
                    continue

                norm_row = _norm(row_text)
                candidates: list[str] = []
                for nm in group_names:
                    parts = nm.split()
                    reversed_nm = " ".join(reversed(parts)) if len(parts) >= 2 else nm
                    if nm and nm in norm_row:
                        candidates.append(nm)
                    elif reversed_nm and reversed_nm in norm_row:
                        candidates.append(nm)

                if len(candidates) == 1:
                    flagged_by_group.setdefault(current_group, set()).add(candidates[0])

    return flagged_by_group


_RE_POOL = re.compile(r"\bPool\s+\d+\b", re.IGNORECASE)
_RE_NAME_CLUB = re.compile(r"^(?P<name>.+?)(?:,\s*(?P<club>.+))?$")
_RE_LEADING_CODE = re.compile(r"^\s*(?:\d{1,3})\s+(?=\S)")

# Remainder patterns after bold MID
_RE_REMAINDER_WITH_CODES = re.compile(
    r"^\s*(?P<p1code>\d{1,3})\s+(?P<p1>.+?)\s*[-–]\s*(?P<p2code>\d{1,3})\s+(?P<p2>.+?)"
    r"(?:\s+(?P<rest>(?:[\d,\s:+-]+|WO)))?$"
    # r"(?:\s+(?P<rest>[\d,\s:+-]+))?$"
)
_RE_REMAINDER_NO_CODES = re.compile(
    r"^\s*(?P<p1>.+?)\s*[-–]\s*(?P<p2>.+?)"
    r"(?:\s+(?P<rest>(?:[\d,\s:+-]+|WO)))?$"
    # r"(?:\s+(?P<rest>[\d,\s:+-]+))?$"
)

_RE_MATCH_WITH_CODES = re.compile(
    r"^\s*(?P<mid>\d{1,4})\s+(?P<p1code>\d{1,3})\s+(?P<p1>.+?)\s*[-–]\s*(?P<p2code>\d{1,3})\s+(?P<p2>.+?)"
    r"(?:\s+(?P<rest>(?:[\d,\s:+-]+|WO)))?$"
    # r"(?:\s+(?P<rest>[\d,\s:+-]+))?$"
)

_RE_MATCH_NO_CODES = re.compile(
    r"^\s*(?P<mid>\d{1,4})\s+(?P<p1>.+?)\s*[-–]\s*(?P<p2>.+?)"
    r"(?:\s+(?P<rest>(?:[\d,\s:+-]+|WO)))?$"
    # r"(?:\s+(?P<rest>[\d,\s:+-]+))?$"
)

def _split_name_club(raw: str) -> dict:
    s = _RE_LEADING_CODE.sub("", raw.strip())
    m = _RE_NAME_CLUB.match(s)
    name = (m.group("name") if m else s).strip()
    club = (m.group("club") if m else None)
    return {"raw": raw, "name": name, "club": (club.strip() if club else None)}

# def _tokenize_right(s: str) -> list[str]:
#     """Return signed tokens as strings: '+9', '-8', '11', ... (we normalize later)."""
#     if not s:
#         return []
#     s = re.sub(r"\s*,\s*", " ", s.strip())
#     return re.findall(r"[+-]?\d+", s)

def _tokenize_right(s: str) -> list[str]:
    """Return signed tokens as strings: '+9', '-8', '11', ... or ['WO'] for walkovers."""
    if not s:
        return []
    s = s.strip()
    # If the entire cell indicates a walkover (exact 'WO'), capture it
    if re.fullmatch(r"WO", s, flags=re.IGNORECASE):
        return ["WO"]
    # Otherwise, normalize commas and parse numeric tokens
    s = re.sub(r"\s*,\s*", " ", s)
    return re.findall(r"[+-]?\d+", s)


def _infer_best_of_from_sign(tokens: list[str]) -> int | None:
    """
    Infer 'best of' using winner's game count:
    best_of = 2*max(p1_wins, p2_wins) - 1
    """
    p1_games = p2_games = 0
    for raw in tokens or []:
        if not re.fullmatch(r"[+-]?\d+", raw.strip()):
            continue
        v = int(raw)
        if v >= 0:
            p1_games += 1
        else:
            p2_games += 1
    if p1_games == 0 and p2_games == 0:
        return None
    return 2 * max(p1_games, p2_games) - 1

def _extract_rows_group_stage_with_attrs(pdf_bytes: bytes) -> list[dict]:
    """
    Returns rows with attrs:
      { "text": "...", "words": [..], "bold_mid": "123" or None, "tail_text": "..." }
    """
    rows: list[dict] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            words = page.extract_words(x_tolerance=2, y_tolerance=3, keep_blank_chars=False) or []
            if not words:
                continue

            row_map: dict[int, list[dict]] = {}
            rid, last_top = 0, None
            for w in sorted(words, key=lambda w: (round(w["top"], 1), w["x0"])):
                top = round(w["top"], 1)
                if last_top is None or abs(top - last_top) > 3.0:
                    rid += 1
                    last_top = top
                    row_map[rid] = []
                row_map[rid].append(w)

            for words_in_row in row_map.values():
                words_in_row.sort(key=lambda w: w["x0"])
                row_text = " ".join(w["text"] for w in words_in_row).strip()
                if not row_text:
                    continue

                bold_mid = None
                tail_words = words_in_row[:]
                if tail_words:
                    w0 = tail_words[0]
                    font = w0.get("fontname", "")
                    if w0["text"].isdigit() and 1 <= len(w0["text"]) <= 4 and ("Bold" in font or "bold" in font.lower()):
                        bold_mid = w0["text"]
                        tail_words = tail_words[1:]

                tail_text = " ".join(w["text"] for w in tail_words).strip()
                rows.append({
                    "text": row_text,
                    "words": words_in_row,
                    "bold_mid": bold_mid,
                    "tail_text": tail_text,
                })
    return rows

def _parse_groups_pdf(pdf_bytes: bytes) -> list[dict]:
    rows = _extract_rows_group_stage_with_attrs(pdf_bytes)
    groups: list[dict] = []
    current: dict | None = None

    def _debug_unmatched(row_text: str):
        if re.match(r"^\s*\d+\s+\S", row_text) or " - " in row_text or " – " in row_text:
            logging.debug(f"[group-parse] unmatched row: {row_text}")

    for row in rows:
        text = row["text"]

        # Pool header
        m_pool = _RE_POOL.search(text)
        if m_pool:
            current = {"name": m_pool.group(0), "matches": []}
            groups.append(current)
            continue
        if not current:
            continue

        mid = row["bold_mid"]
        tail = row["tail_text"]

        # Branch A: bold MID
        if mid:
            m = _RE_REMAINDER_WITH_CODES.match(tail) or _RE_REMAINDER_NO_CODES.match(tail)
            if not m:
                _debug_unmatched(text)
                continue

            p1_str, p2_str = m.group("p1").strip(), m.group("p2").strip()
            lt = text.lower()
            if "," not in p1_str or "," not in p2_str:
                continue
            if "tt coordinator" in lt or "programlicens" in lt or "http://" in lt or "https://" in lt:
                continue

            p1code = m.groupdict().get("p1code")
            p2code = m.groupdict().get("p2code")
            rest   = m.group("rest") or ""

            current["matches"].append({
                "match_id_ext": mid,
                "p1_code": p1code,
                "p2_code": p2code,
                "p1": _split_name_club(p1_str),
                "p2": _split_name_club(p2_str),
                "tokens": _tokenize_right(rest),
            })
            continue

        # Branch B: plain MID at row start
        m = _RE_MATCH_WITH_CODES.match(text) or _RE_MATCH_NO_CODES.match(text)
        if m:
            p1_str, p2_str = m.group("p1").strip(), m.group("p2").strip()
            lt = text.lower()
            if "," not in p1_str or "," not in p2_str:
                continue
            if "tt coordinator" in lt or "programlicens" in lt or "http://" in lt or "https://" in lt:
                continue

            match_id_ext = m.group("mid").strip()
            p1code = m.groupdict().get("p1code")
            p2code = m.groupdict().get("p2code")
            rest   = m.group("rest") or ""

            current["matches"].append({
                "match_id_ext": match_id_ext,
                "p1_code": p1code,
                "p2_code": p2code,
                "p1": _split_name_club(p1_str),
                "p2": _split_name_club(p2_str),
                "tokens": _tokenize_right(rest),
            })
            continue

        # Branch C: no MID; parse names
        m2 = _RE_REMAINDER_WITH_CODES.match(text) or _RE_REMAINDER_NO_CODES.match(text)
        if m2:
            p1_str, p2_str = m2.group("p1").strip(), m2.group("p2").strip()
            lt = text.lower()
            if "," not in p1_str or "," not in p2_str:
                continue
            if "tt coordinator" in lt or "programlicens" in lt or "http://" in lt or "https://" in lt:
                continue

            rest = m2.group("rest") or ""
            current["matches"].append({
                "match_id_ext": None,
                "p1_code":      m2.groupdict().get("p1code"),
                "p2_code":      m2.groupdict().get("p2code"),
                "p1":           _split_name_club(p1_str),
                "p2":           _split_name_club(p2_str),
                "tokens":       _tokenize_right(rest),
            })
            continue

        _debug_unmatched(text)

    return groups

def _split_name_club(raw: str) -> dict:
    s = _RE_LEADING_CODE.sub("", raw.strip())
    m = _RE_NAME_CLUB.match(s)
    name = (m.group("name") if m else s).strip()
    club = (m.group("club") if m else None)

    # OnData stage=3 sometimes repeats page-1 rows on page-2 and appends '*' to club.
    overlay = False
    if club:
        club = club.strip()
        if club.endswith("*"):
            overlay = True
            club = club[:-1].rstrip()

    return {"raw": raw, "name": name, "club": (club or None), "overlay": overlay}

def _dedupe_groups(groups: list[dict]) -> list[dict]:
    """
    De-duplicate repeated 'overlay' rows that appear on some OnData stage=3 PDFs.

    Context:
      - Certain tournaments produce a second page that repeats the same matches
        and appends '*' to the club names.
      - We want exactly one row per actual match.

    Strategy:
      - Build a canonical key: normalized p1, normalized p2, and normalized token CSV.
      - For each key, keep the "best" row by score:
            non-overlay  > overlay
            with codes   > without codes
            with matchid > without matchid
      - Return groups with duplicates removed and original group structure preserved.
    """
    def norm_name(n: str | None) -> str:
        return _norm(n or "")

    def score(match: dict) -> tuple[int, int, int]:
        # Higher is better
        is_overlay = 1 if ((match.get("p1") or {}).get("overlay") or (match.get("p2") or {}).get("overlay")) else 0
        has_codes  = 1 if (match.get("p1_code") and match.get("p2_code")) else 0
        has_mid    = 1 if match.get("match_id_ext") else 0
        # prefer non-overlay => invert overlay bit in score
        return (1 - is_overlay, has_codes, has_mid)

    # index best match per canonical key
    best_by_key: dict[tuple[str, str, str], dict] = {}
    key_to_groupnames: dict[tuple[str, str, str], list[str]] = {}

    for g in groups:
        gname = g.get("name")
        for mm in g.get("matches", []):
            # tokens already normalized later when building row; for dedupe we reformat here too
            tokens_raw = mm.get("tokens") or []
            tokens_csv = _normalize_sign_tokens(tokens_raw)
            key = (norm_name((mm.get("p1") or {}).get("name")),
                   norm_name((mm.get("p2") or {}).get("name")),
                   tokens_csv.strip())

            cand = best_by_key.get(key)
            if cand is None or score(mm) > score(cand):
                best_by_key[key] = mm
            key_to_groupnames.setdefault(key, []).append(gname)

    # Rebuild groups: keep only best matches per key; assign to their original group
    # (If a key appeared in multiple groups by error, we keep it in the first seen group.)
    kept_keys = set()
    out: list[dict] = []
    for g in groups:
        gname = g.get("name")
        new_matches: list[dict] = []
        for mm in g.get("matches", []):
            tokens_csv = _normalize_sign_tokens(mm.get("tokens") or [])
            key = (norm_name((mm.get("p1") or {}).get("name")),
                   norm_name((mm.get("p2") or {}).get("name")),
                   tokens_csv.strip())
            if key in kept_keys:
                continue
            # keep only if this group is the first where the best candidate appeared
            if best_by_key.get(key) is mm and key_to_groupnames.get(key, [gname])[0] == gname:
                new_matches.append(mm)
                kept_keys.add(key)
        if new_matches:
            out.append({"name": gname, "matches": new_matches})
    return out
