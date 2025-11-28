# src/resolvers/resolve_tournament_classes.py
"""
Resolver for tournament classes.

This module handles the resolution of tournament_class_raw -> tournament_class,
including type/structure detection, validation, and parent-child relationships.

Parent-Child Relationship Detection (added 2025-11-28):
    After upserting all classes, a second pass detects B-playoff classes by looking
    for "~B" suffix in shortname (e.g., "P12~B"). These are linked to their parent
    class ("P12") via tournament_class_id_parent foreign key.
    
    This relationship is used by resolve_tournament_class_matches.py to find
    players in the parent class when they're not found in the B-class entry list.
"""

import sqlite3
from typing import Optional, List
import re
import pdfplumber
from utils import OperationLogger, parse_date, _download_pdf_ondata_by_tournament_class_and_stage
from models.tournament_class import TournamentClass
from models.tournament_class_raw import TournamentClassRaw
from models.tournament import Tournament
from datetime import date
from config import RESOLVE_CLASSES_CUTOFF_DATE, RESOLVE_CLASS_ID_EXTS


# RESOLVE_CLASS_ID_EXTS = ['30834']
# RESOLVE_CLASS_ID_EXTS = ['30830']

# RESOLVE_CLASS_ID_EXTS = ['15438']
# RESOLVE_CLASS_ID_EXTS = ['31053']  # dubbel

# RESOLVE_CLASS_ID_EXTS = ['31063']



debug = False

POOL_HEADING_KEYWORDS = ("pool", "pulje", "poule", "grupp", "gruppe", "group")
FINAL_GROUP_HEADING_KEYWORDS = (
    "slutspel",
    "slutspil",
    "sluttspel",
    "sluttspill",
    "finalspel",
    "finalspil",
    "finalrunde",
    "finalrunda",
    "finalerunde",
    "final group",
)

def resolve_tournament_classes(cursor, run_id=None) -> List[TournamentClass]:
    """
    Resolve tournament_class_raw -> tournament_class.
    Fetch all raw entries, resolve dependencies, infer fields, validate, and upsert to regular table.
    Returns list of successfully upserted TournamentClass objects.
    """
    logger = OperationLogger(
        verbosity       = 2,
        print_output    = False,
        log_to_db       = True,
        cursor          = cursor,
        object_type     = "tournament_class",
        run_type        = "resolve",
        run_id          = run_id
    )

    # Fetch all raw tournament class records
    raw_objects = TournamentClassRaw.get_all(cursor)

    # Sort by date for consistent processing
    raw_objects = sorted(
        raw_objects,
        key=lambda r: (0, r.startdate) if r.startdate else (1, date.max),
    )

    # ------------------------------------------------------------
    # DEBUG FILTER: override date filtering completely
    # ------------------------------------------------------------
    if RESOLVE_CLASS_ID_EXTS:
        allowed = {str(x) for x in RESOLVE_CLASS_ID_EXTS}
        before = len(raw_objects)
        raw_objects = [
            r for r in raw_objects
            if r.tournament_class_id_ext
            and str(r.tournament_class_id_ext) in allowed
        ]
        logger.info(
            f"Debug filter active: {len(raw_objects)} entries match "
            f"RESOLVE_CLASS_ID_EXTS={RESOLVE_CLASS_ID_EXTS} "
            f"(filtered out {before - len(raw_objects)})"
        )

    # ------------------------------------------------------------
    # DATE CUTOFF (only applied when debug filter is inactive)
    # ------------------------------------------------------------
    if not RESOLVE_CLASS_ID_EXTS and RESOLVE_CLASSES_CUTOFF_DATE:
        cutoff = parse_date(RESOLVE_CLASSES_CUTOFF_DATE)
        before_filter = len(raw_objects)
        raw_objects = [
            r for r in raw_objects
            if r.startdate and r.startdate >= cutoff
        ]
        logger.info(
            f"Cutoff {cutoff}: {len(raw_objects)} remain "
            f"(filtered out {before_filter - len(raw_objects)})"
        )
    elif not RESOLVE_CLASS_ID_EXTS:
        logger.info("No RESOLVE_CLASSES_CUTOFF_DATE set -> resolving ALL classes")

    raw_count = len(raw_objects)
    logger.info(f"Resolving {raw_count} raw tournament classes...")

    if not raw_objects:
        logger.failed({}, "No tournament class data found in tournament_class_raw")
        return []
    
    # Filter out classes with "reservlista" in shortname or longname
    raw_objects = [
        raw for raw in raw_objects
        if not (raw.shortname and   "reservlista" in raw.shortname.lower()) and
           not (raw.longname and    "reservlista" in raw.longname.lower())
    ]
    logger.info(f"After filtering 'reservlista', {len(raw_objects)} entries remain... (filtered out {raw_count - len(raw_objects)} entries)")

    # Pre-fetch all tournament_id mappings for efficiency
    all_ext_ids = list({
        str(raw.tournament_id_ext).zfill(6)
        for raw in raw_objects if raw.tournament_id_ext is not None
    })
    if all_ext_ids:
        tournament_id_map = Tournament.get_id_map_by_ext(
            cursor, all_ext_ids, raw_objects[0].data_source_id
        )
        missing_ext_ids = set(all_ext_ids) - set(tournament_id_map.keys())
        if missing_ext_ids:
            logger.warning({}, f"Missing tournaments for tournament_id_ext values: {', '.join(sorted(missing_ext_ids))}")
    else:
        tournament_id_map = {}
        logger.warning({}, "No valid tournament_id_ext values found in tournament_class_raw")

    classes = []
    seen_ext_ids = set()

    for raw in raw_objects:
        logger_keys = {
            "row_id": str(raw.row_id) if raw.row_id else "None",
            "tournament_id_ext": str(raw.tournament_id_ext).zfill(6) if raw.tournament_id_ext else "None",
            "tournament_class_id_ext": str(raw.tournament_class_id_ext) if raw.tournament_class_id_ext else "None",
            "shortname": raw.shortname or "None",
            "longname": raw.longname or "None",
            "startdate": str(raw.startdate) if raw.startdate else "None"
        }

        # Prevent duplicates in same run
        if raw.tournament_class_id_ext is not None:
            if raw.tournament_class_id_ext in seen_ext_ids:
                logger.skipped(
                    logger_keys, f"Duplicate tournament_class_id_ext {raw.tournament_class_id_ext} in same batch"
                )
                continue
            seen_ext_ids.add(raw.tournament_class_id_ext)

        # Resolve tournament_id from ext
        if raw.tournament_id_ext is None:
            logger.failed(
                logger_keys,
                f"Missing tournament_id_ext for class {raw.tournament_class_id_ext}",
            )
            continue
        tournament_id_ext = str(raw.tournament_id_ext).zfill(6)
        tournament_id = tournament_id_map.get(tournament_id_ext)
        if not tournament_id:
            logger.failed(
                logger_keys,
                f"No tournament found matching tournament_id_ext: {tournament_id_ext}",
            )
            continue

        # Validate and prepare tournament class data
        structure_id = _infer_structure_id(raw.raw_stages)
        if debug:
            logger.info(logger_keys.copy(), f"Inferred structure_id={structure_id} for tournament_class_id_ext={raw.tournament_class_id_ext}")
        if structure_id == 2:
            structure_id = _refine_group_structure(
                raw,
                tournament_id_ext,
                logger=logger,
                logger_keys=logger_keys,
            )
        if debug:
            logger.info(logger_keys.copy(), f"Refined structure_id={structure_id} for tournament_class_id_ext={raw.tournament_class_id_ext}")

        tournament_class = TournamentClass(
            tournament_class_id_ext         = raw.tournament_class_id_ext,
            tournament_id                   = tournament_id,
            tournament_class_type_id        = _detect_type_id(raw.shortname or "", raw.longname or ""),
            tournament_class_structure_id   = structure_id,
            ko_tree_size                    = raw.ko_tree_size,
            startdate                       = raw.startdate,
            longname                        = raw.longname,
            shortname                       = raw.shortname,
            gender                          = raw.gender,
            max_rank                        = raw.max_rank,
            max_age                         = raw.max_age,
            url                             = raw.url,
            data_source_id                  = raw.data_source_id,
        )

        if debug:
            logger.info(logger_keys.copy(), f"Inferred type_id={tournament_class.tournament_class_type_id} for tournament_class_id_ext={raw.tournament_class_id_ext}")

        is_valid, error_message = tournament_class.validate()
        if not is_valid:
            logger.failed(logger_keys, f"Validation failed: {error_message}")
            continue

        classes.append(tournament_class)

    # Upsert tournament classes
    valid_classes = []
    for tc in classes:
        try:
            action = tc.upsert(cursor)
            if action:
                logger.success(
                    {
                        "tournament_class_id_ext": str(tc.tournament_class_id_ext)
                        if tc.tournament_class_id_ext
                        else "None"
                    },
                    f"Tournament class successfully {action}",
                )
                valid_classes.append(tc)
            else:
                logger.warning(
                    {
                        "tournament_class_id_ext": str(tc.tournament_class_id_ext)
                        if tc.tournament_class_id_ext
                        else "None"
                    },
                    "No changes made during upsert",
                )
        except sqlite3.IntegrityError as e:
            logger.failed(
                {
                    "tournament_class_id_ext": str(tc.tournament_class_id_ext)
                    if tc.tournament_class_id_ext
                    else "None"
                },
                f"Upsert failed due to integrity error: {e}",
            )
            continue

    # ─────────────────────────────────────────────────────────────────────────
    # SECOND PASS: Detect and set parent class relationships
    # ─────────────────────────────────────────────────────────────────────────
    # B-playoff classes (e.g., "P12~B", "H2~B") have players who didn't advance 
    # from the main class group stage. We detect these by looking for the "~B" 
    # suffix in shortname and linking them to their parent class.
    # ─────────────────────────────────────────────────────────────────────────
    parent_links_set = 0
    if valid_classes:
        # Group classes by tournament_id for efficient parent lookup
        classes_by_tournament: dict[int, list[TournamentClass]] = {}
        for tc in valid_classes:
            classes_by_tournament.setdefault(tc.tournament_id, []).append(tc)
        
        for tournament_id, tournament_classes in classes_by_tournament.items():
            # Build lookup: shortname -> tournament_class for this tournament
            shortname_to_class: dict[str, TournamentClass] = {}
            for tc in tournament_classes:
                if tc.shortname:
                    shortname_to_class[tc.shortname] = tc
            
            # Find B-playoff classes and link to parent
            for tc in tournament_classes:
                if not tc.shortname:
                    continue
                
                # Detect B-playoff pattern: shortname contains "~B" (e.g., "P12~B")
                # Also check longname for " B" suffix (e.g., "P12 B")
                parent_shortname = None
                
                if "~B" in tc.shortname:
                    # Extract base shortname: "P12~B" -> "P12"
                    parent_shortname = tc.shortname.replace("~B", "")
                elif tc.shortname.endswith(" B") and len(tc.shortname) > 2:
                    # Fallback: "P12 B" -> "P12"
                    parent_shortname = tc.shortname[:-2]
                
                if not parent_shortname:
                    continue
                
                # Look up parent class in same tournament
                parent_class = shortname_to_class.get(parent_shortname)
                if parent_class and parent_class.tournament_class_id:
                    # Set parent relationship
                    TournamentClass.set_parent_class(
                        cursor,
                        tc.tournament_class_id,
                        parent_class.tournament_class_id
                    )
                    tc.tournament_class_id_parent = parent_class.tournament_class_id
                    parent_links_set += 1
                    
                    if debug:
                        logger.info(
                            {"tournament_class_id_ext": tc.tournament_class_id_ext},
                            f"Set parent class: '{tc.shortname}' -> '{parent_class.shortname}' "
                            f"(parent_id={parent_class.tournament_class_id})"
                        )
    
    if parent_links_set > 0:
        logger.info(f"Set {parent_links_set} parent class relationships")

    logger.summarize()

    return valid_classes

def _detect_type_id(shortname: str, longname: str) -> int:
    l = (longname or "").lower()
    up = (shortname or "").upper()
    tokens = [t for t in re.split(r"[^A-ZÅÄÖ]+", up) if t]
    short_lower = (shortname or "").lower()
    short_words = re.findall(r"\b\w+\b", short_lower)
    long_lower = (longname or "").lower()

    # Team (4)
    if (
        re.search(r"\blag\b", short_lower)
        or re.search(r"\bteams?\b", short_lower)
    ):
        return 4
    if (
        re.search(r"\b(herr(?:ar)?|dam(?:er)?)\s+lag\b", l)
        or "herrlag" in l
        or "damlag" in l
    ):
        return 4
    if any(t in {"HL", "DL", "HLAG", "DLAG", "LAG", "TEAM"} for t in tokens):
        return 4
    if re.search(r"\b[HD]L\d+\b", up) or re.search(r"\b[HD]LAG\d*\b", up):
        return 4
    if short_words and any("lag" in word for word in short_words):
        return 4
    if "lag" in long_lower or "team" in long_lower or "teams" in long_lower:
        return 4

    # Doubles (2) – robust detection for standard abbreviations, mixed variants,
    # compound Swedish/Nordic terms (e.g., "Pojkdubbel", "Herrjuniordubbel"), and generic keywords.
    # Prioritizes specific patterns to minimize false positives on singles or team events.
    # Falls back to a simple substring check for "dubbel"/"double" to catch all embedded compounds reliably.
    if up.startswith(("HD", "DD", "WD", "MD", "MXD", "FD", "PD", "HJD")):  # Expanded: PD (Pojk), HJD (Herrjunior), FD (Flick)
        return 2

    # Catch every Swedish/Nordic mixed-doubles variant in one shot:
    #   Mixdubbel, Mixeddubbel, Mixdedubbel, Mixad dubbel, Mix dubbel, etc.
    #   (Handles optional spaces and common spelling variations.)
    if re.search(r"""
        \b
        (?:mix(?:ed|ad|de)?|mixed)   # Prefix: mix / mixed / mixad / mixde / mixed
        \s*                          # Optional space(s) between prefix and type
        (?:dubbel|dubble|double)d?   # Suffix: dubbel / dubble / double + optional trailing "d"
        \b
    """, l + " " + short_lower, re.IGNORECASE | re.VERBOSE):
        return 2

    # Standalone "Mixed" for youth/junior classes (common shorthand for mixed doubles in Nordic TT tournaments).
    #   Only triggers if no explicit singles indicators (e.g., "singel", "single") are present to avoid false positives.
    if re.search(r"\b mixed \b", l + " " + short_lower, re.IGNORECASE) \
    and not any(singles_kw in l.lower() for singles_kw in ["singel", "single", "enkelt"]):
        return 2

    # Enhanced detection for compound-word doubles common in Swedish/Nordic tournaments:
    #   Pojkdubbel, Herrdubbel, Damdubbel, Flickdubbel (and variants with age qualifiers like Herrjuniordubbel).
    #   Allows optional age/gender modifiers (e.g., "junior", "senior") between prefix and "dubbel" for compounds.
    #   Targets exact compounds to avoid over-matching (e.g., won't catch "subdubbel" in unrelated text).
    compound_doubles_pattern = r"""
        \b
        (?:pojk|herr|dam|flick|her|da|po|fli)  # Gender/age prefix: pojk(pojkar), herr, dam, flick(flickor), shorthands
        (?:junior|senior|vet|ungdom|veteran|\d+)?  # Optional age qualifier (junior/senior/veteran/youth + optional age digits)
        (?:dubbel|dubble|dobbel|dobbelt)         # Doubles suffix (no optional "d" here to avoid "dubbeld" false positives)
        \b
    """
    if re.search(compound_doubles_pattern, l + " " + short_lower, re.IGNORECASE | re.VERBOSE):
        return 2

    # Fallback for isolated generic doubles keywords (e.g., "Dubbel", "Doubles", "Familjedubbel").
    #   Uses word boundaries for standalone matches.
    if re.search(r"\b(doubles?|dubbel|dubble|dobbel|dobbelt|familjedubbel)\b", l + " " + short_lower, re.IGNORECASE):
        return 2

    # Ultimate fallback: Simple substring check for embedded "dubbel"/"double" in compounds
    #   (e.g., "Pojkdubbel", "Herrjuniordubbel"). Safe in tournament name context—false positives rare.
    #   Case-insensitive, non-overlapping.
    if re.search(r"(?i)(dubbel|doubles?|double)", l + " " + short_lower):
        return 2

    # Final check for abbreviation tokens in shortname (e.g., "PD" from "PD14", "HJD" from "HJD18").
    if any(tag in {"HD", "DD", "WD", "MD", "MXD", "FD", "PD", "HJD"} for tag in tokens):  # Expanded to match new startswith
        return 2

    # Unknown/garbage starting with XB/XG (9)
    if up.startswith(("XB", "XG")):
        return 9

    # Default Singles (1)
    return 1


def _line_startswith_keyword(raw_line: str, keywords: tuple[str, ...]) -> bool:
    if not raw_line:
        return False
    stripped = raw_line.strip().lower()
    for kw in keywords:
        kw_lower = kw.lower()
        if stripped.startswith(kw_lower):
            next_char_index = len(kw_lower)
            if next_char_index >= len(stripped):
                return True
            next_char = stripped[next_char_index]
            if not next_char.isalpha():
                return True
    return False

def _infer_structure_id(raw_stages: Optional[str]) -> int:
    """
    Derive structure from the comma-separated stages string.

    NOTE! This has to be adjusted for other sources like Stupa Events!
    """
    if not raw_stages:
        return 9

    try:
        stages = set(int(s) for s in raw_stages.split(",") if s.strip().isdigit())
    except ValueError:
        return 9

    has_groups          = any(s in {3, 4} for s in stages)
    has_ko              = 5 in stages
    has_final_results   = 6 in stages

    if has_groups and has_ko:
        return 1  # STRUCT_GROUPS_AND_KO
    if has_groups and not has_ko:
        return 2  # STRUCT_GROUPS_ONLY (may be refined to 4 later)
    if not has_groups and has_ko:
        return 3  # STRUCT_KO_ONLY
    
    return 9

def _refine_group_structure(
    raw: TournamentClassRaw,
    tournament_id_ext: str,
    *,
    logger: OperationLogger,
    logger_keys: dict,
) -> int:
    """
    Distinguish between plain groups-only (2) and groups→groups (4) structures
    by inspecting the group-stage PDFs for 'Slutspel' sections.

    Falls back to 2 on any parsing/download issues.
    """
    # We only know how to refine when stage 3 or 4 exists.
    if not raw.raw_stages:
        return 2
    try:
        stages = {int(s) for s in raw.raw_stages.split(",") if s.strip().isdigit()}
    except ValueError:
        return 2
    candidate_stages = [st for st in (4, 3) if st in stages]
    if not candidate_stages:
        return 2

    cid_ext = str(raw.tournament_class_id_ext or "")
    for stage in candidate_stages:
        pdf_path, _downloaded, msg = _download_pdf_ondata_by_tournament_class_and_stage(
            tournament_id_ext=tournament_id_ext,
            class_id_ext=cid_ext,
            stage=stage,
            force_download=False,
        )
        if not pdf_path:
            if msg:
                logger.warning(
                    logger_keys,
                    f"Could not load stage {stage} PDF for structure refinement: {msg}",
                )
            continue

        try:
            with pdfplumber.open(pdf_path) as pdf:
                texts = [page.extract_text() or "" for page in pdf.pages]
        except Exception as exc:
            logger.warning(
                logger_keys,
                f"Failed to parse stage {stage} PDF for structure refinement: {exc}",
            )
            continue

        in_slutspel = False
        got_heading = False
        carried_with_star = 0
        new_without_star = 0

        for text in texts:
            if not text:
                continue
            for line in text.splitlines():
                raw_line = line.strip()
                if not raw_line:
                    continue
                # Detect a dedicated 'Slutspel' section heading at the start
                # of a line (not in the class title like 'D-slutspel').
                if _line_startswith_keyword(raw_line, FINAL_GROUP_HEADING_KEYWORDS):
                    in_slutspel = True
                    got_heading = True
                    continue

                if not in_slutspel:
                    continue

                # Within the Slutspel section, look for lines that contain
                # numeric score tokens. Some are carried results (starred),
                # others are new cross-pool matches (no star).
                if not any(ch.isdigit() for ch in raw_line):
                    continue

                has_star_score = bool(re.search(r"\*[^\d\-\+]*[+\-]?\d", raw_line))
                if has_star_score:
                    carried_with_star += 1
                elif "*" not in raw_line:
                    new_without_star += 1

        if debug:
            logger.info(
                logger_keys.copy(),
                f"Refine structure stage={stage}: heading={got_heading}, carried_star={carried_with_star}, new_plain={new_without_star}",
            )

        if got_heading and carried_with_star > 0 and new_without_star > 0:
            return 4  # STRUCT_GROUPS_AND_GROUPS

        if stage == 4:
            pool_players: set[str] = set()
            slutspel_players: set[str] = set()
            current_group: Optional[str] = None

            for text in texts:
                if not text:
                    continue
                for line in text.splitlines():
                    raw_line = line.strip()
                    if not raw_line:
                        continue
                    if _line_startswith_keyword(raw_line, POOL_HEADING_KEYWORDS):
                        current_group = "pool"
                        continue
                    if _line_startswith_keyword(raw_line, FINAL_GROUP_HEADING_KEYWORDS):
                        current_group = "slutspel"
                        continue

                    if current_group not in {"pool", "slutspel"}:
                        continue

                    m = re.match(r"^\d+\s+(.*)$", raw_line)
                    if not m:
                        continue
                    rest = m.group(1)
                    # Cut off trailing numeric stats; keep only the name/club segment.
                    m_name = re.match(r"^(.*?)(?:\s+\d.*)?$", rest)
                    name_part = (m_name.group(1) if m_name else rest).strip()
                    if not name_part:
                        continue

                    if current_group == "pool":
                        pool_players.add(name_part)
                    else:
                        slutspel_players.add(name_part)

            if slutspel_players:
                overlap = len(pool_players & slutspel_players)
                if debug:
                    logger.info(
                        logger_keys.copy(),
                        f"Refine structure stage=4 overlap: pools={len(pool_players)}, slutspel={len(slutspel_players)}, overlap={overlap}",
                    )
                # If most final-group players also appeared in an earlier pool,
                # this strongly suggests a groups→groups structure.
                threshold = max(4, int(0.7 * len(slutspel_players)))
                if overlap >= threshold:
                    return 4  # STRUCT_GROUPS_AND_GROUPS

    return 2
