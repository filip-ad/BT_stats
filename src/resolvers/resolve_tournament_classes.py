# src/resolve_tournament_classes.py
import sqlite3
from typing import Optional, List
import re
from utils import OperationLogger
from models.tournament_class import TournamentClass
from models.tournament_class_raw import TournamentClassRaw
from models.tournament import Tournament
from datetime import date

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

    raw_count = len(raw_objects)
    logger.info(f"Resolving {raw_count} tournament_class_raw entries...")

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
        tournament_class = TournamentClass(
            tournament_class_id_ext=raw.tournament_class_id_ext,
            tournament_id=tournament_id,
            tournament_class_type_id=_detect_type_id(raw.shortname or "", raw.longname or ""),
            tournament_class_structure_id=_infer_structure_id(raw.raw_stages),
            startdate=raw.startdate,
            longname=raw.longname,
            shortname=raw.shortname,
            gender=raw.gender,
            max_rank=raw.max_rank,
            max_age=raw.max_age,
            url=raw.url,
            data_source_id=raw.data_source_id,
        )

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

    logger.summarize()

    return valid_classes

def _detect_type_id(shortname: str, longname: str) -> int:
    l = (longname or "").lower()
    up = (shortname or "").upper()
    tokens = [t for t in re.split(r"[^A-ZÅÄÖ]+", up) if t]

    # Team (4)
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

    # Doubles (2)
    if up.startswith(("HD", "DD", "WD", "MD", "MXD", "FD")):
        return 2
    if re.search(r"\b(doubles?|dubbel|dubble|dobbel|dobbelt|familjedubbel)\b", l):
        return 2
    if any(tag in tokens for tag in {"HD", "DD", "WD", "MD", "MXD", "FD"}):
        return 2

    # Unknown/garbage starting with XB/XG (9)
    if up.startswith(("XB", "XG")):
        return 9

    # Default Singles (1)
    return 1

def _infer_structure_id(raw_stages: Optional[str]) -> int:
    """
    Derive structure from the comma-separated stages string.
    """
    if not raw_stages:
        return 9

    try:
        stages = set(int(s) for s in raw_stages.split(",") if s.strip().isdigit())
    except ValueError:
        return 9

    has_groups = any(s in {3, 4} for s in stages)
    has_ko = 5 in stages

    if has_groups and has_ko:
        return 1  # STRUCT_GROUPS_AND_KO
    if has_groups and not has_ko:
        return 2  # STRUCT_GROUPS_ONLY
    if not has_groups and has_ko:
        return 3  # STRUCT_KO_ONLY
    return 9