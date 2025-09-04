# src/resolvers/resolve_tournament_classes.py
import logging
from typing import Optional
import re
from utils import OperationLogger
from models.tournament_class import TournamentClass
from models.tournament_class_raw import TournamentClassRaw
from models.tournament import Tournament

def detect_type_id(shortname: str, longname: str) -> int:
    l  = (longname or "").lower()
    up = (shortname or "").upper()
    tokens = [t for t in re.split(r"[^A-ZÅÄÖ]+", up) if t]

    # --- Team (4) ---
    if (re.search(r"\b(herr(?:ar)?|dam(?:er)?)\s+lag\b", l)
        or "herrlag" in l or "damlag" in l):
        return 4
    if any(t in {"HL", "DL", "HLAG", "DLAG", "LAG", "TEAM"} for t in tokens):
        return 4
    if re.search(r"\b[HD]L\d+\b", up) or re.search(r"\b[HD]LAG\d*\b", up):
        return 4

    # --- Doubles (2) ---
    # prefix handles cases like "HDEliteYdr"
    if up.startswith(("HD","DD","WD","MD","MXD","FD")):
        return 2
    if re.search(r"\b(doubles?|dubbel|dubble|dobbel|dobbelt|Familjedubbel)\b", l):
        return 2
    if any(tag in tokens for tag in {"HD","DD","WD","MD","MXD","FD"}):
        return 2
    
    # --- Unknown/garbage starting with XD (9) ---
    if up.startswith(("XB", "XG")):
        return 9

    # --- Default Singles (1) ---
    return 1

def infer_structure_id(raw_stages: Optional[str]) -> int:
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

def resolve_tournament_classes(cursor) -> None:
    """
    Resolve tournament classes: Fetch pending raw entries using class method (those not yet in regular table),
    resolve dependencies (e.g., tournament_id), infer fields, validate,
    upsert to regular table if successful. Do not delete or mark raw entries.
    """
    logger = OperationLogger(
        verbosity=2,
        print_output=False,
        log_to_db=False,
        cursor=cursor
    )

    pending_raws = TournamentClassRaw.get_pending(cursor)

    if not pending_raws:
        logger.info("No pending tournament_class_raw entries to resolve.")
        return

    logger.info(f"Resolving {len(pending_raws)} pending tournament_class_raw entries...")

    for tc_raw in pending_raws:
        logger_keys = {'shortname': tc_raw.shortname or 'unknown', 'startdate': str(tc_raw.startdate or 'None'), 'raw_id': str(tc_raw.row_id)}

        try:
            # Resolve tournament_id from ext
            tournament_ids = Tournament.get_internal_tournament_ids(
                cursor, [str(tc_raw.tournament_id_ext)], tc_raw.data_source_id
            )
            if not tournament_ids:
                logger.failed(logger_keys, f"No matching tournament for ext_id {tc_raw.tournament_id_ext} (ds {tc_raw.data_source_id})")
                continue
            tournament_id = tournament_ids[0]

            # Prepare dict for TournamentClass
            d = tc_raw.__dict__.copy()
            d['tournament_id'] = tournament_id
            d['tournament_class_type_id'] = detect_type_id(d['shortname'] or '', d['longname'] or '')
            if d.get('raw_stages'):
                d['tournament_class_structure_id'] = infer_structure_id(d['raw_stages'])
            d.pop('row_id', None)
            d.pop('tournament_id_ext', None)
            d.pop('raw_stages', None)
            d.pop('raw_stage_hrefs', None)
            d['date'] = d.pop('startdate', None)  # Map startdate to date for TournamentClass

            # Create TournamentClass
            tournament_class = TournamentClass.from_dict(d)

            # Proper validation
            validation_result = tournament_class.validate(logger, logger_keys)
            if validation_result["status"] != "success":
                logger.failed(logger_keys, validation_result["reason"])
                continue

            # Upsert to regular table
            tournament_class.upsert(cursor, logger, logger_keys)

            logger.success(logger_keys, "Tournament class resolved and upserted")

        except Exception as e:
            logger.failed(logger_keys, f"Exception during resolution: {e}")
            continue