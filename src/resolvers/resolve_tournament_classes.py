# src/resolvers/resolve_tournament_classes.py

import logging
import sqlite3
from utils import OperationLogger
from models.tournament_class import TournamentClass

def resolve_tournament_classes(cursor: sqlite3.Cursor, logger: OperationLogger) -> None:
    """
    Resolve tournament classes: Select unprocessed raw entries, validate, upsert to regular table,
    then mark as processed.
    """
    # Select unprocessed raw entries
    cursor.execute("""
        SELECT * FROM tournament_class_raw
        WHERE processed = 0
        ORDER BY row_created ASC;
    """)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    raw_dicts = [dict(zip(columns, row)) for row in rows]

    if not raw_dicts:
        logger.info("No unprocessed tournament_class_raw entries to resolve.")
        return

    logger.info(f"Resolving {len(raw_dicts)} unprocessed tournament_class_raw entries...")

    for raw_dict in raw_dicts:
        item_key = f"{raw_dict['shortname']} ({raw_dict['date']}) (raw_id: {raw_dict['tournament_class_raw_id']})"

        try:
            # Create TournamentClass from raw dict (fields align, extra ignored)
            tournament_class = TournamentClass.from_dict(raw_dict)

            # Proper validation
            validation_result = tournament_class.validate(logger, item_key)
            if validation_result["status"] != "success":
                # Optionally update raw with error, but for now just skip upsert
                logger.failed(item_key, validation_result["reason"])
                continue

            # Upsert to regular table
            tournament_class.upsert(cursor, logger, item_key)

            # Mark raw as processed
            cursor.execute(
                "UPDATE tournament_class_raw SET processed = 1, row_updated = CURRENT_TIMESTAMP WHERE tournament_class_raw_id = ?",
                (raw_dict["tournament_class_raw_id"],)
            )
            logger.success(item_key, "Tournament class resolved and upserted")

        except Exception as e:
            logger.failed(item_key, f"Exception during resolution: {e}")
            continue