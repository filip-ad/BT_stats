# src/resolvers/resolve_player_rankings.py

from typing import List
from models.player_ranking_raw import PlayerRankingRaw
from models.player_ranking import PlayerRanking
from models.player import Player
from utils import OperationLogger
from db import get_conn

def resolve_player_rankings(cursor, run_id=None) -> List[PlayerRanking]:
    """Resolving player_ranking_raw to player_ranking. Handles mapping, validation, and upsert."""
    # Initializing logger
    logger = OperationLogger(
        verbosity=2,
        print_output=False,
        log_to_db=True,
        cursor=cursor,
        object_type="player_ranking",
        run_type="resolve",
        run_id=run_id
    )

    logger.info("Resolving player rankings...", to_console=True)

    # Checking row count in player_ranking_raw
    cursor.execute("SELECT COUNT(*) FROM player_ranking_raw")
    row_count = cursor.fetchone()[0]
    logger.info(f"Found {row_count} player ranking records in player_ranking_raw", to_console=True)

    # Fetching all raw ranking records
    raw_objects = PlayerRankingRaw.get_all(cursor)
    if not raw_objects:
        logger.failed({}, "No player ranking data found in player_ranking_raw")
        return []
    
    # Caching valid player_id_ext + data_source_id combinations
    valid_exts = Player.cache_id_ext_set(cursor)
    logger.info(f"Cached {len(valid_exts):,} player_id_ext mappings", to_console=True)

    # Tracking processed counts
    total_inserted = 0
    total_updated = 0
    total_unchanged = 0
    batch_size = 1000
    batch = []

    # Processing raw rankings
    for raw in raw_objects:
        logger.inc_processed()

        # Every 100k processed, log progress
        if logger.processed % 100_000 == 0:
            logger.info(
                f"Processed {logger.processed:,} / {row_count:,} data points "
                f"({logger.processed / row_count:.1%})",
                to_console=True
            )

        logger_keys = {
            "raw_row_id":                   raw.row_id,
            "run_id_ext":                   raw.run_id_ext,
            "run_date":                     raw.run_date,
            "player_id_ext":                raw.player_id_ext,
            "firstname":                    raw.firstname,
            "lastname":                     raw.lastname,
            "year_born":                    raw.year_born,
            "club_name":                    raw.club_name,
            "points":                       raw.points,
            "points_change_since_last":     raw.points_change_since_last,
            "position_world":               raw.position_world,
            "position":                     raw.position
        }

        # Validating raw data
        is_valid, err = raw.validate()
        if not is_valid:
            logger.failed(logger_keys.copy(), err)
            continue

        # Creating PlayerRanking instance
        ranking = PlayerRanking(
            run_id_ext                  = raw.run_id_ext,
            run_date                    = raw.run_date,
            player_id_ext               = raw.player_id_ext,
            points                      = raw.points                    if raw.points is not None else 0,
            points_change_since_last    = raw.points_change_since_last  if raw.points_change_since_last is not None else 0,
            position_world              = raw.position_world            if raw.position_world is not None else 0,
            position                    = raw.position                  if raw.position is not None else 0
        )

        # Adding to batch
        batch.append((ranking, logger_keys))
        if len(batch) >= batch_size:
            # Processing batch
            for ranking, keys in batch:
                # Validating ranking
                is_valid, err = ranking.validate(cursor)
                if not is_valid:
                    logger.failed(keys.copy(), err)
                    continue

                # Upserting ranking
                result = ranking.upsert(cursor)
                if result == "inserted":
                    total_inserted += 1
                    logger.success(keys.copy(), "Player ranking inserted")
                elif result == "updated":
                    total_updated += 1
                    logger.success(keys.copy(), "Player ranking updated")
                elif result == "unchanged":
                    total_unchanged += 1
                    logger.success(keys.copy(), "Player ranking unchanged")
                else:
                    logger.failed(keys.copy(), "Upsert failed")

            # Committing batch
            cursor.connection.commit()
            batch = []

    # Processing remaining batch
    if batch:
        for ranking, keys in batch:
            # Validating ranking
            is_valid, err = ranking.validate(cursor, valid_exts=valid_exts)
            if not is_valid:
                logger.failed(keys.copy(), err)
                continue

            # Upserting ranking
            result = ranking.upsert(cursor)
            if result == "inserted":
                total_inserted += 1
                logger.success(keys.copy(), "Player ranking inserted")
            elif result == "updated":
                total_updated += 1
                logger.success(keys.copy(), "Player ranking updated")
            elif result == "unchanged":
                total_unchanged += 1
                logger.success(keys.copy(), "Player ranking unchanged")
            else:
                logger.failed(keys.copy(), "Upsert failed")

        # Committing final batch
        cursor.connection.commit()

    # Logging summary
    logger.info(
        f"Resolving completed — Total inserted: {total_inserted}, total updated: {total_updated}, total unchanged: {total_unchanged}",
        to_console=True
    )
    logger.summarize()

    return []

def resolve_player_rankings_main():
    """Entry point for resolving player rankings."""
    # Opening database connection
    conn, cursor = get_conn()
    try:
        # Running resolver for latest run
        resolve_player_rankings(cursor, run_id="386")
    finally:
        # Committing and closing connection
        conn.commit()
        conn.close()