# src/resolvers/resolve_player_ranking_groups.py

import logging
import time
from typing import Dict, Set, Tuple
from models.player import Player
from models.player_ranking_group import PlayerRankingGroup
from models.player_license_raw import PlayerLicenseRaw
from models.ranking_group import RankingGroup
from utils import OperationLogger

def resolve_player_ranking_groups(cursor, run_id=None) -> dict:
    """
    Build and APPLY the current (player_id, ranking_group_id) relations from ALL rows in player_license_raw
    that have non-empty ranking_group_raw. Season is irrelevant (current-only model).

    Side effects:
      - Deletes existing player_ranking_group rows for players we’re about to refresh
      - Inserts the new relations (deduped)
      - Logs via OperationLogger

    Returns:
      stats dict (rows_scanned, players_scoped, deleted_rows, inserted, skipped, failed,
                  unmapped_players, unknown_groups, elapsed_sec)
    """

    logger = OperationLogger(
        verbosity       = 2, 
        print_output    = False, 
        log_to_db       = False, 
        cursor          = cursor,
        object_type     = "player_ranking_group",
        run_type        = "resolve",
        run_id          = run_id
    )

    logger.info("Resolving player ranking groups...", to_console=True)

    t0 = time.time()

    # Only profixio as source currently
    data_source_id: int = 3

    # Map external ids -> player_id for the given data source
    cache = Player.cache_id_ext_map(cursor)  # Dict[(ext, ds_id)] -> Player
    ext_to_player: Dict[str, int] = {
        ext: player.player_id
        for (ext, ds_id), player in cache.items()
        if ds_id == data_source_id
    }

    # ranking group lookup: class_short -> id  (via model)
    rg_map: Dict[str, int] = RankingGroup.cache_map(cursor)

    # Fetch ALL rows with non-empty ranking_group_raw (no season filter)  (via model)
    rows = PlayerLicenseRaw.fetch_rows_with_ranking_groups(cursor)
    rows_scanned = len(rows)

    # Determine which players to refresh
    player_ids_to_refresh: Set[int] = set()
    unmapped_players = 0
    for player_id_ext, _ in rows:
        pid = ext_to_player.get(str(player_id_ext))
        if pid:
            player_ids_to_refresh.add(pid)
        else:
            unmapped_players += 1

    # Delete existing rows for those players  (via model)
    deleted_rows = 0
    if player_ids_to_refresh:
        deleted_rows = PlayerRankingGroup.delete_by_player_ids(cursor, player_ids_to_refresh)
        logger.info(f"Deleted {deleted_rows} existing rows for {len(player_ids_to_refresh)} player(s)", to_console=True)
    else:
        logger.info("No players to refresh; delete skipped", to_console=True)

    # Build and insert new relations (deduped)
    seen: Set[Tuple[int, int]] = set()
    inserted = skipped = failed = 0
    unknown_tokens: Dict[str, int] = {}

    for player_id_ext, raw_groups in rows:
        pid = ext_to_player.get(str(player_id_ext))
        if not pid:
            continue  # counted above

        tokens = [t.strip() for t in (raw_groups or "").split(",") if t.strip()]
        for token in tokens:
            rg_id = rg_map.get(token)
            if not rg_id:
                unknown_tokens[token] = unknown_tokens.get(token, 0) + 1
                continue

            key = (pid, rg_id)
            if key in seen:
                continue
            seen.add(key)

            item_key = f"player_id={pid}, rg_id={rg_id}"

            res = PlayerRankingGroup(player_id=pid, ranking_group_id=rg_id).save_to_db(cursor)
            status = res.get("status")
            if status == "success":
                inserted += 1
                logger.success(item_key, "Ranking group updated successfully")
            elif status == "skipped":
                skipped += 1
                logger.skipped(item_key, "Ranking group update skipped")
            else:
                failed += 1
                logger.failed(item_key, f"Ranking group update failed: reason={res.get('reason')}")

    if unknown_tokens:
        sample = sorted(unknown_tokens.items(), key=lambda x: -x[1])[:10]
        logger.warning("ranking_groups", f"Unknown ranking group tokens (top): {sample}")

    stats = {
        "rows_scanned": rows_scanned,
        "players_scoped": len(player_ids_to_refresh),
        "deleted_rows": deleted_rows,
        "inserted": inserted,
        "skipped": skipped,
        "failed": failed,
        "unmapped_players": unmapped_players,
        "unknown_groups": unknown_tokens,
        "elapsed_sec": round(time.time() - t0, 2),
    }

    logger.info(f"rows_scanned={rows_scanned}, players_scoped={stats['players_scoped']}, "
                f"deleted_rows={deleted_rows}, inserted={inserted}, skipped={skipped}, "
                f"failed={failed}, unmapped={unmapped_players}, elapsed={stats['elapsed_sec']}s")

    logger.summarize()

    return stats
