# src/resolve_data.py

import logging
from db import get_conn

from resolvers.resolve_tournaments import resolve_tournaments
from resolvers.resolve_tournament_classes import resolve_tournament_classes
from resolvers.resolve_player_licenses import resolve_player_licenses
# from resolvers.resolve_player_rankings import resolve_player_rankings
from resolvers.resolve_player_ranking_groups import resolve_player_ranking_groups
from resolvers.resolve_player_transitions import resolve_player_transitions
from resolvers.resolve_participants import resolve_participants


def resolve_data(rsv_tournaments=False, rsv_tournament_classes=False, rsv_player_licenses=False, rsv_player_ranking_groups=False, rsv_player_transitions=False, rsv_participants=False):
    """
    Updater entry point: Scrape raw data, process through pipeline, aggregate results.
    """
    conn, cursor = get_conn()

    try:

        # Scrape ondata listed tournaments
        # =============================================================================
        if rsv_tournaments:
            resolve_tournaments(cursor)

        if rsv_tournament_classes:
            resolve_tournament_classes(cursor)

        if rsv_player_licenses:
            resolve_player_licenses(cursor)

        if rsv_player_transitions:
            resolve_player_transitions(cursor)

        if rsv_participants:
            resolve_participants(cursor)

    except Exception as e:
        logging.error(f"Error in resolve_data: {e}", stack_info=True, stacklevel=3, exc_info=True)

    finally:
        conn.commit()
        conn.close()