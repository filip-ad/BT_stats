def upd_group_stage_for_class(cursor, tc: TournamentClass, pdf_bytes: bytes):
    # 0) caches
    part_map_by_player = PlayerParticipant.cache_by_class_id_player(cursor)  # {class_id: {player_id: (participant_id,club_id)}}
    name_map           = Player.cache_name_map(cursor)
    unverified_map     = Player.cache_unverified_name_map(cursor)
    club_map           = Club.cache_name_map(cursor)

    # 1) parse pdf → groups: [{name, matches: [(p1_name, p1_club),(p2_name,p2_club), games: [(s1,s2),...]]}, ...]
    groups = parse_groups_pdf(pdf_bytes)  # you & I can implement next

    # 2) upsert groups
    for g in groups:
        group_id = upsert_group(cursor, tc.tournament_class_id, g["name"])

        # resolve & collect members (participant_ids)
        members = set()
        for m in g["matches"]:
            p1 = resolve_participant_id(
                cursor, tc.tournament_class_id, m["p1_name"], m["p1_club"],
                name_map, unverified_map, part_map_by_player, club_map
            )
            p2 = resolve_participant_id(... same ...)
            if not p1 or not p2:
                record_parse_missing(...)
                continue
            members.update([p1, p2])

            # insert match
            match_id = insert_match(cursor, tc.tournament_class_id, stage_code='GROUP', group_id=group_id, best_of=len(m["games"]))
            insert_side(cursor, match_id, side=1, participant_id=p1)
            insert_side(cursor, match_id, side=2, participant_id=p2)

            for i,(s1,s2) in enumerate(m["games"], start=1):
                insert_game(cursor, match_id, i, s1, s2)

        # upsert members
        for pid in members:
            cursor.execute("""
                INSERT OR IGNORE INTO tournament_group_member(group_id, participant_id)
                VALUES (?,?)
            """, (group_id, pid))

        recompute_group_standings(cursor, group_id)
