#!/usr/bin/env python3
import sqlite3
from collections import defaultdict
from itertools import combinations
from db import get_conn
from utils import sanitize_name, setup_logging
import logging

from models.season import Season

def find_cross_season_duplicates(limit=None):
    conn, cursor = get_conn()
    setup_logging()

    # ── 0) Build caches ─────────────────────────────────────────────
    seasons_cache = Season.cache_by_ext(cursor)  
    cursor.execute(
        "SELECT ca.club_id_ext, c.name "
        "FROM club_alias ca JOIN club c ON ca.club_id = c.club_id"
    )
    club_cache = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.execute("SELECT player_id_ext, player_id FROM player_alias")
    alias_int_map = {row[0]: row[1] for row in cursor.fetchall()}

    # ── 1) Pull raw license rows ────────────────────────────────────
    cursor.execute("""
      SELECT season_id_ext, club_id_ext, player_id_ext,
             firstname, lastname, year_born
        FROM player_license_raw
       WHERE TRIM(firstname) <> ''
         AND TRIM(lastname)  <> ''
         AND year_born IS NOT NULL
    """)
    rows = cursor.fetchall()
    conn.close()

    # ── 2) Bucket by (fn,ln,yb) → ext → set((season,club)) ────────
    buckets = defaultdict(lambda: defaultdict(set))
    for season_ext, club_ext, ext, fn, ln, yb in rows:
        key = (sanitize_name(fn), sanitize_name(ln), int(yb))
        buckets[key][ext].add((season_ext, club_ext))

    # ── 3) Build suspect list, skipping already-aliased groups ──────
    suspects = []
    for (fn, ln, yb), ext_map in buckets.items():
        ext_ids = sorted(ext_map)
        if len(ext_ids) < 2:
            continue

        # skip if >1 externals already map to the same internal ID
        mapped_exts = [e for e in ext_ids if e in alias_int_map]
        internal_ids = {alias_int_map[e] for e in mapped_exts}
        if len(mapped_exts) > 1 and len(internal_ids) == 1:
            continue

        best_score = 0.0
        for a, b in combinations(ext_ids, 2):
            recs_a, recs_b = ext_map[a], ext_map[b]

            # 1) Gather seasons & count shared-season club differences
            seasons_a = {s for s,_ in recs_a}
            seasons_b = {s for s,_ in recs_b}
            common_seasons = seasons_a & seasons_b

            diff_count = 0
            for s in common_seasons:
                club_a = next(c for ss,c in recs_a if ss == s)
                club_b = next(c for ss,c in recs_b if ss == s)
                if club_a != club_b:
                    diff_count += 1

            # Skip pairs with >1 same-season different-club appearances
            if diff_count > 1:
                continue

            # 2) Jaccard on exact (season,club)
            inter = len(recs_a & recs_b)
            union = len(recs_a | recs_b)
            jp = inter / union if union else 0.0

            # 3) Club-only Jaccard
            clubs_a = {c for _,c in recs_a}
            clubs_b = {c for _,c in recs_b}
            inter_c = len(clubs_a & clubs_b)
            union_c = len(clubs_a | clubs_b)
            cj = inter_c / union_c if union_c else 0.0

            # 4) Season adjacency & transfer scoring
            # Compute minimum gap between any seasons
            gap = min(abs(x - y) for x in seasons_a for y in seasons_b)

            if diff_count == 0:
                # either shared season(s) at same club, or back-to-back with same club
                adjacency = 1.0 if gap <= 1 else max(0.0, 1 - gap/10)
            else:
                # exactly one shared season but in different clubs → likely transfer
                adjacency = 0.5

            # 5) Combined score
            combined = max(jp, cj, adjacency)
            best_score = max(best_score, combined)


        seasons_spanned = sorted({s for ext in ext_ids for s, _ in ext_map[ext]})
        suspects.append({
            "name":           f"{fn} {ln}",
            "year_born":      yb,
            "ext_ids":        ext_ids,
            "records":        ext_map,
            "score":          best_score,
            "seasons_spanned": seasons_spanned
        })

    # ── 4) Sort & log ────────────────────────────────────────────────
    suspects.sort(key=lambda x: (x["score"], -len(x["ext_ids"])), reverse=True)

    if not suspects:
        logging.info("✅ No cross-season duplicates detected.")
        return

    logging.info(f"⚠️  Suspicious cross-season duplicates (top {limit or len(suspects)}):")
    print(f"⚠️  Suspicious cross-season duplicates (top {limit or len(suspects)}):")
    for idx, s in enumerate(suspects[:limit] if limit else suspects, start=1):
        logging.info(f"{idx}. {s['name']} (b. {s['year_born']})")

        # external → internal
        for ext in s["ext_ids"]:
            internal = alias_int_map.get(ext, "(unmapped)")
            logging.info(f"    • Ext {ext}  → Int {internal}")

        # seasons spanned
        season_descs = ", ".join(
            f"{seasons_cache[se].description} [{se}]"
            for se in s["seasons_spanned"]
        )
        logging.info(f"    Seasons spanned: {season_descs}")

        # score
        logging.info(f"    Score: {s['score']:.2f}")

        # per-external club@season breakdown
        for ext in s["ext_ids"]:
            logging.info(f"    - {ext}:")
            for se, club_ext in sorted(s["records"][ext]):
                club_name = club_cache.get(club_ext, f"(ID {club_ext})")
                season_desc = (
                    seasons_cache[se].description
                    if se in seasons_cache else f"(ID {se})"
                )
                logging.info(f"        {club_name} ({season_desc})")
        logging.info("")  # blank line

if __name__ == "__main__":
    find_cross_season_duplicates(limit=20)
