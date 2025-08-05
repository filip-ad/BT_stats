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
    cursor.execute("SELECT ca.club_id_ext, c.name "
                   "FROM club_alias ca JOIN club c ON ca.club_id = c.club_id")
    club_cache = {row[0]: row[1] for row in cursor.fetchall()}

    # Map external → internal player_id
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

        # # If *all* these externals already map to the *same* internal ID, skip:
        # internal_ids = {alias_int_map.get(e) for e in ext_ids if e in alias_int_map}
        # if len(internal_ids) == 1 and len(internal_ids) == len(ext_ids):
        #     # every ext_id in this group is known and points to the same player_id
        #     continue

        # How many externals do we actually have a mapping for?
        mapped_exts = [e for e in ext_ids if e in alias_int_map]
        internal_ids = {alias_int_map[e] for e in mapped_exts}

        # If more than one external, and they all point to the same internal, skip:
        if len(mapped_exts) > 1 and len(internal_ids) == 1:
            continue        

        # compute the combined similarity score
        best_score = 0.0
        for a, b in combinations(ext_ids, 2):
            recs_a, recs_b = ext_map[a], ext_map[b]

            # 3.1) Jaccard over (season,club)
            inter = len(recs_a & recs_b)
            union = len(recs_a | recs_b)
            jp = inter/union if union else 0.0

            # 3.2) Club‐only Jaccard
            ca = {club for _, club in recs_a}
            cb = {club for _, club in recs_b}
            inter_c = len(ca & cb)
            union_c = len(ca | cb)
            cj = inter_c/union_c if union_c else 0.0

            # 3.3) Season adjacency
            sa = {s for s,_ in recs_a}
            sb = {s for s,_ in recs_b}
            gap = min(abs(x-y) for x in sa for y in sb)
            adj = 1.0 if gap == 1 else max(0.0, 1 - gap/10)

            combined = max(jp, cj, adj)
            best_score = max(best_score, combined)

        seasons_spanned = sorted({s for ext in ext_ids for s,_ in ext_map[ext]})
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
    find_cross_season_duplicates(limit=500)
