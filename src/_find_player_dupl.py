#!/usr/bin/env python3
import sqlite3
from collections import defaultdict
from itertools import combinations
import logging

from db import get_conn
from utils import sanitize_name, setup_logging
from models.season import Season

def _season_label(seasons_cache, season_ext):
    s = seasons_cache.get(season_ext)
    if not s:
        return f"(ID {season_ext})"
    # be tolerant to attribute names
    return getattr(s, "description", None) or getattr(s, "season_label", None) or str(season_ext)

def find_cross_season_duplicates(limit=None):
    conn, cursor = get_conn()
    setup_logging()

    # ── 0) Build caches ─────────────────────────────────────────────
    seasons_cache = Season.cache_by_ext(cursor)

    # club_id_ext -> display name (prefer shortname)
    cursor.execute("""
        SELECT ce.club_id_ext, COALESCE(NULLIF(TRIM(c.shortname),''), c.longname, 'Unknown')
        FROM club_id_ext ce
        JOIN club c ON c.club_id = ce.club_id
    """)
    club_cache = {int(row[0]): row[1] for row in cursor.fetchall()}

    # external player id (TEXT) -> internal player_id
    cursor.execute("""
        SELECT player_id_ext, player_id
        FROM player_alias
        WHERE player_id_ext IS NOT NULL AND TRIM(player_id_ext) <> ''
    """)
    alias_int_map = {str(row[0]): row[1] for row in cursor.fetchall()}

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
        ext_key = str(ext)                     # normalize to TEXT to match alias_int_map
        buckets[key][ext_key].add((int(season_ext), int(club_ext)))

    # ── 3) Build suspect list, skipping already-aliased groups ──────
    suspects = []
    for (fn, ln, yb), ext_map in buckets.items():
        ext_ids = sorted(ext_map)
        if len(ext_ids) < 2:
            continue

        # skip if all mapped externals collapse to the same internal id
        mapped_exts = [e for e in ext_ids if e in alias_int_map]
        internal_ids = {alias_int_map[e] for e in mapped_exts}
        if len(mapped_exts) > 1 and len(internal_ids) == 1:
            continue

        best_score = 0.0
        for a, b in combinations(ext_ids, 2):
            recs_a, recs_b = ext_map[a], ext_map[b]

            seasons_a = {s for s,_ in recs_a}
            seasons_b = {s for s,_ in recs_b}
            common_seasons = seasons_a & seasons_b

            # count same-season different-club occurrences
            diff_count = 0
            for s in common_seasons:
                club_a = next(c for ss,c in recs_a if ss == s)
                club_b = next(c for ss,c in recs_b if ss == s)
                if club_a != club_b:
                    diff_count += 1
            if diff_count > 1:
                continue

            # Jaccard on exact (season,club)
            inter = len(recs_a & recs_b)
            union = len(recs_a | recs_b)
            jp = inter / union if union else 0.0

            # Club-only Jaccard
            clubs_a = {c for _,c in recs_a}
            clubs_b = {c for _,c in recs_b}
            inter_c = len(clubs_a & clubs_b)
            union_c = len(clubs_a | clubs_b)
            cj = inter_c / union_c if union_c else 0.0

            # Season adjacency / transfer scoring
            gap = min(abs(x - y) for x in seasons_a for y in seasons_b)
            if diff_count == 0:
                adjacency = 1.0 if gap <= 1 else max(0.0, 1 - gap/10)
            else:
                adjacency = 0.5

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
        print("✅ No cross-season duplicates detected.")
        return

    print(f"⚠️  Suspicious cross-season duplicates (top {limit or len(suspects)}):")
    logging.info(f"⚠️  Suspicious cross-season duplicates (top {limit or len(suspects)}):")
    for idx, s in enumerate(suspects[:limit] if limit else suspects, start=1):
        # print(f"{idx}. {s['name']} (b. {s['year_born']})")
        logging.info(f"{idx}. {s['name']} (b. {s['year_born']})")

        # external → internal
        for ext in s["ext_ids"]:
            internal = alias_int_map.get(ext, "(unmapped)")
            # print(f"    • Ext {ext}  → Int {internal}")
            logging.info(f"    • Ext {ext}  → Int {internal}")

        # seasons spanned
        season_descs = ", ".join(_season_label(seasons_cache, se) + f" [{se}]" for se in s["seasons_spanned"])
        # print(f"    Seasons spanned: {season_descs}")
        logging.info(f"    Seasons spanned: {season_descs}")

        # print(f"    Score: {s['score']:.2f}")
        logging.info(f"    Score: {s['score']:.2f}")

        # per-external breakdown
        for ext in s["ext_ids"]:
            # print(f"    - {ext}:")
            logging.info(f"    - {ext}:")
            for se, club_ext in sorted(s["records"][ext]):
                club_name = club_cache.get(club_ext, f"(club_ext {club_ext})")
                # print(f"        {club_name} ({_season_label(seasons_cache, se)})")
                logging.info(f"        {club_name} ({_season_label(seasons_cache, se)})")
        # print("")


        # --- 5) Emit a copy-paste DUPLICATE_EXT_GROUPS line ----------------
        # format ext ids: prefer ints; if any are non-numeric, quote them
        ints, nonints = [], []
        for e in s["ext_ids"]:
            try:
                ints.append(int(str(e)))
            except ValueError:
                nonints.append(str(e))

        ints.sort()
        nonints.sort()

        parts = [str(i) for i in ints] + [f"'{x}'" for x in nonints]
        stmt = "{" + ", ".join(parts) + f"}},     # {s['name']} (b. {s['year_born']})"


        logging.info("Statement to copy -> upd_players_verified():")
        logging.info(stmt)


        logging.info("Statement to copy -> upd_players_verified(): %s", stmt)        

if __name__ == "__main__":
    find_cross_season_duplicates(limit=0)
