# # src/upd_clubs.py


# import logging
# from db import get_conn
# from clubs_data import CLUBS, CLUB_ALIASES, CLUB_EXT_IDS, CLUBS_COUNTRY_TEAMS, CLUB_ALIASES_COUNTRY_TEAMS

# def upd_clubs():
#     """
#     1) Insert canonical clubs into `club`.
#     2) Insert any extra name-aliases into `club_name_alias`.
#     3) Insert external-ID mappings into `club_ext_id`.
#     """
#     conn, cursor = get_conn()
#     logging.info("Updating clubs...")
#     print("ℹ️  Updating clubs...")

#     try:
#         # Combine clubs and country teams, tagging their source
#         all_clubs = [(club, "canonical clubs") for club in CLUBS] + \
#                     [(club, "country teams") for club in CLUBS_COUNTRY_TEAMS]

#         # Track insertions by category
#         results = {"canonical clubs": {"attempted": 0, "inserted": 0},
#                 "country teams": {"attempted": 0, "inserted": 0}}

#         for (club_id, shortname, longname, club_type, city, country_code, remarks,
#             homepage, active, district_id), category in all_clubs:
#             cursor.execute("""
#                 INSERT OR IGNORE INTO club (
#                     club_id, shortname, longname, club_type,
#                     city, country_code, remarks,
#                     homepage, active, district_id
#                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#             """, (club_id, shortname, longname, club_type, city, country_code,
#                 remarks, homepage, active, district_id))
            
#             results[category]["attempted"] += 1
#             if cursor.rowcount == 1:
#                 results[category]["inserted"] += 1

#         # Log results for each category
#         for category, stats in results.items():
#             ignored = stats["attempted"] - stats["inserted"]
#             print(f"ℹ️  {category.capitalize()}: attempted={stats['attempted']}, "
#                 f"inserted={stats['inserted']}, ignored={ignored}")
#             logging.info(f"{category.capitalize()}: attempted={stats['attempted']}, "
#                         f"inserted={stats['inserted']}, ignored={ignored}")

#     except Exception as e:
#         logging.error(f"Error inserting clubs: {e}")
#         conn.close()
#         return

#     # 2) name-aliases
#     # If you don’t want the canonical names also repeated here,
#     # just fill CLUB_ALIASES with the extra ones you care about.
#     inserted_aliases = 0
#     ignored_aliases = []  # collect the ones INSERT OR IGNORE skipped
#     for club_id, alias_text, alias_type in CLUB_ALIASES:
#         cursor.execute("""
#             INSERT OR IGNORE INTO club_name_alias (
#                 club_id,
#                 alias,
#                 alias_type
#             ) VALUES (?, ?, ?)
#         """, (club_id, alias_text, alias_type))
#         if cursor.rowcount == 1:
#             inserted_aliases += 1       
#         else:
#             # rowcount == 0 means it was ignored
#             ignored_aliases.append((club_id, alias_text, alias_type)) 

#     attempted = len(CLUB_ALIASES)
#     ignored = attempted - inserted_aliases
#     print(f"ℹ️  Name-aliases:    attempted={attempted}, inserted={inserted_aliases}, ignored={ignored}")
#     logging.info(f"Name-aliases: attempted={attempted}, inserted={inserted_aliases}, ignored={ignored}")

#     inserted_aliases = 0
#     ignored_aliases = []  # collect the ones INSERT OR IGNORE skipped
#     for club_id, alias_text, alias_type in CLUB_ALIASES_COUNTRY_TEAMS:
#         cursor.execute("""
#             INSERT OR IGNORE INTO club_name_alias (
#                 club_id,
#                 alias,
#                 alias_type
#             ) VALUES (?, ?, ?)
#         """, (club_id, alias_text, alias_type))
#         if cursor.rowcount == 1:
#             inserted_aliases += 1       
#         else:
#             # rowcount == 0 means it was ignored
#             ignored_aliases.append((club_id, alias_text, alias_type)) 

#     attempted = len(CLUB_ALIASES_COUNTRY_TEAMS)
#     ignored = attempted - inserted_aliases
#     print(f"ℹ️  Name-aliases country teams:    attempted={attempted}, inserted={inserted_aliases}, ignored={ignored}")
#     logging.info(f"Name-aliases country teams:    attempted={attempted}, inserted={inserted_aliases}, ignored={ignored}")

#     # print to debug
#     # if ignored_aliases:
#     #     print("\n📋 Ignored name-aliases:")
#     #     for cid, alias, atype in ignored_aliases:
#     #         print(f"  club_id={cid!r}, alias={alias!r}, alias_type={atype!r}")

#     # 3) external IDs
#     inserted_ext = 0
#     for club_id, club_id_ext in CLUB_EXT_IDS:
#         cursor.execute("""
#             INSERT OR IGNORE INTO club_ext_id (
#                 club_id,
#                 club_id_ext
#             ) VALUES (?, ?)
#         """, (club_id, club_id_ext))
#         if cursor.rowcount == 1:
#             inserted_ext += 1
#     print(f"ℹ️  External IDs:    attempted={len(CLUB_EXT_IDS)}, inserted={inserted_ext}, ignored={len(CLUB_EXT_IDS)-inserted_ext}")
#     logging.info(f"External IDs:    attempted={len(CLUB_EXT_IDS)}, inserted={inserted_ext}, ignored={len(CLUB_EXT_IDS)-inserted_ext}")


#     conn.commit()
#     conn.close()
#     logging.info("Club update complete.")


# src/upd_clubs.py
import logging
from typing import Dict, Tuple, List
from db import get_conn
from utils import print_db_insert_results
from clubs_data import (
    CLUBS,
    CLUB_ALIASES,
    CLUB_EXT_IDS,
    CLUBS_COUNTRY_TEAMS,
    CLUB_ALIASES_COUNTRY_TEAMS,
)

def upd_clubs(dry_run: bool = False) -> None:
    """
    Update canonical clubs, their aliases, and external-ID mappings.

    Steps
    -----
    1) Insert canonical clubs into `club` (separate counts for "canonical clubs" and "country teams").
    2) Insert name aliases into `club_name_alias` (separate counts for clubs and country teams).
    3) Insert external IDs into `club_ext_id`.
    4) Print per-category counts + pretty summary (inserted / attempted).
    5) Print standardized DB summary via print_db_insert_results(db_results).

    Idempotent: uses INSERT OR IGNORE everywhere.
    """

    conn, cursor = get_conn()
    logging.info("Updating clubs...")
    print("ℹ️  Updating clubs...")

    db_results: List[dict] = []

    # ─────────────────────────────────────────────────────────────────────────────
    # 1) Canonical clubs
    # ─────────────────────────────────────────────────────────────────────────────
    categories: Dict[str, Dict[str, int]] = {
        "canonical clubs": {"attempted": 0, "inserted": 0, "ignored": 0},
        "country teams":   {"attempted": 0, "inserted": 0, "ignored": 0},
    }

    def _insert_clubs(rows: List[Tuple], category: str) -> None:
        """
        Insert a batch of clubs into `club`.
        Each row: (club_id, shortname, longname, club_type, city, country_code, remarks, homepage, active, district_id)
        """
        for (
            club_id,
            shortname,
            longname,
            club_type,
            city,
            country_code,
            remarks,
            homepage,
            active,
            district_id,
        ) in rows:
            categories[category]["attempted"] += 1
            try:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO club (
                        club_id, shortname, longname, club_type,
                        city, country_code, remarks, homepage, active, district_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        club_id,
                        shortname,
                        longname,
                        club_type,
                        city,
                        country_code,
                        remarks,
                        homepage,
                        active,
                        district_id,
                    ),
                )
                if cursor.rowcount == 1:
                    categories[category]["inserted"] += 1
                    db_results.append({
                        "status": "success",
                        "club_id": club_id,
                        "reason": f"Inserted {category[:-1]}"  # drop trailing 's' for readability
                    })
                else:
                    categories[category]["ignored"] += 1
                    db_results.append({
                        "status": "skipped",
                        "club_id": club_id,
                        "reason": f"{category.capitalize()} already existed"
                    })
            except Exception as e:
                db_results.append({
                    "status": "failed",
                    "club_id": club_id,
                    "reason": f"{category.capitalize()} insert error: {e}"
                })

    _insert_clubs(CLUBS, "canonical clubs")
    _insert_clubs(CLUBS_COUNTRY_TEAMS, "country teams")

    # # Per-category prints (match your requested lines)
    # for cat, stats in categories.items():
    #     print(
    #         f"ℹ️  {cat.capitalize()}: attempted={stats['attempted']}, "
    #         f"inserted={stats['inserted']}, ignored={stats['ignored']}"
    #     )
    #     logging.info(
    #         f"{cat.capitalize()}: attempted={stats['attempted']}, "
    #         f"inserted={stats['inserted']}, ignored={stats['ignored']}"
    #     )

    # ─────────────────────────────────────────────────────────────────────────────
    # 2) Name aliases (club_name_alias)
    # ─────────────────────────────────────────────────────────────────────────────
    def _insert_aliases(rows: List[Tuple[int, str, str]], label: str) -> Tuple[int, int, int]:
        attempted = len(rows)
        inserted = 0
        for (club_id, alias_text, alias_type) in rows:
            try:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO club_name_alias (club_id, alias, alias_type)
                    VALUES (?, ?, ?)
                    """,
                    (club_id, alias_text, alias_type),
                )
                if cursor.rowcount == 1:
                    inserted += 1
                    db_results.append({
                        "status": "success",
                        "club_id": club_id,
                        "reason": f"Inserted {label[:-1]}"  # remove trailing 's' in label
                    })
                else:
                    db_results.append({
                        "status": "skipped",
                        "club_id": club_id,
                        "reason": f"{label} already existed"
                    })
            except Exception as e:
                db_results.append({
                    "status": "failed",
                    "club_id": club_id,
                    "reason": f"{label} insert error: {e}"
                })
        ignored = attempted - inserted
        # print(f"ℹ️  {label}: attempted={attempted}, inserted={inserted}, ignored={ignored}")
        # logging.info(f"{label}: attempted={attempted}, inserted={inserted}, ignored={ignored}")
        return attempted, inserted, ignored

    aliases_attempted_1, aliases_inserted_1, aliases_ignored_1 = _insert_aliases(CLUB_ALIASES, "Name-aliases")
    aliases_attempted_2, aliases_inserted_2, aliases_ignored_2 = _insert_aliases(
        CLUB_ALIASES_COUNTRY_TEAMS, "Name-aliases (country teams)"
    )

    # ─────────────────────────────────────────────────────────────────────────────
    # 3) External IDs (club_ext_id)
    # ─────────────────────────────────────────────────────────────────────────────
    attempted_ext = len(CLUB_EXT_IDS)
    inserted_ext = 0
    for (club_id, club_id_ext) in CLUB_EXT_IDS:
        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO club_ext_id (club_id, club_id_ext)
                VALUES (?, ?)
                """,
                (club_id, club_id_ext),
            )
            if cursor.rowcount == 1:
                inserted_ext += 1
                db_results.append({
                    "status": "success",
                    "club_id": club_id,
                    "reason": "Inserted external ID"
                })
            else:
                db_results.append({
                    "status": "skipped",
                    "club_id": club_id,
                    "reason": "External ID already existed"
                })
        except Exception as e:
            db_results.append({
                "status": "failed",
                "club_id": club_id,
                "reason": f"External ID insert error: {e}"
            })
    # ignored_ext = attempted_ext - inserted_ext
    # print(f"ℹ️  External IDs:  attempted={attempted_ext}, inserted={inserted_ext}, ignored={ignored_ext}")
    # logging.info(f"External IDs: attempted={attempted_ext}, inserted={inserted_ext}, ignored={ignored_ext}")

    # ─────────────────────────────────────────────────────────────────────────────
    # 4) Manual review backlog (club_missing)
    # ─────────────────────────────────────────────────────────────────────────────
    try:
        cursor.execute("SELECT COUNT(*) FROM club_missing")
        pending_review = cursor.fetchone()[0] or 0
    except Exception:
        pending_review = 0  # if table doesn't exist yet


    # ─────────────────────────────────────────────────────────────────────────────
    # Commit or rollback
    # ─────────────────────────────────────────────────────────────────────────────
    if dry_run:
        conn.rollback()
    else:
        conn.commit()
    conn.close()

    # ─────────────────────────────────────────────────────────────────────────────
    # Pretty summary (inserted / attempted) — exactly mirrors the per-category lines
    # ─────────────────────────────────────────────────────────────────────────────
    print("\n──────────────── Summary ────────────────")
    print(f"   DRY RUN: {'ON' if dry_run else 'OFF'}")
    print(f"   Canonical clubs:     {categories['canonical clubs']['inserted']:,} / {categories['canonical clubs']['attempted']:,}")
    print(f"   Club aliases:        {aliases_inserted_1:,} / {aliases_attempted_1:,}")
    print(f"   Country teams:       {categories['country teams']['inserted']:,} / {categories['country teams']['attempted']:,}")
    print(f"   Country aliases:     {aliases_inserted_2:,} / {aliases_attempted_2:,}")
    print(f"   External IDs:        {inserted_ext:,} / {attempted_ext:,}")
    print("")
    print(f"   Pending review: {pending_review:,} [clubs_missing]")   # ← add this line
    print("─────────────────────────────────────────\n")

    # ─────────────────────────────────────────────────────────────────────────────
    # Standardized summary (same style used elsewhere)
    # ─────────────────────────────────────────────────────────────────────────────
    print_db_insert_results(db_results)
    print("")

if __name__ == "__main__":
    upd_clubs(dry_run=False)
