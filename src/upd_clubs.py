# src/upd_clubs.py

import logging
from typing import Dict, Tuple, List
from db import get_conn
from utils import print_db_insert_results
from clubs_data import (
    CLUBS,
    CLUB_ALIASES,
    CLUB_ID_EXT,
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
    3) Insert external IDs into `club_id_ext`.
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
                    logging.info(f"{category.capitalize()} {shortname} already existed for club_id={club_id}")
            except Exception as e:
                db_results.append({
                    "status": "failed",
                    "club_id": club_id,
                    "reason": f"{category.capitalize()} insert error: {e}"
                })
                print(f"❌ Error inserting {category} for club_id={club_id}: {e}")

    _insert_clubs(CLUBS, "canonical clubs")
    _insert_clubs(CLUBS_COUNTRY_TEAMS, "country teams")

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
                print(f"❌ Error inserting {label} for club_id={club_id}: {e}")
        ignored = attempted - inserted
        return attempted, inserted, ignored

    aliases_attempted_1, aliases_inserted_1, aliases_ignored_1 = _insert_aliases(CLUB_ALIASES, "Name-aliases")
    aliases_attempted_2, aliases_inserted_2, aliases_ignored_2 = _insert_aliases(
        CLUB_ALIASES_COUNTRY_TEAMS, "Name-aliases (country teams)"
    )

    # ─────────────────────────────────────────────────────────────────────────────
    # 3) External IDs (club_id_ext)
    # ─────────────────────────────────────────────────────────────────────────────
    attempted_ext = len(CLUB_ID_EXT)
    inserted_ext = 0
    for (club_id, club_id_ext) in CLUB_ID_EXT:
        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO club_id_ext (club_id, club_id_ext)
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
                # logging.info(f"External ID already existed for club_id={club_id}")
        except Exception as e:
            db_results.append({
                "status": "failed",
                "club_id": club_id,
                "reason": f"External ID insert error: {e}"
            })
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