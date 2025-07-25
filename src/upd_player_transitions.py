# src/upd_player_transitions.py

from db import get_conn, get_from_db_season
import re
from datetime import datetime
import logging
from collections import defaultdict
from models.player_license import PlayerLicense
from utils import print_db_insert_results

def upd_player_transitions():
    conn, cursor = get_conn()
    load_results = []

    try:
        logging.info("Updating player transitions...")
        print("ℹ️  Updating player transitions...")

        cursor.execute('''
            SELECT 
                count(*)
            FROM player_transition_raw
        ''')

        rows = cursor.fetchone()
        count = rows[0] if rows else 0

        print(f"ℹ️  Found {count} player transitions in player_transition_raw")
        logging.info(f"Found {count} player transitions in player_transition_raw")

        cursor.execute('''
            SELECT row_id, season_id_ext, season_label, firstname, lastname, date_born, year_born, club_from, club_to, transition_date
            FROM player_transition_raw
        ''')

        rows = cursor.fetchall()

        if not rows:
            print("⚠️ No player transition data found in player_transition_raw.")
            logging.warning("No player transition data found in player_transition_raw.")
            return []

        for row in rows:
            (row_id, season_id_ext, season_label, firstname, lastname, date_born, year_born, club_from, club_to, transition_date_str) = row

            # Capitalize firstname and lastname
            firstname = firstname.strip().capitalize()
            lastname = lastname.strip().capitalize()

            # Detect duplicates in raw data
            cursor.execute("""
                SELECT row_id, SUBSTR(license_info_raw, INSTR(license_info_raw, '(') + 1, 10) AS valid_from_raw
                FROM player_license_raw
                WHERE player_id_ext = ? AND club_id_ext = ? AND season_id_ext = ?
                AND LOWER(TRIM(SUBSTR(license_info_raw, 1, INSTR(license_info_raw, '(') - 1))) = LOWER(?)
                ORDER BY valid_from_raw
            """, (
                player_id_ext,
                club_id_ext,
                season_id_ext,
                f"{license_type} {license_age_group}".strip() if license_age_group else license_type.strip()
            ))

            duplicate_rows = cursor.fetchall()
            if len(duplicate_rows) > 1:
                # Get the current row's index in duplicate_rows
                current_index = next((i for i, r in enumerate(duplicate_rows) if r[0] == row_id), -1)
                if current_index > 0:  # Not the earliest
                    logging.warning(f"Duplicate license detected for player_id_ext {player_id_ext}: {license_type} "
                                f"(age group: {license_age_group}) for club_id_ext {club_id_ext}, season_id_ext {season_id_ext}")
                    load_results.append({
                        "status": "failed",
                        "row_id": row_id,
                        "reason": "Player has duplicate license type and age group for the same club and season"
                    })
                    continue
            
            try:
                valid_from = datetime.strptime(license_date, "%Y.%m.%d").date()

            except Exception as e:
                msg = f"Date parsing error for {license_part}: {e}"
                logging.warning(msg)
                load_results.append({
                    "status": "failed",
                    "row_id": row_id,
                    "reason": msg
                })

            # Fetch season data
            season_data = get_from_db_season(cursor, season_label=season_label)
            if not season_data:
                msg = f"No matching season found for season_label '{season_label}'"
                logging.warning(msg)
                load_results.append({
                    "status": "failed",
                    "row_id": row_id,
                    "reason": msg
                })
                continue
            season_id = season_data["season_id"]
            valid_to = season_data["season_end_date"]

            # Map player_id_ext to player_id
            cursor.execute("SELECT player_id FROM player WHERE player_id_ext = ?", (player_id_ext,))
            player_result = cursor.fetchone()
            if not player_result:
                msg = f"Foreign key violation: player_id_ext {player_id_ext} does not exist in player table"
                logging.warning(msg)
                load_results.append({
                    "status": "failed",
                    "row_id": row_id,
                    "reason": msg
                })
                continue
            player_id = player_result[0]

            # Map club_id_ext to club_id
            cursor.execute("SELECT club_id FROM club WHERE club_id_ext = ?", (club_id_ext,))
            club_result = cursor.fetchone()
            if not club_result:
                msg = f"Foreign key violation: club_id_ext {club_id_ext} does not exist in club table"
                logging.warning(msg)
                load_results.append({
                    "status": "failed",
                    "row_id": row_id,
                    "reason": msg
                })
                continue
            club_id = club_result[0]

            # Map season_id_ext to season_id
            cursor.execute("SELECT season_id FROM season WHERE season_id_ext = ?", (season_id_ext,))
            season_result = cursor.fetchone()
            if not season_result:
                msg = f"Foreign key violation: season_id_ext {season_id_ext} does not exist in season table"
                logging.warning(msg)
                load_results.append({
                    "status": "failed",
                    "row_id": row_id,
                    "reason": msg
                })
                continue
            season_id = season_result[0]

            # Map license_type and license_age_group to license_id
            if license_age_group is None:
                cursor.execute("SELECT license_id FROM license WHERE license_type = ? AND (license_age_group IS NULL OR license_age_group = '')", 
                            (license_type,))
            else:
                cursor.execute("SELECT license_id FROM license WHERE license_type = ? AND license_age_group = ?", 
                            (license_type, license_age_group))
            license_result = cursor.fetchone()
            if not license_result:
                msg = f"Foreign key violation: license_type {license_type} and age group {license_age_group} do not exist in license table"
                logging.warning(msg)
                load_results.append({
                    "status": "failed",
                    "row_id": row_id,
                    "reason": msg
                })
                continue
            license_id = license_result[0]

            # Create PlayerLicense object
            player_license = PlayerLicense(
                player_id=player_id,
                club_id=club_id,
                season_id=season_id,
                license_id=license_id,
                valid_from=valid_from,
                valid_to=valid_to
            )

            # Save to database
            result = player_license.save_to_db(cursor)
            load_results.append(result)
            
        print_db_insert_results(load_results)

    except Exception as e:
        logging.error(f"Failed to process licenses: {e}")
        print(f"❌ Failed to process licenses: {e}")
        return load_results

    finally:
        conn.commit()
        conn.close()
        logging.info("-------------------------------------------------------------------")