# src/upd_clubs.py

# src/upd_clubs.py

import logging
from db import get_conn
from clubs_data import CLUBS, CLUB_ALIASES, CLUB_EXT_IDS

def upd_clubs():
    """
    1) Insert canonical clubs into `club`.
    2) Insert any extra name-aliases into `club_name_alias`.
    3) Insert external-ID mappings into `club_ext_id`.
    """
    conn, cursor = get_conn()
    logging.info("Updating clubs...")
    print("ℹ️  Updating clubs...")

    try: 

        # 1) canonical clubs
        # Note this is for clubs only, not national teams, club_type is defaulted to 'club'.
        for (
            club_id, shortname, longname,
            city, country_code, remarks,
            homepage, active, district_id
        ) in CLUBS:
            cursor.execute("""
                INSERT OR IGNORE INTO club (
                    club_id,
                    shortname,
                    longname,
                    club_type,
                    city,
                    country_code,
                    remarks,
                    homepage,
                    active,
                    district_id
                ) VALUES (?, ?, ?, 'club', ?, ?, ?, ?, ?, ?)
            """, (
                club_id,
                shortname,
                longname,
                city,
                country_code,
                remarks,
                homepage,
                active,
                district_id
            ))
        logging.info(f"Inserted/ignored {len(CLUBS)} canonical clubs")

    except Exception as e:
        logging.error(f"Error inserting clubs: {e}")
        conn.close()
        return

    # 2) name-aliases
    # If you don’t want the canonical names also repeated here,
    # just fill CLUB_ALIASES with the extra ones you care about.
    for club_id, alias_text, alias_type in CLUB_ALIASES:
        cursor.execute("""
            INSERT OR IGNORE INTO club_name_alias (
                club_id,
                alias,
                alias_type
            ) VALUES (?, ?, ?)
        """, (club_id, alias_text, alias_type))
    logging.info(f"Inserted/ignored {len(CLUB_ALIASES)} name-aliases")
    print(f"ℹ️  Inserted/ignored {len(CLUB_ALIASES)} name-aliases")

    # 3) external IDs
    for club_id, club_id_ext in CLUB_EXT_IDS:
        cursor.execute("""
            INSERT OR IGNORE INTO club_ext_id (
                club_id,
                club_id_ext
            ) VALUES (?, ?)
        """, (club_id, club_id_ext))
    logging.info(f"Inserted/ignored {len(CLUB_EXT_IDS)} external IDs")
    print(f"ℹ️  Inserted/ignored {len(CLUB_EXT_IDS)} external IDs")

    conn.commit()
    conn.close()
    logging.info("Club update complete.")