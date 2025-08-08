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
        inserted_clubs = 0
        for (
            club_id, shortname, longname, club_type,
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
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
            ))
            if cursor.rowcount == 1:
                inserted_clubs += 1    
        print(f"ℹ️  Canonical clubs: attempted={len(CLUBS)}, inserted={inserted_clubs}, ignored={len(CLUBS)-inserted_clubs}")
        logging.info(f"Canonical clubs: attempted={len(CLUBS)}, inserted={inserted_clubs}, ignored={len(CLUBS)-inserted_clubs}")

    except Exception as e:
        logging.error(f"Error inserting clubs: {e}")
        conn.close()
        return

    # 2) name-aliases
    # If you don’t want the canonical names also repeated here,
    # just fill CLUB_ALIASES with the extra ones you care about.
    inserted_aliases = 0
    ignored_aliases = []  # collect the ones INSERT OR IGNORE skipped
    for club_id, alias_text, alias_type in CLUB_ALIASES:
        cursor.execute("""
            INSERT OR IGNORE INTO club_name_alias (
                club_id,
                alias,
                alias_type
            ) VALUES (?, ?, ?)
        """, (club_id, alias_text, alias_type))
        if cursor.rowcount == 1:
            inserted_aliases += 1       
        else:
            # rowcount == 0 means it was ignored
            ignored_aliases.append((club_id, alias_text, alias_type)) 

    attempted = len(CLUB_ALIASES)
    ignored = attempted - inserted_aliases
    print(f"ℹ️  Name-aliases:    attempted={attempted}, inserted={inserted_aliases}, ignored={ignored}")

    logging.info(f"Name-aliases: attempted={attempted}, inserted={inserted_aliases}, ignored={ignored}")

    # print to debug
    # if ignored_aliases:
    #     print("\n📋 Ignored name-aliases:")
    #     for cid, alias, atype in ignored_aliases:
    #         print(f"  club_id={cid!r}, alias={alias!r}, alias_type={atype!r}")

    # 3) external IDs
    inserted_ext = 0
    for club_id, club_id_ext in CLUB_EXT_IDS:
        cursor.execute("""
            INSERT OR IGNORE INTO club_ext_id (
                club_id,
                club_id_ext
            ) VALUES (?, ?)
        """, (club_id, club_id_ext))
        if cursor.rowcount == 1:
            inserted_ext += 1
    print(f"ℹ️  External IDs:    attempted={len(CLUB_EXT_IDS)}, inserted={inserted_ext}, ignored={len(CLUB_EXT_IDS)-inserted_ext}")
    logging.info(f"External IDs:    attempted={len(CLUB_EXT_IDS)}, inserted={inserted_ext}, ignored={len(CLUB_EXT_IDS)-inserted_ext}")


    conn.commit()
    conn.close()
    logging.info("Club update complete.")