# src/upd_clubs.py

# src/upd_clubs.py

import logging
from db import get_conn
from clubs_data import CLUBS, CLUB_ALIASES, CLUB_EXT_IDS, CLUBS_COUNTRY_TEAMS, CLUB_ALIASES_COUNTRY_TEAMS

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
        # Combine clubs and country teams, tagging their source
        all_clubs = [(club, "canonical clubs") for club in CLUBS] + \
                    [(club, "country teams") for club in CLUBS_COUNTRY_TEAMS]

        # Track insertions by category
        results = {"canonical clubs": {"attempted": 0, "inserted": 0},
                "country teams": {"attempted": 0, "inserted": 0}}

        for (club_id, shortname, longname, club_type, city, country_code, remarks,
            homepage, active, district_id), category in all_clubs:
            cursor.execute("""
                INSERT OR IGNORE INTO club (
                    club_id, shortname, longname, club_type,
                    city, country_code, remarks,
                    homepage, active, district_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (club_id, shortname, longname, club_type, city, country_code,
                remarks, homepage, active, district_id))
            
            results[category]["attempted"] += 1
            if cursor.rowcount == 1:
                results[category]["inserted"] += 1

        # Log results for each category
        for category, stats in results.items():
            ignored = stats["attempted"] - stats["inserted"]
            print(f"ℹ️  {category.capitalize()}: attempted={stats['attempted']}, "
                f"inserted={stats['inserted']}, ignored={ignored}")
            logging.info(f"{category.capitalize()}: attempted={stats['attempted']}, "
                        f"inserted={stats['inserted']}, ignored={ignored}")

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

    inserted_aliases = 0
    ignored_aliases = []  # collect the ones INSERT OR IGNORE skipped
    for club_id, alias_text, alias_type in CLUB_ALIASES_COUNTRY_TEAMS:
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

    attempted = len(CLUB_ALIASES_COUNTRY_TEAMS)
    ignored = attempted - inserted_aliases
    print(f"ℹ️  Name-aliases country teams:    attempted={attempted}, inserted={inserted_aliases}, ignored={ignored}")
    logging.info(f"Name-aliases country teams:    attempted={attempted}, inserted={inserted_aliases}, ignored={ignored}")

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