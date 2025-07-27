# dedupe_clubs.py (run this in your src directory)

from db import get_conn
from collections import defaultdict
import difflib
import logging
from utils import setup_logging
setup_logging()

# Simple SOUNDEX implementation for phonetic matching
def soundex(name):
    if not name:
        return ""
    name = name.upper()
    soundex_code = name[0]
    for char in name[1:]:
        if char in 'BFPV':
            soundex_code += '1'
        elif char in 'CGJKQZSX':
            soundex_code += '2'
        elif char in 'DT':
            soundex_code += '3'
        elif char in 'L':
            soundex_code += '4'
        elif char in 'MN':
            soundex_code += '5'
        elif char in 'R':
            soundex_code += '6'
        if len(soundex_code) == 4:
            break
    return soundex_code.ljust(4, '0')

def get_base_name(long_name):
    if not long_name:
        return ""
    # Common prefixes to remove
    prefixes = ["bordtennis", "pingis", "idrottsför", "förening", "ifk", "kfum", "kultur &", "stratos"]
    # Common suffixes to remove
    suffixes = ["klubb", "förening", "idrottsförening", "bordtennisklubb", "pingisklubb"]

    name_lower = long_name.lower()

    # Remove prefixes
    for prefix in prefixes:
        if name_lower.startswith(prefix):
            long_name = long_name[len(prefix):].strip()
            name_lower = long_name.lower()

    # Remove suffixes
    for suffix in suffixes:
        if name_lower.endswith(suffix):
            long_name = long_name[:-len(suffix)].strip()
            name_lower = long_name.lower()

    return long_name

def dedupe_clubs():
    conn, cursor = get_conn()
    try:
        # Fetch all clubs
        cursor.execute("""
            SELECT club_id, club_id_ext, name, long_name, district_id
            FROM club
            ORDER BY long_name
        """)
        clubs = cursor.fetchall()

        if not clubs:
            logging.info("No clubs found in the table.")
            return

        # Group by district_id for more accurate matching (duplicates likely in same district)
        clubs_by_district = defaultdict(list)
        for club in clubs:
            clubs_by_district[club[4]].append(club)  # district_id

        potential_duplicates = []
        for district_id, district_clubs in clubs_by_district.items():
            for i in range(len(district_clubs)):
                for j in range(i + 1, len(district_clubs)):
                    club1 = district_clubs[i]
                    club2 = district_clubs[j]
                    base_name1 = get_base_name(club1[3] or "")
                    base_name2 = get_base_name(club2[3] or "")
                    similarity = difflib.SequenceMatcher(None, base_name1.lower(), base_name2.lower()).ratio() * 100
                    soundex1 = soundex(base_name1)
                    soundex2 = soundex(base_name2)
                    if similarity > 80:
                        potential_duplicates.append((club1, club2, similarity, soundex1 == soundex2))

        if not potential_duplicates:
            logging.info("No potential duplicates found.")
            return

        logging.info("Potential Duplicates:")
        for dup1, dup2, sim, soundex_match in potential_duplicates:
            logging.info(f" - Club {dup1[0]} (ext {dup1[1]}, long_name '{dup1[3]}') vs Club {dup2[0]} (ext {dup2[1]}, long_name '{dup2[3]}')")
            logging.info(f"   Similarity: {sim:.2f}%, Soundex Match: {soundex_match}")
            logging.info("")

        # Generate SQL for merging (assume you specify canonical club_id for each group)
        # Example: For Askims group, canonical = 66
        # Update manually in the groups below
        duplicate_groups = [
            # Example group
            {
                "canonical_id": 66,  # Replace with your chosen master
                "duplicates": [67, 68]  # club_ids to merge into canonical
            },
            # Add more groups from your review
        ]

        sql_statements = []
        for group in duplicate_groups:
            canonical_id = group["canonical_id"]
            for dup_id in group["duplicates"]:
                # Insert duplicate into club_variant
                cursor.execute("""
                    SELECT club_id_ext, name, long_name, 'original_scrape' AS source
                    FROM club WHERE club_id = ?
                """, (dup_id,))
                row = cursor.fetchone()
                if row:
                    sql_statements.append(f"""
                    INSERT INTO club_variant (club_id, club_id_ext, name, long_name, source)
                    VALUES ({canonical_id}, {row[0]}, '{row[1]}', '{row[2]}', '{row[3]}');
                    """)

                # Update references (e.g., in player_ranking; add other tables as needed)
                sql_statements.append(f"""
                UPDATE player_ranking
                SET club_id = {canonical_id}
                WHERE club_id = {dup_id};
                """)

                # Optional: Delete old duplicate from club table after migration
                # sql_statements.append(f"DELETE FROM club WHERE club_id = {dup_id};")

        if sql_statements:
            logging.info("Generated SQL for Merging (run these in your DB):")
            for sql in sql_statements:
                logging.info(sql)

    except Exception as e:
        logging.error(f"Error during dedupe: {e}")
        logging.info(f"❌ Error during dedupe: {e}")

    finally:
        conn.close()

if __name__ == "__main__":
    dedupe_clubs()