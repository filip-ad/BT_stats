# src/clubs_data_add.py
# 
# Additional club data to be merged into clubs_data.py
# Generated: 2025-11-29
# 
# This file contains:
# 1. CLUB_ALIASES_ADD - Aliases for EXISTING clubs that are still failing to match
#    (after the normalize_key improvements for ö↔ø and hyphen/slash handling)
# 2. CLUBS_ADD - NEW clubs (mostly foreign) that don't exist in the database
#
# To use: Copy relevant entries into clubs_data.py, then run upd_clubs.py
#
# PROGRESS:
# - normalize_key fix resolved ~2000 log entries (Virum Sorgenfri, Hilleröd, Brönshöj, etc.)
# - 876 unique clubs still missing
# - This file adds aliases for clubs that exist but have name variations not caught by normalization

# =============================================================================
# PART 1: ALIASES FOR EXISTING CLUBS (still needed after normalize_key fix)
# =============================================================================
# These clubs exist in the database but need additional aliases because the
# name variation is not a simple hyphen/slash or ö↔ø issue.
# Format: (club_id, 'alias_name', 'short'|'long')

CLUB_ALIASES_ADD = [
    # -------------------------------------------------------------------------
    # NOTE: The highest-frequency Swedish clubs (Spårvägens, Ängby, Norrtulls, etc.)
    # are NOT club resolution failures - they ARE matching correctly!
    # The log warnings are about PLAYER LICENSE DATE VALIDATION, not club matching.
    # 
    # Only clubs with "Club matched by prefix similarity" warnings need aliases.
    # -------------------------------------------------------------------------
    
    # -------------------------------------------------------------------------
    # HIGH PRIORITY - 100+ occurrences in logs (actual prefix match warnings)
    # -------------------------------------------------------------------------
    
    # IFAH Parasport (id=596) - 295 occurrences - missing short "IFAH" alias
    (596, 'IFAH', 'short'),
    
    # Virum-Sorgenfri BTK (id=1039) - 195 occurrences for "Virum BTK" alone
    (1039, 'Virum BTK', 'short'),
    (1039, 'Virum Club', 'short'),
    
    # Kvik Næstved BTK (id=1355) - 194 occurrences for "Næstved Bordtennis"
    (1355, 'Næstved Bordtennis', 'short'),
    (1355, 'Naestved', 'short'),
    (1355, 'Nästved BTK', 'short'),
    
    # Søhøjlandets BTK - 140 occurrences - NEED TO CHECK IF EXISTS
    # TODO: Search for this club or create new entry
    
    # HØST IF - 135 occurrences - NEED TO CHECK IF EXISTS  
    # TODO: Search for this club or create new entry
    
    # Odder BTK (id=1219) - 118 occurrences for "Odder Igf"
    (1219, 'Odder Igf', 'short'),
    (1219, 'Odder IGF', 'short'),
    
    # Mälarenergi BTK (id=1410) - 117 occurrences for "Team Mälarenergi BTK"
    (1410, 'Team Mälarenergi BTK', 'short'),
    
    # Holbæk BTK (id=1181) - 111 occurrences for "Holbæk Bordtennis Klub"
    (1181, 'Holbæk Bordtennis Klub', 'short'),
    
    # FIFH BTK (id=1405) - 103 occurrences for "FIFH"
    (1405, 'FIFH', 'short'),
    
    # -------------------------------------------------------------------------
    # NORWEGIAN CLUB ALIASES - High frequency prefix matches
    # These clubs exist but need aliases for exact matching
    # -------------------------------------------------------------------------
    
    # Fjell-Kameraterne (id=1096) - 809 occurrences
    # Raw format includes ",IL" suffix
    (1096, 'Fjell-Kameraterne,IL', 'short'),
    (1096, 'Fjell-Kameraterne, IL', 'short'),
    (1096, 'Fjell Kameraterne', 'short'),
    (1096, 'Fjell Kameratene', 'short'),
    
    # Oppegård IL (id=1017) - 646 occurrences
    # Raw uses just "Oppegård" without "IL"
    (1017, 'Oppegård', 'short'),
    (1017, 'Oppegard', 'short'),
    (1017, 'Oppegård IL', 'short'),
    
    # Randesund IL (id=1002) - 421 occurrences
    # Raw uses "Randesund IL - BTG"
    (1002, 'Randesund IL - BTG', 'short'),
    (1002, 'Randesund IL-BTG', 'short'),
    (1002, 'Randesund IL BTG', 'short'),
    (1002, 'Randesund', 'short'),
    
    # Larkollen IL (id=1015) - 213 occurrences
    # Raw uses just "Larkollen" without "IL"
    (1015, 'Larkollen', 'short'),
    
    # Sandefjord TIF (id=1003) - 124 occurrences
    # Raw uses "Sandefjord TIF-BTG"
    (1003, 'Sandefjord TIF-BTG', 'short'),
    (1003, 'Sandefjord TIF BTG', 'short'),
    (1003, 'Sandefjord', 'short'),
    
    # Siggerud IL (id=???) - 72 occurrences - CHECK IF EXISTS
    # B-72 Lørenskog (id=1011) - 42 occurrences - Already has aliases
    
    # Tingvoll BTK (id=1014) - 63 occurrences
    (1014, 'Tingvoll', 'short'),
    (1014, 'Tingvoll BTK NOR', 'short'),
    
    # Husøy og Føynland IF (id=1008) - 40 occurrences
    # Multiple spelling variations
    (1008, 'Husøy og Føynland', 'short'),
    (1008, 'Husøy og Foynland', 'short'),
    
    # Bodø BTK (id=1012) - 32 occurrences
    (1012, 'Bodø BT', 'short'),
    (1012, 'Bodo BTK', 'short'),
    
    # -------------------------------------------------------------------------
    # MEDIUM PRIORITY - 50-100 occurrences
    # -------------------------------------------------------------------------
    
    # B 77 Rødovre (id=1397) - 90 occurrences for "B 77"
    (1397, 'B 77', 'short'),
    
    # Viborg BTK (id=1299) - 89 occurrences for "Viborg Bordtennis Klub"
    (1299, 'Viborg Bordtennis Klub', 'short'),
    (1299, 'Viborgs BTK', 'short'),
    
    # Ås IF (id=411) - 77 occurrences for "Ås IF Bordtennis"
    (411, 'Ås IF Bordtennis', 'short'),
    (411, 'Ås Pingis', 'short'),
    
    # -------------------------------------------------------------------------
    # SWEDISH CLUBS - 100-200 occurrences - need aliases for exact matching
    # -------------------------------------------------------------------------
    
    # Sunnersta AIF (id=330) - 151 occurrences
    # Has "Sunnersta BTK/PS" but raw uses "Sunnersta AIF"
    (330, 'Sunnersta AIF', 'short'),
    
    # KFUM Jönköping IA (id=713) - 145 occurrences
    # Ensure exact match
    (713, 'KFUM Jönköping IA', 'short'),
    (713, 'KFUM Jonkoping IA', 'short'),
    
    # Kungsbacka BTK (id=140) - 144 occurrences
    # Has "Kungsbacka BTS" but raw uses "Kungsbacka BTK"
    (140, 'Kungsbacka BTK', 'short'),
    
    # Södertälje BTK (id=774) - 139 occurrences
    # Has "Södertälje PS" but raw uses "Södertälje BTK"
    (774, 'Södertälje BTK', 'short'),
    (774, 'Sodertälje BTK', 'short'),
    
    # IFK Lund (id=457) - 138 occurrences + 89 for "IFK Lund Bordtennis"
    (457, 'IFK Lund', 'short'),
    (457, 'IFK Lund Bordtennis', 'short'),
    
    # Ljungsbro BTK (id=935) - 132 occurrences
    (935, 'Ljungsbro BTK', 'short'),
    
    # IFA Eskilstuna truncations (id=761) - 132+113+99 occurrences
    # Various PDF truncations of "Idrottsföreningen För Alla Eskilstuna"
    (761, 'Idrottsföreningen För Alla Eskilst', 'short'),
    (761, 'Idrottsföreningen För Alla Eskilstun', 'short'),
    (761, 'Idrottsföreningen För Alla Eskil', 'short'),
    (761, 'Idrottsföreningen För Alla Eskilstu', 'short'),
    (761, 'Idrottsföreningen För Alla', 'short'),
    (761, 'IFA Eskilstuna', 'short'),
    
    # Boo KFUM IA (id=571) - 127 occurrences
    (571, 'Boo KFUM IA', 'short'),
    
    # Tabergs SK (id=742) - 126 occurrences
    # Has "Tabergs PK" but raw uses "Tabergs SK"
    (742, 'Tabergs SK', 'short'),
    
    # Oskarshamns BTK (id=733) - 124 occurrences
    # Has "Oskarshamns PK" but raw uses "Oskarshamns BTK"
    (733, 'Oskarshamns BTK', 'short'),
    
    # Stratos Enköping (id=329) - 123 occurrences
    (329, 'Stratos Enköping', 'short'),
    (329, 'Stratos Enkoping', 'short'),
    
    # Gefle PK (id=298) - 123 occurrences
    (298, 'Gefle PK', 'short'),
    
    # Västers BTK (id=949) - 118 occurrences
    (949, 'Västers BTK', 'short'),
    (949, 'Vasters BTK', 'short'),
    
    # Tyresö BTK (id=664) - 116 occurrences
    (664, 'Tyresö BTK', 'short'),
    (664, 'Tyreso BTK', 'short'),
    
    # Vårgårda IK (id=278) - 102 occurrences
    (278, 'Vårgårda IK', 'short'),
    (278, 'Vargarda IK', 'short'),
    
    # Upplands Väsby BTK (id=667) - 98 occurrences
    (667, 'Upplands Väsby BTK', 'short'),
    (667, 'Upplands Vasby BTK', 'short'),
    
    # BTK Enig (id=690) - 94 occurrences
    (690, 'BTK Enig', 'short'),
    
    # Åstorps BTK (id=517) - 93 occurrences
    (517, 'Åstorps BTK', 'short'),
    (517, 'Astorps BTK', 'short'),
    
    # Linköpings PK (id=934) - 92 occurrences
    (934, 'Linköpings PK', 'short'),
    (934, 'Linkopings PK', 'short'),
    
    # Rotebro BTK (id=628) - 91 occurrences
    # Has "Rotebro PK" but raw uses "Rotebro BTK"
    (628, 'Rotebro BTK', 'short'),
    
    # Vassunda IF (id=338) - 90 occurrences
    (338, 'Vassunda IF', 'short'),
    
    # IFK Österåkers BTK (id=601) - 90 occurrences
    (601, 'IFK Österåkers BTK', 'short'),
    (601, 'IFK Osterakers BTK', 'short'),
    
    # Järfälla BTF (id=608) - 85 occurrences
    (608, 'Järfälla BTF', 'short'),
    (608, 'Jarfalla BTF', 'short'),
    
    # Tomelilla AIS (id=509) - 82 occurrences
    (509, 'Tomelilla AIS', 'short'),
    
    # Malmö IF (id=482) - 108 occurrences
    (482, 'Malmö IF', 'short'),
    (482, 'Malmo IF', 'short'),
    
    # Dansk BTU (id=1115) - 74+ occurrences for ØBTU variations
    (1115, 'ØBTU', 'short'),
    (1115, 'ØBTU/Dk', 'short'),
    (1115, 'ØBTU/Denmark', 'short'),
    (1115, 'ØSTDANMARKS BORD', 'short'),
    (1115, 'ØSTDANMARKS BORDTEN', 'short'),
    (1115, 'ØSTDANMARKS BORDTENNIS', 'short'),
    
    # -------------------------------------------------------------------------
    # DANISH CLUBS - Prefix match warnings
    # -------------------------------------------------------------------------
    
    # Brønderslev (id=1207) - 268 occurrences
    # Has "Brønderslev BTK" but raw uses just "Brønderslev"
    (1207, 'Brønderslev', 'short'),
    (1207, 'Bronderslev', 'short'),
    
    # Team Egedal / Ølstykke BTK (id=1061) - 107 occurrences
    (1061, 'Team Egedal / Ølstykke BTK', 'short'),
    (1061, 'Team Egedal/Ølstykke BTK', 'short'),
    (1061, 'Team Egedal', 'short'),
    
    # OB Bordtennis (id=1033) - 65 occurrences
    (1033, 'OB Bordtennis', 'short'),
    
    # Bornholm (id=1389) - 51 occurrences
    (1389, 'Bornholm', 'short'),
    
    # Korup Bordtennis (id=1287) - 45 occurrences
    (1287, 'Korup Bordtennis', 'short'),
    (1287, 'Korup', 'short'),
    
    # Amager BTK DEN (id=1036) - 45 occurrences
    (1036, 'Amager BTK DEN', 'short'),
    
    # Faaborg Bordtennisklub (id=1374) - 42 occurrences
    (1374, 'Faaborg Bordtennisklub', 'short'),
    
    # Vestegnen (id=1245) - 29 occurrences
    (1245, 'Vestegnen', 'short'),
    
    # Greve Bordtennis (id=1357) - 27 occurrences
    (1357, 'Greve Bordtennis', 'short'),
    
    # -------------------------------------------------------------------------
    # NORWEGIAN CLUBS - More prefix match aliases
    # -------------------------------------------------------------------------
    
    # Siggerud IL BTK (id=1274) - 72 occurrences
    # Has "Siggerud IL BTG" but raw uses "Siggerud IL BTK"
    (1274, 'Siggerud IL BTK', 'short'),
    (1274, 'Siggerud IL', 'short'),
    (1274, 'Siggerud', 'short'),
    
    # Myra Ungdoms- og IL (id=???) - 30 occurrences
    # Need to check if exists
    
    # Kobra BTK NOR (id=???) - 25 occurrences
    # Need to check if exists
    
    # -------------------------------------------------------------------------
    # SWEDISH CLUBS - More prefix match aliases
    # -------------------------------------------------------------------------
    
    # Jägarnäs-Ludvika (id=32) - 61 occurrences
    (32, 'Jägarnäs-Ludvika', 'short'),
    (32, 'Jägarnäs Ludvika', 'short'),
    (32, 'Jagernas-Ludvika', 'short'),
    
    # BTK Triton (id=1434) - 57 occurrences
    # Has "Triton" but raw uses "Bordtennisklubben Triton"
    (1434, 'Bordtennisklubben Triton', 'short'),
    (1434, 'BTK Triton', 'short'),
    
    # Stratos Enköping variants (id=329) - 52+28 occurrences
    (329, 'Stratos Enköping B', 'short'),
    (329, 'Stratos Enköping BT', 'short'),
    (329, 'Stratos Enkoping B', 'short'),
    
    # Gefle Pingis (id=298) - 45 occurrences
    (298, 'Gefle Pingis', 'short'),
    
    # Ås Pingis (id=411) - 36 occurrences - moved to proper section
    # Already added above
    
    # Upplands Väsby (id=667) - 34 occurrences (without BTK)
    (667, 'Upplands Väsby', 'short'),
    
    # KFUM Bordtennis Katrineholm (id=764) - 34 occurrences
    # Truncated version - has KFUM Katrineholm
    (764, 'KFUM Bordtennis Katrinehol', 'short'),  # Truncated
    (764, 'KFUM Bordtennis Katrineholm', 'short'),
    
    # Nyköpings Oxelösund (id=768) - 29 occurrences
    (768, 'Nyköpings Oxelösund', 'short'),
    (768, 'Nykopings Oxelosund', 'short'),
    
    # London Academy/Eng (id=1110) - 159 occurrences
    (1110, 'London Academy/Eng', 'short'),
    (1110, 'London/Eng', 'short'),
    
    # Hvidovre BT (id=1034) - 38 occurrences
    (1034, 'Hvidovre BT', 'short'),
    
    # NTG (id=1076) - 66 occurrences for "NTG NOR"
    (1076, 'NTG NOR', 'short'),
    (1076, 'NTG Norge', 'short'),
    
    # -------------------------------------------------------------------------
    # LOWER PRIORITY - 20-50 occurrences
    # -------------------------------------------------------------------------
    
    # Roskilde BTK 61 (id=1047) - various truncations
    (1047, 'Roskilde Bordtennis', 'short'),
    (1047, 'Roskilde BTK 61', 'short'),
    (1047, 'Roskilde', 'short'),
    (1047, 'Roskilde/Dk', 'short'),
    
    # Hvidovre BT (id=1034) - truncated names from PDF parsing
    (1034, 'Hvidovre BT', 'short'),
    (1034, 'Hvidovre Bordtenn', 'short'),
    (1034, 'Hvidovre Bordtenni', 'short'),
    
    # Spånga Tennis och BTK (id=643)
    (643, 'Spånga Tennis O BTK', 'short'),
    (643, 'Spånga TBK', 'short'),
    
    # IFAE (id=761)
    (761, 'IFAE', 'short'),
    (761, 'IF FA Eskilstuna', 'short'),
    
    # Stratos Enköping (id=329)
    (329, 'Stratos Enk BTK', 'short'),
    (329, 'Stratos Enköping BT', 'short'),
    
    # -------------------------------------------------------------------------
    # ADDITIONAL ALIASES (from Part 2 analysis)
    # -------------------------------------------------------------------------
    
    # Askims BTK (id=66) - 93 occurrences for "Askim"
    (66, 'Askim', 'short'),
    (66, 'Askim IF', 'short'),
    
    # Ängby SK (id=679) - various spellings
    (679, 'Ängby', 'short'),
    (679, 'Ångby Sportsklubb', 'long'),
    
    # Svanesunds GIF (id=265) - 49 occurrences
    (265, 'Svanesund GIF', 'short'),
    
    # Kvik Næstved BTK (id=1355) - additional variation
    (1355, 'Bordtennisklubben Kvik Næstved', 'long'),
    
    # Ejby BTK (id=1183) - 30 occurrences
    (1183, 'BTK Ejby', 'short'),
    
    # Billund IF (id=1214) - 55 occurrences for "11-Billund"
    (1214, '11-Billund', 'short'),
    (1214, 'Billund Idrætsforening', 'short'),
    
    # Randers Freja (id=1144) - 39 occurrences
    (1144, 'Randers Bordtennis', 'short'),
    
    # Allerød BTK (id=1055) - already covered by normalize but explicit
    (1055, 'Alleröd Bordtennisklubb', 'long'),
    
    # Fusion TTC (id=5221 - new) - variations
    # Note: Will be added via CLUB_ALIASES_NEW
    
    # Table Tennis Ireland (id=5241 - new) - variations  
    # Note: Will be added via CLUB_ALIASES_NEW
    
    # Hillerød (id=1038) - combined variation
    (1038, 'Hillerød/Hvidovre/Dk', 'short'),
    
    # B 75 Hirtshals (id=1007)
    (1007, '01-B 75', 'short'),
    
    # Brighton TTC - handled by new club + alias
    
    # London TT Academy - handled by new club + alias
    
    # Spårvägens BTK (id=644) - variation
    (644, 'Spårväg', 'short'),
    
    # Brønshøj BTK (id=1040) - already covered by normalize
    (1040, 'Brönshöj BT', 'short'),
    
    # -------------------------------------------------------------------------
    # PART 3 ADDITIONS - Danish numbered format clubs
    # -------------------------------------------------------------------------
    # Many Danish clubs appear with a district number prefix like "12-Åbenrå"
    
    # Åbenrå BTK (id=5275) - Danish numbered format
    (5275, '12-Åbenrå', 'short'),
    
    # Daugård IF - need to find or create
    # Searching shows likely not in DB, need new club
    
    # HØST IF (id=5212) - Danish numbered format
    (5212, '09-HØST', 'short'),
    
    # Esbjerg BTK (id=1147) - Danish numbered format
    (1147, '11-Esbjerg BTK', 'short'),
    (1147, 'Esbjerg Tennis Club', 'short'),
    
    # Alsted-Fjenneslev - need to find or create
    
    # Odder IGF (id=1219) - Danish numbered format
    (1219, '09-Odder', 'short'),
    
    # Søhøjlandets BTK (id=5211) - Danish numbered format
    (5211, '08-Søhøjlandets BTK', 'short'),
    
    # Holbæk BTK (id=1181) - Danish numbered format
    (1181, '18-Holbæk', 'short'),
    
    # Ejby BTK (id=1183) - Danish numbered format
    (1183, '19-Ejby', 'short'),
    
    # Fårevejle - need to find or create
    
    # BTK Grenå (id=1149) - Danish numbered format
    (1149, '09-BTK Grenå', 'short'),
    (1149, 'BTK Grena', 'short'),
    
    # Vejen BTK (id=1212) - Danish numbered format
    (1212, '11-Vejen BTK', 'short'),
    
    # Hammarby IF BTK (id=81) - variation without "K"
    (81, 'Hammarby IF BT', 'short'),
    
    # Ribe BTK (id=1017) - Danish numbered format
    (1017, '11-Ribe', 'short'),
    
    # -------------------------------------------------------------------------
    # PART 3 ADDITIONS - More missing clubs/aliases
    # -------------------------------------------------------------------------
    
    # Årby (id=5355 - new) - see CLUBS_ADD
    
    # Lidzbark Warminski - Polish club, see CLUBS_ADD
    
    # Grønland - see CLUBS_ADD
    
    # Haderslev BTK (id=1197)
    (1197, 'Haderslev BTK', 'short'),
    
    # Flensborg UF - German club, see CLUBS_ADD
    
    # Bergen Handicapidrettslag - typo for Bergen Paraidrettslag (id=5330)
    (5330, 'Bergen Hanicapidrettslag', 'short'),
    (5330, 'Bergen Handicapidrettslag', 'short'),
    
    # Maarduu - Estonian club, see CLUBS_ADD
    
    # SV Fockbek - German club, see CLUBS_ADD
    
    # Ålesund BTK - Norwegian club, see CLUBS_ADD
    
    # Parkinson Göteborg - Swedish para club, see CLUBS_ADD
    
    # Lia BTK - Norwegian club, see CLUBS_ADD
    
    # Reerslev IF - Danish club, added to CLUBS_ADD as 5400
    # (1224, 'Reerslev IF', 'short'),  # REMOVED - wrong ID, added to CLUBS_ADD
    
    # -------------------------------------------------------------------------
    # PART 3 ADDITIONS - More typo aliases
    # -------------------------------------------------------------------------
    
    # Silkeborg BTK (id=1035) - common typo "Silkesborg"
    (1035, 'Silkesborg BTK', 'short'),
    
    # Bordtennisklubben - generic name, probably needs investigation
    # Could be a truncation of any "Bordtennisklubben X"
    
    # -------------------------------------------------------------------------
    # PART 4 ADDITIONS - Aliases for clubs converted from CLUBS_ADD
    # These were incorrectly added as new clubs but existing clubs cover them
    # -------------------------------------------------------------------------
    
    # Nesodden BTK → Nesodden IF (id=1108) - same club, different name
    (1108, 'Nesodden BTK', 'short'),
    (1108, 'Nesodden Bordtennisklubb', 'long'),
    
    # London TT Academy → London Academy (id=1110) - same club
    (1110, 'London TT Academy', 'short'),
    (1110, 'London Table Tennis Academy', 'long'),
    
    # Table Tennis Ireland → Ireland (id=5079) - federation alias
    (5079, 'Table Tennis Ireland', 'short'),
    
    # Norges BTF → Norway (id=5127) - federation alias  
    (5127, 'Norges BTF', 'short'),
    (5127, 'Norges Bordtennisforbund', 'long'),
    
    # Team Nørreå → Team Nørre (id=1127) - same club
    (1127, 'Team Nørreå', 'short'),
    
    # St Petersburg → St Petersburg TT (id=1248) - same club
    (1248, 'St Petersburg', 'short'),
    
    # Finland TTA → Finland (id=5059) - federation alias
    (5059, 'Finland TTA', 'short'),
    (5059, 'Finland Table Tennis Association', 'long'),
    
    # Aspire Qatar → Aspire/Qatar (id=1247) - same club
    (1247, 'Aspire Qatar', 'short'),
    (1247, 'Aspire Academy Qatar', 'long'),
    
    # Val d'Oise (id=1417) - apostrophe variation
    (1417, 'Val dOise', 'short'),
    
    # Pechatniki (id=1339) - short form
    (1339, 'Pechatniki', 'short'),
    
    # Moscow TTC (id=1338) - already matches
    (1338, 'Moscow TTC', 'short'),
    
    # Daugård IF (id=1206) - already matches
    (1206, 'Daugård IF', 'short'),
    (1206, 'Daugård Idrætsforening', 'long'),
    
    # Myra IL → Myra (id=1100) - same club
    (1100, 'Myra IL', 'short'),
    (1100, 'Myra Ungdoms- og Idrettslag', 'long'),
    
    # Kobra BTK (id=1029) - already matches
    (1029, 'Kobra BTK', 'short'),
    (1029, 'Kobra Bordtennisklubb', 'long'),
    
    # Mejlans BF (id=1119) - already matches
    (1119, 'Mejlans BF', 'short'),
    (1119, 'Mejlans Bollförening', 'long'),
    
    # Skjoldar (id=1094) - already matches but add alias
    (1094, 'Skjoldar', 'short'),
    
    # Lübecker TS (id=1053) - already matches
    (1053, 'Lübecker TS', 'short'),
    (1053, 'Lübecker Turnerschaft', 'long'),
]

# =============================================================================
# PART 2: NEW FOREIGN CLUBS (need to be created)
# =============================================================================
# These clubs don't exist in the database and need new entries.
# Format: (club_id, shortname, longname, club_type_id, city, country_code, 
#          remarks, homepage, active, district_id)
# 
# Starting from club_id 5200 (current max real id is 5199, 9999 is Unknown)

CLUBS_ADD = [
    # -------------------------------------------------------------------------
    # NORWEGIAN CLUBS (NOR) - High frequency
    # -------------------------------------------------------------------------
    (5200, 'Vikåsen BTK', 'Vikåsen Bordtennisklubb', 1, 'Trondheim', 'NOR', None, None, 1, None),  # 282 occ
    (5201, 'Bærums Verk', 'Bærums Verk Bordtennis', 1, 'Bærum', 'NOR', None, None, 1, None),  # 269 occ
    (5202, 'Bergen PIL', 'Bergen Paraidrettslag', 1, 'Bergen', 'NOR', None, None, 1, None),  # 220 occ
    # REMOVED: (5203, 'Nesodden BTK') - Same club as 1108 Nesodden IF, added as alias instead
    (5204, 'Nidaros BTK', 'Nidaros Bordtennisklubb', 1, 'Trondheim', 'NOR', None, None, 1, None),  # 105 occ
    (5205, 'Fredrikstad BTK', 'Fredrikstad Bordtennisklubb', 1, 'Fredrikstad', 'NOR', None, None, 1, None),  # 101 occ
    (5206, 'Stjørdal BTK', 'Stjørdal Bordtennisklubb', 1, 'Stjørdal', 'NOR', None, None, 1, None),  # 68 occ
    # REMOVED: (5207, 'Skjoldar') - DUPLICATE of 1094 Skjoldar IL
    
    # -------------------------------------------------------------------------
    # DANISH CLUBS (DEN) - High frequency
    # -------------------------------------------------------------------------
    (5210, 'Rising Stars Danmark', 'Rising Stars Danmark', 1, None, 'DEN', None, None, 1, None),  # 228 occ
    (5211, 'Søhøjlandets BTK', 'Søhøjlandets Bordtennisklub', 1, None, 'DEN', None, None, 1, None),  # 140 occ
    (5212, 'HØST IF', 'HØST Idrætsforening', 1, None, 'DEN', None, None, 1, None),  # 135 occ
    (5213, 'TIK Taastrup', 'TIK Taastrup Bordtennis', 1, 'Taastrup', 'DEN', None, None, 1, None),  # 70 occ
    
    # -------------------------------------------------------------------------
    # ENGLISH CLUBS (ENG) - High frequency
    # -------------------------------------------------------------------------
    (5220, 'Brighton TTC', 'Brighton Table Tennis Club', 1, 'Brighton', 'ENG', None, None, 1, None),  # 115 occ
    (5221, 'Fusion TTC', 'Fusion Table Tennis Club', 1, None, 'ENG', None, None, 1, None),  # 88 occ
    # REMOVED: (5222, 'London TT Academy') - Same as 1110 London Academy, added as alias instead
    
    # -------------------------------------------------------------------------
    # GERMAN CLUBS (DEU) - High frequency
    # -------------------------------------------------------------------------
    (5230, 'TTG 207', 'TTG 207 Ahrensburg/Großhansdorf', 1, 'Ahrensburg', 'DEU', None, None, 1, None),  # 108 occ
    (5231, 'Krummesser SV', 'Krummesser Sportverein', 1, 'Krummesse', 'DEU', None, None, 1, None),  # 90 occ
    (5232, 'VfB Lübeck', 'VfB Lübeck', 1, 'Lübeck', 'DEU', None, None, 1, None),  # 81 occ
    (5233, 'TSV Kronshagen', 'TSV Kronshagen', 1, 'Kronshagen', 'DEU', None, None, 1, None),  # 53 occ
    (5234, 'Team Ratzeburg', 'Team Ratzeburg', 1, 'Ratzeburg', 'DEU', None, None, 1, None),  # 62 occ
    
    # -------------------------------------------------------------------------
    # OTHER COUNTRIES
    # -------------------------------------------------------------------------
    (5240, 'Estonia LTK Kalev', 'Estonia LTK Kalev', 1, 'Tallinn', 'EST', None, None, 1, None),  # 104 occ
    # REMOVED: (5241, 'Table Tennis Ireland') - Ireland placeholder exists as 5079, added as alias instead
    
    # -------------------------------------------------------------------------
    # SPECIAL ENTRIES (Federations, placeholders)
    # -------------------------------------------------------------------------
    (5250, 'Utenlandsk spiller', 'Utenlandsk spiller', 3, None, 'UNK', 'Foreign player placeholder (Norwegian)', None, 1, None),  # 221 occ
    # REMOVED: (5251, 'Norges BTF') - Norway placeholder exists as 5127, added as alias instead
    
    # -------------------------------------------------------------------------
    # PART 2 ADDITIONS - More Norwegian clubs
    # -------------------------------------------------------------------------
    (5260, 'Snarøya BTK', 'Snarøya Bordtennisklubb', 1, 'Oslo', 'NOR', None, None, 1, None),  # 39 occ
    (5261, 'Kløfta IL', 'Kløfta Idrettslag', 1, 'Kløfta', 'NOR', None, None, 1, None),  # 38 occ
    (5262, 'Sørfjell', 'Sørfjell IL', 1, None, 'NOR', None, None, 1, None),  # 35 occ
    (5263, 'NTNUI', 'NTNUI Bordtennis', 1, 'Trondheim', 'NOR', 'NTNU student club', None, 1, None),  # 44 occ
    (5264, 'Sveberg IL', 'Sveberg Idrettslag', 1, None, 'NOR', None, None, 1, None),  # 24 occ
    (5265, 'Lillehammer', 'Lillehammer BTK', 1, 'Lillehammer', 'NOR', None, None, 1, None),  # 22 occ
    (5266, 'Rakkestad RK', 'Rakkestad Racketklubb', 1, 'Rakkestad', 'NOR', None, None, 1, None),  # 22 occ
    (5267, 'Ørsta IL', 'Ørsta IL Bordtennis', 1, 'Ørsta', 'NOR', None, None, 1, None),  # 21 occ
    (5268, 'Eide IL', 'Eide Idrettslag', 1, 'Eide', 'NOR', None, None, 1, None),  # 21 occ
    (5269, 'Verningen BTK', 'Verningen Bordtennisklubb', 1, None, 'NOR', None, None, 1, None),  # 27 occ
    
    # -------------------------------------------------------------------------
    # PART 2 ADDITIONS - More Danish clubs
    # -------------------------------------------------------------------------
    (5270, 'Otterup BTK', 'Otterup Bordtennis Klub', 1, 'Otterup', 'DEN', None, None, 1, None),  # 51 occ
    (5271, 'Ølsted BTK', 'Ølsted Bordtennisklub', 1, 'Ølsted', 'DEN', None, None, 1, None),  # 50 occ
    (5272, 'Nykøbing F BTK', 'Nykøbing Falster BTK af 1975', 1, 'Nykøbing Falster', 'DEN', None, None, 1, None),  # 49 occ
    (5273, 'Rødvig G&I', 'Rødvig Gymnastik og Idrætsforening', 1, 'Rødvig', 'DEN', None, None, 1, None),  # 46 occ
    (5274, 'TKC Fyn', 'TKC Fyn', 1, 'Fyn', 'DEN', None, None, 1, None),  # 39 occ
    (5275, 'Åbenrå BTK', 'Åbenrå Bordtennisklub', 1, 'Åbenrå', 'DEN', None, None, 1, None),  # 39 occ
    # REMOVED: (5276, 'Silkeborg BTK') - DUPLICATE of 1035 Silkeborg BTK
    (5277, 'TST Aarhus', 'TST Aarhus Bordtennis', 1, 'Aarhus', 'DEN', None, None, 1, None),  # 61 occ
    (5278, 'Nexø BTK', 'Nexø Bordtennisklub', 1, 'Nexø', 'DEN', None, None, 1, None),  # 40 occ
    (5279, 'Gundsølille SG&IF', 'Gundsølille Sport og Idrætsforening', 1, 'Gundsølille', 'DEN', None, None, 1, None),  # 31 occ
    (5280, 'Solrød BTK', 'Solrød Bordtennisklub', 1, 'Solrød', 'DEN', None, None, 1, None),  # 25 occ
    (5281, 'Boldklubben Stefan', 'Boldklubben Stefan', 1, None, 'DEN', None, None, 1, None),  # 22 occ
    (5282, 'Dalby BTK', 'Dalby Bordtennisklub', 1, 'Dalby', 'DEN', None, None, 1, None),  # 22 occ
    (5283, 'Skårup IF', 'Skårup Idrætsforening', 1, 'Skårup', 'DEN', None, None, 1, None),  # 65 occ
    (5284, 'Nyråd IF', 'Nyråd Idrætsforening', 1, 'Nyråd', 'DEN', None, None, 1, None),  # 64 occ
    (5285, 'Parabordtennis DEN', 'Parabordtennis Denmark', 2, None, 'DEN', 'Danish Para TT', None, 1, None),  # 37 occ
    (5286, 'JIF Hakoah', 'JIF Hakoah', 1, None, 'DEN', None, None, 1, None),  # 59 occ
    
    # -------------------------------------------------------------------------
    # PART 2 ADDITIONS - More German clubs
    # -------------------------------------------------------------------------
    (5290, 'TSV Schlutup', 'TSV Schlutup von 1907 e.V.', 1, 'Lübeck', 'DEU', None, None, 1, None),  # 52 occ
    (5291, 'TuRa Harksheide', 'TuRa Harksheide', 1, 'Norderstedt', 'DEU', None, None, 1, None),  # 50 occ
    (5292, 'Nortorf', 'Nortorf TTC', 1, 'Nortorf', 'DEU', None, None, 1, None),  # 45 occ
    # REMOVED: (5293, 'Lübecker TS') - DUPLICATE of 1053 Lübecker TS
    (5294, 'SV Friedrichsort', 'SV Friedrichsort', 1, 'Kiel', 'DEU', None, None, 1, None),  # 33 occ
    (5295, 'TSV Heist', 'TSV Heist', 1, 'Heist', 'DEU', None, None, 1, None),  # 32 occ
    (5296, 'TSV Melsdorf', 'TSV Melsdorf', 1, 'Melsdorf', 'DEU', None, None, 1, None),  # 29 occ
    (5297, 'TSV Vineta-Audorf', 'TSV Vineta-Audorf', 1, 'Rendsburg', 'DEU', None, None, 1, None),  # 28 occ
    (5298, 'TuS Mettenhof', 'TuS H/Mettenhof', 1, 'Kiel', 'DEU', None, None, 1, None),  # 27 occ
    (5299, 'TTC Ramsharde', 'TTC Ramsharde Flensburg', 1, 'Flensburg', 'DEU', None, None, 1, None),  # 22 occ
    (5300, 'SV Böblingen', 'SV Böblingen', 1, 'Böblingen', 'DEU', None, None, 1, None),  # 20 occ
    (5301, 'SG WTB 61', 'SG WTB 61/Eilbeck', 1, 'Hamburg', 'DEU', None, None, 1, None),  # 25 occ
    
    # -------------------------------------------------------------------------
    # PART 2 ADDITIONS - Other countries / Federations
    # -------------------------------------------------------------------------
    
    # NATIONAL FEDERATION PLACEHOLDERS (type 2)
    # These are used when a player represents their national federation rather than a club
    (5000, 'Iceland', 'Íslands Borðtennissamband', 2, None, 'ISL', 'Iceland TT Federation', None, 1, None),  # Island BTF
    (5059, 'Finland', 'Suomen Pöytätennisliitto', 2, None, 'FIN', 'Finland TT Association', None, 1, None),  # Finland TTA
    (5079, 'Ireland', 'Table Tennis Ireland', 2, None, 'IRL', 'Ireland TT Federation', None, 1, None),  # TTI
    (5127, 'Norway', 'Norges Bordtennisforbund', 2, None, 'NOR', 'Norway TT Federation', None, 1, None),  # Norges BTF
    
    (5310, 'Ligue Occitanie', 'Ligue Occitanie FRA', 2, None, 'FRA', 'French regional federation', None, 1, None),  # 78 occ
    (5311, 'MilanoTTC Academy', 'MilanoTTC Academy ITA', 1, 'Milano', 'ITA', None, None, 1, None),  # 73 occ
    (5312, 'Aile Francophone', 'Aile Francophone TT BEL', 1, None, 'BEL', None, None, 1, None),  # 57 occ
    (5313, 'Cote dAzur', 'Region A. Cote dAzur', 2, None, 'FRA', 'French regional', None, 1, None),  # 56 occ
    (5314, 'Uppsala Life IF', 'Uppsala Life IF', 1, 'Uppsala', 'SWE', None, None, 1, None),  # 53 occ
    (5315, 'KIFU', 'KIFU', 1, None, 'FIN', None, None, 1, None),  # 51 occ
    (5316, 'Green House', 'Green House TTC', 1, None, 'ENG', None, None, 1, None),  # 51 occ
    # REMOVED: (5317, 'Pechatniki') - DUPLICATE of 1339 Pechatniki TTC
    (5318, 'Grantham Academy', 'Grantham Academy', 1, 'Grantham', 'ENG', None, None, 1, None),  # 48 occ
    # REMOVED: (5319, 'Val dOise') - DUPLICATE of 1417 Val d'Oise
    (5320, 'Norway Region East', 'Norway Region East', 2, None, 'NOR', 'Regional team', None, 1, None),  # 46 occ
    (5321, 'C.J.Hainaut', 'C.J.Hainaut BEL', 1, None, 'BEL', None, None, 1, None),  # 45 occ
    (5322, 'TTC Viljandi Sakala', 'TTC Viljandi Sakala', 1, 'Viljandi', 'EST', None, None, 1, None),  # 39 occ
    (5323, 'Viimsi PINX', 'Viimsi PINX', 1, 'Viimsi', 'EST', None, None, 1, None),  # 28 occ
    (5324, 'ÖSTK Pingis', 'ÖSTK Pingis', 1, None, 'SWE', None, None, 1, None),  # 51 occ
    (5325, 'City Uni Hong Kong', 'City University of Hong Kong', 1, 'Hong Kong', 'HKG', None, None, 1, None),  # 63 occ
    (5326, 'MRKS Gdansk', 'MRKS Gdansk', 1, 'Gdansk', 'POL', None, None, 1, None),  # 35 occ
    (5327, 'Club Normandie', 'Club Normandie', 2, None, 'FRA', 'French regional', None, 1, None),  # 35 occ
    (5328, 'Oulun PT-86', 'Oulun Pöytätennis -86', 1, 'Oulu', 'FIN', None, None, 1, None),  # 34 occ
    (5329, 'Pole Espoir PdlL', 'Pole Espoir Pays De La Loire', 2, None, 'FRA', 'French training center', None, 1, None),  # 32 occ
    (5330, 'Bergen Paraidrettslag', 'Bergen Paraidrettslag', 1, 'Bergen', 'NOR', 'Para sports', None, 1, None),  # 32 occ
    (5331, 'Bordtennisdeild KR', 'Bordtennisdeild KR', 1, None, 'ISL', 'Iceland', None, 1, None),  # 36 occ
    (5332, 'DTU Bordtennis', 'DTU Bordtennis', 1, None, 'DEN', 'Technical University', None, 1, None),  # 35 occ
    (5333, 'Færøerne BTK', 'Færøerne BTK', 1, None, 'FRO', 'Faroe Islands', None, 1, None),  # 42 occ
    (5334, 'Gui Sportsklubb', 'Gui Sportsklubb', 1, None, 'NOR', None, None, 1, None),  # 42 occ
    (5335, 'YPTS', 'YPTS', 1, None, 'FIN', None, None, 1, None),  # 30 occ
    # REMOVED: (5336, 'Moscow TTC') - DUPLICATE of 1338 Moscow TTC
    (5337, 'Penthouse CPH', 'Penthouse CPH', 1, 'Copenhagen', 'DEN', None, None, 1, None),  # 27 occ
    # REMOVED: (5338, 'Aspire Qatar') - DUPLICATE of 1247 Aspire/Qatar
    (5339, 'Team Jutland', 'Team Jutland', 2, None, 'DEN', 'Regional team', None, 1, None),  # 24 occ
    (5340, 'Åbo', 'Åbo BTK', 1, 'Turku', 'FIN', None, None, 1, None),  # 23 occ
    (5341, 'Imås', 'Imås IL', 1, None, 'NOR', None, None, 1, None),  # 34 occ
    (5342, 'TPHU', 'TPHU', 1, None, 'DEN', None, None, 1, None),  # 21 occ
    (5343, 'Joola Plymouth', 'Joola Plymouth TTC', 1, 'Plymouth', 'ENG', None, None, 1, None),  # 20 occ
    (5344, 'Courbevoie TTC', 'Courbevoie TTC', 1, 'Courbevoie', 'FRA', None, None, 1, None),  # 20 occ
    (5345, 'Team England', 'Team England', 2, None, 'ENG', 'National team', None, 1, None),  # 20 occ
    (5346, 'Sv Paralandslaget', 'Svenska Paralandslaget', 2, None, 'SWE', 'Swedish Para team', None, 1, None),  # 20 occ
    (5347, 'Chinese Taipei', 'Chinese Taipei', 2, None, 'TPE', 'National team', None, 1, None),  # 34 occ
    (5348, 'Kirchberg', 'Kirchberg TTC', 1, 'Kirchberg', 'AUT', None, None, 1, None),  # 25 occ
    (5349, 'SU Inzing', 'SU Inzing', 1, 'Inzing', 'AUT', None, None, 1, None),  # 21 occ
    (5350, 'Rishon LeZion', 'Rishon LeZion TTC', 1, 'Rishon LeZion', 'ISR', None, None, 1, None),  # 20 occ
    
    # -------------------------------------------------------------------------
    # PART 3 ADDITIONS - More clubs
    # -------------------------------------------------------------------------
    
    # Danish clubs
    # REMOVED: (5355, 'Daugård IF') - DUPLICATE of 1206 Daugård IF
    (5356, 'Alsted-Fjenneslev', 'Alsted-Fjenneslev IF', 1, None, 'DEN', None, None, 1, None),  # 27 occ
    (5357, 'Fårevejle IF', 'Fårevejle Idrætsforening', 1, 'Fårevejle', 'DEN', None, None, 1, None),  # 20 occ
    (5358, 'Litauen', 'Lithuania BTF', 2, None, 'LTU', 'Lithuanian federation', None, 1, None),  # 20 occ
    
    # German clubs
    (5360, 'SV Fockbek', 'SV Fockbek', 1, 'Fockbek', 'DEU', None, None, 1, None),  # 19 occ
    (5361, 'Flensborg UF', 'Flensborg UF', 1, 'Flensburg', 'DEU', None, None, 1, None),  # 19 occ
    
    # Norwegian clubs
    (5365, 'Ålesund BTK', 'Ålesund Bordtennisklubb', 1, 'Ålesund', 'NOR', None, None, 1, None),  # 18 occ
    (5366, 'Lia BTK', 'Lia Bordtennisklubb', 1, None, 'NOR', None, None, 1, None),  # 17 occ
    (5367, 'Årby G og IF', 'Årby Gymnastik og Idrætsforening', 1, None, 'NOR', None, None, 1, None),  # 17 occ
    
    # Estonian clubs
    (5370, 'Maarduu', 'Maarduu LTK', 1, 'Maarduu', 'EST', None, None, 1, None),  # 19 occ
    
    # Swedish clubs
    (5375, 'Parkinson Göteborg', 'Parkinson Göteborg', 1, 'Göteborg', 'SWE', 'Para club', None, 1, None),  # 18 occ
    
    # Polish clubs
    (5380, 'Lidzbark Warminski', 'Lidzbark Warminski TTC', 1, 'Lidzbark Warminski', 'POL', None, None, 1, None),  # 17 occ
    
    # Other
    (5385, 'Grønland', 'Grønland BTF', 2, None, 'GRL', 'Greenland', None, 1, None),  # 17 occ
    
    # -------------------------------------------------------------------------
    # PART 4 ADDITIONS - Nov 29 2025 - More missing clubs from log analysis
    # -------------------------------------------------------------------------
    
    # Danish clubs - from "Club not found" warnings
    (5400, 'Reerslev IF', 'Reerslev Idrætsforening', 1, 'Reerslev', 'DEN', None, None, 1, None),  # log shows this club missing
    
    # -------------------------------------------------------------------------
    # PART 4 - ALL REMOVED - These were duplicates or should be aliases:
    # -------------------------------------------------------------------------
    # REMOVED: (5390, 'Team Nørreå') - Same as 1127 Team Nørre, added as alias
    # REMOVED: (5391, 'Myra IL') - DUPLICATE of 1100 Myra
    # REMOVED: (5392, 'Kobra BTK') - DUPLICATE of 1029 Kobra BTK
    # REMOVED: (5393, 'St Petersburg') - DUPLICATE of 1248 St Petersburg TT, added as alias
    # REMOVED: (5394, 'Mejlans BF') - DUPLICATE of 1119 Mejlans BF
    # REMOVED: (5395, 'Island BTF') - Iceland exists as 5000, added as alias
    # REMOVED: (5396, 'Finland TTA') - Finland exists as 5059, added as alias
]

# Aliases for new clubs
CLUB_ALIASES_NEW = [
    # TTG 207 variations
    (5230, 'TTG 207 Ahrensburg/ GroBhansdorf', 'short'),
    (5230, 'TTG 207 Ahrensburg/ GroBhansd', 'short'),
    
    # Brighton TTC variations  
    (5220, 'Brighton Table Tennis Club', 'long'),
    (5220, 'Brighton TTC ENG', 'short'),
    
    # Fusion TTC variations
    (5221, 'Fusion TTC ENG', 'short'),
    (5221, 'Fusion TTC/Eng', 'short'),
    
    # London TT Academy variations - NOW USES id=1110 (London Academy)
    (1110, 'London table tennis academy', 'short'),
    (1110, 'London/Eng', 'short'),
    (1110, 'London Aca. ENG', 'short'),
    
    # Table Tennis Ireland - NOW USES id=5079 (Ireland)
    (5079, 'Table tennis of Ireland', 'short'),
    
    # Norges BTF variations - NOW USES id=5127 (Norway)
    (5127, 'Norges Bordtennisforbund', 'short'),
    (5127, 'Norges Bordtennisförbund', 'short'),
    (5127, 'Norges Bordtennisforbu', 'short'),
    (5127, 'Norges Bordtennisforb', 'short'),
    
    # -------------------------------------------------------------------------
    # PART 2 - Additional aliases for new clubs
    # -------------------------------------------------------------------------
    
    # Norwegian clubs
    (5263, 'NTNUI - bordtennis', 'short'),
    (5266, 'Rakkestad Racketklubb', 'long'),
    (5267, 'Ørsta IL-BTG', 'short'),
    (5260, 'Snarøya BT', 'short'),
    (5330, 'Bergen Paraidrettslag', 'long'),
    (5330, 'Bergen PIL', 'short'),  # Duplicate prevention
    (5341, 'Imås IL', 'long'),
    (5334, 'Gui Sportsklubb', 'long'),
    
    # Danish clubs
    (5270, 'Otterup Bordtennis Klub', 'long'),
    (5270, 'Otterup Bordtennis Kl', 'short'),
    (5272, 'Nykøbing Falster BTK af 1975', 'long'),
    (5277, 'TST Aarhus Bordtennis', 'long'),
    (5277, 'TST Aarhus Bordt', 'short'),
    (5277, 'TST Aarhus Bo', 'short'),
    (5278, 'Nexö Bordtennisklubb', 'short'),
    (5278, 'Nexö Bordtennis Club', 'short'),
    (5279, 'Gundsølille Sg&if', 'short'),
    (5283, 'Skårup IdrætsForening', 'long'),
    (5285, 'Parabordtennis Denmark', 'long'),
    (5332, 'DTU bordtennis', 'short'),
    (5333, 'Færøyene BTK', 'short'),
    (5333, 'Færøerne', 'short'),
    (5337, 'Penthouse', 'short'),
    (5337, 'Penthouse CPH DEN', 'short'),
    
    # German clubs
    (5290, 'TSV Schlutup von 1907 e.V.', 'long'),
    (5291, 'Tura Harksheide', 'short'),
    (5298, 'TuS H/Mettenhof', 'short'),
    (5298, 'TuS H/Mettenhof/Ty', 'short'),
    (5299, 'TTC Ramsharde Flensburg', 'long'),
    (5301, 'SG WTB 61/Eilbeck', 'long'),
    
    # Other countries
    (5310, 'Ligue Occitanie FRA', 'short'),
    (5310, 'Reg. Occitanie FRA', 'short'),
    (5311, 'MilanoTTC Aca. ITA', 'short'),
    (5311, 'MilanoTTC Aca', 'short'),
    (5312, 'Aile Francophone TT BEL', 'long'),
    (5313, 'Reg. A. Cote dAzur', 'short'),
    (5316, 'Green House TTC', 'long'),
    (5316, 'Greenhouse', 'short'),
    (5316, 'Greenhouse/Eng', 'short'),
    (5316, 'Greenhouse TTC Eng', 'short'),
    # REMOVED: (5317, 'Pechatniki/Ru', 'short') - Now uses 1339 Pechatniki TTC
    (1339, 'Pechatniki/Ru', 'short'),
    (5318, 'Grantham Academy ENG', 'short'),
    (5318, 'Grantham Academy EN', 'short'),
    # REMOVED: (5319, 'Val d Oise/Fr', 'short') - Now uses 1417 Val d'Oise
    (1417, 'Val d Oise/Fr', 'short'),
    (5320, 'Norway Region East', 'long'),
    (5321, 'C.J.Hainaut BEL', 'long'),
    (5322, 'TTC Viljandi Sakala', 'long'),
    (5323, 'Viimsi PINX EST', 'short'),
    (5323, 'Viimsi LTK/Est', 'short'),
    (5325, 'City University of Hong Kong', 'long'),
    (5327, 'Club Normandie/Fra', 'short'),
    (5328, 'Oulun Pöytätennis -86', 'long'),
    (5329, 'Pole Espoir Pays De La Loire', 'long'),
    # REMOVED: (5336, 'Moscow/Ru', 'short') - Now uses 1338 Moscow TTC
    (1338, 'Moscow/Ru', 'short'),
    (1338, 'Moscow/Ry', 'short'),
    # REMOVED: (5338, ...) - Now uses 1247 Aspire/Qatar
    (1247, 'Asprie/Qatar', 'short'),
    (1247, 'Aspire Academy/Qa', 'short'),
    (1247, 'Aspire Academy QAT', 'short'),
    (5343, 'Joola Playmouth TTC ENG', 'short'),
    (5344, 'Courbevoie TTC FRA', 'short'),
    (5346, 'Sv Paralandslaget', 'short'),
    (5346, 'Sv Handikapplandslaget', 'short'),
    (5347, 'Chinese Thaipee', 'short'),  # Common typo
    (5347, 'TPE', 'short'),
    
    # -------------------------------------------------------------------------
    # PART 3 - Additional aliases for new clubs
    # -------------------------------------------------------------------------
    
    # REMOVED: (5355, '10-Daugård IF', 'short') - Now uses 1206 Daugård IF
    (1206, '10-Daugård IF', 'short'),
    (5356, '18-Alsted-Fjenneslev', 'short'),
    (5357, '18-Fårevejle', 'short'),
    
    # Lithuania typos and variations
    (5358, 'Lithauen', 'short'),
    (5358, 'Litauen', 'short'),
    (5358, 'Lithuania', 'short'),
    
    # German clubs
    (5360, 'SV Fockbeck', 'short'),  # typo variation
    
    # Norwegian
    (5367, 'Årby G og IF', 'long'),
    
    # Estonian
    (5370, 'Maardu LTK', 'short'),
    (5370, 'Maarduu LTK', 'short'),
    
    # Polish
    (5380, 'Lidzbark Warminksii', 'short'),  # typo variation
    
    # -------------------------------------------------------------------------
    # PART 4 - Aliases for newly added clubs (Nov 29 2025)
    # UPDATED: These now point to the correct existing clubs
    # -------------------------------------------------------------------------
    
    # Team Nørreå → Team Nørre (id=1127)
    (1127, 'Team Norrea', 'short'),
    
    # Myra IL → Myra (id=1100) 
    (1100, 'Myra Ungdoms- og IL', 'short'),
    
    # Kobra BTK (id=1029)
    (1029, 'Kobra BTK NOR', 'short'),
    (1029, 'Kobra', 'short'),
    
    # St Petersburg → St Petersburg TT (id=1248)
    (1248, 'St Petersburg/Ry', 'short'),
    (1248, 'St Petersburg/Ru', 'short'),
    
    # Mejlans BF (id=1119)
    (1119, 'Mejlans BF/Fi', 'short'),
    (1119, 'Mejlans', 'short'),
    