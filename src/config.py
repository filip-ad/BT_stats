# config.py: 

LOG_FILE = "../data/logs/log.log"
LOG_LEVEL = "INFO"  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL

DB_NAME = "../data/table_tennis.db"

TOURNAMENTS_URL = "https://resultat.ondata.se/?viewAll=1"
TOURNAMENTS_START_DATE = "2025-06-15"

CLASSES_MAX_TOURNAMENTS = 3  # Maximum number of tournaments to scrape classes from

LICENSES_URL = "https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_public.php"

SCRAPE_MAX_CLUBS = 0    # How many clubs to iterate, 0 for all clubs
SCRAPE_SEASONS = 0      # Amount of seasons to iterate for each club, always starting with the oldest, 0 for all seasons

SCRAPE_RANKING_RUNS = 0         # How many rankings runs to scrape, 0 for all runs
SCRAPE_RANKING_ORDER = "oldest"  # Order of ranking runs to scrape, "oldest" or "newest"