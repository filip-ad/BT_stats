# config.py: 

LOG_FILE                            = "../data/logs/log.log"
LOG_LEVEL                           = "INFO"  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
DB_NAME                             = "../data/table_tennis.db"

SCRAPE_TOURNAMENTS_URL              = "https://resultat.ondata.se/?viewAll=1"
SCRAPE_TOURNAMENTS_START_DATE       = "2000-06-15" # Date format: YYYY-MM-DD
SCRAPE_TOURNAMENTS_ORDER            = "newest"  # Order of tournaments to scrape, "oldest" or "newest"

CLASSES_MAX_TOURNAMENTS             = 3  # Maximum number of tournaments to scrape classes from

LICENSES_URL                        = "https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_public.php"

SCRAPE_LICENSES_MAX_CLUBS           = 5        # How many clubs to iterate, 0 for all clubs
SCRAPE_LICENSES_NBR_OF_SEASONS      = 1         # Amount of seasons to iterate for each club, always starting with the oldest, 0 for all seasons
SCRAPE_LICENSES_ORDER               = "newest"  # Order of seasons to scrape, "oldest" or "newest"

SCRAPE_RANKINGS_NBR_OF_RUNS         = 1         # How many rankings runs to scrape, 0 for all runs
SCRAPE_RANKINGS_ORDER               = "newest"  # Order of ranking runs to scrape, "oldest" or "newest"

SCRAPE_TRANSITIONS_NBR_OF_SEASONS   = 0         # Amount of seasons to iterate for each club, always starting with the oldest, 0 for all seasons
SCRAPE_TRANSITIONS_ORDER            = "newest"  # Order of seasons to scrape, "oldest" or "newest"