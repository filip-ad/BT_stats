# config.py: 

LOG_FILE                                = "../data/logs/log.log"
LOG_LEVEL                               = "INFO"    # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
DB_NAME                                 = "../data/table_tennis.db"
PDF_CACHE_DIR                           = "data/pdfs"

SCRAPE_LICENSES_MAX_CLUBS               = 0         # How many clubs to iterate, 0 for all clubs
SCRAPE_LICENSES_NBR_OF_SEASONS          = 1         # Amount of seasons to iterate for each club, always starting with the oldest, 0 for all seasons
SCRAPE_LICENSES_ORDER                   = "newest"  # Order of seasons to scrape, "oldest" or "newest"

SCRAPE_RANKINGS_NBR_OF_RUNS             = 1         # How many rankings runs to scrape, 0 for all runs
SCRAPE_RANKINGS_ORDER                   = "newest"  # Order of ranking runs to scrape, "oldest" or "newest"

SCRAPE_TRANSITIONS_NBR_OF_SEASONS       = 1         # Amount of seasons to iterate for each club, always starting with the oldest, 0 for all seasons
SCRAPE_TRANSITIONS_ORDER                = "newest"  # Order of seasons to scrape, "oldest" or "newest"

SCRAPE_TOURNAMENTS_CUTOFF_DATE          = "2025-10-01"  # Date format: YYYY-MM-DD
SCRAPE_TOURNAMENTS_ORDER                = "newest"      # Order of tournaments to scrape, "oldest" or "newest"
SCRAPE_TOURNAMENT_SBTFOTT_URL           = "https://sbtfott.stupaevents.com/#/events"

SCRAPE_CLASSES_MAX_TOURNAMENTS          = None         # Maximum number of tournaments to scrape classes from
SCRAPE_CLASSES_TOURNAMENT_ID_EXTS       = None       # List (TEXT) ['123', '234'], None for all

# Update defaults to use None for "no limit/all"
SCRAPE_PARTICIPANTS_CUTOFF_DATE         = '2000-01-01'          # Date format: YYYY-MM-DD, None for all
SCRAPE_PARTICIPANTS_MAX_CLASSES         = None                  # Maximum number of classes to scrape participants from, None for all classes
SCRAPE_PARTICIPANTS_CLASS_ID_EXTS       = ['30018']                  # List (TEXT) ['123', '234'], None for all
SCRAPE_PARTICIPANTS_TNMT_ID_EXTS        = None                  # List (TEXT) ['123', '234'], None for all
SCRAPE_PARTICIPANTS_ORDER               = "newest"              # Order of classes to scrape participants from, "oldest" or "newest"

RESOLVE_ENTRIES_CUTOFF_DATE             = '2025-09-01'          # Date format: YYYY-MM-DD, None for all

