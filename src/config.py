# config.py: 

LOG_FILE                                = "../data/logs/log.log"
LOG_LEVEL                               = "INFO"    # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
DB_NAME                                 = "../data/table_tennis.db"

SCRAPE_LICENSES_MAX_CLUBS               = 0         # How many clubs to iterate, 0 for all clubs
SCRAPE_LICENSES_NBR_OF_SEASONS          = 1         # Amount of seasons to iterate for each club, always starting with the oldest, 0 for all seasons
SCRAPE_LICENSES_ORDER                   = "newest"  # Order of seasons to scrape, "oldest" or "newest"
LICENSES_URL                            = "https://www.profixio.com/fx/ranking_sbtf/ranking_sbtf_public.php"

SCRAPE_RANKINGS_NBR_OF_RUNS             = 1         # How many rankings runs to scrape, 0 for all runs
SCRAPE_RANKINGS_ORDER                   = "newest"  # Order of ranking runs to scrape, "oldest" or "newest"

SCRAPE_TRANSITIONS_NBR_OF_SEASONS       = 1         # Amount of seasons to iterate for each club, always starting with the oldest, 0 for all seasons
SCRAPE_TRANSITIONS_ORDER                = "newest"  # Order of seasons to scrape, "oldest" or "newest"

SCRAPE_TOURNAMENTS_CUTOFF_DATE          = "2000-07-01"  # Date format: YYYY-MM-DD
SCRAPE_TOURNAMENTS_ORDER                = "oldest"      # Order of tournaments to scrape, "oldest" or "newest"
SCRAPE_TOURNAMENTS_URL_ONDATA           = "https://resultat.ondata.se/?viewAll=1"
SCRAPE_TOURNAMENT_SBTFOTT_URL           = "https://sbtfott.stupaevents.com/#/events"

SCRAPE_CLASSES_MAX_TOURNAMENTS          = 0         # Maximum number of tournaments to scrape classes from
SCRAPE_CLASSES_TOURNAMENT_ID_EXTS       = ['001240']

# Update defaults to use None for "no limit/all"
SCRAPE_PARTICIPANTS_CUTOFF_DATE         = None  # Date format: YYYY-MM-DD, None for all
SCRAPE_PARTICIPANTS_MAX_CLASSES         = 10  # Maximum number of classes to scrape participants from, None for all classes
SCRAPE_PARTICIPANTS_CLASS_ID_EXTS       = ['29954']  # List (TEXT) ['123', '234'], None for all
SCRAPE_PARTICIPANTS_TNMT_ID_EXTS        = None
SCRAPE_PARTICIPANTS_ORDER               = "newest"      # Order of classes to scrape participants from, "oldest" or "newest"


DOWNLOAD_PDF_NBR_OF_CLASSES             = 1         # Download PDF:s for max this many classes before breaking
DOWNLOAD_PDF_TOURNAMENT_ID_EXT          = 678       # Download all PDF:s for this tournament with external ID