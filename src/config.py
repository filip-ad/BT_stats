# config.py

LOG_FILE                                = "../data/logs/log.log"
LOG_LEVEL                               = "INFO"    # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
DB_NAME                                 = "../data/table_tennis.db"
PUBLIC_DB_NAME                          = "../data/pingiskollen_public.db"
PDF_CACHE_DIR                           = "data/pdfs"

SCRAPE_LICENSES_MAX_CLUBS               = 0         # How many clubs to iterate, 0 for all clubs
SCRAPE_LICENSES_NBR_OF_SEASONS          = 1         # Amount of seasons to iterate for each club, always starting with the oldest, 0 for all seasons
SCRAPE_LICENSES_ORDER                   = "newest"  # Order of seasons to scrape, "oldest" or "newest"

SCRAPE_RANKINGS_NBR_OF_RUNS             = 1         # How many rankings runs to scrape, 0 for all runs
SCRAPE_RANKINGS_ORDER                   = "newest"  # Order of ranking runs to scrape, "oldest" or "newest"

SCRAPE_TRANSITIONS_NBR_OF_SEASONS       = 1         # Amount of seasons to iterate for each club, always starting with the oldest, 0 for all seasons
SCRAPE_TRANSITIONS_ORDER                = "newest"  # Order of seasons to scrape, "oldest" or "newest"

# Profixio leagues
SCRAPE_LEAGUES_SEASON_IDS               = None      # List like ['768'] to force specific seasons (None = auto/current)
SCRAPE_LEAGUES_ONLY_CURRENT             = False     # When True, only scrape the current season (starred in nav)
SCRAPE_LEAGUES_MAX_SEASONS              = 1      # Max seasons to process (applied after filters), None for all
SCRAPE_LEAGUES_MAX_LEAGUES_PER_SEASON   = 5      # Limit leagues per season (for testing), None for all
SCRAPE_LEAGUES_MAX_FIXTURES             = None      # Optional cap per league when testing
SCRAPE_LEAGUES_SKIP_SEEN_MATCHES        = False      # If True, skip fetching match reports already seen and older than the freshness window
SCRAPE_LEAGUES_SKIP_SEEN_MATCHES_DAYS   = 0         # Always refetch fixtures within the last N days; older fixtures can be skipped if already seen
SCRAPE_LEAGUES_REQUEST_DELAY            = 0       # Seconds to sleep between HTTP requests to Profixio (helps avoid hammering)
SCRAPE_LEAGUES_SEASONS_ORDER            = "newest"  # "newest" or "oldest" when ordering seasons
SCRAPE_LEAGUES_CACHE_HTML               = True      # Cache raw HTML for match reports locally to reduce re-fetching
SCRAPE_LEAGUES_CACHE_HTML_DIR           = "data/profixio_league_data_cache"
SCRAPE_LEAGUES_CACHE_HTML_DAYS          = 30        # Consider cached HTML fresh for N days (None = always use cache)

SCRAPE_TOURNAMENTS_CUTOFF_DATE          = "2025-11-01"  # Date format: YYYY-MM-DD
SCRAPE_TOURNAMENTS_ORDER                = "newest"      # Order of tournaments to scrape, "oldest" or "newest"
SCRAPE_TOURNAMENT_SBTFOTT_URL           = "https://sbtfott.stupaevents.com/#/events"

SCRAPE_CLASSES_MAX_TOURNAMENTS          = 50         # Maximum number of tournaments to scrape classes from
SCRAPE_CLASSES_TOURNAMENT_ID_EXTS       = None       # List (TEXT) ['123', '234'], None for all
RESOLVE_CLASS_ID_EXTS                   = None       # List (TEXT) ['123', '234

# Update defaults to use None for "no limit/all"
SCRAPE_PARTICIPANTS_CUTOFF_DATE         = '2025-11-01'          # Date format: YYYY-MM-DD, None for all
SCRAPE_PARTICIPANTS_MAX_CLASSES         = None                  # Maximum number of classes to scrape participants from, None for all classes
SCRAPE_PARTICIPANTS_CLASS_ID_EXTS       = None                 # List (TEXT) ['123', '234'], None for all
SCRAPE_PARTICIPANTS_TNMT_ID_EXTS        = None                  # List (TEXT) ['123', '234'], None for all
SCRAPE_PARTICIPANTS_ORDER               = "oldest"              # Order of classes to scrape participants from, "oldest" or "newest"

RESOLVE_CLASSES_CUTOFF_DATE             = '2000-01-01'          # Date format: YYYY-MM-DD, None for all

RESOLVE_ENTRIES_CUTOFF_DATE             = '2000-06-01'          # Date format: YYYY-MM-DD, None for all
RESOLVE_ENTRIES_CLASS_ID_EXTS           = None                   # List (TEXT) ['123', '234'], None for all

RESOLVE_MATCHES_CUTOFF_DATE             = '2000-06-01'          # Date format: YYYY-MM-DD, None for all

# Placeholder wiring used by the match resolver when a Vacant/WO side needs a
# real participant record. Keep these IDs in sync with the seed data in the DB.
PLACEHOLDER_PLAYER_ID                   = 99999
PLACEHOLDER_PLAYER_NAME                 = "Unknown Player"
PLACEHOLDER_CLUB_ID                     = 9999

