# src/utils_scripts/pdf_search.py   ←  FINAL + shows active filters
import os
import re
import time
import sys
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from pypdf import PdfReader

# ==================== CONFIG & FILTERS ====================
SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parent.parent
ROOT_FOLDER = PROJECT_ROOT / "data" / "pdfs"

KEYWORD = "hamren"                                   # default if no CLI argument

ALLOWED_STAGES = {4, 5, 6, 8}                        # empty set {} = all stages
# ALLOWED_STAGES = set()                             # ← uncomment for ALL stages

TOURNAMENT_MIN = 0
TOURNAMENT_MAX = 999999
# Example:
# TOURNAMENT_MIN = 1200
# TOURNAMENT_MAX = 1400

MAX_MATCHES = 10
MAX_WORKERS = os.cpu_count() * 2
# ========================================================

def extract_ids(pdf_path: Path):
    path_str = str(pdf_path)
    tour_match = re.search(r"tournament_(\d+)", path_str)
    class_match = re.search(r"class_(\d+)", path_str)
    stage_match = re.search(r"stage_(\d+)\.pdf", path_str)

    if not (tour_match and class_match and stage_match):
        return None, None, None, None

    tournament_raw = tour_match.group(1)
    tournament_id_padded = tournament_raw.zfill(6)
    tournament_id_int = int(tournament_raw)
    class_id = class_match.group(1)
    stage = int(stage_match.group(1))

    return tournament_id_padded, tournament_id_int, class_id, stage


def build_links(tournament_id: str, class_id: str, stage: int):
    tournament_url = f"https://resultat.ondata.se/{tournament_id}/"
    class_pdf_url = f"https://resultat.ondata.se/ViewClassPDF.php?classID={class_id}&stage={stage}"
    return tournament_url, class_pdf_url


def search_single_pdf(args):
    pdf_path, keyword = args
    try:
        reader = PdfReader(pdf_path)
        if reader.is_encrypted:
            try: reader.decrypt("")
            except: return None
        for page in reader.pages:
            text = page.extract_text() or ""
            if keyword.lower() in text.lower():
                return str(pdf_path)
    except:
        pass
    return None


def get_active_filters_description():
    filters = []
    if ALLOWED_STAGES:
        filters.append(f"stages {', '.join(map(str, sorted(ALLOWED_STAGES)))}")
    if TOURNAMENT_MIN > 0 or TOURNAMENT_MAX < 999999:
        min_str = f"{TOURNAMENT_MIN:06d}" if TOURNAMENT_MIN > 0 else "any"
        max_str = f"{TOURNAMENT_MAX:06d}" if TOURNAMENT_MAX < 999999 else "any"
        filters.append(f"tournament ID {min_str}–{max_str}")
    return " + ".join(filters) if filters else "no filters"


def find_pdfs_with_keyword(keyword: str):
    root_path = Path(ROOT_FOLDER).resolve()
    if not root_path.exists():
        print(f"ERROR: Folder not found → {root_path}")
        return

    filtered_pdfs = []
    for p in root_path.rglob("*.pdf"):
        tid_pad, tid_int, cid, stage = extract_ids(p)
        if tid_pad is None:
            continue
        if ALLOWED_STAGES and stage not in ALLOWED_STAGES:
            continue
        if tid_int < TOURNAMENT_MIN or tid_int > TOURNAMENT_MAX:
            continue
        filtered_pdfs.append(p)

    total = len(filtered_pdfs)
    if total == 0:
        print("No PDFs match the current filters.")
        return

    filtered_pdfs.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    filter_desc = get_active_filters_description()
    print(f"Searching for '{keyword}' in {total:,} PDFs")
    if filter_desc != "no filters":
        print(f"   Active filters → {filter_desc}")
    print()

    start_time = time.time()
    matches = []

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(search_single_pdf, (p, keyword)): p for p in filtered_pdfs}

        for future in as_completed(futures):
            result = future.result()
            if result:
                matches.append(result)
                if len(matches) >= MAX_MATCHES:
                    for f in futures:
                        f.cancel()
                    break

    elapsed = time.time() - start_time
    scanned = sum(f.done() for f in futures)

    print("═" * 80)
    if matches:
        print(f"Done! Found {len(matches)} match(es) in {elapsed:.2f} seconds")
        if len(matches) == MAX_MATCHES:
            print(f"   (stopped early after ~{scanned:,} of {total:,} filtered PDFs)\n")

        for m in matches:
            tid_pad, tid_int, cid, stage = extract_ids(Path(m))
            print(m)
            if tid_pad and cid is not None and stage is not None:
                t_url, c_url = build_links(tid_pad, cid, stage)
                print(f"   → Class PDF : {c_url}")
                print(f"   → Tournament: {t_url}")
            print()
    else:
        print(f"No matches found for '{keyword}'")
    print("═" * 80)


if __name__ == "__main__":
    search_term = sys.argv[1].strip() if len(sys.argv) >= 2 else KEYWORD.strip()

    if not search_term:
        print("No search term! Pass it as argument or set KEYWORD variable.")
        sys.exit(1)

    find_pdfs_with_keyword(search_term)