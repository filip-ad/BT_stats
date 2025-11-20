# src/utils_scripts/download_pdf.py

# A utility script to download tournament class PDFs from OnData.
# Run from the repo root with `PYTHONPATH=src python -m utils_scripts.download_pdf`.

import time
import requests
from pathlib import Path
from db import get_conn
from models.tournament import Tournament
from models.tournament_class import TournamentClass
from utils import parse_date, OperationLogger

PDF_BASE = "https://resultat.ondata.se/ViewClassPDF.php"
CACHE_DIR = Path("data/pdfs")
STAGES = range(1, 7)  # Always try stages 1–6
FORCE_DOWNLOAD = True  # Toggle to force re-downloading every PDF even if cached.

ENABLE_STAGE_PAUSE = True
STAGE_PAUSE_SECONDS = 0.1
MAX_STAGE_ATTEMPTS = 3
RETRY_BASE_DELAY_SECONDS = 0.5
RETRY_BACKOFF_FACTOR = 2.0

STAGE_TARGETS_BY_STRUCTURE = {
    1: list(STAGES),
    2: [1, 2, 3, 4, 6],
    3: [1, 5, 6],
    9: [],
}

def _select_stages_for_structure(structure_id: int | None) -> list[int]:
    if structure_id in STAGE_TARGETS_BY_STRUCTURE:
        return STAGE_TARGETS_BY_STRUCTURE[structure_id]
    return list(STAGES)

DOWNLOAD_PDFS_MAX_NBR_OF_CLASSES    = 10
DOWNLOAD_PDFS_CLASS_ID_EXTS         = None                 # List (TEXT) ['123', '234'], None for all
DOWNLOAD_PDFS_TNMT_ID_EXTS          = None                 # List (TEXT) ['123', '234'], None for all
DOWNLOAD_PDFS_CUTOFF_DATE           = '2025-10-01'         # Date format: YYYY-MM-DD, None for all
DOWNLOAD_PDFS_ORDER                 = 'newest'             # Order of classes to scrape participants from, "oldest" or "newest"

def main():
    conn, cur = get_conn()
    cutoff_date = parse_date(DOWNLOAD_PDFS_CUTOFF_DATE)
    classes = TournamentClass.get_filtered_classes(
        cur,
        data_source_id          = 1,
        cutoff_date             = cutoff_date,
        require_ended           = True,
        allowed_type_ids        = None,
        allowed_structure_ids   = None,
        max_classes             = DOWNLOAD_PDFS_MAX_NBR_OF_CLASSES,
        class_id_exts           = DOWNLOAD_PDFS_CLASS_ID_EXTS,
        tournament_id_exts      = DOWNLOAD_PDFS_TNMT_ID_EXTS,
        order                   = DOWNLOAD_PDFS_ORDER,
    )

    tournament_ids = [tc.tournament_id for tc in classes if tc.tournament_id is not None]
    tid_to_ext = Tournament.get_id_ext_map_by_id(cur, tournament_ids)
    class_rows: list[tuple[TournamentClass, str, str, list[int]]] = []
    missing_ext = 0
    for tc in classes:
        tournament_ext = tid_to_ext.get(tc.tournament_id)
        class_ext = tc.tournament_class_id_ext
        if not tournament_ext or not class_ext:
            missing_ext += 1
            continue
        stage_targets = _select_stages_for_structure(tc.tournament_class_structure_id)
        class_rows.append((tc, tournament_ext, class_ext, stage_targets))

    logger = OperationLogger(
        verbosity       = 2,
        print_output    = True,
        log_to_db       = False,
        object_type     = "pdf_cache",
        run_type        = "download",
    )


    logger.info(
        "setup",
        f"Force download: {FORCE_DOWNLOAD} | Classes found: {len(classes)} | "
        f"Valid: {len(class_rows)} | Missing ext: {missing_ext}",
        show_key=False,
        to_console=True,
    )

    overall_stats = {
        "stages": 0,
        "downloaded": 0,
        "redownloaded": 0,
        "cached": 0,
        "failed": 0,
    }

    for tc, tournament_ext, class_ext, stage_targets in class_rows:
        stats = {"cached": 0, "downloaded": 0, "redownloaded": 0, "failed": 0}
        failure_notes: list[str] = []
        for stage in stage_targets:
            overall_stats["stages"] += 1
            _, status, reason = _fetch_stage_pdf(
                tournament_ext,
                class_ext,
                stage,
                force_download=FORCE_DOWNLOAD,
            )

            logger.inc_processed()

            if status == "cached":
                stats["cached"] += 1
                overall_stats["cached"] += 1
                logger.success("PDF already cached")
            elif status == "downloaded":
                stats["downloaded"] += 1
                overall_stats["downloaded"] += 1
                logger.success("PDF downloaded")
            elif status == "redownloaded":
                stats["redownloaded"] += 1
                overall_stats["redownloaded"] += 1
                logger.success("PDF re-downloaded")
            else:
                stats["failed"] += 1
                overall_stats["failed"] += 1
                failure_notes.append(f"Stage {stage}: {reason}")
                logger.failed(f"PDF download failed: {reason}")

            if ENABLE_STAGE_PAUSE and STAGE_PAUSE_SECONDS > 0:
                time.sleep(STAGE_PAUSE_SECONDS)

        context = {
            "tournament_id_ext": tournament_ext,
            "tournament_class_id_ext": class_ext,
        }
        summary = (
            f"cached={stats['cached']}, downloaded={stats['downloaded']}, "
            f"redownloaded={stats['redownloaded']}, failed={stats['failed']}"
        )
        if failure_notes:
            summary += " | errors: " + "; ".join(failure_notes)
            logger.warning(context, summary, show_key=True, to_console=True)
        else:
            logger.info(context, summary, show_key=True, to_console=True)

    logger.info(
        "class_summary",
        f"{len(class_rows)} classes processed | "
        f"Cached stages: {overall_stats['cached']}, Downloaded: {overall_stats['downloaded']}, "
        f"Re-downloaded: {overall_stats['redownloaded']}, Failed: {overall_stats['failed']}",
        show_key=False,
        to_console=True,
    )
    logger.info(
        "totals",
        f"Stages attempted: {overall_stats['stages']}, cached: {overall_stats['cached']}, "
        f"downloaded: {overall_stats['downloaded']}, redownloaded: {overall_stats['redownloaded']}, "
        f"failed: {overall_stats['failed']}",
        show_key=False,
        to_console=True,
    )

    logger.summarize()
    conn.close()



def get_pdf_path(tournament_id_ext: str, class_id_ext: str, stage: int) -> Path:
    return CACHE_DIR / f"tournament_{tournament_id_ext}" / f"class_{class_id_ext}" / f"stage_{stage}.pdf"



def is_valid_pdf(path: Path) -> bool:
    """Check if file starts with %PDF- header."""
    try:
        with open(path, "rb") as f:
            header = f.read(5)
            return header == b"%PDF-"
    except Exception:
        return False


def _fetch_stage_pdf(tournament_id_ext: str, class_id_ext: str, stage: int, force_download: bool) -> tuple[Path | None, str, str]:
    """
    Ensure a PDF for the given stage exists. Returns (path, status, reason).
    Status is one of {'cached', 'downloaded', 'redownloaded', 'failed'}.
    """
    pdf_path = get_pdf_path(tournament_id_ext, class_id_ext, stage)
    cached_before = pdf_path.exists() and is_valid_pdf(pdf_path)
    if pdf_path.exists() and not cached_before:
        try:
            pdf_path.unlink()
        except Exception:
            pass
        cached_before = False

    if cached_before and not force_download:
        return pdf_path, "cached", "Cached PDF available"

    if cached_before and force_download and pdf_path.exists():
        try:
            pdf_path.unlink()
        except Exception:
            pass

    url = f"{PDF_BASE}?tournamentID={tournament_id_ext}&classID={class_id_ext}&stage={stage}"
    pdf_path.parent.mkdir(parents=True, exist_ok=True)

    last_reason = "Failed to download"
    for attempt in range(1, MAX_STAGE_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=20)
            if resp.status_code == 200 and resp.content.startswith(b"%PDF-"):
                with open(pdf_path, "wb") as handle:
                    handle.write(resp.content)
                status = "redownloaded" if cached_before and force_download else "downloaded"
                return pdf_path, status, "Downloaded PDF"
            last_reason = f"Stage {stage} responded with status {resp.status_code}"
        except Exception as exc:
            last_reason = f"Stage {stage} download error (attempt {attempt}): {exc}"

        if attempt < MAX_STAGE_ATTEMPTS:
            delay = RETRY_BASE_DELAY_SECONDS * (RETRY_BACKOFF_FACTOR ** (attempt - 1))
            if delay > 0:
                time.sleep(delay)

    return None, "failed", last_reason




if __name__ == "__main__":
    main()
