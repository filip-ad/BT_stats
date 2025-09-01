# src/download_pdfs.py

import time
import os
import requests
from pathlib import Path
from db import get_conn

PDF_BASE = "https://resultat.ondata.se/ViewClassPDF.php"
CACHE_DIR = Path("data/pdfs")
STAGES = range(1, 7)  # Always try stages 1–6


def get_pdf_path(tournament_id_ext: str, class_id_ext: str, stage: int) -> Path:
    return CACHE_DIR / f"tournament_{tournament_id_ext}" / f"class_{class_id_ext}" / f"stage_{stage}.pdf"


def format_size(bytes_size: int) -> str:
    """Format size into KB/MB/GB string."""
    if bytes_size < 1024:
        return f"{bytes_size} B"
    elif bytes_size < 1024 * 1024:
        return f"{bytes_size / 1024:.1f} KB"
    elif bytes_size < 1024 * 1024 * 1024:
        return f"{bytes_size / (1024 * 1024):.2f} MB"
    else:
        return f"{bytes_size / (1024 * 1024 * 1024):.2f} GB"


def is_valid_pdf(path: Path) -> bool:
    """Check if file starts with %PDF- header."""
    try:
        with open(path, "rb") as f:
            header = f.read(5)
            return header == b"%PDF-"
    except Exception:
        return False


def download_pdf(tournament_id_ext: str, class_id_ext: str, stage: int, force: bool = False) -> Path | None:
    """Download a PDF if available. Returns path if stored, None if skipped."""
    pdf_path = get_pdf_path(tournament_id_ext, class_id_ext, stage)

    # If file exists but is invalid, remove it so we can try again
    if pdf_path.exists() and not is_valid_pdf(pdf_path):
        print(f"🧹 Removing junk file {pdf_path}")
        pdf_path.unlink()

    if pdf_path.exists() and not force:
        return pdf_path

    url = f"{PDF_BASE}?tournamentID={tournament_id_ext}&classID={class_id_ext}&stage={stage}"
    pdf_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        resp = requests.get(url, timeout=20)
        if resp.status_code != 200 or not resp.content.startswith(b"%PDF-"):
            return None

        with open(pdf_path, "wb") as f:
            f.write(resp.content)
        return pdf_path

    except Exception:
        return None


def main():
    conn, cur = get_conn()
    cur.execute("""
        SELECT t.tournament_id_ext, tc.tournament_class_id_ext
        FROM tournament_class tc
        JOIN tournament t ON t.tournament_id = tc.tournament_id
        WHERE t.tournament_id_ext IS NOT NULL
          AND tc.tournament_class_id_ext IS NOT NULL
    """)
    rows = cur.fetchall()

    total_files = len(rows) * len(STAGES)
    print(f"Found {len(rows)} classes → {total_files} PDFs (planned)\n")

    start = time.time()
    completed = 0
    accumulated_size = 0  # in bytes

    for tid_ext, cid_ext in rows:
        for stage in STAGES:
            t0 = time.time()
            pdf_path = get_pdf_path(tid_ext, cid_ext, stage)

            # If file exists but is junk, clean it
            if pdf_path.exists() and not is_valid_pdf(pdf_path):
                print(f"🧹 Removing junk file {pdf_path}")
                pdf_path.unlink()

            if pdf_path.exists():
                completed += 1
                size = os.path.getsize(pdf_path)
                accumulated_size += size
                elapsed = time.time() - start
                avg_time = elapsed / completed
                remaining = (total_files - completed) * avg_time
                print(f"[{completed}/{total_files}] ⚡ Cached {pdf_path} "
                      f"({format_size(size)}, total {format_size(accumulated_size)}, "
                      f"elapsed {elapsed/60:.1f}m, ETA {remaining/60:.1f}m)")
                continue

            stored = download_pdf(tid_ext, cid_ext, stage)
            completed += 1
            dt = time.time() - t0
            elapsed = time.time() - start
            avg_time = elapsed / completed
            remaining = (total_files - completed) * avg_time

            if stored:
                size = os.path.getsize(stored)
                accumulated_size += size
                print(f"[{completed}/{total_files}] ✅ Stored {stored} "
                      f"({dt:.2f}s, {format_size(size)}, total {format_size(accumulated_size)}, "
                      f"elapsed {elapsed/60:.1f}m, ETA {remaining/60:.1f}m)")
            else:
                print(f"[{completed}/{total_files}] ⚠️ Skipped stage {stage} "
                      f"(elapsed {elapsed/60:.1f}m, ETA {remaining/60:.1f}m)")

    total_time = time.time() - start
    print(f"\nFinished {completed}/{total_files} PDFs in {total_time/60:.1f} minutes "
          f"(total size {format_size(accumulated_size)})")


if __name__ == "__main__":
    main()
