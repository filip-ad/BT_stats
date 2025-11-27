# src/utils_scripts/extract_pdf_metadata.py
"""
ondatadump.py – Complete low-level visual extractor for OnData result PDFs

Purpose:
    Capture *every single piece of visual information* that pdfplumber can see:
    • Every character with its exact font, size, color, bold/italic flags, and coordinates
    • Normalized 0–1 coordinates (great for training ML models)
    • All table lines (horizontal + vertical) and rectangles
    • Page dimensions, cropbox, metadata
    • Global word indexing across pages

Why this matters:
    OnData class PDFs have no logical structure — everything is drawn.
    To achieve 100% reliable parsing (even when layouts change slightly),
    we need the full ground truth. This script gives you exactly that.

Output:
    One JSON line per object:
    - {"type": "pdf_metadata", ...}
    - {"type": "page_objects", ...}   → lines, rects, page size
    - word objects with full char-level detail

Result file example: 20251120_123456_ondatadump.jsonl
"""

import io
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pdfplumber
import requests
from pdfplumber.page import Page  # ← correct type import (fixes previous error)


# ──────────────────────────────────────────────────────────────
# Configuration – change only these two lines
# ──────────────────────────────────────────────────────────────
PDF_URL: str = "https://resultat.ondata.se/ViewClassPDF.php?classID=26159&stage=5"

write_to_file: bool = True          # False → only console (if enabled)
print_to_console: bool = False      # True → huge stdout dump, useful only for debugging
# ──────────────────────────────────────────────────────────────


def download_pdf(url: str) -> bytes:
    """Download PDF and return raw bytes."""
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.content


def get_output_file() -> Optional[Path]:
    """Create timestamped .jsonl file if write_to_file is True."""
    if not write_to_file:
        return None
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    outfile = Path(f"{timestamp}_ondatadump.jsonl")
    print(f"[INFO] Writing output → {outfile}", file=sys.stderr)
    return outfile


def normalize_bbox(bbox: tuple[float, float, float, float], pw: float, ph: float) -> Dict[str, float]:
    """
    Convert absolute coordinates to normalized 0–1 range.
    pdfplumber uses bottom-left origin: (x0, bottom, x1, top)
    """
    x0, y0, x1, y1 = bbox
    return {"nx0": x0/pw, "ny0": y0/ph, "nx1": x1/pw, "ny1": y1/ph}


def extract_lines_and_rects(page: Page) -> Dict[str, Any]:
    """
    Extract all horizontal/vertical lines that form the table grid.
    OnData uses very thin black lines → we filter by thickness.
    
    NOTE: page.rectangles was REMOVED in pdfplumber >=0.10
          OnData doesn't use filled rectangles anyway — only lines.
          So we safely drop rectangles entirely (no data loss).
    """
    lines = page.lines  # This still exists and is perfect

    # Tolerance for "thin" lines (OnData uses ~0.5–1.0 pt lines)
    horiz = [l for l in lines if abs(l["top"] - l["bottom"]) < 3]
    vert  = [l for l in lines if abs(l["x0"]   - l["x1"])     < 3]

    def hline(l: dict) -> dict:
        """Horizontal table line"""
        return {
            "x0": l["x0"],
            "x1": l["x1"],
            "y": round(l["top"], 2),                    # y-position of the line
            "width": l.get("linewidth", 0.5),
            **normalize_bbox((l["x0"], l["bottom"], l["x1"], l["top"]), page.width, page.height),
        }

    def vline(l: dict) -> dict:
        """Vertical table line"""
        return {
            "x": round(l["x0"], 2),
            "y0": l["bottom"],
            "y1": l["top"],
            "width": l.get("linewidth", 0.5),
            **normalize_bbox((l["x0"], l["bottom"], l["x1"], l["top"]), page.width, page.height),
        }

    return {
        "horizontal_lines": [hline(l) for l in horiz],
        "vertical_lines":   [vline(l) for l in vert],
        # rectangles intentionally omitted — not used in OnData PDFs
        # If you ever need true filled rectangles, use page.find_tables() or page.edges instead
    }


def flush_word(
    chars: List[Dict[str, Any]],
    page_no: int,
    page_width: float,
    page_height: float,
    word_idx: int,
) -> Optional[Dict[str, Any]]:
    """
    Convert a list of character dicts into a rich word object.
    Includes per-character details → perfect for detecting partial bold (e.g. "1." only).
    """
    text = "".join(c["text"] for c in chars).strip()
    if not text:
        return None

    char_details = []
    for c in chars:
        font_name = c.get("fontname", "").lower()
        bold = any(token in font_name for token in ["bold", "black", "heavy", "semibold", "demi"])
        italic = any(token in font_name for token in ["italic", "oblique"]) or not c.get("upright", True)

        char_details.append({
            "char": c["text"],
            "fontname": c.get("fontname", ""),
            "size": round(c["size"], 2),
            "bold": bold,
            "italic": italic,
            "color": c.get("non_stroking_color"),   # usually (0,0,0) or red for DNF/etc.
            "x0": round(c["x0"], 2),
            "x1": round(c["x1"], 2),
            "top": round(c["top"], 2),
            "bottom": round(c["bottom"], 2),
        })

    # Word bounding box
    x0 = min(c["x0"] for c in chars)
    x1 = max(c["x1"] for c in chars)
    top = min(c["top"] for c in chars)
    bottom = max(c["bottom"] for c in chars)

    return {
        "page": page_no,
        "word_idx": word_idx,
        "text": text,
        "bbox_abs": {
            "x0": round(x0, 2),
            "x1": round(x1, 2),
            "top": round(top, 2),
            "bottom": round(bottom, 2),
        },
        "bbox_norm": normalize_bbox((x0, bottom, x1, top), page_width, page_height),
        "avg_size": round(sum(c["size"] for c in chars) / len(chars), 2),
        "chars": char_details,
    }


def inspect_pdf(pdf_bytes: bytes) -> None:
    """Main extraction loop – writes everything line-by-line."""
    outfile = get_output_file()
    fh = outfile.open("w", encoding="utf-8") if outfile else None

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            # 1. PDF-level metadata
            meta = {
                "type": "pdf_metadata",
                "pages": len(pdf.pages),
                "info": pdf.metadata or {},
            }
            line = json.dumps(meta, ensure_ascii=False)
            if print_to_console: print(line)
            if fh: fh.write(line + "\n")

            global_word_idx = 0

            for page_no, page in enumerate(pdf.pages, start=1):
                # 2. Page objects (lines, rects, dimensions)
                page_obj = {
                    "type": "page_objects",
                    "page": page_no,
                    "width": page.width,
                    "height": page.height,
                    "cropbox": page.cropbox,
                    **extract_lines_and_rects(page),
                }
                line = json.dumps(page_obj, ensure_ascii=False)
                if print_to_console: print(line)
                if fh: fh.write(line + "\n")

                # 3. Characters → visual lines → words
                chars = sorted(page.chars, key=lambda c: (-c["top"], c["x0"]))  # top-down, left-right

                visual_lines: List[List[dict]] = []
                current_line: List[dict] = []
                prev_top: Optional[float] = None

                for c in chars:
                    if prev_top is None or abs(c["top"] - prev_top) <= 3.5:  # line height tolerance
                        current_line.append(c)
                    else:
                        visual_lines.append(sorted(current_line, key=lambda c: c["x0"]))
                        current_line = [c]
                    prev_top = c["top"]
                if current_line:
                    visual_lines.append(sorted(current_line, key=lambda c: c["x0"]))

                # Process each visual line into words
                for line_chars in visual_lines:
                    word_chars: List[dict] = []
                    prev_char: Optional[dict] = None

                    for c in line_chars:
                        # Whitespace = word break
                        if c["text"].isspace():
                            if word_chars:
                                global_word_idx += 1
                                info = flush_word(word_chars, page_no, page.width, page.height, global_word_idx)
                                if info:
                                    j = json.dumps(info, ensure_ascii=False)
                                    if print_to_console: print(j)
                                    if fh: fh.write(j + "\n")
                                word_chars = []
                            prev_char = c
                            continue

                        # Large horizontal gap = word break
                        if prev_char and (c["x0"] - prev_char["x1"]) > 4.0 and word_chars:
                            global_word_idx += 1
                            info = flush_word(word_chars, page_no, page.width, page.height, global_word_idx)
                            if info:
                                j = json.dumps(info, ensure_ascii=False)
                                if print_to_console: print(j)
                                if fh: fh.write(j + "\n")
                            word_chars = []

                        word_chars.append(c)
                        prev_char = c

                    # Flush final word on line
                    if word_chars:
                        global_word_idx += 1
                        info = flush_word(word_chars, page_no, page.width, page.height, global_word_idx)
                        if info:
                            j = json.dumps(info, ensure_ascii=False)
                            if print_to_console: print(j)
                            if fh: fh.write(j + "\n")

    finally:
        if fh:
            fh.close()
            print(f"[INFO] Extraction complete → {outfile}", file=sys.stderr)


def main() -> None:
    print(f"[INFO] Downloading: {PDF_URL}", file=sys.stderr)
    pdf_bytes = download_pdf(PDF_URL)
    print("[INFO] Starting full visual extraction...", file=sys.stderr)
    inspect_pdf(pdf_bytes)


if __name__ == "__main__":
    main()