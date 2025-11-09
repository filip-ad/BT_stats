#!/usr/bin/env python3
"""
temp_pdf_metadata.py

Hard-coded: downloads the OnData PDF for classID=9998, stage=2
and prints per-word PDF internals to the console (one JSON per line).

Dependencies:
    pip install pdfplumber requests
"""

import io
import json
import sys
from typing import Any, Dict, Iterable, List

import pdfplumber
import requests

PDF_URL = "https://resultat.ondata.se/ViewClassPDF.php?classID=30834&stage=3"

def download_pdf(url: str) -> bytes:
    resp = requests.get(url)
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "").lower()
    if "pdf" not in content_type and not url.lower().endswith(".pdf"):
        print(
            f"[WARN] Content-Type '{content_type}' does not look like a PDF.",
            file=sys.stderr,
        )

    return resp.content


def infer_style_from_fontnames(fontnames: Iterable[str]) -> Dict[str, bool]:
    all_names = " ".join(fontnames).lower()
    bold_tokens = ["bold", "black", "heavy", "semibold", "demi"]
    italic_tokens = ["italic", "oblique", "slanted"]

    is_bold = any(tok in all_names for tok in bold_tokens)
    is_italic = any(tok in all_names for tok in italic_tokens)
    return {"is_bold": is_bold, "is_italic": is_italic}


def group_chars_into_lines(chars: List[Dict[str, Any]], line_tol: float = 2.0):
    """
    Group chars into lines based on their 'top' coordinate.
    Returns: list[list[char_dict]] – each inner list is one line.
    """
    if not chars:
        return []

    sorted_chars = sorted(chars, key=lambda c: (c["top"], c["x0"]))
    lines: List[List[Dict[str, Any]]] = []
    current_line: List[Dict[str, Any]] = []
    current_top = None

    for ch in sorted_chars:
        top = ch["top"]
        if current_top is None:
            current_top = top
            current_line.append(ch)
            continue

        if abs(top - current_top) <= line_tol:
            current_line.append(ch)
        else:
            lines.append(current_line)
            current_line = [ch]
            current_top = top

    if current_line:
        lines.append(current_line)

    for i, line in enumerate(lines):
        lines[i] = sorted(line, key=lambda c: c["x0"])

    return lines


def iter_words_from_line(
    line_chars: List[Dict[str, Any]],
    gap_threshold: float = 3.0,
):
    """
    Yield lists of chars, one list per 'word'.

    A new word is started when:
      * we see a whitespace char, OR
      * the horizontal gap between consecutive chars (x0 - prev.x1)
        is greater than `gap_threshold`.
    """
    current: List[Dict[str, Any]] = []
    prev = None

    for ch in line_chars:
        text = ch["text"]

        # Real whitespace: flush current word and skip the space itself
        if text.isspace():
            if current:
                yield current
                current = []
            prev = ch
            continue

        if prev is not None and not prev["text"].isspace():
            gap = ch["x0"] - prev["x1"]
            if gap > gap_threshold and current:
                # Big horizontal jump => new word
                yield current
                current = []

        current.append(ch)
        prev = ch

    if current:
        yield current


def flush_word(
    word_chars: List[Dict[str, Any]],
    page_number: int,
    word_index: int,
) -> Dict[str, Any]:
    text = "".join(ch["text"] for ch in word_chars)
    if not text.strip():
        return {}

    fonts = sorted({ch.get("fontname", "") for ch in word_chars if ch.get("fontname")})
    sizes = sorted({float(ch.get("size", 0.0)) for ch in word_chars if ch.get("size")})
    non_stroking_colors = sorted(
        {
            tuple(ch.get("non_stroking_color"))
            for ch in word_chars
            if isinstance(ch.get("non_stroking_color"), (list, tuple))
        }
    )
    stroking_colors = sorted(
        {
            tuple(ch.get("stroking_color"))
            for ch in word_chars
            if isinstance(ch.get("stroking_color"), (list, tuple))
        }
    )

    x0 = min(ch["x0"] for ch in word_chars)
    x1 = max(ch["x1"] for ch in word_chars)
    top = min(ch["top"] for ch in word_chars)
    bottom = max(ch["bottom"] for ch in word_chars)

    style_flags = infer_style_from_fontnames(fonts)
    if any(not ch.get("upright", True) for ch in word_chars):
        style_flags["is_italic"] = True

    return {
        "page": page_number,
        "word_index": word_index,
        "text": text,
        "fonts": fonts,
        "sizes": sizes,
        "is_bold": style_flags["is_bold"],
        "is_italic": style_flags["is_italic"],
        "non_stroking_colors": [list(c) for c in non_stroking_colors],
        "stroking_colors": [list(c) for c in stroking_colors],
        "bbox": {"x0": x0, "top": top, "x1": x1, "bottom": bottom},
    }


def inspect_pdf(pdf_bytes: bytes, max_pages: int | None = None):
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        num_pages = len(pdf.pages)
        if max_pages is not None:
            num_pages = min(num_pages, max_pages)

        for page_idx in range(num_pages):
            page = pdf.pages[page_idx]
            page_number = page_idx + 1

            chars = page.chars
            lines = group_chars_into_lines(chars)

            word_index = 0
            for line in lines:
                for word_chars in iter_words_from_line(line, gap_threshold=3.0):
                    word_index += 1
                    word_info = flush_word(word_chars, page_number, word_index)
                    if word_info:
                        print(json.dumps(word_info, ensure_ascii=False))


def main() -> None:
    print(f"[INFO] Downloading: {PDF_URL}", file=sys.stderr)
    pdf_bytes = download_pdf(PDF_URL)
    print("[INFO] Inspecting PDF...", file=sys.stderr)
    inspect_pdf(pdf_bytes, max_pages=None)


if __name__ == "__main__":
    main()
