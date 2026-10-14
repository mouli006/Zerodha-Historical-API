"""Text and tables of an NSE circular or order PDF (pdfplumber). Old circulars print bold text with
every character repeated ('NNNNaaaammmmeeee'); dedupe_chars() removes that. A scanned PDF has no text."""
import io
import zipfile

import pdfplumber


def unzip_pdf(data):
    """A few circulars are zip files; return the first PDF inside (or data unchanged)."""
    if data[:2] != b"PK":
        return data
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        names = [n for n in z.namelist() if n.lower().endswith(".pdf")]
        return z.read(names[0]) if names else b""


def extract(data):
    """{"text": str, "tables": [[[cell, ...], ...], ...], "pages": int}; text "" for a scan."""
    data = unzip_pdf(data)
    if not data:
        return {"text": "", "tables": [], "pages": 0}
    texts, tables = [], []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for page in pdf.pages:
            page = page.dedupe_chars()
            texts.append(page.extract_text() or "")
            tables.extend(page.extract_tables())
        pages = len(pdf.pages)
    return {"text": "\n".join(texts).strip(), "tables": tables, "pages": pages}
