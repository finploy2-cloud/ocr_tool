# extractors/layout.py
import re
import unicodedata

def normalize_text(text: str) -> str:
    if not text:
        return ""
    t = unicodedata.normalize("NFKC", text)
    t = re.sub(r"\r\n", "\n", t)
    t = re.sub(r"[ \t]+", " ", t)
    # Join hyphenated line breaks: devel-\nopment -> development
    t = re.sub(r"(\w)-\n(\w)", r"\1\2", t)
    # Remove excessive blank lines
    t = re.sub(r"\n{3,}", "\n\n", t)
    # Trim repeating header/footer heuristics: drop first/last 2 lines if repeated
    lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
    if len(lines) > 6:
        head, tail = lines[:2], lines[-2:]
        filtered = [ln for ln in lines if ln not in head and ln not in tail]
        t = "\n".join(filtered) if filtered else "\n".join(lines)
    return t.strip()
