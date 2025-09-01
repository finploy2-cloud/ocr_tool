# extractors/entities.py
import re
from typing import List, Optional
import phonenumbers
from dateparser import parse as date_parse
import spacy

nlp = spacy.load("en_core_web_sm")

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", re.I)
CTC_RE = re.compile(r"(?:CTC|Current\s+CTC|Salary)\s*[:\-]?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:LPA|Lacs|Lakhs?)", re.I)
NOTICE_RE = re.compile(r"(?:notice period|notice)\s*[:\-]?\s*(\d{1,2})\s*(days?|weeks?|months?)", re.I)

def extract_emails(text: str) -> List[str]:
    return [m.group(0).lower().strip(" ,;") for m in EMAIL_RE.finditer(text)]

def extract_phones(text: str, default_region="IN") -> List[str]:
    found = []
    for match in re.finditer(r"(?:\+?\d[\d\-\s\(\)]{7,}\d)", text):
        num = match.group(0)
        try:
            pn = phonenumbers.parse(num, default_region)
            if phonenumbers.is_valid_number(pn):
                found.append(phonenumbers.format_number(pn, phonenumbers.PhoneNumberFormat.E164))
        except Exception:
            continue
    return list(dict.fromkeys(found))

def extract_ctc(text: str) -> Optional[float]:
    m = CTC_RE.search(text)
    return float(m.group(1)) if m else None

def extract_notice_days(text: str) -> Optional[int]:
    m = NOTICE_RE.search(text)
    if not m:
        return None
    val = int(m.group(1))
    unit = m.group(2).lower()
    if "month" in unit:
        return val * 30
    if "week" in unit:
        return val * 7
    return val

def extract_name(text: str) -> Optional[str]:
    # take first 6 lines and run spaCy person extraction
    head = "\n".join(text.splitlines()[:6])
    doc = nlp(head)
    persons = [ent.text for ent in doc.ents if ent.label_ == "PERSON"]
    return persons[0] if persons else None
