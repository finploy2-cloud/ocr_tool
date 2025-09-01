import os
import json
import logging
import re
from datetime import datetime
from google import genai
from utils.config import GOOGLE_API_KEY

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GENAI_MODEL = os.getenv("GENAI_MODEL", "gemini-2.0-flash")
client = genai.Client(api_key=GOOGLE_API_KEY)

# Load gender names JSON
GENDER_NAMES_FILE = os.path.join(os.path.dirname(__file__), "gender_names.json")
with open(GENDER_NAMES_FILE, "r", encoding="utf-8") as f:
    GENDER_NAMES = json.load(f)

DEFAULT_NA = "#N/A"

# ───────────────────────────────
# Helpers
# ───────────────────────────────
def _response_to_text(response) -> str:
    if not response:
        return ""
    try:
        if hasattr(response, "output_text"):
            return response.output_text.strip()
        return str(response)
    except Exception:
        return str(response)


def _extract_json_from_text(text: str) -> dict:
    if not text:
        return {}
    text = re.sub(r"^```(?:json)?", "", text.strip())
    text = re.sub(r"```$", "", text.strip())
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except Exception:
                pass
    return {"raw_output": text}


def _generate(prompt: str) -> str:
    try:
        resp = client.models.generate_content(model=GENAI_MODEL, contents=prompt)
        return _response_to_text(resp)
    except Exception as e:
        logger.error(f"Gemini API error: {e}")
        return ""


# ───────────────────────────────
# Gender Detection
# ───────────────────────────────
def detect_gender(name: str = "", text: str = "", email: str = "") -> str:
    male_score = 0
    female_score = 0

    def check_name(n: str, weight: int):
        nonlocal male_score, female_score
        if n.lower() in [x.lower() for x in GENDER_NAMES.get("male", [])]:
            male_score += weight
        elif n.lower() in [x.lower() for x in GENDER_NAMES.get("female", [])]:
            female_score += weight

    if name:
        for part in name.split():
            check_name(part, 5)

    if email and "@" in email:
        email_name = email.split("@")[0].split(".")[0]
        check_name(email_name, 2)

    text_lower = text.lower()
    if re.search(r"\b(he|him|his)\b", text_lower):
        male_score += 3
    if re.search(r"\b(she|her|hers)\b", text_lower):
        female_score += 3

    if male_score > female_score:
        return "Male"
    elif female_score > male_score:
        return "Female"
    return DEFAULT_NA


# ───────────────────────────────
# BFSI SCORE NORMALIZATION
# ───────────────────────────────
def normalize_bfsi_score(score):
    try:
        score = float(score)
        if score > 10:
            score = score / 10
        return round(score, 1)
    except (ValueError, TypeError):
        return None


# ───────────────────────────────
# Extract Full CV Fields
# ───────────────────────────────
def extract_full_cv_fields(text: str) -> dict:
    """
    Returns a dict with all keys expected by import_cvs.py, including past companies and FinScore.
    """
    prompt = f"""
Extract resume information as JSON. Leave missing fields blank.
Return keys:
{{
"cv_username": "",
"cv_mobile_number": "",
"cv_email": "",
"cv_dateofbirth": "",
"cv_graduationyear": "",
"cv_current_company": "",
"cv_currentdesignation": "",
"cv_totalexperienceyears": "",
"cv_location_area": "",
"cv_location_city": "",
"cv_location_state": "",
"cv_current_location": "",
"cv_finscore": "",
"cv_pastcompanies": [],
"cv_pastdesignations": [],
"cv_pastduration": []
}}

Resume Text:
{text}
"""
    raw = _generate(prompt)
    result = _extract_json_from_text(raw)

    # Normalize BFSI/FinScore
    result["cv_finscore"] = str(normalize_bfsi_score(result.get("cv_finscore")) or "")

    # Gender detection using name, text, email
    username = result.get("cv_username", DEFAULT_NA)
    email = result.get("cv_email", DEFAULT_NA)
    result["cv_gender"] = detect_gender(username, text, email)

    # Age fallback
    try:
        if result.get("cv_dateofbirth"):
            dob = datetime.strptime(result["cv_dateofbirth"], "%Y-%m-%d")
            result["cv_age"] = str(datetime.now().year - dob.year)
        elif result.get("cv_graduationyear"):
            grad_year = int(result["cv_graduationyear"])
            result["cv_age"] = str(datetime.now().year - grad_year + 22)
        else:
            result["cv_age"] = DEFAULT_NA
    except:
        result["cv_age"] = DEFAULT_NA

    # Ensure location fields exist
    for key in ["cv_location_area", "cv_location_city", "cv_location_state", "cv_current_location"]:
        if key not in result or not result[key]:
            result[key] = DEFAULT_NA

    # Ensure past company fields are lists even if missing
    # Convert lists to comma-separated strings safely (skip None, convert items to str)
    for key in ["cv_pastcompanies", "cv_pastdesignations", "cv_pastduration"]:
      if isinstance(result[key], list):
        cleaned = [str(v) for v in result[key] if v is not None]  # skip None
        result[key] = ", ".join(cleaned) if cleaned else DEFAULT_NA


    # Convert lists to comma-separated strings (optional, can keep as list if preferred)
    for key in ["cv_pastcompanies", "cv_pastdesignations", "cv_pastduration"]:
        if isinstance(result[key], list):
            result[key] = ", ".join(result[key]) if result[key] else DEFAULT_NA

    return result


