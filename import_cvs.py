import os
import re
import json
import logging
import uuid
from datetime import datetime
from typing import Dict, Set
from sqlalchemy import text
from extractors.pdf_utils import extract_text_from_pdf_bytes, extract_text_from_docx_bytes
from utils.db_utils import engine
from llm_utils import extract_full_cv_fields

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

INPUT_FOLDER = "input_cvs"
SUPPORTED_EXTENSIONS = [".pdf", ".docx"]
DEFAULT_NA = "#N/A"

# ─────────────────────────────────────────────
# Load Gender Names from JSON
# ─────────────────────────────────────────────
NAMES_FILE = os.path.join(os.path.dirname(__file__), "gender_names.json")
with open(NAMES_FILE, "r", encoding="utf-8") as f:
    NAME_DB = json.load(f)

MALE_NAMES = set(NAME_DB.get("male", []))
FEMALE_NAMES = set(NAME_DB.get("female", []))

# ─────────────────────────────────────────────
# All Columns
# ─────────────────────────────────────────────
ALL_COLUMNS = [
    "user_id", "username", "mobile_number", "email", "gender",
    "employed","current_company","current_designation","destination","work_experience","current_location","current_salary",
    "cv_username","cv_mobile_number","cv_gender","cv_employed","cv_current_company","cv_sales_experience",
    "cv_work_experience","cv_current_location","cv_current_salary","cv_jobrole","cv_companyname","cv_productscode",
    "cv_sub_productscode","cv_departmentscode","cv_sub_departmentscode","cv_productspecializationcode",
    "cv_depatmentcategorycode","cv_products_text","cv_sub_products_text","cv_specialization_text",
    "cv_departments_text","cv_sub_departments_text","cv_category_text","cv_location_code","cv_age","cv_location_area",
    "cv_location_city","cv_location_state","cv_alternatephone","cv_email","cv_dateofbirth","cv_preferredlocation",
    "cv_highestqualification","cv_specialization","cv_institutename","cv_graduationyear","cv_additionaldegrees",
    "cv_totalexperienceyears","cv_currentcompany","cv_currentdesignation","cv_currentctc","cv_expectedctc",
    "cv_noticeperiod","cv_lastworkingday","cv_pastcompanies","cv_pastdesignations","cv_pastduration",
    "cv_bfsisectorexperience","cv_productexpertise","cv_technicalskills","cv_regulatoryknowledge",
    "cv_domainkeywords","cv_teamhandlingexperienceyesno","cv_achievements","cv_revenuehandled",
    "cv_targetachievement","cv_certifications","cv_languagesknown","cv_linkedinurl","cv_cvscore","cv_jobfitkeywords",
    "cv_possibleroles","cv_relevantjdids","cv_isleadershiprole","cv_locationmatchscore","cv_resumecompletenesscore",
    "cv_source","cv_parsingstatus","cv_parsingtimestamp","cv_originalfilename","cv_summary","cv_pincode"
]

EMAIL_REGEX = re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')
PHONE_REGEX = re.compile(r'(?:\+91[-\s]?)?[6-9]\d{9}')
PINCODE_REGEX = re.compile(r'\b[1-9]\d{5}\b')

# ─────────────────────────────────────────────
# DB Helpers
# ─────────────────────────────────────────────
def get_existing_columns(table_name: str) -> Set[str]:
    with engine.begin() as conn:
        res = conn.execute(text(f"DESCRIBE {table_name}"))
        return {row[0] for row in res.fetchall()}

def find_location_id(area: str, city: str, state: str, free_text_location: str) -> str:
    with engine.begin() as conn:
        if city != DEFAULT_NA and state != DEFAULT_NA:
            q = text("""
                SELECT id FROM locations
                WHERE LOWER(city)=LOWER(:city) AND LOWER(state)=LOWER(:state)
                LIMIT 1
            """)
            row = conn.execute(q, {"city": city.strip(), "state": state.strip()}).fetchone()
            if row:
                return str(row[0])
        if area != DEFAULT_NA and city != DEFAULT_NA:
            q = text("""
                SELECT id FROM locations
                WHERE LOWER(area)=LOWER(:area) AND LOWER(city)=LOWER(:city)
                LIMIT 1
            """)
            row = conn.execute(q, {"area": area.strip(), "city": city.strip()}).fetchone()
            if row:
                return str(row[0])
        if free_text_location != DEFAULT_NA:
            q = text("""
                SELECT id FROM locations
                WHERE :loc LIKE CONCAT('%%', area, '%%')
                   OR :loc LIKE CONCAT('%%', city, '%%')
                   OR :loc LIKE CONCAT('%%', state, '%%')
                LIMIT 1
            """)
            row = conn.execute(q, {"loc": free_text_location}).fetchone()
            if row:
                return str(row[0])
    return DEFAULT_NA

# ─────────────────────────────────────────────
# File Text Extraction
# ─────────────────────────────────────────────
def extract_text_from_file(path: str) -> str:
    with open(path, "rb") as f:
        data = f.read()
    if path.lower().endswith(".pdf"):
        return extract_text_from_pdf_bytes(data)
    if path.lower().endswith(".docx"):
        return extract_text_from_docx_bytes(data)
    return ""

def basic_regex_overrides(text: str) -> Dict[str, str]:
    email = EMAIL_REGEX.findall(text)
    phone = PHONE_REGEX.findall(text)
    pincode = PINCODE_REGEX.findall(text)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return {
        "cv_email": email[0].strip() if email else DEFAULT_NA,
        "cv_mobile_number": re.sub(r"\s|-", "", phone[0]) if phone else DEFAULT_NA,
        "cv_pincode": pincode[0] if pincode else DEFAULT_NA,
        "cv_summary": " ".join(lines[:4])[:500] if lines else DEFAULT_NA,
    }

# ─────────────────────────────────────────────
# Gender Detection (Weighted + Robust)
# ─────────────────────────────────────────────
def detect_gender(text: str, extracted_name: str = DEFAULT_NA, llm_gender: str = DEFAULT_NA, email: str = DEFAULT_NA) -> str:
    male_score, female_score = 0, 0
    text_lower = text.lower()

    # 1️⃣ Pronouns (small weight)
    if any(w in text_lower for w in [" he ", " him ", " his "]):
        male_score += 2
    if any(w in text_lower for w in [" she ", " her ", " hers "]):
        female_score += 2

    # 2️⃣ Titles/Prefix
    if re.search(r"\bMr\.?\b", text, re.IGNORECASE):
        male_score += 1
    if re.search(r"\b(Ms|Mrs|Miss)\.?\b", text, re.IGNORECASE):
        female_score += 1

    # 3️⃣ LLM extracted gender
    if llm_gender not in (None, DEFAULT_NA):
        if llm_gender.lower() == "male":
            male_score += 3
        elif llm_gender.lower() == "female":
            female_score += 3

    # 4️⃣ Name JSON matching (all parts)
    if extracted_name != DEFAULT_NA:
        for part in extracted_name.split():
            part_lower = part.lower()
            if part_lower in MALE_NAMES:
                male_score += 5
            if part_lower in FEMALE_NAMES:
                female_score += 5

    # 5️⃣ Email first name matching
    if email != DEFAULT_NA:
        first_email_name = email.split("@")[0].split(".")[0].lower()
        if first_email_name in MALE_NAMES:
            male_score += 2
        if first_email_name in FEMALE_NAMES:
            female_score += 2

    # Decide final gender
    if male_score == 0 and female_score == 0:
        return DEFAULT_NA
    return "Male" if male_score >= female_score else "Female"

# ─────────────────────────────────────────────
# Clean Mobile Number
# ─────────────────────────────────────────────
def clean_mobile_number(raw_number: str) -> str:
    if raw_number == DEFAULT_NA:
        return DEFAULT_NA
    digits = re.sub(r"\D", "", raw_number)
    if len(digits) > 10:
        digits = digits[-10:]
    return digits if len(digits) == 10 else DEFAULT_NA

# ─────────────────────────────────────────────
# CV Score
# ─────────────────────────────────────────────
def calculate_cv_score(ai_data: Dict[str, str]) -> str:
    """Calculate CV score based on FinScore if present."""
    finscore = ai_data.get("cv_cvscore", DEFAULT_NA)
    try:
        return str(round(float(finscore), 1)) if finscore != DEFAULT_NA else "0"
    except Exception:
        return "0"

# ─────────────────────────────────────────────
# Age Calculation
# ─────────────────────────────────────────────
def calculate_age(ai_data: Dict[str, str]) -> str:
    try:
        if ai_data.get("cv_dateofbirth", DEFAULT_NA) != DEFAULT_NA:
            dob = datetime.strptime(ai_data["cv_dateofbirth"], "%Y-%m-%d")
            return str(datetime.now().year - dob.year)
        elif ai_data.get("cv_graduationyear", DEFAULT_NA) != DEFAULT_NA:
            grad_year = int(ai_data["cv_graduationyear"])
            return str(datetime.now().year - grad_year + 22)
    except Exception:
        pass
    return DEFAULT_NA

# ─────────────────────────────────────────────
# Process Single File
# ─────────────────────────────────────────────
def process_file(file_path: str, existing_cols: Set[str]) -> Dict[str, str]:
    text_data = extract_text_from_file(file_path)
    if not text_data.strip():
        logger.warning(f"No text extracted from: {file_path}")
        return {}

    # Extract all CV fields including past companies and FinScore
    ai_data = extract_full_cv_fields(text_data)
    for key, value in ai_data.items():
     if isinstance(value, list):
        cleaned = [str(v) for v in value if v]  # remove None or empty
        ai_data[key] = ", ".join(cleaned) if cleaned else DEFAULT_NA
        
    # Overrides from regex (email, phone, pincode, summary)
    overrides = basic_regex_overrides(text_data)
    for k, v in overrides.items():
        if v != DEFAULT_NA:
            ai_data[k] = v

    # File metadata
    ai_data["cv_originalfilename"] = os.path.basename(file_path)
    ai_data["cv_location_code"] = find_location_id(
        ai_data.get("cv_location_area", DEFAULT_NA),
        ai_data.get("cv_location_city", DEFAULT_NA),
        ai_data.get("cv_location_state", DEFAULT_NA),
        ai_data.get("cv_current_location", DEFAULT_NA)
    )
    ai_data["cv_parsingtimestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ai_data["cv_source"] = "OCR_UPLOAD"
    ai_data["cv_parsingstatus"] = "PARSED"

   # UUID & username
    ai_data["user_id"] = str(uuid.uuid4())
    username = ai_data.get("cv_username") or DEFAULT_NA  # ensure not None

# Always define lines first
    lines = [ln.strip() for ln in text_data.splitlines() if ln.strip()]

    if username == DEFAULT_NA or not str(username).strip():
     if lines:
        username = lines[0]
    elif ai_data.get("cv_email") and ai_data.get("cv_email") != DEFAULT_NA:
        username = ai_data["cv_email"].split("@")[0]
    else:
        username = DEFAULT_NA

    ai_data["username"] = username

    # Mobile & email
    raw_mobile = ai_data.get("cv_mobile_number") or DEFAULT_NA  # ensure not None
    ai_data["mobile_number"] = clean_mobile_number(raw_mobile)
    ai_data["email"] = ai_data.get("cv_email") or DEFAULT_NA

    # Gender
    ai_data["gender"] = detect_gender(text_data, username, ai_data.get("cv_gender", DEFAULT_NA), ai_data.get("cv_email", DEFAULT_NA))

    # Age
    ai_data["cv_age"] = calculate_age(ai_data)

    # CV Score
    ai_data["cv_cvscore"] = calculate_cv_score(ai_data)

    # Ensure past company fields exist and are strings
    for key in ["cv_pastcompanies", "cv_pastdesignations", "cv_pastduration"]:
        if key not in ai_data or not ai_data[key]:
            ai_data[key] = DEFAULT_NA
        elif isinstance(ai_data[key], list):
            ai_data[key] = ", ".join(ai_data[key]) if ai_data[key] else DEFAULT_NA

    # Return only columns that exist in DB
    return {k: ai_data.get(k, DEFAULT_NA) for k in ALL_COLUMNS if k in existing_cols}

# ─────────────────────────────────────────────
# Insert into DB
# ─────────────────────────────────────────────
def insert_into_db(row_data: Dict[str, str], table: str = "candidate_details"):
    if not row_data:
        return
    cols = list(row_data.keys())
    placeholders = ", ".join([f":{c}" for c in cols])
    columns_sql = ", ".join(cols)
    query = text(f"INSERT INTO {table} ({columns_sql}) VALUES ({placeholders})")
    with engine.begin() as conn:
        conn.execute(query, row_data)

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
ERROR_FOLDER = "error_cvs"
os.makedirs(ERROR_FOLDER, exist_ok=True)
LOG_FILE = "processed_files.log"

def log_file_status(file_name: str, status: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {file_name} | {status}\n")

def main():
    files = [f for f in os.listdir(INPUT_FOLDER) if os.path.splitext(f)[1].lower() in SUPPORTED_EXTENSIONS]
    logger.info(f"Found {len(files)} files to process")

    existing_cols = get_existing_columns("candidate_details")
    if not existing_cols:
        logger.error("Could not read columns from candidate_details. Aborting.")
        return

    total_processed = 0
    total_errors = 0

    for file_name in files:
        path = os.path.join(INPUT_FOLDER, file_name)
        logger.info(f"Processing: {file_name}")

        try:
            data = process_file(path, existing_cols)
            if not data:  # File could not be processed
                logger.warning(f"Failed to process: {file_name}. Moving to error folder.")
                os.rename(path, os.path.join(ERROR_FOLDER, file_name))
                log_file_status(file_name, "ERROR")
                total_errors += 1
                continue

            insert_into_db(data)
            logger.info(f"Inserted record for: {file_name}")
            log_file_status(file_name, "PROCESSED")
            total_processed += 1

        except Exception as e:
            logger.error(f"Exception while processing {file_name}: {e}")
            os.rename(path, os.path.join(ERROR_FOLDER, file_name))
            log_file_status(file_name, f"ERROR | {str(e)}")
            total_errors += 1

    # ───────────── Summary ─────────────
    logger.info(f"Processing Completed. Total Files: {len(files)}, Successfully Processed: {total_processed}, Errors: {total_errors}")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | SUMMARY | Total: {len(files)} | Processed: {total_processed} | Errors: {total_errors}\n")

if __name__ == "__main__":
    main()
