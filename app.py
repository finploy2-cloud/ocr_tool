# app.py
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from extractors.pdf_utils import extract_text_from_pdf_bytes, extract_text_from_docx_bytes, is_scanned_pdf_bytes
from extractors.layout import normalize_text
from extractors.entities import extract_emails, extract_phones, extract_name
from llm_utils import extract_information_from_text, extract_finscore_from_text
from db.crud import upsert_candidate_details

app = FastAPI()

@app.post("/upload_resume")
async def upload_resume(file: UploadFile = File(...), cv_source: str = Form(...), upload_to_db: bool = Form(False), mobile_number: str = Form(None)):
    raw = await file.read()
    if file.content_type == "application/pdf":
        text = extract_text_from_pdf_bytes(raw)
    elif file.content_type in ("application/vnd.openxmlformats-officedocument.wordprocessingml.document", "application/msword"):
        text = extract_text_from_docx_bytes(raw)
    else:
        raise HTTPException(400, "Only PDF or DOCX")

    text = normalize_text(text)

    # Local extraction (backup)
    emails = extract_emails(text)
    phones = extract_phones(text)
    name = extract_name(text)

    # LLM extraction (try; fallback to local)
    llm_info = {}
    try:
        llm_info = extract_information_from_text(text) or {}
    except Exception:
        llm_info = {}

    # Merge: prefer LLM if present, otherwise local
    details = {}
    # mapping: cv_username -> username etc. (see mapping below)
    mapping = {
        "cv_username": "username", "cv_mobile_number": "mobile_number", "cv_gender":"gender",
        "cv_current_company":"current_company", "cv_jobrole":"jobrole",
        "cv_location_city":"current_location", "cv_current_salary":"current_salary",
        "cv_products_text":"products", "cv_sub_products_text":"sub_products",
        "cv_location_code":"location_code","cv_age":"age"
    }
    # fill from LLM
    for k,v in mapping.items():
        val = llm_info.get(k)
        if val not in (None, ""):
            details[v] = val

    # fallback local
    if "mobile_number" not in details and phones:
        details["mobile_number"] = phones[0]
    if "username" not in details and name:
        details["username"] = name
    if "username" not in details and emails:
        details["username"] = emails[0].split("@")[0]

    # add other metadata
    details["resume"] = f"uploaded_{file.filename}"
    details["created"] = None  # DB default
    details["cv_summary"] = extract_finscore_from_text(text)

    if upload_to_db and details.get("mobile_number"):
        upsert_candidate_details(details, table_name="candidate_details", unique_key="mobile_number")

    return {"status":"ok", "extracted": details}
