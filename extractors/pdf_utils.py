# extractors/pdf_utils.py
import fitz
import io
import re
import logging
import numpy as np
from PIL import Image
import pytesseract
import cv2

from utils.config import TESSERACT_CMD

# Configure Tesseract path if provided
if TESSERACT_CMD:
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def is_scanned_pdf_bytes(file_bytes: bytes, min_chars: int = 200) -> bool:
    """Check if PDF is scanned by counting text characters."""
    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        text_chars = 0
        for page in doc:
            txt = page.get_text("text") or "" # type: ignore
            text_chars += len(re.sub(r"\s+", "", txt))
            if text_chars >= min_chars:
                return False  # enough text → not scanned
        return True
    except Exception as e:
        logger.warning(f"Scanned detection error: {e}", exc_info=True)
        return True


def preprocess_image_cv2(img_rgb: np.ndarray):
    """Convert to grayscale, threshold, and deskew."""
    gray = cv2.cvtColor(img_rgb, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (3, 3), 0)
    _, thr = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    # Deskew
    coords = np.column_stack(np.where(thr < 255))
    if coords.size == 0:
        return thr
    angle = cv2.minAreaRect(coords)[-1]
    if angle < -45:
        angle = -(90 + angle)
    else:
        angle = -angle
    (h, w) = thr.shape[:2]
    M = cv2.getRotationMatrix2D((w // 2, h // 2), angle, 1.0)
    rotated = cv2.warpAffine(thr, M, (w, h), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)
    return rotated


def ocr_pdf_bytes(file_bytes: bytes, dpi: int = 300, lang: str = "eng") -> str:
    """Perform OCR on scanned PDF pages."""
    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        texts = []
        zoom = dpi / 72  # convert DPI to zoom factor
        matrix = fitz.Matrix(zoom, zoom)
        for p in range(len(doc)):
            page = doc.load_page(p)
            pix = page.get_pixmap(matrix=matrix) # type: ignore
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            arr = np.array(img.convert("RGB"))
            proc = preprocess_image_cv2(arr)
            txt = pytesseract.image_to_string(proc, lang=lang, config="--oem 3 --psm 6")
            texts.append(txt)
        return "\n".join(texts)
    except Exception as e:
        logger.error("OCR failed", exc_info=True)
        return ""


def extract_text_from_pdf_bytes(file_bytes: bytes) -> str:
    """Extract text from PDF, fallback to OCR if text is sparse."""
    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        text = "\n".join(page.get_text("text") for page in doc) # type: ignore
        if len(re.sub(r"\s+", "", text)) >= 200:
            return text
        logger.info("PDF text is sparse → running OCR")
        return ocr_pdf_bytes(file_bytes)
    except Exception as e:
        logger.warning("Primary PDF extraction failed → OCR fallback", exc_info=True)
        return ocr_pdf_bytes(file_bytes)


def extract_text_from_docx_bytes(file_bytes: bytes) -> str:
    """Extract text from DOCX file."""
    try:
        from docx import Document
        stream = io.BytesIO(file_bytes)
        doc = Document(stream)
        return "\n".join([p.text for p in doc.paragraphs if p.text])
    except Exception as e:
        logger.error("DOCX extract failed", exc_info=True)
        return ""
