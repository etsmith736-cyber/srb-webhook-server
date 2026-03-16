"""
GHL → Google Sheets Webhook Receiver
=====================================
Receives appointment events from Go High Level workflows and writes
them to the "Sales Calls" tab in the SRB Master Spreadsheet.

Two event types:
  1. appointment_created  → add/update a row (dedup by email)
  2. appointment_status   → update the Showed column only

Authentication:
  - Google Sheets: service account credentials via GOOGLE_SERVICE_ACCOUNT_JSON env var
  - GHL API: token via GHL_TOKEN env var

All sensitive values are read from environment variables — nothing is hardcoded.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import requests as http_requests
import uvicorn

# Google Sheets API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ─── Configuration (from environment variables) ───────────────

SPREADSHEET_ID     = os.environ.get("SPREADSHEET_ID",     "143pCbA2rktBqI-t3EYUjZBiZNZv0i-WxuPobN9wKRW0")
GHL_TOKEN          = os.environ.get("GHL_TOKEN",          "")
GHL_LOCATION_ID    = os.environ.get("GHL_LOCATION_ID",    "n4rqgABEMiHBL5Ui84JV")
GHL_BASE_URL       = os.environ.get("GHL_BASE_URL",       "https://services.leadconnectorhq.com")
PORT               = int(os.environ.get("PORT",           "8000"))

# Google Service Account JSON — stored as a single-line JSON string in the env var
GOOGLE_SA_JSON     = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")

# ─── GHL custom field IDs (used only in API fallback path) ────

CF_UTM_CALL   = "PIP4Uqb6byKTtlkCdNru"
CF_UTM_STAGE  = "pbbiyB60QSfnuBZpop1f"
CF_UTM_SOURCE = "hQrjQsnMLfIMD8eLTtAY"

# ─── User ID → Name mapping ───────────────────────────────────

USER_MAP = {
    "nNREQ8LGK7my0DEGCenm": "Claudia Tomczyk",
    "YZfd4b5zbbQhpmTTBhPx": "Ethan Smith",
    "mcfag3Em0f7ZIIT0c98q": "Hello SRB",
    "ORFU36VD6SYSwafqzE4N": "Kelsey Horne",
    "mqwuyuRbWIp7A64l7twV": "Simone Lea",
    "Yio8In2ThZTJAAtagX3d": "Sofia Bernardi",
    "UUbR4IxnbflYaWe3jP9Y": "Swetashree Badu",
    "SUahnITFo9jHoijb8ReT": "Vada Rey-Matias",
}

# ─── Ad source keywords ───────────────────────────────────────

AD_SOURCES = {"META", "FACEBOOK", "INSTAGRAM", "GOOGLE", "TIKTOK", "YOUTUBE", "LINKEDIN"}

# ─── Status mapping for Showed column ────────────────────────

STATUS_MAP = {
    "showed":    "Showed",
    "no_show":   "No-Show",
    "noshow":    "No-Show",
    "no-show":   "No-Show",
    "cancelled": "Cancelled",
    "canceled":  "Cancelled",
}

# ─── Timezone ─────────────────────────────────────────────────

AEST = timezone(timedelta(hours=10))

# ─── Column indices (0-based) ─────────────────────────────────

COL = {
    "Date Booked":              0,
    "Date of Call":             1,
    "First Name":               2,
    "Last Name":                3,
    "Email":                    4,
    "Phone":                    5,
    "Showed":                   6,
    "Closed":                   7,
    "Cash Collected (AUD)":     8,
    "Number of Payments":       9,
    "Contracted Revenue (AUD)": 10,
    "Lead Source":              11,
    "Call Source":              12,
    "Webinar ID":               13,
    "Stage":                    14,
    "Sales Rep":                15,
    "Notes":                    16,
}

# ─── Logging ──────────────────────────────────────────────────

log_handlers = [logging.StreamHandler(sys.stdout)]
# Only add file handler if we have a writable path (not on Railway)
log_file = os.environ.get("LOG_FILE", "")
if log_file:
    log_handlers.append(logging.FileHandler(log_file))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=log_handlers,
)
logger = logging.getLogger("webhook")

# ─── Google Sheets Service ────────────────────────────────────

_sheets_service = None

def get_sheets_service():
    """Build and cache the Google Sheets API service using service account credentials."""
    global _sheets_service
    if _sheets_service is not None:
        return _sheets_service

    if not GOOGLE_SA_JSON:
        logger.error("GOOGLE_SERVICE_ACCOUNT_JSON env var is not set — cannot access Google Sheets")
        return None

    try:
        sa_info = json.loads(GOOGLE_SA_JSON)
        creds = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        _sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        logger.info("Google Sheets service initialised successfully")
        return _sheets_service
    except Exception as e:
        logger.error(f"Failed to initialise Google Sheets service: {e}")
        return None

# ─── FastAPI App ──────────────────────────────────────────────

app = FastAPI(title="GHL Webhook Receiver")

# ─── Payload Extraction Helpers ───────────────────────────────

def extract_field(body: dict, *keys: str, default: str = "") -> str:
    """
    Try multiple key names across the top-level body, a 'customData'
    sub-object, and a nested 'contact' sub-object. Returns the first
    non-empty string found, or `default`.
    """
    custom_data = body.get("customData", {}) or {}
    contact_obj = body.get("contact", {}) or {}

    for key in keys:
        for source in (body, custom_data, contact_obj):
            val = source.get(key)
            if val is not None and str(val).strip():
                return str(val).strip()
    return default


def extract_tags(body: dict) -> list[str]:
    """Extract contact tags from any location in the payload."""
    for source in (body, body.get("customData", {}) or {}, body.get("contact", {}) or {}):
        tags = source.get("tags")
        if tags and isinstance(tags, list):
            return [str(t).lower() for t in tags]
        # GHL sometimes sends tags as a comma-separated string
        if tags and isinstance(tags, str):
            return [t.strip().lower() for t in tags.split(",") if t.strip()]
    return []

# ─── Business Logic ───────────────────────────────────────────

def determine_lead_source(utm_source: str, tags: list[str]) -> str:
    if utm_source and utm_source.upper() in AD_SOURCES:
        return "Ad"
    for tag in tags:
        if "ad lead" in tag:
            return "Ad"
    return "Needs Review"


def determine_call_source(utm_call: str) -> str:
    return "Webinar" if utm_call else "Misc"

# ─── GHL API Helpers (fallback only) ──────────────────────────

def ghl_get_contact(contact_id: str) -> dict:
    """Fetch full contact details from GHL (used only when payload fields are absent)."""
    if not GHL_TOKEN:
        logger.warning("GHL_TOKEN not set — cannot perform API fallback lookup")
        return {}
    try:
        r = http_requests.get(
            f"{GHL_BASE_URL}/contacts/{contact_id}",
            headers={
                "Authorization": f"Bearer {GHL_TOKEN}",
                "Version": "2021-07-28",
                "Accept": "application/json",
            },
            timeout=10,
        )
        r.raise_for_status()
        return r.json().get("contact", {})
    except Exception as e:
        logger.error(f"Failed to fetch contact {contact_id}: {e}")
        return {}


def get_cf_value(contact: dict, field_id: str) -> str:
    for cf in contact.get("customFields", []):
        if cf.get("id") == field_id:
            return str(cf.get("value", "")).strip()
    return ""

# ─── Google Sheets Helpers ────────────────────────────────────

def sheets_read_all() -> list[list[str]]:
    """Read all data rows from the Sales Calls tab."""
    service = get_sheets_service()
    if not service:
        return []
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range="'Sales Calls'!A:Q",
        ).execute()
        return result.get("values", [])
    except HttpError as e:
        logger.error(f"Sheets read error: {e}")
        return []
    except Exception as e:
        logger.error(f"Failed to read sheet: {e}")
        return []


def find_row_by_email(email: str) -> Optional[int]:
    """Return 1-based row number for an email match, or None."""
    rows = sheets_read_all()
    email_lower = email.strip().lower()
    for i, row in enumerate(rows):
        if i == 0:
            continue  # skip header
        if len(row) > COL["Email"] and row[COL["Email"]].strip().lower() == email_lower:
            return i + 1
    return None


def sheets_append_row(values: list[str]):
    """Append a new row to the Sales Calls tab."""
    service = get_sheets_service()
    if not service:
        logger.error("Cannot append row — Sheets service unavailable")
        return
    try:
        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range="'Sales Calls'!A:Q",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [values]},
        ).execute()
        logger.info("Row appended successfully")
    except HttpError as e:
        logger.error(f"Sheets append error: {e}")
    except Exception as e:
        logger.error(f"Failed to append row: {e}")


def sheets_update_row(row_number: int, values: list[str]):
    """Overwrite an existing row (1-based) in the Sales Calls tab."""
    service = get_sheets_service()
    if not service:
        logger.error("Cannot update row — Sheets service unavailable")
        return
    try:
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'Sales Calls'!A{row_number}:Q{row_number}",
            valueInputOption="USER_ENTERED",
            body={"values": [values]},
        ).execute()
        logger.info(f"Row {row_number} updated successfully")
    except HttpError as e:
        logger.error(f"Sheets update error: {e}")
    except Exception as e:
        logger.error(f"Failed to update row {row_number}: {e}")


def sheets_update_cell(row_number: int, col_letter: str, value: str):
    """Update a single cell in the Sales Calls tab."""
    service = get_sheets_service()
    if not service:
        logger.error("Cannot update cell — Sheets service unavailable")
        return
    try:
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'Sales Calls'!{col_letter}{row_number}",
            valueInputOption="USER_ENTERED",
            body={"values": [[value]]},
        ).execute()
        logger.info(f"Cell {col_letter}{row_number} updated to '{value}'")
    except HttpError as e:
        logger.error(f"Sheets cell update error: {e}")
    except Exception as e:
        logger.error(f"Failed to update cell {col_letter}{row_number}: {e}")

# ─── Event Handlers ───────────────────────────────────────────

def handle_appointment_created(body: dict):
    """Handle a new appointment booking — add or update a row."""

    first_name = extract_field(body, "first_name", "firstName")
    last_name  = extract_field(body, "last_name",  "lastName")
    email      = extract_field(body, "email")
    phone      = extract_field(body, "phone")
    utm_call   = extract_field(body, "utm_call")
    utm_stage  = extract_field(body, "utm_stage")
    utm_source = extract_field(body, "utm_source")
    tags       = extract_tags(body)

    start_time_str = extract_field(
        body, "appointment_start", "startTime", "start_time",
        "appointmentStartTime",
    )
    # Also check inside nested calendar object (GHL standard payload)
    calendar_obj = body.get("calendar", {}) or {}
    if not start_time_str:
        start_time_str = str(calendar_obj.get("startTime", ""))

    assigned_user = extract_field(
        body, "assigned_user", "assignedUserId", "assigned_user_id",
        "assignedUser", "calendarOwnerId",
    )

    contact_id = extract_field(body, "contact_id", "contactId", "contact.id")

    # Fallback to GHL API if email is missing
    if not email and contact_id:
        logger.info(f"Email not in payload — falling back to GHL API for contact {contact_id}")
        contact = ghl_get_contact(contact_id)
        if contact:
            first_name  = first_name  or contact.get("firstName", "")
            last_name   = last_name   or contact.get("lastName",  "")
            email       = email       or contact.get("email",     "")
            phone       = phone       or contact.get("phone",     "")
            utm_call    = utm_call    or get_cf_value(contact, CF_UTM_CALL)
            utm_stage   = utm_stage   or get_cf_value(contact, CF_UTM_STAGE)
            utm_source  = utm_source  or get_cf_value(contact, CF_UTM_SOURCE)
            tags        = tags        or [t.lower() for t in contact.get("tags", [])]

    if not email:
        logger.warning("No email found in payload or GHL API — skipping")
        return

    now_aest    = datetime.now(AEST)
    date_booked = now_aest.strftime("%d/%m/%Y")

    date_of_call = ""
    if start_time_str:
        try:
            dt = datetime.fromisoformat(str(start_time_str).replace("Z", "+00:00"))
            date_of_call = dt.astimezone(AEST).strftime("%d/%m/%Y")
        except Exception:
            # If it's a human-readable string like "Wednesday, 18 March 2026 5:30 PM", keep as-is
            date_of_call = str(start_time_str)[:20] if start_time_str else ""

    lead_source = determine_lead_source(utm_source, tags)
    call_source = determine_call_source(utm_call)
    sales_rep   = USER_MAP.get(assigned_user, assigned_user)

    logger.info(
        f"Processing appointment_created: email={email} | lead_source={lead_source} | "
        f"call_source={call_source} | utm_call={utm_call} | utm_stage={utm_stage} | "
        f"sales_rep={sales_rep} | date_of_call={date_of_call}"
    )

    row = [
        date_booked,    # A: Date Booked
        date_of_call,   # B: Date of Call
        first_name,     # C: First Name
        last_name,      # D: Last Name
        email,          # E: Email
        phone,          # F: Phone
        "",             # G: Showed
        "",             # H: Closed
        "",             # I: Cash Collected (AUD)
        "",             # J: Number of Payments
        "",             # K: Contracted Revenue (AUD)
        lead_source,    # L: Lead Source
        call_source,    # M: Call Source
        utm_call,       # N: Webinar ID
        utm_stage,      # O: Stage
        sales_rep,      # P: Sales Rep
        "",             # Q: Notes
    ]

    existing_row = find_row_by_email(email)
    if existing_row:
        logger.info(f"Duplicate found at row {existing_row} for {email} — updating (reschedule)")
        all_rows = sheets_read_all()
        if existing_row - 1 < len(all_rows):
            old = all_rows[existing_row - 1]
            for preserve_col in [
                "Showed", "Closed", "Cash Collected (AUD)",
                "Number of Payments", "Contracted Revenue (AUD)", "Notes",
            ]:
                idx = COL[preserve_col]
                if idx < len(old) and old[idx].strip():
                    row[idx] = old[idx]
        sheets_update_row(existing_row, row)
    else:
        logger.info(f"New contact {email} — appending row")
        sheets_append_row(row)


def handle_appointment_status(body: dict):
    """Handle an appointment status update — update the Showed column only."""
    contact_id = extract_field(body, "contact_id", "contactId", "contact.id")
    status = extract_field(
        body, "status", "appointmentStatus", "appointment_status",
    ).lower()

    # Also check inside nested appointment/calendar objects
    appt = body.get("appointment", {}) or {}
    cal  = body.get("calendar", {}) or {}
    if not status:
        status = (
            appt.get("appointmentStatus") or
            appt.get("status") or
            cal.get("appoinmentStatus") or   # note: GHL typo "appoinment"
            cal.get("status") or
            ""
        ).lower()
    if not contact_id:
        contact_id = appt.get("contactId", "")

    showed_value = STATUS_MAP.get(status)
    if not showed_value:
        logger.info(f"Status '{status}' not in STATUS_MAP — ignoring")
        return

    email = extract_field(body, "email")
    if not email and contact_id:
        logger.info(f"Email not in payload — fetching from GHL API for contact {contact_id}")
        contact = ghl_get_contact(contact_id)
        email = contact.get("email", "").strip()

    if not email:
        logger.warning("No email found — cannot locate row to update")
        return

    existing_row = find_row_by_email(email)
    if not existing_row:
        logger.warning(f"No row found for {email} — cannot update Showed")
        return

    sheets_update_cell(existing_row, "G", showed_value)
    logger.info(f"Updated Showed to '{showed_value}' for {email} at row {existing_row}")

# ─── Webhook Endpoint ─────────────────────────────────────────

@app.post("/webhook")
async def webhook(request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}

    logger.info(f"Webhook received:\n{json.dumps(body, indent=2, default=str)[:6000]}")

    event_type = extract_field(body, "type", "event", "event_type").lower()

    if event_type in ("appointment_created", "appointment.created", "created", "booked"):
        handle_appointment_created(body)

    elif event_type in (
        "appointment_status", "appointment.status", "status_update",
        "status", "showed", "no_show", "noshow", "cancelled",
    ):
        handle_appointment_status(body)

    else:
        # Infer from payload content when no explicit type field
        status_val = extract_field(
            body, "status", "appointmentStatus", "appointment_status",
        ).lower()
        # Also check GHL calendar object for status
        cal_status = (body.get("calendar", {}) or {}).get("appoinmentStatus", "").lower()
        effective_status = status_val or cal_status

        if effective_status and effective_status in STATUS_MAP:
            handle_appointment_status(body)
        elif extract_field(body, "contact_id", "contactId") or extract_field(body, "email"):
            handle_appointment_created(body)
        else:
            logger.info("Could not determine event type from payload — logged above")

    return JSONResponse(content={"status": "ok"}, status_code=200)


@app.get("/health")
async def health():
    sa_configured = bool(GOOGLE_SA_JSON)
    ghl_configured = bool(GHL_TOKEN)
    return {
        "status": "healthy",
        "timestamp": datetime.now(AEST).isoformat(),
        "google_sheets_auth": "configured" if sa_configured else "MISSING — set GOOGLE_SERVICE_ACCOUNT_JSON",
        "ghl_token": "configured" if ghl_configured else "MISSING — set GHL_TOKEN",
    }


@app.get("/")
async def root():
    return {
        "service": "GHL Webhook Receiver",
        "version": "1.2.0",
        "webhook_endpoint": "POST /webhook",
        "health_endpoint": "GET /health",
    }

# ─── Main ─────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
