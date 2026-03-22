"""
GHL → Google Sheets Webhook Receiver
=====================================
Receives appointment events from Go High Level workflows and writes
them to the "Sales Calls" tab in the SRB Master Spreadsheet.

Two event types:
  1. appointment_created  → add/update a row (dedup by email)
  2. appointment_status   → update the Showed column only

Google Sheets access is via the `gws` CLI (pre-authenticated).
GHL contact enrichment is via the GHL API v2.
"""

import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import requests as http_requests
import uvicorn

# ─── Configuration ────────────────────────────────────────────

SPREADSHEET_ID = "143pCbA2rktBqI-t3EYUjZBiZNZv0i-WxuPobN9wKRW0"
SHEET_TAB = "Sales Calls"

GHL_TOKEN = "pit-0fb92538-98d8-4398-ad6b-6714a566bdbd"
GHL_LOCATION_ID = "n4rqgABEMiHBL5Ui84JV"
GHL_BASE_URL = "https://services.leadconnectorhq.com"

# Custom field IDs (discovered from GHL API)
CF_UTM_CALL = "PIP4Uqb6byKTtlkCdNru"
CF_UTM_STAGE = "pbbiyB60QSfnuBZpop1f"
CF_UTM_SOURCE = "hQrjQsnMLfIMD8eLTtAY"

# Relevant calendar IDs (sales call calendars)
SALES_CALENDARS = {
    "fGWCul6EemzLTJATJ2s6",   # Roadmap Call
    "VTvpuQtlglY2zIDgrp6z",   # Roadmap Call (Extended)
    "4Bo2wnrhzScvPAMiwlyj",   # Roadmap Call - Claudia
    "VDs66QI1O5XGXvVtjArN",   # Roadmap Call - Sofia
    "Bqmtru7sLYjxHAVcZFri",   # Roadmap Call - SRC
}

# User ID → Name mapping
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

# Status mapping for Showed column
STATUS_MAP = {
    "showed": "Showed",
    "no_show": "No-Show",
    "noshow": "No-Show",
    "no-show": "No-Show",
    "cancelled": "Cancelled",
    "canceled": "Cancelled",
}

# AEST timezone (UTC+10)
AEST = timezone(timedelta(hours=10))

# Column indices in the sheet (0-based for internal use, 1-based for Sheets API)
COL = {
    "Date Added": 0,
    "Appointment Time": 1,
    "First Name": 2,
    "Last Name": 3,
    "Email": 4,
    "Phone": 5,
    "Showed": 6,
    "Closed": 7,
    "Cash Collected (AUD)": 8,
    "Number of Payments": 9,
    "Contracted Revenue (AUD)": 10,
    "Lead Source": 11,
    "Call Source": 12,
    "Webinar ID": 13,
    "Stage": 14,
    "Sales Rep": 15,
    "Notes": 16,
    "Date of Purchase": 17,
}

# ─── Logging ──────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/home/ubuntu/webhook-server/webhook.log"),
    ],
)
logger = logging.getLogger("webhook")

# ─── FastAPI App ──────────────────────────────────────────────

app = FastAPI(title="GHL Webhook Receiver")


# ─── GHL API Helpers ──────────────────────────────────────────

GHL_HEADERS = {
    "Authorization": f"Bearer {GHL_TOKEN}",
    "Version": "2021-07-28",
    "Accept": "application/json",
}


def ghl_get_contact(contact_id: str) -> dict:
    """Fetch full contact details from GHL."""
    try:
        r = http_requests.get(
            f"{GHL_BASE_URL}/contacts/{contact_id}",
            headers=GHL_HEADERS,
            timeout=10,
        )
        r.raise_for_status()
        return r.json().get("contact", {})
    except Exception as e:
        logger.error(f"Failed to fetch contact {contact_id}: {e}")
        return {}


def get_custom_field(contact: dict, field_id: str) -> str:
    """Extract a custom field value from a contact dict."""
    for cf in contact.get("customFields", []):
        if cf.get("id") == field_id:
            return str(cf.get("value", "")).strip()
    return ""


def determine_lead_source(contact: dict) -> str:
    """Determine Lead Source from contact data."""
    utm_source = get_custom_field(contact, CF_UTM_SOURCE).upper()
    tags = [t.lower() for t in contact.get("tags", [])]

    # Check utm_source for ad indicators
    ad_sources = {"META", "FACEBOOK", "INSTAGRAM", "GOOGLE", "TIKTOK", "YOUTUBE", "LINKEDIN"}
    if utm_source and utm_source in ad_sources:
        return "Ad"

    # Check tags for ad indicators
    ad_tag_keywords = ["ad", "paid", "meta", "facebook", "instagram", "google"]
    for tag in tags:
        for kw in ad_tag_keywords:
            if kw in tag:
                return "Ad"

    # Check if there's any utm_source at all (could be organic social)
    if utm_source:
        organic_sources = {"ORGANIC", "REFERRAL", "DIRECT", "EMAIL", "PODCAST"}
        if utm_source in organic_sources:
            return "Organic"
        # Unknown utm_source — flag for review
        return "Needs Review"

    # No utm_source at all
    if tags:
        return "Needs Review"

    return "Needs Review"


def determine_call_source(contact: dict) -> str:
    """Determine Call Source from utm_call custom field."""
    utm_call = get_custom_field(contact, CF_UTM_CALL)
    if utm_call:
        return "Webinar"
    return "Misc"


# ─── Google Sheets Helpers (via gws CLI) ──────────────────────

def sheets_read_all() -> list[list[str]]:
    """Read all data from the Sales Calls tab."""
    try:
        result = subprocess.run(
            [
                "gws", "sheets", "+read",
                "--spreadsheet", SPREADSHEET_ID,
                "--range", f"'{SHEET_TAB}'!A:Q",
            ],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode != 0:
            logger.error(f"gws read error: {result.stderr}")
            return []
        data = json.loads(result.stdout)
        return data.get("values", [])
    except Exception as e:
        logger.error(f"Failed to read sheet: {e}")
        return []


def find_row_by_email(email: str) -> Optional[int]:
    """Find the 1-based row number of a contact by email. Returns None if not found."""
    rows = sheets_read_all()
    email_lower = email.strip().lower()
    for i, row in enumerate(rows):
        if i == 0:
            continue  # skip header
        if len(row) > COL["Email"]:
            if row[COL["Email"]].strip().lower() == email_lower:
                return i + 1  # 1-based row number
    return None


def sheets_append_row(values: list[str]):
    """Append a new row to the Sales Calls tab."""
    try:
        payload = json.dumps({"values": [values]})
        result = subprocess.run(
            [
                "gws", "sheets", "spreadsheets", "values", "append",
                "--params", json.dumps({
                    "spreadsheetId": SPREADSHEET_ID,
                    "range": f"'{SHEET_TAB}'!A:Q",
                    "valueInputOption": "RAW",
                    "insertDataOption": "INSERT_ROWS",
                }),
                "--json", payload,
            ],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode != 0:
            logger.error(f"gws append error: {result.stderr}")
        else:
            logger.info(f"Row appended successfully")
    except Exception as e:
        logger.error(f"Failed to append row: {e}")


def sheets_update_row(row_number: int, values: list[str]):
    """Update an existing row (1-based) in the Sales Calls tab."""
    try:
        payload = json.dumps({"values": [values]})
        result = subprocess.run(
            [
                "gws", "sheets", "spreadsheets", "values", "update",
                "--params", json.dumps({
                    "spreadsheetId": SPREADSHEET_ID,
                    "range": f"'{SHEET_TAB}'!A{row_number}:Q{row_number}",
                    "valueInputOption": "RAW",
                }),
                "--json", payload,
            ],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode != 0:
            logger.error(f"gws update error: {result.stderr}")
        else:
            logger.info(f"Row {row_number} updated successfully")
    except Exception as e:
        logger.error(f"Failed to update row {row_number}: {e}")


def sheets_update_cell(row_number: int, col_letter: str, value: str):
    """Update a single cell in the Sales Calls tab."""
    try:
        payload = json.dumps({"values": [[value]]})
        result = subprocess.run(
            [
                "gws", "sheets", "spreadsheets", "values", "update",
                "--params", json.dumps({
                    "spreadsheetId": SPREADSHEET_ID,
                    "range": f"'{SHEET_TAB}'!{col_letter}{row_number}",
                    "valueInputOption": "RAW",
                }),
                "--json", payload,
            ],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode != 0:
            logger.error(f"gws cell update error: {result.stderr}")
        else:
            logger.info(f"Cell {col_letter}{row_number} updated to '{value}'")
    except Exception as e:
        logger.error(f"Failed to update cell {col_letter}{row_number}: {e}")


# ─── Event Handlers ───────────────────────────────────────────

def handle_appointment_created(payload: dict):
    """Handle a new appointment booking — add or update a row."""
    contact_id = payload.get("contact_id") or payload.get("contactId") or ""
    appointment = payload.get("appointment", payload)

    if not contact_id:
        # Try to extract from nested structures
        contact_id = appointment.get("contactId", "")

    if not contact_id:
        logger.warning("No contact_id in payload, skipping")
        return

    # Fetch full contact from GHL API
    contact = ghl_get_contact(contact_id)
    if not contact:
        logger.warning(f"Could not fetch contact {contact_id}")
        return

    email = contact.get("email", "").strip()
    if not email:
        logger.warning(f"Contact {contact_id} has no email, skipping")
        return

    # Extract data
    first_name = contact.get("firstName", "") or ""
    last_name = contact.get("lastName", "") or ""
    phone = contact.get("phone", "") or ""

    # Date Added = now (AEST)
    now_aest = datetime.now(AEST)
    date_booked = now_aest.strftime("%Y-%m-%d %H:%M AEST")

    # Date of Call = appointment start time
    # Also check calendar nested object (GHL workflow sends startTime inside calendar{})
    calendar_data = payload.get("calendar", {})
    start_time_str = (
        calendar_data.get("startTime")
        or appointment.get("startTime")
        or appointment.get("start_time")
        or payload.get("startTime")
        or payload.get("start_time")
        or ""
    )
    date_of_call = ""
    if start_time_str:
        # GHL sends times in the calendar's timezone (AEST/Brisbane)
        # Just clean the ISO string directly: "2026-03-24T16:00:00" -> "2026-03-24 16:00 AEST"
        try:
            clean = start_time_str.replace("Z", "").split("+")[0]  # strip timezone suffix
            parts = clean.split("T")
            date_part = parts[0]  # "2026-03-24"
            time_part = parts[1][:5] if len(parts) > 1 else ""  # "16:00"
            date_of_call = f"{date_part} {time_part} AEST" if time_part else date_part
        except Exception:
            date_of_call = start_time_str

    # Lead Source
    lead_source = determine_lead_source(contact)

    # Call Source & Webinar ID
    utm_call = get_custom_field(contact, CF_UTM_CALL)
    call_source = "Webinar" if utm_call else "Misc"
    webinar_id = utm_call

    # Stage
    utm_stage = get_custom_field(contact, CF_UTM_STAGE)

    # Sales Rep — try multiple sources
    sales_rep = ""

    # 1. Try calendar title: "Roadmap Call: Contact Name and Sales Rep Name"
    calendar_obj = payload.get("calendar", {})
    cal_title = calendar_obj.get("title", "")
    if cal_title and " and " in cal_title:
        # Title format: "Roadmap Call: Contact Name and Sales Rep Name"
        after_and = cal_title.split(" and ")[-1].strip()
        if after_and:
            sales_rep = after_and

    # 2. Fall back to assignedUserId / assigned_user lookup
    if not sales_rep:
        assigned_user_id = (
            payload.get("assigned_user")          # GHL workflow custom field
            or appointment.get("assignedUserId")
            or appointment.get("assigned_user_id")
            or payload.get("assignedUserId")
            or payload.get("assigned_user_id")
            or ""
        )
        sales_rep = USER_MAP.get(assigned_user_id, assigned_user_id)  # fall back to raw value if not in map

    # Build the row (17 columns: A through Q)
    row = [
        date_booked,          # A: Date Added
        date_of_call,         # B: Appointment Time
        first_name,           # C: First Name
        last_name,            # D: Last Name
        email,                # E: Email
        phone,                # F: Phone
        "",                   # G: Showed (blank)
        "",                   # H: Closed (blank)
        "",                   # I: Cash Collected (AUD) (blank)
        "",                   # J: Number of Payments (blank)
        "",                   # K: Contracted Revenue (AUD) (blank)
        lead_source,          # L: Lead Source
        call_source,          # M: Call Source
        webinar_id,           # N: Webinar ID
        utm_stage,            # O: Stage
        sales_rep,            # P: Sales Rep
        "",                   # Q: Notes (blank)
    ]

    # Deduplication: check if email already exists
    existing_row = find_row_by_email(email)
    if existing_row:
        logger.info(f"Duplicate found at row {existing_row} for {email} — updating")
        # Preserve existing Showed, Closed, Cash, Payments, Revenue, Notes
        existing_data = sheets_read_all()
        if existing_row - 1 < len(existing_data):
            old = existing_data[existing_row - 1]
            # Preserve columns G-K and Q if they have values
            for preserve_col in ["Showed", "Closed", "Cash Collected (AUD)",
                                 "Number of Payments", "Contracted Revenue (AUD)", "Notes"]:
                idx = COL[preserve_col]
                if idx < len(old) and old[idx].strip():
                    row[idx] = old[idx]
        sheets_update_row(existing_row, row)
    else:
        logger.info(f"New contact {email} — appending row")
        sheets_append_row(row)


def handle_appointment_status(payload: dict):
    """Handle an appointment status update — update Showed column only."""
    contact_id = payload.get("contact_id") or payload.get("contactId") or ""
    status = (
        payload.get("status")
        or payload.get("appointmentStatus")
        or payload.get("appointment_status")
        or ""
    ).lower().strip()

    # Also check nested appointment object
    appointment = payload.get("appointment", {})
    if not status:
        status = (appointment.get("appointmentStatus") or appointment.get("status") or "").lower().strip()
    if not contact_id:
        contact_id = appointment.get("contactId", "")

    if not contact_id or not status:
        logger.warning(f"Missing contact_id or status in status update payload")
        return

    showed_value = STATUS_MAP.get(status)
    if not showed_value:
        logger.info(f"Status '{status}' not mapped to Showed value, ignoring")
        return

    # Get contact email to find the row
    contact = ghl_get_contact(contact_id)
    email = contact.get("email", "").strip()
    if not email:
        logger.warning(f"Contact {contact_id} has no email, cannot find row")
        return

    existing_row = find_row_by_email(email)
    if not existing_row:
        logger.warning(f"No row found for {email} to update status")
        return

    # Update Showed column (G) always
    sheets_update_cell(existing_row, "G", showed_value)
    logger.info(f"Updated Showed to '{showed_value}' for {email} at row {existing_row}")
    # Also update Closed column (H) based on status
    if showed_value in ("Cancelled", "No-Show"):
        sheets_update_cell(existing_row, "H", showed_value)
        logger.info(f"Updated Closed to '{showed_value}' for {email} at row {existing_row}")
    elif showed_value == "Showed":
        sheets_update_cell(existing_row, "H", "Maybe")
        logger.info(f"Updated Closed to 'Maybe' for {email} at row {existing_row}")


# ─── Webhook Endpoint ────────────────────────────────────────

@app.post("/webhook")
async def webhook(request: Request):
    """Main webhook endpoint for GHL workflow events."""
    try:
        body = await request.json()
    except Exception:
        body = {}

    # Log the full payload
    logger.info(f"Webhook received: {json.dumps(body, indent=2, default=str)[:5000]}")

    # Determine event type from the payload
    # GHL workflows send custom webhook payloads — the event type is
    # determined by which workflow triggers it. We'll use a "type" field
    # that we instruct the user to include in the webhook body.
    event_type = (
        body.get("type")
        or body.get("event")
        or body.get("event_type")
        or ""
    ).lower().strip()

    if event_type in ("appointment_created", "appointment.created", "created", "booked"):
        handle_appointment_created(body)
    elif event_type in ("appointment_status", "appointment.status", "status_update",
                        "status", "showed", "no_show", "noshow", "cancelled"):
        handle_appointment_status(body)
    else:
        # If no explicit type, try to infer from payload content
        if body.get("appointmentStatus") or body.get("status"):
            status_val = (body.get("appointmentStatus") or body.get("status") or "").lower()
            if status_val in STATUS_MAP:
                handle_appointment_status(body)
            else:
                handle_appointment_created(body)
        elif body.get("contact_id") or body.get("contactId"):
            handle_appointment_created(body)
        else:
            logger.info(f"Unknown event type: '{event_type}', payload logged above")

    return JSONResponse(content={"status": "ok"}, status_code=200)


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.now(AEST).isoformat()}


@app.get("/")
async def root():
    """Root endpoint — confirms server is running."""
    return {
        "service": "GHL Webhook Receiver",
        "version": "1.0.0",
        "webhook_endpoint": "POST /webhook",
        "health_endpoint": "GET /health",
    }


# ─── Main ─────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
