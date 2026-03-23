"""
GHL + Stripe → Google Sheets Webhook Receiver
===============================================
Receives events from Go High Level workflows and Stripe, writing
results to the "Sales Calls" tab in the SRB Master Spreadsheet.

Event types handled:
  1. appointment_created        → add/update a row (dedup by email)
  2. appointment_status         → update Showed (G) and Closed (H) columns
  3. opportunity_stage_update   → on "Won (Closed)": mark H=Closed,
                                   look up Stripe for I/J/K, or
                                   highlight row yellow if no data
                                 → on "Lost": mark H=No
  4. pipeline_lost              → mark H=No

Stripe webhook (POST /stripe-webhook):
  5. payment_intent.succeeded   → update I/J/K or append Unmatched
  6. invoice.payment_succeeded  → update I/J/K or append Unmatched
  7. charge.succeeded           → update I/J/K or append Unmatched

Authentication:
  - Google Sheets: service account credentials via GOOGLE_SERVICE_ACCOUNT_JSON env var
  - GHL API: token via GHL_TOKEN env var
  - Stripe: STRIPE_API_KEY and STRIPE_WEBHOOK_SECRET env vars

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

SPREADSHEET_ID        = os.environ.get("SPREADSHEET_ID",        "143pCbA2rktBqI-t3EYUjZBiZNZv0i-WxuPobN9wKRW0")
GHL_TOKEN             = os.environ.get("GHL_TOKEN",             "pit-0fb92538-98d8-4398-ad6b-6714a566bdbd")
GHL_LOCATION_ID       = os.environ.get("GHL_LOCATION_ID",       "n4rqgABEMiHBL5Ui84JV")
GHL_BASE_URL          = os.environ.get("GHL_BASE_URL",          "https://services.leadconnectorhq.com")
PORT                  = int(os.environ.get("PORT",              "8000"))
STRIPE_API_KEY        = os.environ.get("STRIPE_API_KEY",        "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
GOOGLE_SA_JSON        = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")

import stripe
if STRIPE_API_KEY:
    stripe.api_key = STRIPE_API_KEY

# ─── GHL custom field IDs ─────────────────────────────────────

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
    "Date Added":               0,
    "Appointment Time":         1,
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
    "Date of Purchase":         17,
}

# ─── Logging ──────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
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

app = FastAPI(title="GHL + Stripe Webhook Receiver")

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
    """Read all data from the Sales Calls tab."""
    service = get_sheets_service()
    if not service:
        return []
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=SPREADSHEET_ID,
                range="'Sales Calls'!A:R",
            )
            .execute()
        )
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
            range="'Sales Calls'!A:R",
            valueInputOption="RAW",
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
            range=f"'Sales Calls'!A{row_number}:R{row_number}",
            valueInputOption="RAW",
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
            valueInputOption="RAW",
            body={"values": [[value]]},
        ).execute()
        logger.info(f"Cell {col_letter}{row_number} updated to '{value}'")
    except HttpError as e:
        logger.error(f"Sheets cell update error: {e}")
    except Exception as e:
        logger.error(f"Failed to update cell {col_letter}{row_number}: {e}")


def sheets_highlight_row(row_number: int, red: float, green: float, blue: float):
    """Highlight an entire row in the Sales Calls tab with the specified RGB color."""
    service = get_sheets_service()
    if not service:
        logger.error("Cannot highlight row — Sheets service unavailable")
        return
    try:
        sheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets = sheet_metadata.get("sheets", [])
        sheet_id = None
        for s in sheets:
            if s.get("properties", {}).get("title") == "Sales Calls":
                sheet_id = s.get("properties", {}).get("sheetId")
                break
        if sheet_id is None:
            logger.error("Could not find sheet ID for 'Sales Calls' tab")
            return
        row_index = row_number - 1
        request = {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row_index,
                    "endRowIndex": row_index + 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": red, "green": green, "blue": blue}
                    }
                },
                "fields": "userEnteredFormat.backgroundColor",
            }
        }
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [request]},
        ).execute()
        logger.info(f"Row {row_number} highlighted successfully")
    except Exception as e:
        logger.error(f"Failed to highlight row {row_number}: {e}")

# ─── Event Handlers ───────────────────────────────────────────

def handle_opportunity_won(body: dict):
    """Handle an opportunity stage change to 'Won (Closed)'."""
    email = extract_field(body, "email")
    if not email:
        contact_id = extract_field(body, "contact_id", "contactId")
        if contact_id:
            contact = ghl_get_contact(contact_id)
            email = contact.get("email", "").strip()
    if not email:
        logger.warning("No email found for opportunity won event — cannot locate row")
        return

    existing_row = find_row_by_email(email)
    if not existing_row:
        logger.warning(f"No row found for {email} — cannot update Closed status")
        return

    # 1. Mark column H as "Closed"
    sheets_update_cell(existing_row, "H", "Closed")
    logger.info(f"Updated Closed status for {email} at row {existing_row}")

    # 2. Look up Stripe data
    if not STRIPE_API_KEY:
        logger.warning("STRIPE_API_KEY not set — skipping Stripe lookup, highlighting row yellow")
        sheets_highlight_row(existing_row, 1.0, 1.0, 0.0)
        return

    try:
        customers = stripe.Customer.search(query=f"email:'{email}'", limit=1)
        if not customers.data:
            logger.info(f"No Stripe customer found for {email} — highlighting row yellow")
            sheets_highlight_row(existing_row, 1.0, 1.0, 0.0)
            return

        customer_id = customers.data[0].id

        # Include trialing subscriptions — ThriveCart uses trialing status
        CANCELLED_STATUSES = {"canceled", "incomplete_expired"}
        subs = stripe.Subscription.list(customer=customer_id, limit=5)
        best_sub = next(
            (s for s in subs.data if s.get("status") not in CANCELLED_STATUSES),
            None,
        )

        amount_aud = 0.0
        num_payments = 1
        contracted_revenue_aud = 0.0
        found_data = False
        is_primary = True

        if best_sub:
            sub = best_sub
            currency = sub.currency
            exchange_rate = get_exchange_rate(currency, "AUD")
            payments, total_rev, is_primary = get_stripe_subscription_details(sub.id)
            initial_amount = 0.0
            for item in sub.get("items", {}).get("data", []):
                price = item.get("price", {})
                quantity = item.get("quantity", 1)
                unit_amount = price.get("unit_amount", 0) / 100.0
                initial_amount += unit_amount * quantity
            amount_aud = initial_amount * exchange_rate
            num_payments = payments
            contracted_revenue_aud = total_rev * exchange_rate
            found_data = True
            logger.info(f"Found subscription (status={sub.get('status')}) for {email} (primary_strategy={is_primary})")
        else:
            is_primary = False
            charges = stripe.Charge.list(customer=customer_id, limit=10)
            for charge in charges.data:
                if charge.status == "succeeded" and charge.paid:
                    currency = charge.currency
                    exchange_rate = get_exchange_rate(currency, "AUD")
                    amount_aud = (charge.amount / 100.0) * exchange_rate
                    num_payments = 1
                    contracted_revenue_aud = amount_aud
                    found_data = True
                    logger.info(f"Found recent successful charge for {email} [fallback]")
                    break

        if found_data:
            sheets_update_cell(existing_row, "I", f"{amount_aud:.2f}")
            sheets_update_cell(existing_row, "J", str(num_payments))
            sheets_update_cell(existing_row, "K", f"{contracted_revenue_aud:.2f}")
            logger.info(f"Updated financial data for {email} from Stripe lookup")
            if not is_primary:
                logger.info(f"Fallback strategy used for {email} — highlighting row orange")
                sheets_highlight_row(existing_row, 1.0, 200 / 255, 100 / 255)
        else:
            logger.info(f"No active subscriptions or successful charges found for {email} — highlighting row yellow")
            sheets_highlight_row(existing_row, 1.0, 1.0, 0.0)

    except Exception as e:
        logger.error(f"Error during Stripe lookup for {email}: {e}")
        sheets_highlight_row(existing_row, 1.0, 1.0, 0.0)


def handle_pipeline_lost(body: dict):
    """Handle pipeline stage moved to Lost — update Closed column to No."""
    email = extract_field(body, "email")
    if not email:
        contact_id = extract_field(body, "contact_id", "contactId")
        if contact_id:
            contact = ghl_get_contact(contact_id)
            email = contact.get("email", "").strip()
    if not email:
        logger.warning("No email found for pipeline_lost event — cannot locate row")
        return

    existing_row = find_row_by_email(email)
    if not existing_row:
        logger.warning(f"No row found for {email} — cannot update pipeline stage")
        return

    sheets_update_cell(existing_row, "H", "No")
    logger.info(f"Updated Closed to 'No' for {email} at row {existing_row}")


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
        body, "appointment_start", "startTime", "start_time", "appointmentStartTime",
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
    date_booked = now_aest.strftime("%Y-%m-%d")

    date_of_call = ""
    if start_time_str:
        try:
            s = str(start_time_str).strip()
            # Try ISO format first (e.g. 2026-03-27T07:30:00+00:00 or 2026-03-27T07:30:00Z)
            clean = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(clean)
            date_of_call = dt.astimezone(AEST).strftime("%Y-%m-%d")
        except Exception:
            try:
                # Try human-readable GHL format: "Friday, 27 March 2026 5:30 PM"
                # Strip day-of-week prefix if present
                if ", " in s:
                    s = s.split(", ", 1)[1]
                # Try common human-readable patterns
                for fmt in ("%d %B %Y %I:%M %p", "%d %B %Y", "%B %d, %Y %I:%M %p", "%B %d, %Y"):
                    try:
                        dt = datetime.strptime(s, fmt)
                        date_of_call = dt.strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue
            except Exception:
                pass
        if not date_of_call:
            # Last resort: take first 10 chars if it looks like a date
            date_of_call = str(start_time_str)[:10]

    lead_source = determine_lead_source(utm_source, tags)
    call_source = determine_call_source(utm_call)

    # Sales Rep — try calendar title first, then assigned_user lookup
    sales_rep = ""
    cal_title = calendar_obj.get("title", "")
    if cal_title and " and " in cal_title:
        after_and = cal_title.split(" and ")[-1].strip()
        if after_and:
            sales_rep = after_and
    if not sales_rep:
        sales_rep = USER_MAP.get(assigned_user, assigned_user)

    logger.info(
        f"Processing appointment_created: email={email} | lead_source={lead_source} | "
        f"call_source={call_source} | utm_call={utm_call} | utm_stage={utm_stage} | "
        f"sales_rep={sales_rep} | date_of_call={date_of_call}"
    )

    row = [
        date_booked,    # A: Date Added
        date_of_call,   # B: Appointment Time
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
        "",             # R: Date of Purchase
    ]

    existing_row = find_row_by_email(email)
    if existing_row:
        logger.info(f"Duplicate found at row {existing_row} for {email} — updating (reschedule)")
        all_rows = sheets_read_all()
        if existing_row - 1 < len(all_rows):
            old = all_rows[existing_row - 1]
            for preserve_col in [
                "Showed", "Closed", "Cash Collected (AUD)",
                "Number of Payments", "Contracted Revenue (AUD)", "Notes", "Date of Purchase",
            ]:
                idx = COL[preserve_col]
                if idx < len(old) and old[idx].strip():
                    row[idx] = old[idx]
        sheets_update_row(existing_row, row)
    else:
        logger.info(f"New contact {email} — appending row")
        sheets_append_row(row)


def handle_appointment_status(body: dict):
    """Handle an appointment status update — update Showed (G) and Closed (H) columns."""
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
            cal.get("appoinmentStatus") or   # GHL typo "appoinment"
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

# ─── Stripe Helpers ─────────────────────────────────────────────

def get_exchange_rate(from_currency: str, to_currency: str = "AUD") -> float:
    """Fetch exchange rate using a free API. Fallback to 1.0 if it fails."""
    from_currency = from_currency.upper()
    to_currency = to_currency.upper()
    if from_currency == to_currency:
        return 1.0
    try:
        r = http_requests.get(f"https://open.er-api.com/v6/latest/{from_currency}", timeout=5)
        r.raise_for_status()
        data = r.json()
        rate = data.get("rates", {}).get(to_currency)
        if rate:
            return float(rate)
    except Exception as e:
        logger.error(f"Failed to fetch exchange rate for {from_currency} to {to_currency}: {e}")
    return 1.0


def get_stripe_subscription_details(subscription_id: str) -> tuple:
    """
    Fetch subscription details from Stripe.
    Returns (number_of_payments, total_contracted_revenue_in_original_currency, is_primary_strategy).
    is_primary_strategy is True only when Strategy 0 (tc_fixed_rebills) was used.

    Strategy priority order:
    0. ThriveCart metadata 'tc_fixed_rebills'  → is_primary_strategy=True
         total_payments = int(tc_fixed_rebills) + 1  (rebills after initial payment)
    1. SubscriptionSchedule phases (iterations sum)  → is_primary_strategy=False
    2. Generic metadata field 'total_payments'        → is_primary_strategy=False
    3. cancel_at vs billing_cycle_anchor estimate     → is_primary_strategy=False
    4. ThriveCart product name parsing                → is_primary_strategy=False
    5. Default: 1 payment at the Stripe per-cycle amount → is_primary_strategy=False
    """
    import re as _re
    if not STRIPE_API_KEY or not subscription_id:
        return 1, 0.0, False
    try:
        sub = stripe.Subscription.retrieve(subscription_id, expand=["schedule"])
        metadata = sub.get("metadata", {}) or {}

        per_cycle_amount = 0.0
        for item in sub.get("items", {}).get("data", []):
            price = item.get("price", {})
            quantity = item.get("quantity", 1)
            unit_amount = price.get("unit_amount", 0) / 100.0
            per_cycle_amount += unit_amount * quantity

        # --- Strategy 0: ThriveCart tc_fixed_rebills metadata (PRIMARY) ---
        tc_rebills_raw = metadata.get("tc_fixed_rebills")
        if tc_rebills_raw is not None:
            try:
                tc_rebills = int(tc_rebills_raw)
                num_payments = tc_rebills + 1
                total_amount = per_cycle_amount * num_payments
                logger.info(
                    f"Subscription {subscription_id}: {num_payments} payments "
                    f"via tc_fixed_rebills={tc_rebills} [primary strategy]"
                )
                return num_payments, total_amount, True
            except (ValueError, TypeError):
                logger.warning(
                    f"Subscription {subscription_id}: could not parse "
                    f"tc_fixed_rebills='{tc_rebills_raw}'"
                )

        # --- Strategy 1: SubscriptionSchedule phases ---
        schedule = sub.get("schedule")
        if schedule and isinstance(schedule, dict):
            phases = schedule.get("phases", [])
            if phases:
                total_iterations = sum(p.get("iterations", 1) for p in phases)
                if total_iterations > 1:
                    total_amount = per_cycle_amount * total_iterations
                    logger.info(
                        f"Subscription {subscription_id}: {total_iterations} payments via schedule [fallback]"
                    )
                    return total_iterations, total_amount, False

        # --- Strategy 2: generic metadata['total_payments'] ---
        meta_total = metadata.get("total_payments")
        if meta_total:
            try:
                num_payments = int(meta_total)
                total_amount = per_cycle_amount * num_payments
                logger.info(
                    f"Subscription {subscription_id}: {num_payments} payments via metadata [fallback]"
                )
                return num_payments, total_amount, False
            except (ValueError, TypeError):
                pass

        # --- Strategy 3: cancel_at vs billing_cycle_anchor ---
        cancel_at = sub.get("cancel_at")
        billing_anchor = sub.get("billing_cycle_anchor")
        price_data = sub.get("items", {}).get("data", [{}])[0].get("price", {})
        interval = price_data.get("recurring", {}).get("interval", "month")
        interval_count = price_data.get("recurring", {}).get("interval_count", 1)
        if cancel_at and billing_anchor:
            from datetime import datetime as _dt
            start_dt = _dt.utcfromtimestamp(billing_anchor)
            end_dt   = _dt.utcfromtimestamp(cancel_at)
            delta_days = (end_dt - start_dt).days
            if interval == "day":
                cycles = delta_days / interval_count
            elif interval == "week":
                cycles = delta_days / (7 * interval_count)
            elif interval == "month":
                cycles = delta_days / (30.44 * interval_count)
            elif interval == "year":
                cycles = delta_days / (365.25 * interval_count)
            else:
                cycles = 1
            num_payments = max(1, round(cycles))
            total_amount = per_cycle_amount * num_payments
            logger.info(
                f"Subscription {subscription_id}: {num_payments} payments via cancel_at estimate [fallback]"
            )
            return num_payments, total_amount, False

        # --- Strategy 4: ThriveCart product name parsing ---
        product_name = ""
        items_data = sub.get("items", {}).get("data", [])
        if items_data:
            product_name = (
                items_data[0].get("price", {}).get("product", {})
                if isinstance(items_data[0].get("price", {}).get("product"), dict)
                else ""
            )
            if not product_name:
                product_name = (
                    items_data[0].get("price", {}).get("nickname", "") or
                    metadata.get("tc_product_name", "") or
                    metadata.get("product_name", "")
                )
        tc_name_match = _re.search(
            r"thrivecart-[\w-]+-(?P<cents>\d+)-(?:\w+)-aud(?P<rebills>\d+)rebills",
            str(product_name),
            _re.IGNORECASE,
        )
        if tc_name_match:
            try:
                name_amount = int(tc_name_match.group("cents")) / 100.0
                name_rebills = int(tc_name_match.group("rebills"))
                num_payments = name_rebills + 1
                effective_amount = name_amount if per_cycle_amount == 0.0 else per_cycle_amount
                total_amount = effective_amount * num_payments
                logger.info(
                    f"Subscription {subscription_id}: {num_payments} payments via "
                    f"ThriveCart product name (amount={effective_amount}) [fallback]"
                )
                return num_payments, total_amount, False
            except (ValueError, TypeError) as e:
                logger.warning(f"Subscription {subscription_id}: ThriveCart name parse failed: {e}")

        # --- Strategy 5: default — single payment ---
        logger.info(f"Subscription {subscription_id}: defaulting to 1 payment [fallback]")
        return 1, per_cycle_amount, False

    except Exception as e:
        logger.error(f"Failed to fetch Stripe subscription {subscription_id}: {e}")
        return 1, 0.0, False


def handle_stripe_payment(event: dict):
    """Process a successful Stripe payment."""
    data_object = event["data"]["object"]
    event_type  = event["type"]

    customer_email  = ""
    amount_paid     = 0.0
    currency        = "aud"
    payment_date    = datetime.now(AEST).strftime("%Y-%m-%d")
    stripe_payment_id = data_object.get("id", "")
    subscription_id = None

    if event_type == "payment_intent.succeeded":
        customer_email = data_object.get("receipt_email")
        amount_paid    = data_object.get("amount_received", 0) / 100.0
        currency       = data_object.get("currency", "aud")
        if not customer_email and data_object.get("customer"):
            try:
                customer = stripe.Customer.retrieve(data_object["customer"])
                customer_email = customer.get("email")
            except Exception:
                pass
    elif event_type == "invoice.payment_succeeded":
        customer_email  = data_object.get("customer_email")
        amount_paid     = data_object.get("amount_paid", 0) / 100.0
        currency        = data_object.get("currency", "aud")
        subscription_id = data_object.get("subscription")
    elif event_type == "charge.succeeded":
        customer_email = (
            data_object.get("billing_details", {}).get("email") or
            data_object.get("receipt_email")
        )
        amount_paid = data_object.get("amount", 0) / 100.0
        currency    = data_object.get("currency", "aud")

    if not customer_email:
        logger.warning(f"Stripe event {event_type} missing customer email. ID: {stripe_payment_id}")
        return

    exchange_rate = get_exchange_rate(currency, "AUD")
    amount_aud    = amount_paid * exchange_rate

    num_payments           = 1
    contracted_revenue_aud = amount_aud
    is_primary             = True

    if subscription_id:
        payments, total_rev, is_primary = get_stripe_subscription_details(subscription_id)
        num_payments           = payments
        contracted_revenue_aud = total_rev * exchange_rate

    logger.info(
        f"Stripe Payment: {customer_email}, Amount: {amount_aud:.2f} AUD, "
        f"Payments: {num_payments}, Rev: {contracted_revenue_aud:.2f} AUD, "
        f"primary_strategy={is_primary}"
    )

    row_num = find_row_by_email(customer_email)
    if row_num:
        sheets_update_cell(row_num, "I", f"{amount_aud:.2f}")
        sheets_update_cell(row_num, "J", str(num_payments))
        sheets_update_cell(row_num, "K", f"{contracted_revenue_aud:.2f}")
        logger.info(f"Updated Stripe payment for {customer_email} at row {row_num}")
        if subscription_id and not is_primary:
            logger.info(f"Fallback strategy used for {customer_email} — highlighting row orange")
            sheets_highlight_row(row_num, 1.0, 200 / 255, 100 / 255)
    else:
        pass  # No matching row — unmatched payment handling to be built later

# ─── Webhook Endpoints ────────────────────────────────────────

@app.post("/stripe-webhook")
async def stripe_webhook(request: Request):
    payload    = await request.body()
    sig_header = request.headers.get("stripe-signature")

    if not STRIPE_WEBHOOK_SECRET:
        logger.error("STRIPE_WEBHOOK_SECRET is not set")
        return JSONResponse(content={"error": "Webhook secret not configured"}, status_code=500)

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except ValueError:
        logger.error("Invalid Stripe payload")
        return JSONResponse(content={"error": "Invalid payload"}, status_code=400)
    except stripe.error.SignatureVerificationError:
        logger.error("Invalid Stripe signature")
        return JSONResponse(content={"error": "Invalid signature"}, status_code=400)

    if event["type"] in ["payment_intent.succeeded", "invoice.payment_succeeded", "charge.succeeded"]:
        handle_stripe_payment(event)

    return JSONResponse(content={"status": "success"}, status_code=200)


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

    elif event_type in ("pipeline_lost", "opportunity_lost", "lost"):
        handle_pipeline_lost(body)

    elif event_type in ("opportunity_stage_update", "opportunity.stage_update", "pipeline_stage_update"):
        stage_name = extract_field(
            body, "pipleline_stage", "pipeline_stage", "stage_name", "opportunity_stage", "stageName"
        ).lower()
        if "won" in stage_name or "closed" in stage_name:
            handle_opportunity_won(body)
        elif "lost" in stage_name:
            handle_pipeline_lost(body)
        else:
            logger.info(f"Ignoring opportunity stage update: {stage_name}")

    else:
        # Infer from payload content when no explicit type field

        # --- Pipeline / opportunity detection (runs BEFORE appointment fallback) ---
        stage_name = extract_field(
            body,
            "pipleline_stage",   # GHL typo — primary field in live payloads
            "pipeline_stage",
            "stage_name",
            "opportunity_stage",
            "stageName",
        ).lower()

        is_pipeline_event = bool(
            stage_name
            or body.get("pipeline_name")
            or body.get("pipelineName")
            or body.get("opportunity_name")
            or body.get("opportunityName")
        )

        if is_pipeline_event:
            if "won" in stage_name or "closed" in stage_name:
                logger.info(f"Implicit pipeline Won/Closed detected: stage='{stage_name}'")
                handle_opportunity_won(body)
            elif "lost" in stage_name:
                logger.info(f"Implicit pipeline Lost detected: stage='{stage_name}'")
                handle_pipeline_lost(body)
            else:
                logger.info(f"Ignoring pipeline event with stage='{stage_name}' (not Won/Closed/Lost)")
            return JSONResponse(content={"status": "ok"}, status_code=200)

        # --- Appointment status / created fallback ---
        status_val = extract_field(
            body, "status", "appointmentStatus", "appointment_status",
        ).lower()
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
    sa_configured             = bool(GOOGLE_SA_JSON)
    ghl_configured            = bool(GHL_TOKEN)
    stripe_configured         = bool(STRIPE_API_KEY)
    stripe_webhook_configured = bool(STRIPE_WEBHOOK_SECRET)
    return {
        "status": "healthy",
        "timestamp": datetime.now(AEST).isoformat(),
        "google_sheets_auth":    "configured" if sa_configured             else "MISSING — set GOOGLE_SERVICE_ACCOUNT_JSON",
        "ghl_token":             "configured" if ghl_configured            else "MISSING — set GHL_TOKEN",
        "stripe_api_key":        "configured" if stripe_configured         else "MISSING — set STRIPE_API_KEY",
        "stripe_webhook_secret": "configured" if stripe_webhook_configured else "MISSING — set STRIPE_WEBHOOK_SECRET",
    }


@app.get("/")
async def root():
    return {
        "service": "GHL + Stripe Webhook Receiver",
        "version": "2.0.0",
        "ghl_webhook_endpoint":    "POST /webhook",
        "stripe_webhook_endpoint": "POST /stripe-webhook",
        "health_endpoint":         "GET /health",
    }

# ─── Main ─────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
