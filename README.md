# GHL → Google Sheets Webhook Receiver

A FastAPI server that receives appointment events from Go High Level (GHL) workflows and writes them to a Google Sheet.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/webhook` | Receives GHL appointment events |
| GET | `/health` | Health check — confirms auth config status |
| GET | `/` | Service info |

## Event Types

### `appointment_created`
Triggered when a new call is booked. Adds a new row to the **Sales Calls** tab (or updates an existing row if the email already exists — handles reschedules).

### `appointment_status`
Triggered when an appointment status changes. Updates the **Showed** column only for the matching email row.

## Environment Variables

All of the following must be set in Railway (or your hosting environment):

| Variable | Required | Description |
|----------|----------|-------------|
| `GOOGLE_SERVICE_ACCOUNT_JSON` | **Required** | Full JSON content of the Google service account key file (single line, no newlines) |
| `GHL_TOKEN` | **Required** | GHL Private Integration Token (`pit-...`) |
| `SPREADSHEET_ID` | Optional | Google Sheet ID (defaults to the SRB Master Sheet) |
| `GHL_LOCATION_ID` | Optional | GHL sub-account location ID (defaults to Rising Ventures) |
| `GHL_BASE_URL` | Optional | GHL API base URL (defaults to `https://services.leadconnectorhq.com`) |
| `PORT` | Set by Railway | Port to listen on (Railway sets this automatically) |
| `LOG_FILE` | Optional | Path to write log file (omit on Railway — logs go to stdout) |

## Google Sheets Setup

The server writes to the **Sales Calls** tab with these columns (A–Q):

`Date Booked | Date of Call | First Name | Last Name | Email | Phone | Showed | Closed | Cash Collected (AUD) | Number of Payments | Contracted Revenue (AUD) | Lead Source | Call Source | Webinar ID | Stage | Sales Rep | Notes`

### Service Account Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or use an existing one)
3. Enable the **Google Sheets API**
4. Create a **Service Account** and download the JSON key
5. **Share the Google Sheet** with the service account email (Editor access)
6. Copy the entire contents of the JSON key file and set it as the `GOOGLE_SERVICE_ACCOUNT_JSON` environment variable

## GHL Workflow Setup

### Workflow 1: Appointment Created

**Trigger:** Appointment Status → Status: Confirmed/Booked  
**Filter:** Calendars: Roadmap Call, Roadmap Call (Extended), Roadmap Call - Claudia, Roadmap Call - Sofia

**Webhook Action:**
```json
{
  "type": "appointment_created",
  "contact_id": "{{contact.id}}",
  "first_name": "{{contact.first_name}}",
  "last_name": "{{contact.last_name}}",
  "email": "{{contact.email}}",
  "phone": "{{contact.phone}}",
  "utm_call": "{{contact.utm_call}}",
  "utm_stage": "{{contact.utm_stage}}",
  "utm_source": "{{contact.utm_source}}",
  "appointment_start": "{{appointment.start_time}}",
  "assigned_user": "{{appointment.calendar_owner_id}}",
  "calendar_id": "{{appointment.calendar_id}}"
}
```

### Workflow 2: Appointment Status Updated

**Trigger:** Appointment Status → Status: Showed / No Show / Cancelled

**Webhook Action:**
```json
{
  "type": "appointment_status",
  "contact_id": "{{contact.id}}",
  "email": "{{contact.email}}",
  "status": "{{appointment.status}}"
}
```

## Local Development

```bash
pip install -r requirements.txt
export GOOGLE_SERVICE_ACCOUNT_JSON='{"type":"service_account",...}'
export GHL_TOKEN='pit-...'
python server.py
```

## Deployment on Railway

1. Connect this GitHub repo to a new Railway project
2. Set all required environment variables in Railway → Variables
3. Railway will auto-deploy on every push to `main`
4. Copy the Railway-provided public URL and update your GHL webhook actions
