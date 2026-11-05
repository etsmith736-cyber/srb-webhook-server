"""Stubbed tests: new GHL sub-account custom-field ids with old-id fallback."""
import sys, types, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
os.environ.setdefault("SPREADSHEET_ID", "test")
for name in ["uvicorn", "requests", "stripe"]:
    sys.modules.setdefault(name, types.ModuleType(name))
fastapi = types.ModuleType("fastapi")
class _App:
    def __getattr__(self, k):
        def deco(*a, **kw):
            def inner(f): return f
            return inner
        return deco
fastapi.FastAPI = lambda *a, **kw: _App()
fastapi.Request = object
sys.modules["fastapi"] = fastapi
resp = types.ModuleType("fastapi.responses"); resp.JSONResponse = object
sys.modules["fastapi.responses"] = resp
gapi = types.ModuleType("googleapiclient")
disc = types.ModuleType("googleapiclient.discovery"); disc.build = lambda *a, **kw: None
errs = types.ModuleType("googleapiclient.errors")
class HttpError(Exception): pass
errs.HttpError = HttpError
sys.modules.update({"googleapiclient": gapi, "googleapiclient.discovery": disc, "googleapiclient.errors": errs})
g = types.ModuleType("google"); go = types.ModuleType("google.oauth2")
sa = types.ModuleType("google.oauth2.service_account")
sa.Credentials = types.SimpleNamespace(from_service_account_info=lambda *a, **kw: None)
sys.modules.update({"google": g, "google.oauth2": go, "google.oauth2.service_account": sa})

import server  # noqa: E402

def c(**fields):
    return {"customFields": [{"id": k, "value": v} for k, v in fields.items()]}

NEW = {cf["label"]: cf["id"][0] for cf in server.CUSTOM_FIELD_CONFIG}
OLD = {cf["label"]: cf["id"][-1] for cf in server.CUSTOM_FIELD_CONFIG}

def test_new_location_ids_resolve():
    contact = c(**{NEW["Urgency"]: "Now - ready", NEW["Finances"]: "Yes - I have", NEW["SOB"]: "$5-10K/month"})
    out = {cf["label"]: server._extract_custom_field(contact, cf["id"]) for cf in server.CUSTOM_FIELD_CONFIG}
    assert out["Urgency"] == "Now - ready"
    assert out["Finances"] == "Yes - I have"
    assert out["SOB"] == "$5-10K/month"
    assert out["Goal"] == ""

def test_old_location_ids_still_resolve():
    contact = c(**{OLD["Goal"]: "$20-50K/month", OLD["Type"]: "Mindset/Life"})
    assert server._extract_custom_field(contact, dict((cf["label"], cf["id"]) for cf in server.CUSTOM_FIELD_CONFIG)["Goal"]) == "$20-50K/month"
    assert server._extract_custom_field(contact, server.CUSTOM_FIELD_CONFIG[0]["id"]) == "Mindset/Life"

def test_first_non_empty_wins_in_order():
    ids = server.CF_UTM_STAGE
    contact = c(**{ids[0]: "", ids[1]: "live", ids[2]: "old"})
    assert server.get_cf_value(contact, ids) == "live"
    contact = c(**{ids[2]: "old", ids[0]: "pre"})
    assert server.get_cf_value(contact, ids) == "pre"

def test_plain_string_id_still_works():
    assert server._extract_custom_field(c(abc="x"), "abc") == "x"
    assert server.get_cf_value(c(abc=" y "), "abc") == "y"
    assert server._extract_custom_field({}, ("a", "b")) == ""
    assert server._extract_custom_field(None, "a") == ""

def test_utm_campaign_fallback_ids_present():
    assert server.CF_UTM_CAMPAIGN[0] == "xyfCRADV3Gwgaos6x4pb"
    assert server.CF_UTM_CONTENT[0] == "snp7lPN3AfwteoQQe3Gq"
