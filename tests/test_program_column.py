"""Stubbed tests for the Program (column Z) auto-fill in server.py."""
import sys, types, os
from unittest import mock

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
resp = types.ModuleType("fastapi.responses")
resp.JSONResponse = object
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
from unittest import mock as _m

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("PASS  " if cond else "FAIL  ") + name + (f"  {detail}" if detail and not cond else ""))


class Sheet:
    def __init__(self, row_num=100, cash="", program="", deposit=""):
        self.row_num = row_num
        self.cells = {}
        if cash: self.cells[("I", row_num)] = cash
        if program: self.cells[("Z", row_num)] = program
        if deposit: self.cells[("AI", row_num)] = deposit
        row = [""] * 36
        row[8] = cash
        self.row = row
        self.patches = []

    def __enter__(self):
        S = server
        p = lambda n, v: self.patches.append(_m.patch.object(S, n, v))
        p("find_row_by_email", lambda e, **kw: self.row_num)
        p("sheets_read_all", lambda *a, **kw: [[""] * 36] * (self.row_num - 1) + [self.row])
        p("sheets_read_cell", lambda r, c, **kw: self.cells.get((c, r), ""))
        p("sheets_update_cell", lambda r, c, v, **kw: self.cells.__setitem__((c, r), v))
        p("sheets_update_range", lambda *a, **kw: None)
        p("sheets_batch_update_ranges", lambda *a, **kw: (True, ""))
        p("sheets_highlight_row", lambda *a, **kw: None)
        p("get_exchange_rate", lambda *a, **kw: 1.0)
        p("find_plan_subscription_for_charge", lambda *a, **kw: None)
        for x in self.patches: x.start()
        return self

    def __exit__(self, *a):
        for x in reversed(self.patches): x.stop()

    @property
    def z(self):
        return self.cells.get(("Z", self.row_num), "")


def charge(amount, desc):
    return {"type": "charge.succeeded", "data": {"object": {
        "id": "ch_1", "amount": int(amount * 100), "currency": "aud", "created": 1790000000,
        "description": desc, "customer": "cus_1", "billing_details": {"email": "a@b.com"}}}}


def run():
    f = server.program_from_product
    cases = {
        "Purchase of SCALE SCHOOL MASTERMIND (Abeer) via ThriveCart": "Scale School Tier 1",
        "Purchase of SCALE SCHOOL TIER 1 (ANA) via ThriveCart": "Scale School Tier 1",
        "SCALE SCHOOL TIER 1 - LOUISE": "Scale School Tier 1",
        "SCALE SCHOOL MASTERMIND (Split Pay)": "Scale School Tier 1",
        "SCALE SCHOOL TIER 2 Julie": "Scale School Tier 2",
        "Purchase of SCALE SCHOOL TIER 2 via ThriveCart": "Scale School Tier 2",
        "SCALE SCHOOL FOUNDATIONS": "Scale School Foundations",
        "SCALE SCHOOL BOARDROOM": "Scale School Boardroom",
        "Purchase of SCALE SCHOOL MASTERMIND DEPOSIT ($250 AUD) via ThriveCart": "",
        "SCALE SCHOOL DEPOSIT ($250 AUD)": "",
        "1:1 SESSON WITH SOFIA": "",
        "SCALE SCHOOL TIER ": "",
        "": "",
    }
    for text, want in cases.items():
        got = f(text)
        check(f"map {text!r} -> {want!r}", got == want, f"got {got!r}")

    # Stripe sale on a fresh row fills Z
    with Sheet() as s:
        server.handle_stripe_payment(charge(10000, "Purchase of SCALE SCHOOL MASTERMIND (Gemma) via ThriveCart"))
    check("stripe sale fills blank Z", s.z == "Scale School Tier 1", s.z)

    # Never overwrite a typed Program
    with Sheet(program="Scale School Tier 2") as s:
        server.handle_stripe_payment(charge(10000, "Purchase of SCALE SCHOOL MASTERMIND via ThriveCart"))
    check("existing Z untouched", s.z == "Scale School Tier 2", s.z)

    # GHL won handler already wrote I/J/K: Stripe event still fills blank Z
    with Sheet(cash="$1,000.00") as s:
        server.handle_stripe_payment(charge(1000, "Purchase of SCALE SCHOOL TIER 1 via ThriveCart"))
    check("Z filled even when cash already present", s.z == "Scale School Tier 1", s.z)

    # Deposit never writes Z
    with Sheet() as s:
        server.handle_stripe_payment(charge(250, "Purchase of SCALE SCHOOL MASTERMIND DEPOSIT ($250 AUD) via ThriveCart"))
    check("deposit leaves Z blank", s.z == "", s.z)

    # Unknown product leaves Z blank but the sale is still processed
    with Sheet() as s:
        server.handle_stripe_payment(charge(500, "1:1 SESSON WITH SOFIA"))
    check("unknown product leaves Z blank", s.z == "", s.z)

    # A Sheets error inside record_program never raises
    with Sheet() as s:
        with _m.patch.object(server, "sheets_read_cell", side_effect=RuntimeError("boom")):
            try:
                server.record_program(100, "SCALE SCHOOL TIER 2")
                ok = True
            except Exception:
                ok = False
    check("record_program swallows errors", ok)

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        sys.exit(1)


if __name__ == "__main__":
    run()
