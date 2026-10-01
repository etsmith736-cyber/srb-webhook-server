"""Stubbed tests for the deposit-column behaviour in server.py."""
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

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("PASS  " if cond else "FAIL  ") + name + (f"  {detail}" if detail and not cond else ""))


class Sheet:
    """Minimal in-memory stand-in for the Sales Calls tab."""

    def __init__(self, row=None, deposit="", row_num=100):
        self.cells = {}
        self.row_num = row_num
        self.range_writes = []
        self.batch_writes = []
        if deposit:
            self.cells[("AI", row_num)] = deposit
        self.row = row or [""] * 18

    def install(self, monkey):
        monkey.setattr(server, "find_row_by_email", lambda e, **kw: self.row_num)
        monkey.setattr(server, "sheets_read_all", lambda *a, **kw: [[""] * 18] * (self.row_num - 1) + [self.row])
        monkey.setattr(server, "sheets_read_cell", lambda r, c, **kw: self.cells.get((c, r), ""))
        monkey.setattr(server, "sheets_update_cell", lambda r, c, v, **kw: self.cells.__setitem__((c, r), v))
        monkey.setattr(server, "sheets_highlight_row", lambda *a, **kw: None)
        monkey.setattr(server, "get_exchange_rate", lambda *a, **kw: 1.0)

        def upd_range(row_number, start_col, values, **kw):
            self.range_writes.append((start_col, list(values)))
        monkey.setattr(server, "sheets_update_range", upd_range)

        def batch(updates, tab="Sales Calls", value_input_option="RAW"):
            self.batch_writes.append((updates, value_input_option))
            return True, ""
        monkey.setattr(server, "sheets_batch_update_ranges", batch)


def charge_event(amount, description, email="a@b.com", created=1790000000, customer="cus_1"):
    return {"type": "charge.succeeded", "data": {"object": {
        "id": "ch_1", "amount": int(amount * 100), "currency": "aud", "created": created,
        "description": description, "customer": customer,
        "billing_details": {"email": email}}}}


def run():
    mp = mock.patch
    # 1. $250 deposit -> AI/AJ only
    s = Sheet()
    with mock.patch.object(server, "find_plan_subscription_for_charge", lambda *a, **kw: None):
        m = mock.MagicMock()
        with _monkey(s):
            server.handle_stripe_payment(charge_event(250, "Purchase of SCALE SCHOOL MASTERMIND DEPOSIT ($250 AUD) via ThriveCart"))
    check("deposit writes AI/AJ", len(s.batch_writes) == 1 and s.batch_writes[0][0][0]["range"].startswith("AI"), str(s.batch_writes))
    check("deposit uses USER_ENTERED", s.batch_writes and s.batch_writes[0][1] == "USER_ENTERED")
    check("deposit writes no sales columns", s.range_writes == [], str(s.range_writes))

    # 2. $100 deposit survives the front-end filter
    s = Sheet()
    with _monkey(s):
        server.handle_stripe_payment(charge_event(100, "Purchase of SCALE SCHOOL DEPOSIT ($100 AUD) via ThriveCart"))
    check("$100 deposit is recorded, not binned", len(s.batch_writes) == 1, str(s.batch_writes))

    # 3. $7 front-end still ignored
    s = Sheet()
    with _monkey(s):
        server.handle_stripe_payment(charge_event(7, "7-Figure Funnel VIP Access (AU$7)"))
    check("$7 front-end still ignored", s.batch_writes == [] and s.range_writes == [])

    # 4. plan purchase on a row with a $250 deposit -> 1000 / 12 / 12000
    s = Sheet(deposit="$250.00")
    with mock.patch.object(server, "find_plan_subscription_for_charge", lambda *a, **kw: "sub_1"), \
         mock.patch.object(server, "get_stripe_subscription_details", lambda sid: (12, 12000.0, True)):
        with _monkey(s):
            server.handle_stripe_payment(charge_event(750, "Purchase of SCALE SCHOOL MASTERMIND (PLAN) via ThriveCart"))
    check("plan + deposit -> 1000/12/12000", s.range_writes and s.range_writes[0] == ("I", ["1000.00", "12", "12000.00"]), str(s.range_writes))

    # 5. PIF on a row with a $250 deposit -> 10000 / 1 / 10000
    s = Sheet(deposit="250")
    with mock.patch.object(server, "find_plan_subscription_for_charge", lambda *a, **kw: None):
        with _monkey(s):
            server.handle_stripe_payment(charge_event(9750, "Purchase of SCALE SCHOOL MASTERMIND (Marianne) via ThriveCart"))
    check("PIF + deposit -> 10000/1/10000", s.range_writes and s.range_writes[0] == ("I", ["10000.00", "1", "10000.00"]), str(s.range_writes))

    # 6. no deposit -> unchanged behaviour
    s = Sheet()
    with mock.patch.object(server, "find_plan_subscription_for_charge", lambda *a, **kw: None):
        with _monkey(s):
            server.handle_stripe_payment(charge_event(10000, "Purchase of SCALE SCHOOL TIER 1 via ThriveCart"))
    check("no deposit -> unchanged", s.range_writes and s.range_writes[0] == ("I", ["10000.00", "1", "10000.00"]), str(s.range_writes))

    # 7. duplicate deposit is not doubled
    s = Sheet(deposit="$100.00")
    with _monkey(s):
        server.handle_stripe_payment(charge_event(100, "Purchase of SCALE SCHOOL DEPOSIT ($100 AUD) via ThriveCart"))
    check("duplicate deposit ignored", s.batch_writes == [], str(s.batch_writes))

    # 8. deposit with no matching row writes nothing
    s = Sheet(); s.row_num = None
    with _monkey(s):
        server.handle_stripe_payment(charge_event(250, "Purchase of DEPOSIT ($250 AUD) via ThriveCart"))
    check("deposit without a row writes nothing", s.batch_writes == [] and s.range_writes == [])

    # 9. invoice line item naming is detected too
    ev = {"type": "invoice.payment_succeeded", "data": {"object": {
        "id": "in_1", "customer_email": "a@b.com", "amount_paid": 25000, "currency": "aud",
        "created": 1790000000, "subscription": None,
        "lines": {"data": [{"description": "SCALE SCHOOL DEPOSIT ($250 AUD)"}]}}}}
    s = Sheet()
    with _monkey(s):
        server.handle_stripe_payment(ev)
    check("deposit detected on an invoice line", len(s.batch_writes) == 1, str(s.batch_writes))

    # 10. subscription.created backfills when cash = first instalment + $100 deposit
    s = Sheet(row=["", "2026-09-20"] + [""] * 16, deposit="100")
    s.row[server.COL["Cash Collected (AUD)"]] = "850"
    s.row[server.COL["Number of Payments"]] = "1"
    sub = {"type": "customer.subscription.created", "data": {"object": {
        "id": "sub_1", "customer": "cus_1", "currency": "aud"}}}
    fake_stripe = types.SimpleNamespace(Customer=types.SimpleNamespace(retrieve=lambda cid: {"email": "a@b.com"}))
    with mock.patch.object(server, "stripe", fake_stripe), \
         mock.patch.object(server, "get_stripe_subscription_details", lambda sid: (12, 12000.0, True)):
        with _monkey(s):
            server.handle_subscription_created(sub)
    check("sub.created backfills J/K with a deposit on the row",
          s.range_writes and s.range_writes[0] == ("J", ["12", "12000.00"]), str(s.range_writes))

    # 11. an unrelated amount is still left alone
    s = Sheet(row=["", "2026-09-20"] + [""] * 16)
    s.row[server.COL["Cash Collected (AUD)"]] = "2800"
    s.row[server.COL["Number of Payments"]] = "1"
    with mock.patch.object(server, "stripe", fake_stripe), \
         mock.patch.object(server, "get_stripe_subscription_details", lambda sid: (12, 12000.0, True)):
        with _monkey(s):
            server.handle_subscription_created(sub)
    check("sub.created still ignores a non-instalment row", s.range_writes == [], str(s.range_writes))

    # 12. name matching
    check("name matcher: positives", all(server.is_deposit_payment(x) for x in [
        "SCALE SCHOOL DEPOSIT", "Purchase of DEPOSIT ($250 AUD) via ThriveCart",
        "SCALE SCHOOL MASTERMIND DEPOSIT ($250 AUD)", "deposits"]))
    check("name matcher: negatives", not any(server.is_deposit_payment(x) for x in [
        "SCALE SCHOOL TIER 1", "7-Figure Funnel VIP Access (AU$7)", "MASTERMIND (PLAN)", ""]))

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("FAILED:", FAIL)
        sys.exit(1)


class _monkey:
    def __init__(self, sheet):
        self.sheet = sheet
        self.patchers = []

    def __enter__(self):
        class M:
            def __init__(inner, outer): inner.outer = outer
            def setattr(inner, obj, name, value):
                p = mock.patch.object(obj, name, value)
                p.start(); inner.outer.patchers.append(p)
        self.sheet.install(M(self))
        return self

    def __exit__(self, *a):
        for p in reversed(self.patchers):
            p.stop()


if __name__ == "__main__":
    run()
