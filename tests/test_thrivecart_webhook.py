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
from urllib.parse import urlencode


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("PASS  " if cond else "FAIL  ") + name + (f"  {detail}" if detail and not cond else ""))


class Sheet:
    def __init__(self, row_num=1113, cash="", deposit=""):
        self.row_num, self.cells, self.ranges, self.batches, self.colours = row_num, {}, [], [], []
        if cash: self.cells[("I", row_num)] = cash
        if deposit: self.cells[("AI", row_num)] = deposit

    def patches(self):
        m = mock.patch.object
        def batch(updates, tab="Sales Calls", value_input_option="RAW"):
            self.batches.append((updates, value_input_option)); return True, ""
        return [
            m(server, "find_row_by_email", lambda e, **kw: self.row_num),
            m(server, "sheets_read_cell", lambda r, c, **kw: self.cells.get((c, r), "")),
            m(server, "sheets_update_cell", lambda r, c, v, **kw: self.cells.__setitem__((c, r), v)),
            m(server, "sheets_update_range", lambda r, c, v, **kw: self.ranges.append((c, list(v)))),
            m(server, "sheets_batch_update_ranges", batch),
            m(server, "sheets_highlight_row", lambda *a, **kw: self.colours.append(a)),
            m(server, "get_exchange_rate", lambda *a, **kw: 1.0),
        ]


def run_with(sheet, payload):
    ps = sheet.patches()
    for p in ps: p.start()
    try:
        return server.handle_thrivecart_order(payload)
    finally:
        for p in ps: p.stop()


def order(processor="paypal", total=1000000, name="SCALE SCHOOL MASTERMIND",
          plan="Pay In Full: $10,000 (+ Receive 1 Bonus Month)", email="kimhamilton9@icloud.com",
          frequency="single", future=None, event="order.success", mode="live"):
    o = {"processor": processor, "total": total,
         "charges": [{"type": "product", "name": name, "payment_plan_name": plan,
                      "amount": total, "frequency": frequency}]}
    if future: o["future_charges"] = future
    return {"event": event, "mode": mode, "order_id": "43696899", "currency": "AUD",
            "customer": {"name": "Kim Hamilton", "email": email}, "order": o}


def run():
    # form decoding of ThriveCart's nested keys
    raw = urlencode({"event": "order.success", "customer[email]": "a@b.com", "order[processor]": "paypal",
                     "order[total]": "1000000", "order[charges][0][name]": "X",
                     "order[charges][1][name]": "Y", "purchases[0]": "X"}).encode()
    p = server.parse_nested_form(raw)
    check("nested form decode", p["customer"]["email"] == "a@b.com" and p["order"]["charges"][1]["name"] == "Y"
          and p["purchases"] == ["X"], str(p))

    # 1. Kim Hamilton: PayPal PIF -> 10000 / 1 / 10000 + DOP
    s = Sheet(); st = run_with(s, order())
    check("paypal PIF writes I/J/K", s.ranges == [("I", ["10000.00", "1", "10000.00"])], str(s.ranges))
    check("paypal PIF writes DOP", ("R", 1113) in s.cells and st == "sale row 1113", st)
    check("PIF not highlighted", s.colours == [])

    # 2. Stripe orders are left to /stripe-webhook
    s = Sheet(); st = run_with(s, order(processor="stripe"))
    check("stripe order ignored", s.ranges == [] and st.startswith("ignored processor"), st)

    # 3. PayPal payment plan worded in months
    s = Sheet(); run_with(s, order(total=96500, plan="Payment Plan: $965 NZD Per Month For 14 Months", frequency="month"))
    check("plan for 14 months -> 965/14/13510", s.ranges == [("I", ["965.00", "14", "13510.00"])], str(s.ranges))

    # 4. PayPal deposit -> AI/AJ only
    s = Sheet(); run_with(s, order(total=25000, name="SCALE SCHOOL DEPOSIT ($250 AUD)", plan=""))
    check("paypal deposit -> AI/AJ only", len(s.batches) == 1 and s.batches[0][0][0]["range"].startswith("AI")
          and s.ranges == [], str(s.batches))

    # 5. existing cash is never overwritten
    s = Sheet(cash="$1,000.00"); run_with(s, order())
    check("filled row untouched", s.ranges == [])

    # 6. deposit already on row is added to a PIF
    s = Sheet(deposit="250"); run_with(s, order(total=975000))
    check("deposit + PIF -> 10000/1/10000", s.ranges == [("I", ["10000.00", "1", "10000.00"])], str(s.ranges))

    # 7. unknown plan length -> written, flagged orange
    s = Sheet(); run_with(s, order(total=100000, plan="Mastermind Plan", frequency="month"))
    check("unknown plan -> orange", s.ranges and s.colours, f"{s.ranges} {s.colours}")

    # 8. front-end + test mode + other events ignored
    s = Sheet(); a = run_with(s, order(total=700, name="VIP", plan=""))
    b = run_with(s, order(mode="test")); c = run_with(s, order(event="order.refund"))
    check("front-end/test/other ignored", s.ranges == [] and s.batches == [], f"{a} {b} {c}")

    # 9. no matching row writes nothing
    s = Sheet(row_num=None); st = run_with(s, order())
    check("unmatched writes nothing", s.ranges == [] and st == "sale unmatched", st)

    # 10. *_str dollar field preferred over cents
    check("total_str preferred", server._tc_money({"total": "1000000", "total_str": "10000.00"}, "total") == 10000.0)

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    run()
