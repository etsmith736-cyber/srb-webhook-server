"""Stubbed tests for the currency-format enforcement on money writes.

Run: uv run python tests/test_currency_format.py   (or: python tests/test_currency_format.py)

No network: the Sheets service is replaced with a fake that records the
batchUpdate requests, so we assert on what the server WOULD send.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("GOOGLE_SA_JSON", "")
os.environ.setdefault("SPREADSHEET_ID", "test-sheet")

import server  # noqa: E402


class FakeExecutable:
    def __init__(self, result=None):
        self._result = result or {}

    def execute(self):
        return self._result


class FakeValues:
    def __init__(self, log):
        self.log = log

    def update(self, **kwargs):
        self.log.append(("values.update", kwargs))
        return FakeExecutable({"updatedCells": 1})

    def batchUpdate(self, **kwargs):
        self.log.append(("values.batchUpdate", kwargs))
        return FakeExecutable({"totalUpdatedCells": 1})

    def append(self, **kwargs):
        self.log.append(("values.append", kwargs))
        return FakeExecutable({})


class FakeSpreadsheets:
    def __init__(self, log):
        self.log = log

    def values(self):
        return FakeValues(self.log)

    def get(self, **kwargs):
        return FakeExecutable(
            {"sheets": [{"properties": {"title": "Sales Calls", "sheetId": 0}}]}
        )

    def batchUpdate(self, **kwargs):
        self.log.append(("batchUpdate", kwargs))
        return FakeExecutable({})


class FakeService:
    def __init__(self, log):
        self.log = log

    def spreadsheets(self):
        return FakeSpreadsheets(self.log)


def setup():
    log = []
    server._sheets_service = FakeService(log)
    server._sales_calls_sheet_id = None
    return log


def format_requests(log):
    out = []
    for name, kwargs in log:
        if name != "batchUpdate":
            continue
        for req in kwargs.get("body", {}).get("requests", []):
            rc = req.get("repeatCell", {})
            if "numberFormat" in rc.get("fields", ""):
                rng = rc["range"]
                out.append((rng["startColumnIndex"], rng["startRowIndex"] + 1,
                            rc["cell"]["userEnteredFormat"]["numberFormat"]["pattern"]))
    return out


def check(name, condition):
    print(("PASS  " if condition else "FAIL  ") + name)
    return bool(condition)


def main():
    results = []

    # 1. Writing cash (I) formats I and K of that row.
    log = setup()
    server.sheets_update_cell(1063, "I", "1000", value_input_option="USER_ENTERED")
    results.append(check("cash write formats I and K",
                         format_requests(log) == [(8, 1063, '"$"#,##0.00'),
                                                  (10, 1063, '"$"#,##0.00')]))

    # 2. A non-money cell write formats nothing.
    log = setup()
    server.sheets_update_cell(1063, "G", "Showed")
    results.append(check("non-money cell write formats nothing", format_requests(log) == []))

    # 3. sheets_update_range starting at I covers the money columns.
    log = setup()
    server.sheets_update_range(1063, "I", ["1000", "12", "12000"],
                               value_input_option="USER_ENTERED")
    results.append(check("I:K range write formats the row",
                         [r[1] for r in format_requests(log)] == [1063, 1063]))

    # 4. A range write on A:B (dates) formats nothing.
    log = setup()
    server.sheets_update_range(1063, "A", ["2026-09-29", "2026-09-30"],
                               value_input_option="USER_ENTERED")
    results.append(check("date range write formats nothing", format_requests(log) == []))

    # 5. A full-row write (A:R) formats the money columns.
    log = setup()
    server.sheets_update_row(500, [""] * 18)
    results.append(check("full row write formats money columns",
                         [r[0] for r in format_requests(log)] == [8, 10]))

    # 6. Batch update: only rows whose ranges touch I or K get formatted.
    log = setup()
    server.sheets_batch_update_ranges([
        {"range": "U10:X10", "values": [["a", "b", "c", "d"]]},
        {"range": "I42:K42", "values": [["1000", "12", "12000"]]},
        {"range": "AI7:AJ7", "values": [["250", "2026-09-25"]]},
    ])
    results.append(check("batch write formats only the money row",
                         sorted({r[1] for r in format_requests(log)}) == [42]))

    # 7. Deposit columns AI/AJ alone never trigger a currency stamp.
    results.append(check("AI/AJ range is not a money range",
                         server._money_rows_in_updates(
                             [{"range": "AI7:AJ7", "values": [[1]]}]) == []))

    # 8. _touches_money boundaries.
    results.append(check("_touches_money A..R true", server._touches_money("A", 18)))
    results.append(check("_touches_money L..N false", not server._touches_money("L", 3)))
    results.append(check("_touches_money K alone true", server._touches_money("K", 1)))

    # 9. A failing format call never breaks the value write.
    class ExplodingService(FakeService):
        def spreadsheets(self):
            sheets = FakeSpreadsheets(self.log)

            def boom(**kwargs):
                raise RuntimeError("Sheets is down")

            sheets.batchUpdate = boom
            return sheets

    log = []
    server._sheets_service = ExplodingService(log)
    server._sales_calls_sheet_id = 0
    try:
        server.sheets_update_cell(1063, "I", "1000")
        results.append(check("format failure does not raise", True))
    except Exception as exc:  # pragma: no cover
        results.append(check(f"format failure does not raise ({exc})", False))

    # 10. Other tabs are left alone.
    log = setup()
    server.sheets_update_cell(5, "I", "x", tab="Triage Calls")
    results.append(check("Triage Calls is never currency-formatted", format_requests(log) == []))

    server._sheets_service = None
    print(f"\n{sum(results)}/{len(results)} passed")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
