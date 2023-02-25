import os, sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../utils")
from factory import newDB


def test(p, car_claim, car_claim_sink):
    def process(record):
        record["new_column_str"] = "str"
        record["new_column_int"] = 12345
        return record

    p.readFrom(car_claim).js(process).writeTo(car_claim_sink)


def check(p, car_claim, car_claim_sink):
    sink_client = newDB(car_claim_sink)
    if not hasattr(sink_client, "query"):
        return False

    rows = sink_client.query()
    if len(rows) == 0:
        return False
    row = rows[0]
    if row.get("new_column_str", "") != "str":
        return False
    if row.get("new_column_int", 0) != 12345:
        return False
    return True
