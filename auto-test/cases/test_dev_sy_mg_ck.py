# desc: 一个生成宽表的主从合并任务, 带结果校验
import os, sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../utils")
from factory import newDB

def test(Pipeline, p, car_claim, car_policy, car_claim_sink):
    p.config({"type": "initial_sync"})
    p = p.readFrom(car_claim)
    p2 = Pipeline(mode="sync")
    p2 = p2.readFrom(car_policy)
    p.merge(p2, [("POLICY_ID", "POLICY_ID")], targetPath="policy").writeTo(car_claim_sink)

def check(Pipeline, p, car_claim, car_policy, car_claim_sink):
    sink_client = newDB(car_claim_sink)
    if not hasattr(sink_client, "query"):
        return False
    if not hasattr(sink_client, "count"):
        return False

    rows = sink_client.query(condition={"POLICY_ID" : "888"})
    if len(rows) == 0:
        return False
    row = rows[0]
    print(row)
    if row.get("policy", {}).get("CAR_MODEL", "") != "E":
        return False
    return True
