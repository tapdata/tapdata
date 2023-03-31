# desc: 一个源到目标的无任何数据处理的增量数据开发任务


def test(p, car_claim, car_claim_sink):
    p.config({"type": "cdc"})
    p.readFrom(car_claim).writeTo(car_claim_sink)
