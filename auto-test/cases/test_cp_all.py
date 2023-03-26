# desc: 一个源到目标的无任何数据处理的数据复制任务


def test(p, car_claim, car_policy, car_claim_sink):
    p.readFrom(car_claim).writeTo(car_claim_sink)
