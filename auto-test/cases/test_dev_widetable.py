def test(Pipeline, p, car_claim, car_policy, car_claim_sink):
    p.readFrom(car_claim)
    p2 = Pipeline()
    p2.readFrom(car_policy)
    p.merge(p2, [("POLICY_ID", "POLICY_ID")]).writeTo(car_claim_sink)
