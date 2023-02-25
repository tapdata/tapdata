def test(p, car_claim, car_claim_sink):
    p.readFrom(car_claim).writeTo(car_claim_sink)
