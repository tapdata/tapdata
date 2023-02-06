# desc: 源到目标, 数据开发, 增加 UDF, 且修改了表结构的任务
def test(p, car_claim, car_claim_sink):
    def process(record):
        record["new_column_str"] = "str"
        record["new_column_int"] = 12345
        return record
    p.readFrom(car_claim).js(process).writeTo(car_claim_sink)
