# desc: 源到目标, 数据开发, 增加 UDF, 且修改了表结构的任务


def test(Pipeline, source, target):
    def process(record):
        record["new_column_str"] = "str"
        record["new_column_int"] = 12345
        return record
    p = Pipeline(mode='sync')
    p.readFrom(source).js(process).writeTo(target)
