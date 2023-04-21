# desc: 一个dummy到dummy的无任何数据处理的全量+增量数据开发任务


def test(Pipeline, source, target):
    p = Pipeline(mode="sync")
    p.readFrom(source).writeTo(target)
