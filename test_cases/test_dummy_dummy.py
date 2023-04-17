# desc: 一个dummy到dummy的无任何数据处理的全量+增量数据开发任务


def test(pipeline, source, processing, target):
    pipeline.readFrom(source).writeTo(target)
