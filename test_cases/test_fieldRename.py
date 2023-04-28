# desc: 一个左连接的全量+增量数据开发任务


def test(Pipeline, source1, source2, target):
    p = Pipeline(mode="sync")
    p = p.readFrom(source1)
    p2 = Pipeline(mode="sync")
    p2 = p2.readFrom(source2)
    p.merge(p2, [("id", "id")], targetPath="policy").writeTo(target)
