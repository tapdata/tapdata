# desc: 一个dummy到dummy的主从合并的全量+增量数据开发任务


def test(Pipeline, source, target):
    p = Pipeline(mode="sync")
    p.config({"type": "initial_sync"})
    p = p.readFrom(source)
    p2 = Pipeline(mode="sync")
    p2 = p2.readFrom(source)
    p.merge(p2, [("id", "id")], targetPath="policy").writeTo(target)
