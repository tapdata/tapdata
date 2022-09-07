# Tapdata Python Sdk

[English](https://github.com/tapdata/tapdata/tree/master/tapshell/docs/Python-Sdk.md)

## 安装

1. 安装 python3.7、pip；
2. 运行 ```pip install tapdata_cli``` 安装sdk；
3. 如果使用Poetry，可以运行 ```poetry add tapdata_cli``` 安装依赖。

## 初始化

```python
server = "127.0.0.1:3000"
access_code = "3324cfdf-7d3e-4792-bd32-571638d4562f"
from tapdata_cli import cli
cli.init(server, access_code)
```

**不支持多线程并发**

它将向服务器发送请求以获取身份信息并将其保存为全局变量。因此，在多次初始化后，“server”和“access_code”变量将被覆盖。

对于需要并发使用不同server和 access_codes 的情况，请使用 Python 的多进程。

## 创建数据源

```python
# create datasource by uri
from tapdata_cli import cli
mongo = cli.DataSource("mongodb", name="source")
mongo.uri("mongodb://localhost:8080")
mongo.validate() # available -> True, disabled -> False
mongo.save() # success -> True, Failure -> False

# create datasource by form
mongo = cli.DataSource("mongodb", name="source")
mongo.host("localhost:8080").db("source").user("user").password("password").type("source").props("")
mongo.validate() # success -> True, Failure -> False
mongo.save() # success -> True, Failure -> False

# list datasource
res = mongo.list()

# res struct
{
    "total": 94,
    "items": [{
        "id": "",
        "lastUpdBy": "",
        "name": "",
        "config": {},
        "connection_type": "",
        "database_type": "",
        "definitionScope": "",
        "definitionVersion": "",
        "definitionGroup": "",
        "definitionPdkId": "",
        ...
    }]
}

# get datasource by name/id

cli.DataSource.get(id="")

# return

{
    "id": "",
    "lastUpdBy": "",
    "name": "",
    "config": {},
    "connection_type": "",
    "database_type": "",
    "definitionScope": "",
    "definitionVersion": "",
    "definitionGroup": "",
    "definitionPdkId": "",
    ...
}

```

## 创建Pipeline

```python
from tapdata_cli import cli

# pipeline create
source = cli.DataSource("mongodb", name="source").uri("").save()
target = cli.DataSource("mongodb", name="target").uri("").save()
p = cli.Pipeline(name="")
p.readFrom("source").writeTo("target")

# pipeline start
p.start()

# pipeline stop
p.stop()

# pipeline delete
p.delete()

# pipeline status
p.status()

# list job object
cli.Job.list()
```

Job 是 Pipeline 的底层实现，所以你可以像使用 pipeline.start() 一样使用 job.start() 。

```python
# init job (get job info) by id
from tapdata_cli import cli
job = cli.Job(id="some id string")
job.save() # success -> True, failure -> False
job.start() # success -> True, failure -> False
```

### 数据处理

```python
from tapdata_cli import cli
source = cli.DataSource("mongodb", name="source").uri("").save()
target = cli.DataSource("mongodb", name="target").uri("").save()
p = cli.Pipeline(name="")
p = p.readFrom("source.player") # source is db, player is table
p.dag.jobType = cli.JobType.sync

# filter cli.FilterType.keep (keep data) / cli.FilterType.delete (delete data)
p = p.filter("id > 2", cli.FilterType.keep)

# filerColumn cli.FilterType.keep (keep column) / cli.FilterType.delete (delete column)
p = p.filterColumn(["name"], cli.FilterType.delete)

# rename
p = p.rename("name", "player_name")

# valueMap
p = p.valueMap("position", 1) # 将 position 字段覆盖为 1

# js
p = p.js("return record;")

p.writeTo("target.player")  # target is db, player is table
```

## API 操作

### 发布Api

```python
from tapdata_cli import cli
cli.Api(name="test", table="source.player").publish() # source is db, player is table
```

### 取消发布Api

```python
from tapdata_cli import cli
cli.Api(name="test").unpublish()
```
