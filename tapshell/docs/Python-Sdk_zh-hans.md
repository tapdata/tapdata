# Tapdata Python Sdk

[English](https://github.com/tapdata/tapdata/tree/master/tapshell/docs/Python-Sdk.md)

*适用版本*

- tapshell / Python-Sdk: ^2.3.0
- tapdata: ^2.9

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

## 数据源

### 创建数据源

使用Python sdk创建数据源，主要有两种模式，分别是form和uri模式

如uri模式创建MongoDB数据源：

```python
from tapdata_cli import cli

connector = "mongodb"  # 数据源类型，mongodb mysql postgres
mongo = cli.DataSource("mongodb", name="mongo")
mongo.uri("mongodb://localhost:8080")  # 数据源uri
mongo.save()
```

或者是form模式创建数据源：

```python
from tapdata_cli import cli

mongo = cli.DataSource("mongodb", name="mongo")
mongo.host("localhost:27017").db("source").username("user").password("password").props("")
mongo.type("source")  # 数据源类型，source -> 只可作为源，target -> 只可作为目标，source_and_target -> 可以作为源和目标（默认）
mongo.save()  # success -> True, Failure -> False
```

更多关于创建数据源的信息请查看[这个文件](https://github.com/tapdata/tapdata/blob/master/tapshell/docs/Param-Check_zh-hans.md)，可以获取更多接口。

### 数据源列表

```python
from tapdata_cli import cli

cli.DataSource().list()

# 返回结构如下所示：

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
        ```

### 根据ID/name获取数据源信息

```python
from tapdata_cli import cli

cli.DataSource(id="")  # 根据id获取数据源信息
cli.DataSource(name="")  # 根据name获取数据源信息
```

## Pipeline

### 数据迁移任务

```python
from tapdata_cli import cli

# 创建数据源
cli.DataSource("mongodb", name="source").uri("").save()
cli.DataSource("mongodb", name="target").uri("").save()

# 创建source和target节点
source = cli.Source("source")
target = cli.Sink("target")

# 默认复制所有表；
# 如果想要自定义多张表，创建源节点时增加table字段
# 如果想要筛选表，传入table_re字段进行正则匹配
source = cli.Source("source", table=["table_1", "table_2", "table_3"], table_re="table_*")
source.config({"migrateTableSelectType": "custom"})  # 更新 migrateTableSelectType配置: 从 all 到 custom

# 创建Pipeline
p = cli.Pipeline(name="example_job")
p.readFrom(source).writeTo(target)
# 启动
p.start()
# 停止
p.stop()
# 删除
p.delete()
# 查看状态
p.status()
# 查看job列表
cli.Job.list()
```

值得一提的是，Job 是 Pipeline 的底层实现，所以你可以像使用 pipeline.start() 一样使用 job.start() 。

```python
# init job (get job info) by id
from tapdata_cli import cli
job = cli.Job(id="some id string")
job.save() # success -> True, failure -> False
job.start() # success -> True, failure -> False
```

### 数据开发任务

在进行数据开发任务之前，需要将任务类型修改为sync：

```python
from tapdata_cli import cli

cli.DataSource("mongodb", name="source").uri("").save()  # 创建源
cli.DataSource("mongodb", name="target").uri("").save()  # 创建目标
p = cli.Pipeline(name="sync_job", mode="sync")  # 设置模式为sync 或者可以沿用 p.dag.jobType = JobType.sync
p.mode(cli.JobType.sync)  # 也可以通过这种方式来修改mode

# 读取节点
p = p.readFrom("source.player") # source is db, player is table  # 快速选择方式 datasource.table
p = p.readFrom(cli.Source("source", table="player", mode="sync"))  # 传入sync模式下源节点

# 再进行具体的操作

# filter cli.FilterType.keep (keep data) / cli.FilterType.delete (delete data)
p = p.filter("id > 2", cli.FilterType.keep)

# filerColumn cli.FilterType.keep (keep column) / cli.FilterType.delete (delete column)
p = p.filterColumn(["name"], cli.FilterType.delete)

# rename
p = p.rename("name", "player_name")

# valueMap
p = p.valueMap("position", 1)

# js
p = p.js("return record;")

p.writeTo("target.player")  # target is db, player is table
```

主从合并：

```python
# merge 主从合并
p2 = cli.Pipeline(name="source_2")  # 创建被合并的Pipeline
p3 = p.merge(p2, [('id', 'id')]).writeTo("target")  # 将Pipeline合并

p3.writeTo("target.player")  # target is db, player is table
```

### 创建 全量/增量 任务

默认情况下，通过Pipeline创建的任务均为"全量+增量"任务。

通过以下方式，可以创建一个全量任务：

```python
from tapdata_cli import cli

p = cli.Pipeline(name="")
p.readFrom("source").writeTo("target")
config = {"type": "initial_sync"}  # 全量任务
p1 = p.config(config=config)
p1.start()
```

如上，将config改成 `{"type": "cdc"}` 可以创建一个增量任务。

Pipeline的配置修改操作都是通过Pipeline.config方法通过config默认参数传入，并做了参数校验。

关于更多的配置修改项，可以查看 [这个文件](https://github.com/tapdata/tapdata/blob/master/tapshell/tapdata_cli/rules.py) ，获取更多配置项。

## API 操作

### 创建/更新 Api服务器

```python
from tapdata_cli import cli

# 创建
cli.ApiServer(name="test", uri="http://127.0.0.1:3000/").save()

# 更新
# 1.获取ApiServer ID
api_server_id = cli.ApiServer.list()["id"]
# 2.更新ApiServer
cli.ApiServer(id=api_server_id, name="test_2", uri="http://127.0.0.1:3000/").save()

# 删除Api服务器
cli.ApiServer(id=api_server_id).delete()
```

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

### 删除Api

```python
from tapdata_cli import cli
cli.Api(name="test").delete()
```

### Api状态

```python
from tapdata_cli import cli
cli.Api().status("test")  # success -> "pending" or "active" / failure -> None
```
