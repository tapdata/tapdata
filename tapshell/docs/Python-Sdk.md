# Tapdata Python Sdk

[English](https://github.com/tapdata/tapdata/tree/master/tapshell/docs/Python-Sdk.md)

*Applicable version*

- tapshell / Python-Sdk: ^2.3.0
- tapdata: ^2.9

## Install

1. Install python3.7、pip；
2. Run ```pip install tapdata_cli``` to install sdk；
3. if use Poetry, run ```poetry add tapdata_cli``` to install dependence.

## Initial

```python
server = "127.0.0.1:3000"
access_code = "3324cfdf-7d3e-4792-bd32-571638d4562f"
from tapdata_cli import cli
cli.init(server, access_code)
```

**Multi-thread concurrency is not supported**

It will send a request to the server to obtain the identity information and save it as a global variable. Therefore, after multiple init the 'server' and 'access_code' variable will be overwritten.

For situations where you need to use different servers and access_codes concurrently, use Python's multiprocess.

## DataSource

### Create DataSource

To create DataSource by Python Sdk, you can do it by form or uri mode.

Example for uri mode:

```python
from tapdata_cli import cli

connector = "mongodb"  # 数据源类型，mongodb mysql postgres
mongo = cli.DataSource("mongodb", name="mongo")
mongo.uri("mongodb://localhost:8080")  # 数据源uri
mongo.save()
```

or form mode:

```python
from tapdata_cli import cli

mongo = cli.DataSource("mongodb", name="mongo")
mongo.host("localhost:27017").db("source").username("user").password("password").props("")
mongo.type("source")  # 数据源类型，source -> 只可作为源，target -> 只可作为目标，source_and_target -> 可以作为源和目标（默认）
mongo.save()  # success -> True, Failure -> False
```

More infomation to create DataSource, please read [this file](https://github.com/tapdata/tapdata/blob/master/tapshell/docs/Param-Check_zh-hans.md).

### DataSource List

```python
from tapdata_cli import cli

cli.DataSource().list()

# return datastruct

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

### get datasource by id/name

```python
from tapdata_cli import cli

cli.DataSource(id="")  # 根据id获取数据源信息
cli.DataSource(name="")  # 根据name获取数据源信息
```

## Pipeline

### Migrate job

```python
from tapdata_cli import cli

# Create DataSource
cli.DataSource("mongodb", name="source").uri("").save()
cli.DataSource("mongodb", name="target").uri("").save()

# Create Source and target node
source = cli.Source("source")
target = cli.Sink("target")

# copy all table by default;
# copy by tables you want to, use table=[]
# filter table, by table_re
source = cli.Source("source", table=["table_1", "table_2", "table_3"], table_re="table_*")
source.config({"migrateTableSelectType": "custom"})  # change migrateTableSelectType: from all to custom

# init pipeline install
p = cli.Pipeline(name="example_job")
p.readFrom(source).writeTo(target)
# start
p.start()
# stop
p.stop()
# delete
p.delete()
# check status
p.status()
# job list
cli.Job.list()
```

Job is th underlying implementation of Pipeline，so you can start job by `job.start()` like `pipeline.start()`。

```python
# init job (get job info) by id
from tapdata_cli import cli
job = cli.Job(id="some id string")
job.save() # success -> True, failure -> False
job.start() # success -> True, failure -> False
```

### Sync job

Before you start a sync job, update job mode to `sync`.

```python
from tapdata_cli import cli

cli.DataSource("mongodb", name="source").uri("").save()  # create datasource
cli.DataSource("mongodb", name="target").uri("").save()  # create target
p = cli.Pipeline(name="sync_job", mode="sync")  # update to sync mode, or use p.dag.jobType = JobType.sync
p.mode(cli.JobType.sync)  # or you can update to sync mode by this way

# read source
p = p.readFrom("source.player") # source is db, player is table
p = p.readFrom(cli.Source("source", table="player", mode="sync"))  # or you init a Source Node in sync mode

# continue to complex operation next

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
p.writeTo(cli.Sink("target", table="player", mode="sync")
```

Master-slave Merge：

```python
p2 = cli.Pipeline(name="source_2")  # create pipeline which will be merged
p3 = p.merge(p2, [('id', 'id')])  # merge p2 and set joinkey, then writeTo a table

p3.writeTo("target.player")  # target is db, player is table
```

### Initial_sync

It's "initial_sync+cdc" mode by default. You can create a "initial_sync" job by this way:

```python
from tapdata_cli import cli

p = cli.Pipeline(name="")
p.readFrom("source").writeTo("target")
config = {"type": "initial_sync"}  # initial_sync job
p1 = p.config(config=config)
p1.start()
```

Change config by config method like `{"type": "cdc"}` to create a initial_sync job。

Python sdk has built-in param verification, you can update config by Pipeline.config, to see more configuration, you can see [this file](https://github.com/tapdata/tapdata/blob/master/tapshell/tapdata_cli/params/job.py)

## Api Operation

### Create/Update Apiserver

```python
from tapdata_cli import cli

# Create
cli.ApiServer(name="test", uri="http://127.0.0.1:3000/").save()

# Update
# 1.Get ApiServer id
api_server_id = cli.ApiServer.list()["id"]
# 2.Update ApiServer and save
cli.ApiServer(id=api_server_id, name="test_2", uri="http://127.0.0.1:3000/").save()

# delete apiserver
cli.ApiServer(id=api_server_id).delete()
```

### Publish Api

```python
from tapdata_cli import cli
cli.Api(name="test", table="source.player").publish() # source is db, player is table
```

### Unpublish Api

```python
from tapdata_cli import cli
cli.Api(name="test").unpublish()
```

### Delete api

```python
from tapdata_cli import cli
cli.Api(name="test").delete()
```

### Check api status

```python
from tapdata_cli import cli
cli.Api().status("test")  # success -> "pending" or "active" / failure -> None
```
