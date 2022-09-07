# Tapdata Python Sdk

[中文文档地址](https://github.com/tapdata/tapdata/tree/master/tapshell/docs/Python-Sdk_zh-hans.md)

## Install

1. Install python 3.7, pip By Yourself.
2. Run ```pip install tapdata_cli``` to install sdk.
3. If you use poetry, please run ```poetry add tapdata_cli``` to install sdk.

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

The SDK supports the following data source operations:

- Mongo
- Mysql
- Postgres
- Oracle
- Kafka

To create MySQL/Mongo:

```python
from tapdata_cli import cli

connector = "mongodb"  # datasource type，mongodb mysql postgres
mongo = cli.DataSource("mongodb", name="mongo")
mongo.uri("mongodb://localhost:8080")  # datasource uri
mongo.save()
```

or:

```python
from tapdata_cli import cli

mongo = cli.DataSource("mongodb", name="mongo")
mongo.host("localhost:27017").db("source").username("user").password("password").props("")
mongo.type("source")  # datasource type，source -> only source，target -> only target，source_and_target -> target and source (default)
mongo.save()  # success -> True, Failure -> False
```

To Create Oracle database:

```python
from tapdata_cli import cli

datasource_name = "ds_name"  # datasource name
oracle = cli.Oracle(datasource_name)
oracle.thinType("SERVICE_NAME")  # connect type SID/SERVER_NAME (database name/service name)
oracle.host("106.55.169.3").password("Gotapd8!").port("3521").schema("TAPDATA").db("TAPDATA").username("tapdata")
oracle.save()
```

To create Kafka datasource:

```python
from tapdata_cli import cli

database_name = "kafka_name"
kafka = cli.Kafka(database_name)
kafka.host("106.xx.xx.x").port("9092")
kafka.save()
```

To create Postgres datasource:

```python
from tapdata_cli import cli

pg = cli.Postgres("jack_postgre") 
pg.host("106.55.169.3").port(5496).db("insurance").username("postgres").password("tapdata").type("source").schema("insurance")
pg.validate()
pg.save()
```

*As for Kafka/Oracle/Postgres, the creation mode is heterogeneous. In the future, a unified interface will be provided in the form of datasource, which is backward compatible and will not affect the existing version.*

### DataSource List

```python
from tapdata_cli import cli

cli.DataSource().list()

# return struct

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

### Get datasource according to ID/name

```python
from tapdata_cli import cli

cli.DataSource(id="")  # by id
cli.DataSource(name="")  # by name
```

## Pipeline

### A simple data migration Job

```python
from tapdata_cli import cli

# Create datasource first
source = cli.DataSource("mongodb", name="source").uri("").save()
target = cli.DataSource("mongodb", name="target").uri("").save()
# create Pipeline
p = cli.Pipeline(name="example_job")
p.readFrom("source").writeTo("target")
# start
p.start()
# stop
p.stop()
# delete
p.delete()
# status
p.status()
# get job list
cli.Job.list()
```

Job is the underlying implementation of pipeline, so you can use job.start() like pipeline.start().

```python
# init job (get job info) by id
from tapdata_cli import cli
job = cli.Job(id="some id string")
job.save() # success -> True, failure -> False
job.start() # success -> True, failure -> False
```

### Data development job

Before performing data development tasks, you need to change the task type to Sync:

```python
from tapdata_cli import cli

source = cli.DataSource("mongodb", name="source").uri("").save()
target = cli.DataSource("mongodb", name="target").uri("").save()
p = cli.Pipeline(name="")
p = p.readFrom("source.player") # source is db, player is table
p.dag.jobType = cli.JobType.sync
```

Then perform specific operations:

```python
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

master slave merge:

```python
# merge
p2 = cli.Pipeline(name="source_2")  # Create merged pipeline
p3 = p.merge(p2, [('id', 'id')]).writeTo("target")  # Merge pipeline

p3.writeTo("target.player")  # target is db, player is table
```

### Create initial_sync/cdc job

By default, all tasks created through pipeline are "full + incremental" job.

You can create a initial_sync job by:

```python
from tapdata_cli import cli

p = cli.Pipeline(name="")
p.readFrom("source").writeTo("target")
config = {"type": "initial_sync"}  # initial_sync
p1 = p.config(config=config)
p1.start()
```

As above, changing config to ` {"type": "cdc"}` can create an incremental task.

All pipeline configuration modification operations are passed in through the `pipeline.config` method through the config default parameters, and the parameters are verified.

For more configuration modification items, please see [this file](https://github.com/tapdata/tapdata/blob/master/tapshell/tapdata_cli/rules.py), get more configuration items.

## API Operation

### Update/Create ApiServer

```python
from tapdata_cli import cli

# create
cli.ApiServer(name="test", uri="http://127.0.0.1:3000/").save()

# update
# 1.get ApiServer id
api_server_id = cli.ApiServer.list()["id"]
# 2.update ApiServer
cli.ApiServer(id=api_server_id, name="test_2", uri="http://127.0.0.1:3000/").save()

# delete
cli.ApiServer(id=api_server_id).delete()
```

### Publish Api

```python
from tapdata_cli import cli
cli.Api(name="test", table="source.player").publish() # source is db, player is table
```

### Unpublish APi

```python
from tapdata_cli import cli
cli.Api(name="test").unpublish()
```

### Delete Api

```python
from tapdata_cli import cli
cli.Api(name="test").delete()
```

### Api Status

```python
from tapdata_cli import cli
cli.Api().status("test")  # success -> "pending" or "active" / failure -> None
```
