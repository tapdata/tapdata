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

## Create DataSource

```python
# create datasource by uri
from tapdata_cli import cli
mongo = cli.DataSource("mongodb", name="source")
mongo.uri("mongodb://localhost:8080")
mongo.validate() # available -> True, disabled -> False
mongo.save() # success -> True, Failure -> False

# create datasource by form
mongo = cli.DataSource("mongodb", name="source")
mongo.host("localhost:8080").db("source").username("user").password("password").type("source").props("")
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

## create Pipeline

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

Job is the underlying implementation of pipeline, so you can use job.start() like pipeline.start().

```python
# init job (get job info) by id
from tapdata_cli import cli
job = cli.Job(id="some id string")
job.save() # success -> True, failure -> False
job.start() # success -> True, failure -> False
```

### data operator

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
p = p.valueMap("position", 1)

# js
p = p.js("return record;")

p.writeTo("target.player")  # target is db, player is table
```

## API Operation

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


