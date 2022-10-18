<img src="https://github.com/tapdata/tapdata-private/raw/master/assets/logo-orange-grey-bar.png" width="300px"/>
<p align="center">
    <a href="https://github.com/tapdata/tapdata/graphs/contributors" alt="Contributors">
        <img src="https://img.shields.io/github/contributors/tapdata/tapdata" /></a>
    <a href="https://github.com/tapdata/tapdata/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/tapdata/tapdata" /></a>
    <a href="https://tapdata.github.io/tapdata">
        <img src="https://github.com/tapdata/tapdata/actions/workflows/docker-image.yml/badge.svg" alt="build status"></a>
</p>

[English Readme](https://github.com/tapdata/tapdata/blob/master/README.md)

[中文简要文档地址](https://github.com/tapdata/tapdata/blob/master/README.zh-CN.md)

## Online Document: https://tapdata.github.io/
## What is Tapdata?
Tapdata is a live data platform designed to connect data silos and provide fresh data to the downstream operational applications & operational analytics. 

## Env Prepare
1. Please make sure you have Docker installed on your machine before you get starated. 
2. Currently we only tested on linux OS(No specific flavor requirement).
3. clone repo: `git clone https://github.com/tapdata/tapdata.git && cd tapdata`

## Last Release Branch
release-v2.9

 
### Quick Use
This is the easiest way to experiment Tapdata:

  run `bash build/quick-use.sh` will pull docker image and start an all-inone container


### Quick Build
Alternatively, you may build the project using following command: 

1. run `bash build/quick-dev.sh` will build a docker image from source and start a all in one container

If you want to build in docker, please install docker and set build/env.sh tapdata_build_env to "docker" (default)

If you want to build in local, please install:
1. JDK
2. maven
set build/env.sh tapdata_build_env to "local"

run `bash build/clean.sh` If you want to clean build target

### Quick Steps
If everything is ok, now you should be in a terminal window, follow next steps, have a try!

#### Create New DataSource
```
# 1. mongodb
source = DataSource("mongodb", "$name").uri("$uri")

# 2. mysql
source = DataSource("mysql", "$name").host("$host").port($port).username("$username").port($port).db("$db")

# 3. pg
source = DataSource("postgres", "$name").host("$host").port($port).username("$username").port($port).db("$db").schema("$schema").logPluginName("wal2json")

# save will check all config, and load schema from source
source.save()
```

#### Preview Table
1. `use $name` will switch datasource context
2. `show tables` will display all tables in current datasource
3. `desc $table_name` will display table schema

#### Migrate A Table
migrate job is real time default
```
# 1. create a pipeline
p = Pipeline("$name")

# 2. use readFrom and writeTo describe a migrate job
p.readFrom("$source_name.$table").write("$sink_name.$table")

# 3. start job
p.start()

# 4. monitor job
p.monitor()
p.logs()

# 5. stop job
p.stop()
```

#### Migrate Aable With UDF
No record schema change support in current version, will support in few days

If you want to change record schema, please use mongodb as sink
```
# 1. define a python function
def fn(record):
    record["x"] = 1
    return record

# 2. using processor between source and target
p.readFrom(...).processor(fn).writeTo(...)
```

#### Migrate Multi Tables
migrate job is real time default

```
# 1. create a pipeline
p = Pipeline("$name")

# 2. use readFrom and writeTo describe a migrate job, multi table relation syntax is a little different
source = Source("$datasource_name", ["table1", "table2"...])
source = Source("$datasource_name", table_re="xxx.*")

# 3. using prefix/suffix add table prefix/suffix
p.readFrom(source).writeTo("$datasource_name", prefix="", suffix="")

# 4. start job
p.start()
```

#### Manager
1. `show datasources` will show all data sources, you can use `delete datasource $name` delete it if now job using it
2. `show jobs` will show all jobs and it's stats
3. `logs job $job_name [limit=20] [t=5] [tail=True]` will show job log
4. `monitor job $job_name` will keep show job metrics
5. `status job $job_name` will show job status(running/stopped...)

## License


Tapdata uses multiple licenses.

The license for a particular work is defined with following prioritized rules:

- License directly present in the file
- LICENSE file in the same directory as the work
- First LICENSE found when exploring parent directories up to the project top level directory

Defaults to Server Side Public License. For PDK Connectors, the license is Apache V2.

## Join now
- [Slack channel](https://join.slack.com/t/tapdatacommunity/shared_invite/zt-1biraoxpf-NRTsap0YLlAp99PHIVC9eA)
