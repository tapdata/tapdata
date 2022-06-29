# Tapdata Live Data Platform

## TO BE PUBLISHED IN 2022.06.30

Tapdata Live Data Platform(tldp) is a real time light data platform, you can use it to do:
1. migrate data between multi sources, including auto create table and type mapping
2. data transform in migrate
3. make wide table using multi source table (TODO)
4. publish wide table api (TODO)

## Quick Start
Please make user you have Docker running on your env, before all things started

### Quick Usage
1. run `bash build/quick-use.sh` will pull and start a all in one container

### Quick Dev
1. run `bash build/quick-dev.sh` will build a docker image from source and start a all in one container

If you want to build in local, please install:
1. JDK and set PATH
2. maven
And set build/env.sh tapdata_build_env to "local"

If you want to build in docker, please install docker and set build/env.sh tapdata_build_env to "docker"

run `bash build/clean.sh` If you want to clean build target

## Quick Docs
### Work Mode
tapshell has two work modes:
1. command line mode: you can use commands like `show jobs` display jobs info
2. lib mode: you can use `p = Pipeline("pipeline"); p.start()`, python code to make and manage your data flow work

### Command Mode Docs
#### `show datasources`

To list the datasources in the Tapdata system, use the command `show datasources` and get the return like below.

``
id     status     database_type        name
03f6b1: ready      mysql               test-mysql
``

| Filed         | **Description**                  |
| ------------- | -------------------------------- |
| id            | Datasource id                    |
| status        | Datasource status(ready/invalid) |
| database_type | The database type of datasource  |
| name          | The name of datasource           |


#### `show jobs`

To list the jobs in the Tapdata system, use the command `show jobs` and get the return like below.

``
a2e1d7: Mysql-2-MongoDB                             draft    custom/initial_sync+cdc
``

| Filed  | **Description**                                              |
| ------ | ------------------------------------------------------------ |
| id     | Job id                                                       |
| Name   | The name of job                                              |
| Status | The status of job                                            |
| type   | The type of job<br />- cluster-clone/initial_sync<br />- cluster-clone/initial_sync+cdc<br />- custom/initial_sync<br />- custom/initial_sync+cdc |

#### `show tables`
After appoint a datasource by using `use datasource_name`, you can use `show tables` to  list the tables of this datasource.

### DataSource Command
#### Switch DataSource
To switch Datasource in the context, issue the `use <datasource>` helper, as in the following example.

``
use <datasource>
``

After appoint a datasource , you can use the command below to list table and get the schema of table:

``
show tables
``

``
desc <table>
```

#### Delete DataSource

To delete a DataSource, use the command below, please ensure no job using it before delete it

``
delete datasource <datasource>
``
`

### Job Command

#### `stop job <JobID/JobName> `

Stop the job  with specified JobID/JobName.



#### `start job <JobID/JobName> `

Start the job  with specified JobID/JobName.

#### `status job <JobID/JobName> `

Get the status of specified job.

#### `delete job <JobID/JobName> `

Delete the specified job.

#### `logs job <JobID/JobName> [options]` 

Get log of specified job. 
##### Options

| Name  | **Required** | Example     | Description                                         |
| ----- | ------------ | ----------- | --------------------------------------------------- |
| t     | No           | t=2         | The duration of tail method                         |
| limit | No           | limit=5     | Specify the last lines when the tail method started |
| tail  | no           | tail=True   | Tail method(Default:false)                          |
| level | no           | level=error | The log level (Default:info)                        |



#### `monitor job <JobID/JobName> `

Display the job monitor status in real-time.

``
job Car_shop_job2 status: running, total sync table: 1, finished synced: 1, total rows: 4, finished rows: 4, speed: 0, estimated time: 0:00:00
``

| Filed            | **Description**                                   |
| ---------------- | ------------------------------------------------- |
| job              | The name of job                                   |
| Status           | The status of job                                 |
| total sync table | The number of table that the job has total synced |
| finished synced  | The number of table that the job has total synced |
| total rows       | The total rows of the tables in this job          |
| finished rows    | The total rows that the job has already synced    |
| Speed            | The real-time speed of this job                   |
| estimated time   | The estimated time of this job                    |

#### `stats job <JobID/JobName> `
display the job monitor status one time


### Lib Mode Docs
The Tapdata shell Library consists of configuration and action. Each configuration declare the attributes of the library  as they pass through the pipeline. 

To create an Lib or make some action, use the following syntax in the Tapdata Shell:

``
<variable> = <lib>.[configurations]
<variable>.<action>
``

#### Example
``
Car_shop = DataSource("mysql","Car_shop").host("127.0.0.1").port(3306).username('root').password('password').db('Car_shop')
Car_shop.validate()
Car_shop.save()
```

#### DataSource
The DataSource Libary is used to create , declare ,validate, save and delete a DataSource.

Declare and configure a new DataSource:

``
My_DataSource = DataSource(<database_type>,<name>,<source_type>).[configurations]
``

| Name           | **Required** | Example                                             | Description                                                  |
| -------------- | ------------ | --------------------------------------------------- | ------------------------------------------------------------ |
| database_type  | Yes          | mysql                                               | The database type of the DataSource.<br />Please refer to [Pre-Build Connectors](../Connectors/pre-build-connectors.md) for details |
| name           | Yes          | "My_DS"                                             | The name of DataSource(Must be unique)                       |
| source_type    | no           | source_and_target (Default)<br />source<br />target | The type of DataSource.<br />For the types supported by each database, please refer to [Pre-Build Connectors](../Connectors/pre-build-connectors.md) . |
| configurations | Yes          |                                                     | Each database type has its own configurations, please refer to  [Pre-Build Connectors](../Connectors/pre-build-connectors.md) |

Trigger a DataSource action:

``
My_DataSource.<action>
``

##### Action

| Name       | **Description**                                              |
| ---------- | ------------------------------------------------------------ |
| save()     | Save the configured DataSource（Will do Validation at the same time） |
| validate() | Validate the configured DataSource                           |
| delete()   | Delete the specific DataSource                               |



##### Example
Create a mysql DataSource.

``
Car_shop = DataSource("mysql","Car_shop").host("127.0.0.1").port(3306).username('root').password('password').db('Car_shop')
Car_shop.save()
``

 

#### Pipeline
The Pipeline Libary is used to configure  a job and trigger a job action. 

Declare and configure a new DataSource:

``
My_Pipeline = Pipeline(<name>).[configurations]
``

| Field | **Required** | Example       | Description |
| ----- | ------------ | ------------- | ----------- |
| name  | Yes          | "My_Pipeline" | Job name    |



##### Configurations

| Field                         | **Required** | Example                                                      | Description                                                  |
| ----------------------------- | ------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `readFrom(<Source>)`          | Yes          | readFrom(Car_shop.Customer)                                  | Declare the Pipeline's read source.Please refer to [Source](#source-type) for Source's details |
| `writeTo(<Sink>, <Relation>)` | Yes          | writeTo(Car_shop.Customer_v1)<br />writeTo(Car_shop.Customer_v1,writeMode=upsert) | Declare the Pipeline's destination.Please refer to [Sink](#sink-type)  and [Relation](#Relation-Type) for more details |
| `rename(string, string)`      | No           | rename("filed1","filed2")                                    |                                                              |
| `js(string)`                  | No           | js('record["updated"]=new Date() ;return record;')<br />js("/mypath/modify.js") | Please refer to [JS Usage](./js-usage.md)  for more details  |
| `filter(string)`              | No           | filter('')                                                   | Only supported when the readFrom source is a single table    |
| `filterColumn([])`            | No           | filterColumn([column1,column2],FilterType.keep or FilterType.delete) | Only supported when the readFrom source is a single table    |
|                               |              |                                                              |                                                              |

Trigger a Job action:

``
My_Pipeline.<action>
``

##### Action

| Name     | **Description**                                              |
| -------- | ------------------------------------------------------------ |
| start()  | Save the configured DataSource（Will do Validation at the same time） |
| stop()   | Validate the configured DataSource                           |
| delete() | Delete the specific DataSource                               |



##### Example

``
Car_shop_job1 = Pipeline("Car_shop_job1").readFrom(Car_shop.Customer).writeTo(Car_shop.Customer_v1,writeMode=upsert)

Car_shop_job1.start()
``

 

#### Source Type
`Source` is used to define and describe a specified DataSource to `readFrom`.The simplest way to use `Source` is`<DataSource>.<table>` like `Car_shop.Customers` which has the same effect as `Source(Car_shop,Customers)`.

``
Source(<DataSource>, <table(string or [] or table_re="regular expression")>, <sql(string)>)
``

| Field        | **Required** | Example                                                      | Description                                                  |
| ------------ | ------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `DataSource` | Yes          | Car_shop                                                     | DataSource name                                              |
| `table`      | Yes          | Customers<br />[Customers,Orders]<br />table_re="Customer.*" | The table of the DataSource.<br />Can use regular expression by table_re. |
| `sql`        | No           | " select * from Customers where name='Edison' "              | Filter conditions of table (Using SQL) <br />**Only supported when the table field is not a array** . |

##### Example
Declare two tables (Customer and Orders) to sync from the Car_shop DataSource.

``
Source(Car_shop, [Customers,Orders])
Source(Car_shop, table_re="Customer.*")
``



Sync table Customer from the Car_shop DataSource with codition.

``
Source(Car_shop, Customers,"select * from Customers where name='Edison'")
``

#### Sink Type
`Sink` is used to define and describe a specified DataSource to `writeTo`.The simplest way to use `Sink` is`<DataSource>.<table>` like `Car_shop_1.Customers` which has the same effect as `Sink(Car_shop_1,Customers)`.

``
Sink(<DataSource>, <table(string or [])>)
``

| Field        | **Required** | Example                               | Description                                                  |
| ------------ | ------------ | ------------------------------------- | ------------------------------------------------------------ |
| `DataSource` | Yes          | Car_shop_1                            | DataSource name                                              |
| `table`      | No           | Customers<br />["Customers","Orders"] | The table of the DataSource.<br />When not declare the table means move all the table decalre in the Source Type |

##### Example

Declare two tables (Customer and Orders) for the writeTo destination Car_shop DataSource.

``
Sink(Car_shop, ["Customers","Orders"])
``

When not declare the table means move all the table decalre in the Source Type

``
Sink(Car_shop)
``

#### Relation Type

***Based on different ReadFrom Source, the available relationship types are also different***
##### SingleTableRelation

`SingleTableRelation` is used to define and describe the configuration of `writeTo` when the ReadFrom Source table is signle.

``
writeMode=<writeMode>,association=[(<column1>,<column2>)],path=<path>,array_key=<array_key>
``

| Field         | **Required** | Example                    | Description                                                  |
| ------------- | ------------ | -------------------------- | ------------------------------------------------------------ |
| `writeMode`   | No           | writeMode=upsert           | Write Mode.<br />insert/upsert/update/merge_embed<br />Default is insert |
| `association` | No           | association=[("id", "id")] | The association of write mode.The specified fields will be used for matching. |
| `path`        | No           |                            |                                                              |
| `array_key`   | No           |                            |                                                              |



##### MultiTableRelation
`MultiTableRelation` is used to define and describe the configuration of `writeTo` when the ReadFrom Source table is multiple.

``
prefix=<writeMode>,subfix=<subfix>,drop_type=<drop_type>
``

| Field       | **Required** | Example                 | Description                                                  |
| ----------- | ------------ | ----------------------- | ------------------------------------------------------------ |
| `prefix`    | No           | prefix="abc_"           |                                                              |
| `subfix`    | No           | subfix="xyz_"           |                                                              |
| `drop_type` | No           | drop_type=DropType.data | Processing method when a table with the same name exists at the target end.<br />DropType.no_drop, DropType.data, DropType.all<br />Default is DropType.no_drop |
