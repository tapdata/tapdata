![](./assets/logo-orange-grey-bar.png)

## OpenSource Will Be Public In 2022.06.30 !

## Online Document: https://tapdata.github.io/

## What is Tapdata?

Tapdata is a live data platform designed to connect data silos and provide fresh data to the downstream operational applications & operational analytics. 

<img width="603" alt="image" src="./assets/tapdata-infra.png">

Tapdata provides two ways to achieve this: Live Data Integration and Live Data Service. 

Live Data Integration is supported by Tapdata's real time data pipelines based on CDC technology, where you can easily connect and capture all the data plus all the changes from disparate data sources, without any custom coding. Tapdata supports many data sources out of box, including dozens of popular databases, you may also use Tapdata's PDK(Plugin Development Kit) quickly add your own data sources. 

Live Data Service is Tapdata's modern approach to the old data integration problem:  streaming data into a centralized data store(currently powered by MongoDB), then serving the data via RESTful API. These APIs are created on-demand, and because they're served by the horizontally scalable, high performant and modern database(instead of source systems), the number of ETL jobs, the performance impact to the source systems, are hence greatly reduced. 

As an alternative, if your data source allows, you may also create data APIs directly from source databases such as Oracle, MySQL, SQLServer etc. 

The term "live has two meanings:

- When you are using Live Data Integrations, Tapdata will collect data in a "live" mode means it will listen for the changes on the source database and capture the change immediately and send it to the pipeline for processing and downstream consumption. Sometime this is called CDC technology. The data is always fresh and lively throughout the data pipeline. 

- When you are using Live Data Services, the backing data store is lively updated by Tapdata Live Data Integration pipelines and stays up-to-date with the source systems.  

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

## License
Tapdata is under the SSPL v1 license. 
