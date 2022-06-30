# Build Connectors with PDK

## What is PDK

**PDK is Plugin Development Kit.** Provide an easier way to develop data source connectors

* **Database connectors**
  * MySql, Oracle, Postgres, etc.
* **SaaS connectors**
  * Facebook, Salesforce, Google docs, etc.
* **Custom connectors**
  * Connect your custom data sources.

PDK connectors provide data source to flow in iDaaS pipeline to process, join, etc, and flow in a target which is also
provided by PDK connectors.

## Quick Start

Java 8+ and Maven need to be installed.

Generate Java Sample Project, then start filling the necessary methods to implement a PDK connector.

```java

@TapConnectorClass("spec.json")
public class SampleConnector extends ConnectorBase {
    
    @Override
    public void discoverSchema(TapConnectionContext connectionContext, Consumer<List<TapTable>> consumer) {
        //TODO Load schema from database, connection information in connectionContext#getConnectionConfig
        //Sample code shows how to define tables with specified fields.
        consumer.accept(list(
                //Define first table
                table("empty-table1")
                        //Define a field named "id", origin field type, whether is primary key and primary key position
                        .add(field("id", "VARCHAR").isPrimaryKey(true))
                        .add(field("description", "TEXT"))
                        .add(field("name", "VARCHAR"))
                        .add(field("age", "DOUBLE"))
        ));
    }

    private void streamRead(TapConnectorContext connectorContext, List<String> tableList, Object streamOffset, int batchSize, StreamReadConsumer consumer) {
        //TODO using CDC APi or log to read stream records from database, use consumer#accept to send to incremental engine.
        
        //Stream read may start slowly, please call below method to mark stream read started.  
        consumer.streamReadStarted();
        //Below is sample code to generate stream records directly
        while(!isShutDown.get()) {
            String tableId = tableList.get(0);
            List<TapEvent> tapEvents = list();
            for (int i = 0; i < batchSize; i++) {
                TapInsertRecordEvent event = insertRecordEvent(map(
                        entry("id", counter.incrementAndGet()),
                        entry("description", "123"),
                        entry("name", "123"),
                        entry("age", 12)
                ), tableId);
                tapEvents.add(event);
            }

            sleep(1000L);
            consumer.accept(tapEvents, null);
        }
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        //TODO write records into database
        //Need to tell incremental engine the write result
        writeListResultConsumer.accept(writeListResult()
                .insertedCount(inserted.get())
                .modifiedCount(updated.get())
                .removedCount(deleted.get()));
    }
}
```

Each connector need to provide a json file like above, described in TapConnector annotation.

spec.json describes three types information,

* properties
  - Connector name, icon and id.
* [configOptions](docs/form-json-schema.md) Form Json Schema
  - Describe a form for user to input the information for how to connect and which database to open.
* [dataTypes](docs/data-type-expressions.md) Data Type Expressions
  - This will be required when current data source need create table with proper types before insert records.
    Otherwise, this key can be empty.
  - Describe the capability of types for current data source.
  - Incremental Engine will generate proper types when table creation is needed.

```json
{
  "properties": {
    "name": "PDK Sample Connector",
    "icon": "icon.jpeg",
    "id": "pdk-sample"
  },
  "configOptions": {
    "connection": {
      "type": "object",
      "properties": {
        "host": {
          "type": "string",
          "title": "Host",
          "required": true,
          "x-decorator": "FormItem",
          "x-component": "Input"
        },
        "port": {
          "type": "number",
          "title": "Port",
          "required": true,
          "x-decorator": "FormItem",
          "x-component": "Input"
        },
        "database": {
          "type": "string",
          "title": "Database",
          "required": true,
          "x-decorator": "FormItem",
          "x-component": "Input"
        }
      }
    }
  },
  "dataTypes": {
    "double": {"bit": 64, "to": "TapNumber"},
    "decimal[($precision,$scale)]": {"precision": [1, 27], "defaultPrecision": 10, "scale": [0, 9], "defaultScale": 0, "to": "TapNumber"}
  }
}

```

## How to create PDK java project?

Requirements
*	java 8 and above
*   maven (https://maven.apache.org/download.cgi)

For Windows
* gitbash (https://gitforwindows.org/)
* setup maven (https://mkyong.com/maven/how-to-install-maven-in-windows/)



Install from source code. Please use gitbash to execute below commands for Windows,

```shell
git clone https://github.com/tapdata/tapdata.git
cd tapdata
mvn clean install
cd plugin-kit
```
Use template to create a project. Assume "group" is "io.tapdata" and "name" is "XDB" and "version" is "0.0.1".
```shell
./bin/pdk template --group io.tapdata --name XDB --version 0.0.1 --output ../connectors
```

Use IntelliJ IDE to open tapdata, you can find it named "xdb-connector" under connectors module.
* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)


## Test Driven Development
Provide json file which given the values of "configOptions" required from "spec.json" file.
```json
{
    "connection": {
      "host": "192.168.153.132",
      "port": 9030,
      "database": "test"
    }
}
```
The json file can be outside of git project as you may don't want to expose your password or ip/port, etc.

To run TDD command,

```shell
./bin/pdk test --testConfig xdb_tdd.json ../connectors/xdb-connector
```
Once PDK connector pass TDD tests, proves that the PDK connector is ready to work in Tapdata. You can also contribute the PDK connector to tapdata open source project.

## Development Guide
PDK need developer to provide below,
* Provide data types expression in spec.json for the database which need create table before record insertion.
* Implement the methods that PDK required.
* Use TDD to verify that the methods are implemented correctly.

[Get started](docs/development-guide.md)

