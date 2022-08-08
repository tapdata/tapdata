## **connection configuration description**
### **1. Kafka installation instructions**
Please follow the instructions below to ensure that the Kafka database is successfully added and used in tapdata.
### **2. use restrictions**
>- only the message format of JSON object string is supported (such as ` {"Id": 1, "name": "Zhang San"} `)
>- create a theme in advance
>- Kafka version 2.3.x
>- if you choose to ignore consumption or push exceptions, the 'offset' of these messages will still be recorded, that is, these messages will not be pushed later, and there is a risk of data loss
>- message push is implemented as' at least once ', and the corresponding consumer should do idempotent operations
   #### **2.1 synchronization mode**
   ##### **full volume only**
   >In this mode, source will start to subscribe and consume from each partition of the topic 'early offset'. If there is a message consumption record before, it will be restored to the previous' offset 'to start consumption
   ##### **incremental only**
   >In this mode, source will start to subscribe and consume from each partition 'latest offset' of the topic. If there is a message consumption record before, it will be restored to the previous' offset 'to start consumption
   ##### **full volume + increment**
   >In this mode, source will skip the full synchronization phase and start from the incremental phase.
   >
>1. If full synchronization has not been carried out, subscription consumption will start from each partition of the topic 'early offset'
>2. Otherwise, subscribe and consume from each partition of the topic 'latest offset'.
>3. If there is a message consumption record before, it will be restored to the previous' offset 'to start consumption
 #### **2.2 node connection**
    |Whether source | target | can be linked|
    | ------------- | ------------- | ---------- |
    |Kafka | elasticsearch | yes|
    |Kafka | redis | yes|
    |Kafka | table | yes|
    |Kafka | collection | yes|
    |Kafka | memory | yes|
    |Elasticsearch | Kafka | yes|
    |Table | Kafka | yes|
    |Redis | Kafka | yes|
    |Collection | Kafka | yes|
    |Memory | Kafka | yes|
    ##### **2.3 data migration**
    |Whether source | target | can be linked|
    | ---------- | ---------- | ---------- |
    |Kafka | MySQL | yes|
    |Kafka | Oracle | yes|
    |Kafka | mongodb | yes|
    |Kafka | DB2 | yes|
    |Kafka | Postgres | yes|
    |Kafka | MSSQL | yes|
    |Kafka | base 8s | yes|
    |Kafka | Sybase ASE | yes|
    |MySQL | Kafka | yes|
    |Oracle | Kafka | yes|
    |Mongodb | Kafka | yes|
    |DB2 | Kafka | yes|
    |Postgres | Kafka | yes|
    |Sybase ASE | Kafka | yes|
    |Base 8s | Kafka | yes|
    |MSSQL | Kafka | yes|
    ###**3. configuration**
    #####**3.1 public configuration**
    |Field name (UI form parameter name) | type | whether it is required | remarks | default value | verification | UI form field name | UI form field component|
    | ---------------------- | ------ | -------- | ------------------- | ------ | ---------------------------------------------------------------------------------------- | ---------------- | ------------------------- |
    |Kafkabootstrapservers | string | is | broker address list | - | host1:port, host2:port, host3:port (such as 192.168.1.1:9092192.168.1.2:9092192.168.1.3:9092) | host list | ` < input type= "text" / > `|
    |Databasetype | string | is | database type | - | fixed value: Kafka | none ` <input type= "hidden" / > `|
    | connection_ Type | string | is | link type | - | enumeration value: source \ | target \ | source_ and_ Target | link type ` < select / > `|
    |Kafkapatterntopics | string | is | topic name regular expression, | - | text length is greater than 0, less than 256 | topic regular expression | ` <input type= "text" / > `|
##### **3.2 Source (Kafka Consumer)**
|Field name (UI form parameter name) | type | whether it is required | remarks | default value | verification | UI form field name | UI form field component|
| ------------------------ | ------- | -------- | ----------------------------------------------------------------------------------- | ------ | --------------------- | -------------------- | ---------------- |
|Kafkaignoreinvalidrecord | Boolean | no | whether to ignore non JSON object format messages. If yes, the message will be ignored when encountering parsing exceptions. Otherwise, stop pulling messages | false | enumeration value: true \ | false | ignore non JSON format messages | ` < select / > `|
##### **3.3 Target (Kafka Producer)**
|Field name (UI form parameter name) | type | whether it is required | remarks | default value | verification | UI form field name | UI form field component|
| ---------------------- | ------- | -------- | -------------------------------------------------------------------------------------------------------- | ------ | --------------------------------------------- | ---------------- | ---------------- |
|Kafkaacks | string | no | ack confirmation mechanism, "0": unconfirmed, "1": only write to master partitions, "-1": write to most ISR partitions, "all": write to all ISR partitions | -1 | enumeration value: "0" 124; "1" | "-1" | | "all" | message push ACK | `<select / > `|
|Kafkacompressiontype | string | no | message compression type: gzip, snappy, lz4, zstd High traffic message compression can improve transmission efficiency. | -| Enumeration value: "gzip" 124; "snappy" | "lz4" | "zstd" | message push compression method | ` < select / > `|
|Kafkaignorepusherror | Boolean | no | whether to ignore the push message exception. If yes, ignore the push message (there is message loss), otherwise stop pushing the message | false | enumeration value: true \ | false | message push ignore exception | ` < select / > `|