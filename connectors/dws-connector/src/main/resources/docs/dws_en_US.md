# Connection Configuration Help

## 1. Supported versions

GaussDB(DWS) 8.1.3

## 2. Connection configuration example

    address:xxxxx
    port:xxxx
    database:test
    schema:test_tapdata
    user:tapdata
    password:tapdata

## 3. Distribute column

In GaussDB(DWS), distribute column refers to the columns used for data distribution in a distribution table. They determine the distribution of data in distributed storage and are crucial for query performance and data distribution balance.

### 3.1 Selection of Distribute column

When creating a table, you can use the DISTRIBUTED BY clause to specify the distribution columns.

```sql
CREATE TABLE my_table (
                        id INT,
                        name VARCHAR,
                        date DATE
) DISTRIBUTED BY (id);
```

If no distribute columns are specified when creating a table, data storage will follow these scenarios:

*   Scenario 1

If the table includes a primary key or unique constraint, HASH distribution is chosen, and the distribute column is the one corresponding to the primary key or unique constraint.

*   Scenario 2

If the table does not include a primary key or unique constraint but contains columns that support distribution, HASH distribution is chosen, and the distribute column is the first column that supports distribution data types.

*   Scenario 3

If the table does not have a primary key or unique constraint and does not have columns supporting distribution data types, ROUNDROBIN distribution is chosen.

### 3.2 Querying distribute column

You can use the following SQL to query the distribute columns of the current table:

```sql
SELECT getdistributekey('"your_schema"."table_name"')
```

### 3.3 Handling distribute column updates

Method 1: GaussDB(DWS) currently does not support updating distribute keys, so you can simply skip the error.

Method 2: Modify the distribute column to a non-updatable column. Here is an example of adjusting the distribute column:

    alter table customer_t1 DISTRIBUTE BY hash (c_customer_sk);

### 3.4 Considerations

*   When performing update operations on the source end, the database log needs to be able to read the data before the update. tapdata checks whether the distribution columns are modified before writing.

  *   If there is no data before the update, an exception will be thrown:

          Current Database cannot support update operation.
  *   If a modification to the distribution column is detected, an exception will be thrown:

          // GaussDB(DWS) database stipulates that distribution columns are not allowed to be updated.
          Distributed key column distributedKey can't be updated in table table_name.Value: beforeData => afterData.

*   Not all types of databases or data tables will record the data before the update, especially in the case of non-relational databases or certain special scenarios, where there is no built-in mechanism to record data before updates. Some database types, such as Redis, may lack an inherent mechanism for recording data prior to updates.

## 4. Partition table

A partition table logically divides a table into several physical blocks or partitions for storage. Each logical table is referred to as a partitioned table, and each physical block is referred to as a partition. A partitioned table is a logical table that does not store data; the data is actually stored in the partitions. When performing conditional queries, the system scans only the partitions that meet the conditions, avoiding full table scans and improving query performance.

### 4.1 Creating partition table

When data needs to be synchronized to a partition table, tapdata does not support automatic creation of partition tables. You need to manually create the partition table in advance. Here's an example of creating a partition table:

```sql
CREATE TABLE web_returns_p1
(
  "WR_RETURNED_DATE_SK"       integer,
  "WR_RETURNED_TIME_SK"       integer,
  "WR_ITEM_SK"                integer NOT NULL,
  "WR_REFUNDED_CUSTOMER_SK"   integer,
  primary key ("WR_RETURNED_DATE_SK","WR_ITEM_SK","WR_REFUNDED_CUSTOMER_SK")
)
  WITH (orientation = column)
  DISTRIBUTE BY HASH ("WR_ITEM_SK","WR_REFUNDED_CUSTOMER_SK")
  PARTITION BY RANGE ("WR_RETURNED_DATE_SK")
(
  PARTITION p2016 VALUES LESS THAN(20201231),
  PARTITION p2017 VALUES LESS THAN(20211231),
  PARTITION p2018 VALUES LESS THAN(20221231),
  PARTITION pxxxx VALUES LESS THAN(maxvalue)
  );
```

### 4.2 Considerations:

*   If a partition table does not have a primary key or unique index, tapdata does not support conflict update operations, and an exception message will be thrown.

    The partitioned table table\_name lacks a primary key or unique index, and does not support conflict update operations. Please switch to the append mode.

*   If you need to use a partition table and select a processing strategy for the target table, it is recommended to retain the table structure. Otherwise, tapdata will automatically create a regular table.

For more details, please refer to the official GaussDB(DWS) documentation:<https://support.huaweicloud.com/dws/index.html>

