# 连接配置帮助

## 1.支持版本

GaussDB(DWS) 8.1.3

## 2.连接配置示例

    地址：xxxxx
    端口：xxxx
    数据库：test
    模型：test_tapdata
    账号：tapdata
    密码：tapdata

## 3.分布列

在GaussDB(DWS)中，分布列是指分布表中用于数据分布的列，它决定了数据在分布式存储中的分布方式。分布列的选择对于查询性能和数据分布均衡至关重要。

### 3.1分布列的选取

在创建表的时候，可以使用 `DISTRIBUTED BY` 子句来指定分布列

```sql
CREATE TABLE my_table (
    id INT,
    name VARCHAR,
    date DATE
) DISTRIBUTED BY (id);
```

如果建表时没有指定分布列，数据会以下几种场景来存储：

*   场景一

    若建表时包含主键/唯一约束，则选取HASH分布，分布列为主键/唯一约束对应的列。
*   场景二

    若建表时不包含主键/唯一约束，但存在数据类型支持作分布列的列，则选取HASH分布，分布列为第一个数据类型支持作分布列的列。
*   场景三

    若建表时不包含主键/唯一约束，也不存在数据类型支持作分布列的列，选取ROUNDROBIN分布。

### 3.2查询分布列

可使用以下sql查询当前表的分布列

```sql
SELECT getdistributekey('"your_schema"."table_name"')
```

### 3.3分布列更新处理

方法一：GaussDB(DWS)目前暂不支持分布键更新，直接跳过该报错

方法二：将分布列修改为一个不会更新的列，以下为调整分布列示例：

```sql
alter table customer_t1 DISTRIBUTE BY hash (c_customer_sk); 
```

### 3.4注意事项

*   源端进行更新操作时，数据库日志中需要能读取到更新前的数据，tapdata会在写入前针对分布列检查是否修改

    *   如果不存在更新前数据，则会抛出以下异常

            Current Database cannot support update operation.
    *   如果监测到分布列进行了修改，则会抛出以下异常

            // GaussDB(DWS)数据库规定了分布列不允许被更新
            Distributed key column distributedKey can't be updated in table table_name.Value: beforeData => afterData.
*   并非所有类型的数据库或数据表都会记录更新前的数据，特别是在非关系型数据库或一些特殊情况下，没有内置的机制来记录更新前的数据。一些可能没有内置更新前的数据记录机制的数据库类型：例如Redis

## 4.分区表

分区表就是把逻辑上的一张表根据分区策略分成几张物理块库进行存储，这张逻辑上的表称之为分区表，物理块称之为分区。分区表是一张逻辑表，不存储数据，数据实际是存储在分区上的。当进行条件查询时，系统只会扫描满足条件的分区，避免全表扫描，从而提升查询性能。

### 4.1分区表的创建

当数据需要同步到分区表中时，tapdata不支持自动创建分区表，需要提前手动创建好分区表，分区表创建示例如下：

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

### 4.2注意事项

*   分区表没有主键或唯一索引时，tapdata不支持冲突更新操作，会抛出以下异常信息

        The partitioned table table_name lacks a primary key or unique index, and does not support conflict update operations. Please switch to the append mode.

<!---->

*   如果需要使用分区表，在选择目标表存在处理策略时，建议保留表结构，否则会由tapdata自动创建普通表

具体可参考GaussDB(DWS)官方文档<https://support.huaweicloud.com/dws/index.html>


