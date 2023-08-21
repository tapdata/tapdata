# 連接配置幫助

## 1.支援版本

GaussDB(DWS) 8.1.3

## 2.連接配置範例

    地址：xxxxx
    端口：xxxx
    資料庫：test
    模型：test_tapdata
    帳號：tapdata
    密碼：tapdata

## 3.分佈欄

在GaussDB(DWS)中，分佈欄是指分佈表中用於數據分佈的欄，它決定了數據在分佈式存儲中的分佈方式。分佈欄的選擇對於查詢性能和數據分佈均衡至關重要。

### 3.1分佈欄的選擇

在創建表的時候，可以使用 `DISTRIBUTED BY` 子句來指定分佈欄

```sql
CREATE TABLE my_table (
    id INT,
    name VARCHAR,
    date DATE
) DISTRIBUTED BY (id);
```

```sql
CREATE TABLE my_table (
    id INT,
    name VARCHAR,
    date DATE
) DISTRIBUTED BY (id);
```

如果建表時沒有指定分佈欄，數據會以下幾種場景來存儲：

*   場景一

    若建表時包含主鍵/唯一約束，則選取HASH分佈，分佈欄為主鍵/唯一約束對應的欄。
*   場景二

    若建表時不包含主鍵/唯一約束，但存在數據類型支持作分佈欄的欄，則選取HASH分佈，分佈欄為第一個數據類型支持作分佈欄的欄。
*   場景三

    若建表時不包含主鍵/唯一約束，也不存在數據類型支持作分佈欄的欄，則選取ROUNDROBIN分佈。

### 3.2查詢分佈欄

可使用以下sql查詢當前表的分佈欄

```sql
SELECT getdistributekey('"your_schema"."table_name"')
```

### 3.3分佈欄更新處理

方法一：GaussDB(DWS)目前暫不支援分佈鍵更新，直接跳過該報錯

方法二：將分佈欄修改為一個不會更新的欄，以下為調整分佈欄示例

```sql
alter table customer_t1 DISTRIBUTE BY hash (c_customer_sk); 
```

### 3.4注意事項

*   源端進行更新操作時，資料庫日誌中需要能讀取到更新前的數據，tapdata會在寫入前針對分佈欄檢查是否修改

  *   如果不存在更新前數據，則會拋出以下異常
      Current Database cannot support update operation.
  *   如果監測到分佈欄進行了修改，則會拋出以下異常

          // GaussDB(DWS)數據庫規定了分佈欄不允許被更新
          Distributed key column distributedKey can't be updated in table table_name.Value: beforeData => afterData.
*   並非所有類型的數據庫或數據表都會記錄更新前的數據，特別是在非關系型數據庫或一些特殊情況下，沒有內置的機製來記錄更新前的數據。一些可能沒有內置更新前的數據記錄機製的數據庫類型：例如Redis

## 4.分區表

分區表就是把邏輯上的一張表根據分區策略分成幾張物理塊庫進行存儲，這張邏輯上的表稱之為分區表，物理塊稱之為分區。分區表是一張邏輯表，不存儲數據，數據實際是存儲在分區上的。當進行條件查詢時，系統只會掃描滿足條件的分區，避免全表掃描，從而提升查詢性能。

### 4.1分區表的創建

當數據需要同步到分區表中時，tapdata不支援自動創建分區表，需要提前手動創建好分區表，分區表創建示例如下：

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

### 4.2注意事項

*   分區表沒有主鍵或唯一索引時，tapdata不支援衝突更新操作，會拋出以下異常信息
    The partitioned table table\_name lacks a primary key or unique index, and does not support conflict update operations. Please switch to the append mode.
*   如果需要使用分區表，在選擇目標表存在處理策略時，建議保留表結構，否則會由tapdata自動創建普通表

具體可參考GaussDB(DWS)官方文檔<https://support.huaweicloud.com/dws/index.html>

